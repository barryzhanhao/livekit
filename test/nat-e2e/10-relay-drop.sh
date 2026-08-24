#!/usr/bin/env bash
# 10-relay-drop — NAT relay-drop recovery E2E.
#
# Drops the edge↔room MEDIA relay plane (port 7883) while the CONTROL plane
# (7884) and the participant stay alive, and asserts the room node forces a full
# reconnect (reconnect_on_publication_error / reconnect_on_subscription_error,
# enabled in the NAT config) so the client rejoins and media is restored —
# instead of the pre-fix behavior where the track's media froze silently.
#
#   ./10-relay-drop.sh              full: build + deploy + test
#   SKIP_DEPLOY=1 ./10-relay-drop.sh  reuse the running deployment
#
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"
NAMESPACE="${NAMESPACE:-default}"
ROOM="nat-relaydrop-$(date +%s)"
WINDOW="${RELAY_DROP_WINDOW:-40}"
LOG="/tmp/nat-relaydrop-$(date +%s).log"

require go kubectl docker kind

log "=== Relay-Drop Recovery Test ==="
log "room:         $ROOM"
log "drop window:  ${WINDOW}s"
log "log:          $LOG"

# ---- 1. Ensure cluster + server (unless reusing the deployment) ----
if ! kubectl cluster-info >/dev/null 2>&1; then
  "$DIR/01-cluster.sh"
fi
if [ "${SKIP_DEPLOY:-}" != "1" ]; then
  log "Building + deploying server (relay-drop wiring + reconnect config)..."
  SKIP_BROWSER=1 "$DIR/02-build-image.sh"
  "$DIR/03-deploy.sh"
  log "server ready"
fi

# ---- 2. Wait for servers to be ready + registered ----
wait_for "server deployments ready" 180 \
  bash -c 'for d in livekit-edge livekit-room; do [ "$(kubectl get deploy/$d -o jsonpath="{.status.readyReplicas}" 2>/dev/null)" = "1" ] || exit 1; done'
sleep "${WARMUP_WAIT:-10}"
log "server nodes warm"

# ---- 3. Seed the room map (cross-node: room pinned to a node) ----
seed_room_map "$ROOM"

# ---- 4. Run the relay-drop client as a Job ----
JOB="nat-relaydrop-client"
kubectl delete job "$JOB" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  namespace: $NAMESPACE
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: client
          image: livekit-nat-dev:stress
          args:
            - "-url"
            - "ws://livekit-edge:7880"
            - "-api-key"
            - "$API_KEY"
            - "-api-secret"
            - "$API_SECRET"
            - "-room"
            - "$ROOM"
            - "-scenario"
            - "relay-drop"
            - "-relay-drop-window"
            - "$WINDOW"
          resources:
            requests: {cpu: "0.5", memory: 512Mi}
            limits: {cpu: "2", memory: 4Gi}
EOF
log "relay-drop client Job created"

# ---- 5. Wait for READY (media established), then drop the media relay ----
POD=""
for i in $(seq 1 120); do
  POD="$(kubectl get pods -n "$NAMESPACE" -l job-name="$JOB" -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null || true)"
  [ -n "$POD" ] && kubectl logs -n "$NAMESPACE" "$POD" 2>/dev/null | grep -q "RELAY_DROP: READY" && break
  sleep 2
done
if [ -z "$POD" ] || ! kubectl logs -n "$NAMESPACE" "$POD" 2>/dev/null | grep -q "RELAY_DROP: READY"; then
  log "client never reached READY (media not established)"
  [ -n "$POD" ] && kubectl logs -n "$NAMESPACE" "$POD" --tail=30 2>/dev/null | tail -30
  exit 1
fi
log "media established (READY); dropping media relay plane (7883)..."

# The EDGE node is the media-relay LISTENER on 7883 (the room DIALS edge:7883, so
# on the edge the connections have LOCAL port 7883). REJECT -> tcp-reset tears
# down the ESTABLISHED relays AND blocks re-establishment; that's fine for the
# drop itself, but the rules MUST be removed shortly after so the rejoined
# session can establish fresh relays. (On the room node the connections have
# EPHEMERAL local ports, so the port orientation is the opposite — the edge side
# is the reliable place to apply these.)
EDGE_NODE="$(kubectl get pod -n "$NAMESPACE" -l app=livekit-edge -o jsonpath='{.items[0].spec.nodeName}')"
docker exec "$EDGE_NODE" iptables -A INPUT  -p tcp --dport 7883 -j REJECT --reject-with tcp-reset
docker exec "$EDGE_NODE" iptables -A OUTPUT -p tcp --sport 7883 -j REJECT --reject-with tcp-reset
DROP_ADDED=1
cleanup() {
  if [ "${DROP_ADDED:-}" = "1" ]; then
    docker exec "$EDGE_NODE" iptables -D INPUT  -p tcp --dport 7883 -j REJECT --reject-with tcp-reset 2>/dev/null || true
    docker exec "$EDGE_NODE" iptables -D OUTPUT -p tcp --sport 7883 -j REJECT --reject-with tcp-reset 2>/dev/null || true
    log "removed relay-drop iptables rules"
  fi
}
trap cleanup EXIT

# ---- 6. Let the drop + forced reconnect propagate, then unblock so the rejoin can establish new relays ----
log "waiting for the room node to detect the drop and force a reconnect..."
sleep 15
log "unblocking media relay port so the rejoined session can establish fresh relays..."
docker exec "$EDGE_NODE" iptables -D INPUT  -p tcp --dport 7883 -j REJECT --reject-with tcp-reset
docker exec "$EDGE_NODE" iptables -D OUTPUT -p tcp --sport 7883 -j REJECT --reject-with tcp-reset
DROP_ADDED=0
log "media relay port unblocked"

# ---- 7. Wait for the client to finish (rejoin + media restored) ----
kubectl wait --for=condition=complete "job/$JOB" -n "$NAMESPACE" --timeout=180s 2>&1 | tail -1 || true

# ---- 8. Read the client's result from its pod logs (job-logs lag behind pod
#      completion; poll the pod until the PASS/FAIL line flushes) ----
CLIENT_POD="$(kubectl get pods -n "$NAMESPACE" -l job-name="$JOB" -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null || true)"
RESULT=""
for i in $(seq 1 15); do
  RESULT="$(kubectl logs -n "$NAMESPACE" "$CLIENT_POD" 2>/dev/null | grep -oE "RELAY_DROP: (PASS|FAIL)" | tail -1 || true)"
  [ -n "$RESULT" ] && break
  sleep 2
done
log "--- client output ---"
kubectl logs -n "$NAMESPACE" "$CLIENT_POD" 2>/dev/null | grep -E "RELAY_DROP" || true

# ---- 9. Assert: room node must have logged the relay-drop reconnect ----
ROOM_POD="$(kubectl get pod -n "$NAMESPACE" -l app=livekit-room -o jsonpath='{.items[0].metadata.name}')"
RECONNECTS="$(kubectl logs -n "$NAMESPACE" "$ROOM_POD" --since=10m 2>/dev/null | grep -cE "nat .*-direction media relay dropped; forcing reconnect" || true)"
ISSUED="$(kubectl logs -n "$NAMESPACE" "$ROOM_POD" --since=10m 2>/dev/null | grep -cE "issuing full reconnect on (publication|subscription) error" || true)"
log "room logs: relay-drop detections=$RECONNECTS reconnect-issued=$ISSUED"

if [ "$RESULT" = "RELAY_DROP: PASS" ]; then
  echo ""
  echo "============================================"
  echo "  RELAY-DROP TEST: PASS"
  echo "  relay drops detected: $RECONNECTS"
  echo "  reconnects issued:    $ISSUED"
  echo "============================================"
  exit 0
fi
echo "============================================"
echo "  RELAY-DROP TEST: FAIL (client did not recover)"
echo "  relay drops detected: $RECONNECTS"
echo "  reconnects issued:    $ISSUED"
echo "============================================"
exit 1
