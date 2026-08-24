#!/usr/bin/env bash
# 03-deploy — deploy Redis + the two LiveKit server pods (edge on kind-worker,
# room on kind-worker2), then pin the room to the room node so the NAT split is
# deterministic (client joins via edge, room hosted on the other node).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

require kubectl
kubectl cluster-info >/dev/null 2>&1 || { echo "no cluster — run ./01-cluster.sh first"; exit 1; }

EDGE_IP="$(edge_node_ip)"
ROOM_IP="$(room_node_ip)"
EDGE2_IP="$(edge2_node_ip 2>/dev/null || true)"
log "edge node ip: $EDGE_IP   room node ip: $ROOM_IP   edge2 node ip: ${EDGE2_IP:-<none>}"

# ---- Redis (sentinel HA) ----
# Remove the old single-instance Deployment/Service if a previous run created
# them (the same object name `redis` now belongs to the HA StatefulSet).
kubectl delete deploy/redis svc/redis -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -f "$DIR/manifests/redis.yaml"
# NOTE: do NOT pipe wait_for output to grep — wait_for suppresses its command's
# stdout, so the only stdout is the log line's timestamp, and `grep -q 1` would
# match the digit "1" in the wall-clock time (a timing-dependent false pass/fail).
wait_for "redis sentinel cluster ready" 240 bash -c '[ "$(kubectl get statefulset/redis -o jsonpath="{.status.readyReplicas}" 2>/dev/null)" = 3 ]'
wait_for "redis master reachable via sentinel" 60 redis_cli ping

# ---- webhook receiver (E2E) ----
kubectl apply -f "$DIR/manifests/webhook-receiver.yaml"
wait_for "webhook receiver running" 120 bash -c '[ "$(kubectl get pod/webhook-receiver -o jsonpath='{.status.phase}' 2>/dev/null)" = Running ]'

# Clear stale node registrations from previous runs (restarts leave random
# node IDs behind; stable IDs below prevent future duplicates).
redis_cli DEL nodes room_node_map >/dev/null 2>&1 || true

# ---- server config configmaps (per-node: advertise_ip/node_ip baked in) ----
# NOTE: LIVEKIT_RTC_ADVERTISE_IP / LIVEKIT_RTC_NODE_IP env vars do NOT bind to the
# NodeIP struct config type (cli string->struct conversion fails), leaving them
# empty and making the edge fall back to DefaultStunServers (unreachable in kind ->
# 10s srflx gather that stalls one-shot/WHIP answers). Bake the IPs into the
# configmap instead.
gen_node_config() { # gen_node_config <node-ip>
  local ip="$1"
  sed -E "s/^  (advertise_ip|node_ip):.*/  \1: $ip/" "$DIR/configs/config.yaml"
}
for role_ip in "edge:$EDGE_IP" "room:$ROOM_IP"; do
  role="${role_ip%%:*}"; ip="${role_ip##*:}"
  kubectl create configmap "nat-config-$role" \
    --from-file=config.yaml=<(gen_node_config "$ip") \
    --dry-run=client -o yaml | kubectl apply -f -
done

# ---- server deployments (hostNetwork => pod IP == node IP; advertise_ip = node IP) ----
gen_server_deploy() { # gen_server_deploy <name> <nat-role> <node-ip>
  local name="$1" role="$2" ip="$3"
  cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: livekit-$name
  namespace: $NAMESPACE
spec:
  replicas: 1
  strategy:
    type: Recreate            # hostNetwork: one pod per node; must free ports before next
  selector: {matchLabels: {app: livekit-$name}}
  template:
    metadata:
      labels:
        app: livekit-$name
        app.kubernetes.io/name: $name
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet   # resolve cluster services (redis) even with hostNetwork
      nodeSelector: {nat-role: $role}
      containers:
        - name: server
          image: $IMG
          args: ["--config", "/etc/livekit/config.yaml"]
          env:
            - {name: POD_IP, valueFrom: {fieldRef: {fieldPath: status.podIP}}}
            - {name: LIVEKIT_REGION, value: "$role"}
            - {name: LIVEKIT_NODE_ID, value: "node-$name"}
            ${NAT_COVERAGE_GOCOVERDIR:-}
          ports:
            - {containerPort: 7880, protocol: TCP}
            - {containerPort: 7881, protocol: TCP}
            - {containerPort: 7882, protocol: UDP}
            - {containerPort: 7883, protocol: TCP}
            - {containerPort: 7884, protocol: TCP}
          volumeMounts:
            - {name: config, mountPath: /etc/livekit}
            ${NAT_COVERAGE_VOLUME_MOUNT:-}
          resources:
            # room node hosts the SFU for every participant in the room; edge holds
            # every client's pion PC + relay bridge. A 1000-client room needs far
            # more than the old 2CPU/1Gi cap (the room pod OOMKilled ~420 clients).
            # 12-core OrbStack host: give the servers headroom (stress pods were
            # starving them at 4 CPU, causing DTLS timeouts on the connect burst).
            limits: {cpu: "8", memory: 6Gi}
      volumes:
        - name: config
          configMap:
            name: nat-config-$name
            items:
              - {key: config.yaml, path: config.yaml}
${NAT_COVERAGE_VOLUME:-}
EOF
}

gen_server_deploy edge edge "$EDGE_IP"  | kubectl apply -f -
gen_server_deploy room room "$ROOM_IP" | kubectl apply -f -
# second edge (multi-edge): the edge2 worker terminates media for clients that
# signal to it; the room node feeds BOTH edges for the same pinned room.
if [ -n "${EDGE2_IP:-}" ]; then
  kubectl create configmap "nat-config-edge2" \
    --from-file=config.yaml=<(gen_node_config "$EDGE2_IP") \
    --dry-run=client -o yaml | kubectl apply -f -
  gen_server_deploy edge2 edge2 "$EDGE2_IP" | kubectl apply -f -
fi
# Image tag is stable ($IMG), so `apply` alone won't roll when the image was
# rebuilt under the same tag — force a rollout so re-running this script always
# runs the latest binary (Recreate strategy frees the hostNetwork ports).
kubectl rollout restart deploy/livekit-edge deploy/livekit-room -n "$NAMESPACE"
if [ -n "${EDGE2_IP:-}" ]; then
  kubectl rollout restart deploy/livekit-edge2 -n "$NAMESPACE"
fi

log "waiting for server pods Ready..."
# rollout status waits for the NEW ReplicaSet (unlike `kubectl wait`, which can
# match the old pod still terminating after a Recreate rollout).
kubectl rollout status deploy/livekit-edge -n "$NAMESPACE" --timeout=180s
kubectl rollout status deploy/livekit-room -n "$NAMESPACE" --timeout=180s
if [ -n "${EDGE2_IP:-}" ]; then
  kubectl rollout status deploy/livekit-edge2 -n "$NAMESPACE" --timeout=180s
fi

# Edge ClusterIP service: stable DNS target for in-cluster consumers (stress test,
# monitoring). The service targets the edge deployment's hostNetwork ports.
kubectl apply -f "$DIR/manifests/edge-service.yaml"

log "waiting for nodes registered in redis..."
wait_for "all nodes registered" 120 nodes_count_ok

log "--- registered nodes ---"
for key in $(redis_cli HKEYS nodes); do
  echo "  node-id: $key  ip: $(redis_cli HGET nodes "$key" | grep -aEo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
done

# ---- pin room -> room node (NAT split determinism) ----
seed_room_map "$ROOM"

export WS_URL="ws://$EDGE_IP:7880"
echo "WS_URL=$WS_URL"
echo "room node ip: $ROOM_IP  (clients must NOT connect here for the split to hold)"
log "deploy complete"
