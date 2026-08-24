#!/usr/bin/env bash
# 07-stress-test — 1000-client NAT-mode stress test.
#
# Deploys the stress-test client as a Kubernetes Job (in-cluster, avoiding the
# host-path bridge flake), monitors its progress, collects per-node metrics via
# kubectl top, and reports aggregate results.
#
# Usage:
#   ./07-stress-test.sh [options]
#
# Options:
#   -n <count>     Total clients (default: 100)
#   -b <size>      Batch size (default: 10)
#   -p <true|false> Publish static VP8 track per client (default: true)
#   -m <seconds>   Metric collection interval (default: 30)
#   -s <shards>    Number of client pods to shard across (default: 1)
#   -k             Keep the Job after completion (default: cleanup)
#   -r <room>      Room name (default: auto-generated nat-stress-<ts>)
#   -h             Show help
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"

# ---- parse options ----
STRESS_N=100
STRESS_BATCH=10
STRESS_PUBLISH=true
METRIC_INTERVAL=30
STRESS_SHARDS=1
KEEP_JOB=false
# Per-client RTP loss report window (Go duration, e.g. "30s"); 0 disables. When
# set, each stress client reports end-to-end downlink loss% over the window —
# the probe for the residual loss seen under 1000-client concurrency.
STRESS_LOSS_WINDOW=0
ROOM="nat-stress-$(date +%s)"

while getopts "n:b:p:m:s:l:kr:h" opt; do
  case "$opt" in
    n) STRESS_N="$OPTARG" ;;
    b) STRESS_BATCH="$OPTARG" ;;
    p) STRESS_PUBLISH="$OPTARG" ;;
    m) METRIC_INTERVAL="$OPTARG" ;;
    s) STRESS_SHARDS="$OPTARG" ;;
    l) STRESS_LOSS_WINDOW="$OPTARG" ;;
    k) KEEP_JOB=true ;;
    r) ROOM="$OPTARG" ;;
    h)
      echo "Usage: $0 [options]"
      echo "  -n <count>     Total clients (default: 100)"
      echo "  -b <size>      Batch size (default: 10)"
      echo "  -p <true|false> Publish static VP8 track per client (default: true)"
      echo "  -m <seconds>   Metric collection interval (default: 30)"
      echo "  -s <shards>    Number of client pods to shard across (default: 1)"
      echo "  -l <duration>  Per-client RTP loss report window, 0=off (default: 0)"
      echo "  -k             Keep the Job after completion (default: cleanup)"
      echo "  -r <room>      Room name (default: auto-generated)"
      exit 0
      ;;
    *) echo "unknown option -$opt"; exit 2 ;;
  esac
done

JOB_NAME="nat-stress-${STRESS_N}"
METRICS_DIR="${WORK:-/tmp}/nat-stress-metrics-$(date +%s)"

require go kubectl docker kind

log "=== Stress Test Configuration ==="
log "clients:      $STRESS_N"
log "batch size:   $STRESS_BATCH"
log "publish:      $STRESS_PUBLISH"
log "loss window:  $STRESS_LOSS_WINDOW"
log "room:         $ROOM"
log "metrics dir:  $METRICS_DIR"
mkdir -p "$METRICS_DIR"

# ---- 1. Ensure cluster is running ----
if ! kubectl cluster-info >/dev/null 2>&1; then
  log "Cluster not found — creating..."
  "$DIR/01-cluster.sh"
fi

# ---- 2. Build images (includes stress-test client) ----
# The stress test does not need the 2.65GB browser image; skip it to avoid slow
# rebuilds and OrbStack's recurring node:22-alpine metadata corruption.
log "Building images (server + stress client)..."
SKIP_BROWSER=1 "$DIR/02-build-image.sh"

# ---- 3. Deploy server + edge service ----
log "Deploying server..."
"$DIR/03-deploy.sh"

# ---- 3b. Warmup: let the freshly-rolled server pods fully register and warm up ----
# The stress Jobs start connecting ~1s after 03-deploy.sh finishes, but the pods
# were just restarted mid-rollout. Clients that connect during that window hit
# DTLS/ICE timeouts on the still-initializing room/edge (observed: every shard's
# FIRST attempt failed 1000-client connect, retries on warm servers passed
# 200/200 with 100% media). Wait for the server deployments to be Ready and the
# room/edge to be registered in redis before creating any stress Jobs.
log "Warming up server nodes (${WARMUP_WAIT:-20}s) after deploy..."
wait_for "server deployments ready" 180 \
  bash -c 'for d in livekit-edge livekit-edge2 livekit-room; do [ "$(kubectl get deploy/$d -o jsonpath="{.status.readyReplicas}" 2>/dev/null)" = "1" ] || exit 1; done'
sleep "${WARMUP_WAIT:-20}"
log "server nodes warm"

# ---- 4. Seed the room map ----
# (seed_room_map uses the room name from common.sh; override with our room)
log "Seeding room map for $ROOM..."
seed_room_map "$ROOM"

# ---- 5. Create the stress-test Job(s) ----
# 1000 clients in a single pod OOMs the node (~4.8Gi free): each client holds a
# pion PC + subscribed tracks. Shard across -s pods, each connecting a disjoint
# range of the same room (via -stress-offset). All shards share the room name so
# media crosses the NAT bridge exactly as one big room.
if [ "$STRESS_SHARDS" -lt 1 ]; then STRESS_SHARDS=1; fi
PER_SHARD=$(( (STRESS_N + STRESS_SHARDS - 1) / STRESS_SHARDS ))
log "Creating ${STRESS_SHARDS} stress-test Job(s) (${STRESS_N} clients, ${PER_SHARD}/shard, batch ${STRESS_BATCH})..."
for s in $(seq 0 $((STRESS_SHARDS - 1))); do
  shard_lo=$(( s * PER_SHARD ))
  shard_n=$(( STRESS_N - shard_lo ))
  if [ "$shard_n" -gt "$PER_SHARD" ]; then shard_n=$PER_SHARD; fi
  if [ "$shard_n" -le 0 ]; then continue; fi
  JOB_NAME_S="${JOB_NAME}-${s}"
  cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB_NAME_S
  namespace: $NAMESPACE
  labels:
    app: nat-stress-test
    app.kubernetes.io/name: stress-test
spec:
  ttlSecondsAfterFinished: 1800
  backoffLimit: 1
  activeDeadlineSeconds: 2400
  template:
    metadata:
      labels:
        app: nat-stress-test
        app.kubernetes.io/name: stress-test
    spec:
      restartPolicy: Never
      containers:
        - name: stress-test
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
            - "stress-test"
            - "-stress-n"
            - "${shard_n}"
            - "-stress-batch"
            - "${STRESS_BATCH}"
            # Must be "-stress-publish=true" (single item): Go's flag package
            # treats a space-separated value after a BOOL flag as a positional
            # argument and STOPS parsing — "-stress-publish true" would silently
            # drop every flag after it (-stress-offset, -stress-loss-window).
            - "-stress-publish=${STRESS_PUBLISH}"
            - "-stress-offset"
            - "${shard_lo}"
            - "-stress-loss-window"
            - "$STRESS_LOSS_WINDOW"
          resources:
            requests:
              cpu: "1"
              memory: 512Mi
            limits:
              # Cap the client pods so they don't starve the room/edge servers on
              # the shared 12-core host (2000 client-side pion PCs at 8 CPU each
              # caused load 20 → server DTLS timeouts on the connect burst).
              cpu: "4"
              # 8Gi not 6Gi: a 200-client pod with the loss tracker can be
              # OOMKilled (exit 137) at 6Gi under full load, truncating the run.
              memory: 8Gi
      initContainers:
        - name: wait-for-edge
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              # Stagger shard starts so the 5 shards don't all burst ~50
              # connections at once onto the single edge node. Each shard waits
              # s * STRESS_SHARD_STAGGER seconds after the edge is reachable. A
              # simultaneous burst saturates the 12-core host's DTLS processing
              # (client-side subscriber handshake > 30s pion timeout → connection
              # fails even though the server-side join succeeded). Real
              # deployments don't have 1000 users connect in the same second.
              # STAGGER_SECS is computed by the driver (literal) — no pod-side
              # variables, so the unquoted heredoc does not need escaping.
              sleep $(( s * ${STRESS_SHARD_STAGGER:-45} ))
              until nc -zv livekit-edge 7880 2>/dev/null; do
                echo "waiting for livekit-edge:7880..."
                sleep 1
              done
              echo "edge ready"
EOF
done

# ---- 6. Wait for Job pods to be created ----
log "Waiting for stress-test pods to start..."
POD_NAME=""
for i in $(seq 1 30); do
  POD_NAME="$(kubectl get pods -n "$NAMESPACE" -l app=nat-stress-test -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -n "$POD_NAME" ]; then
    break
  fi
  sleep 1
done
if [ -z "$POD_NAME" ]; then
  echo "  ✗ stress-test pods never created" >&2
  exit 1
fi
log "Pod: $POD_NAME"

# ---- 7. Start background metric collection ----
# Collect per-node resource usage at intervals during the test
METRICS_FILE="$METRICS_DIR/kubectl-top.txt"
{
  echo "# stress-test metrics: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# clients=$STRESS_N batch=$STRESS_BATCH publish=$STRESS_PUBLISH shards=$STRESS_SHARDS"
  echo "#"
  while true; do
    ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    {
      echo "=== $ts ==="
      kubectl top pods -n "$NAMESPACE" -l 'app.kubernetes.io/name in (edge, room, edge2, stress-test)' --no-headers 2>/dev/null || echo "(top pods not available)"
      kubectl top nodes --no-headers 2>/dev/null || echo "(top nodes not available)"
    } >> "$METRICS_FILE"
    # Check if any stress-test pod is still running
    phase="$(kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Terminated")"
    if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ] || [ "$phase" = "Terminated" ]; then
      break
    fi
    sleep "$METRIC_INTERVAL"
  done
} &
METRICS_PID=$!
cleanup_metrics() {
  kill "$METRICS_PID" 2>/dev/null || true
}
trap cleanup_metrics EXIT

# ---- 8. Follow the stress-test logs ----
log "Following stress-test logs (client will print per-batch progress)..."
kubectl logs -n "$NAMESPACE" "$POD_NAME" --follow --tail=10 || true

# ---- 9. Wait for all shard Jobs to complete ----
log "Waiting for all ${STRESS_SHARDS} shard Job(s) to complete..."
JOB_STATUS=""
# 1000 clients: connect phase alone ~7-9min (batch times degrade as the room
# fills), plus media verification — the 600s default would fire too early.
WAIT_SECS=1500
ALL_COMPLETE=true
for s in $(seq 0 $((STRESS_SHARDS - 1))); do
  JOB_NAME_S="${JOB_NAME}-${s}"
  if ! kubectl get job "$JOB_NAME_S" -n "$NAMESPACE" >/dev/null 2>&1; then
    continue  # shard was skipped (shard_n <= 0)
  fi
  if ! kubectl wait --for=condition=complete "job/$JOB_NAME_S" -n "$NAMESPACE" --timeout="${WAIT_SECS}s" 2>/dev/null; then
    ALL_COMPLETE=false
    # Check if it failed
    if kubectl get job "$JOB_NAME_S" -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null | grep -q '^[1-9]'; then
      log "Shard $s FAILED — showing final logs:"
      kubectl logs -n "$NAMESPACE" "job/$JOB_NAME_S" --tail=30 2>/dev/null || true
    else
      log "Shard $s timed out — showing final logs:"
      kubectl logs -n "$NAMESPACE" "job/$JOB_NAME_S" --tail=30 2>/dev/null || true
    fi
  fi
done
if [ "$ALL_COMPLETE" = true ]; then
  JOB_STATUS="PASS"
  log "Stress test completed successfully (all shards)"
else
  JOB_STATUS="FAIL"
fi

# ---- 10. Collect final metrics ----
sleep 2  # let the metric collector write its final entry
cleanup_metrics
trap - EXIT

log "=== Final per-node metrics ==="
kubectl top pods -n "$NAMESPACE" -l 'app.kubernetes.io/name in (edge, room, edge2)' --no-headers 2>/dev/null || true
kubectl top nodes --no-headers 2>/dev/null || true

# ---- 11. Collect server logs for analysis ----
EDGE_LOG="$METRICS_DIR/edge-server.log"
ROOM_LOG="$METRICS_DIR/room-server.log"
node_logs edge --tail=5000 > "$EDGE_LOG" 2>/dev/null || true
node_logs room --tail=5000 > "$ROOM_LOG" 2>/dev/null || true

# Extract interesting metrics from server logs
echo ""
log "=== Server-side metrics ==="
echo "--- edge: gateway sessions ---"
grep -c 'nat edge gateway session' "$EDGE_LOG" 2>/dev/null && echo "  gateway session events" || echo "  (no gateway events)"
echo "--- edge: media relay bytes ---"
grep -oP 'relay_bytes=\K[0-9]+' "$EDGE_LOG" 2>/dev/null | tail -1 | xargs -I{} echo "  relay_bytes={}" || echo "  (no relay bytes)"
echo "--- room: SFU tracks ---"
grep -c 'track.*published\|Track.*added' "$ROOM_LOG" 2>/dev/null && echo "  track events" || echo "  (no track events)"
echo "--- room: participants ---"
grep -c 'participant.*joined\|participant joined' "$ROOM_LOG" 2>/dev/null && echo "  participant join events" || echo "  (no participant events)"

# ---- 12. Store the client's final output (aggregate all shards) ----
CLIENT_LOG="$METRICS_DIR/stress-client.log"
: > "$CLIENT_LOG"
for s in $(seq 0 $((STRESS_SHARDS - 1))); do
  JOB_NAME_S="${JOB_NAME}-${s}"
  if kubectl get job "$JOB_NAME_S" -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "===== shard $s ($JOB_NAME_S) =====" >> "$CLIENT_LOG"
    # Full log, not a tail: the cleanup WARN flood (data-channel DTLS timeouts
    # from every stopping client) otherwise pushes the loss report + aggregate
    # stdout lines out of the last 200 lines.
    kubectl logs -n "$NAMESPACE" "job/$JOB_NAME_S" --tail=-1 >> "$CLIENT_LOG" 2>/dev/null || true
  fi
done
log "Client log saved to $CLIENT_LOG"

# ---- 13. Summary ----
echo ""
echo "============================================"
echo "  STRESS TEST SUMMARY"
echo "============================================"
echo "  clients:       $STRESS_N"
echo "  batch size:    $STRESS_BATCH"
echo "  publish:       $STRESS_PUBLISH"
echo "  room:          $ROOM"
echo "  result:        $JOB_STATUS"
echo "  metrics dir:   $METRICS_DIR"
echo "============================================"

# Print the final line from the client (PASS/FAIL)
echo ""
echo "--- Client result ---"
grep -E 'STRESS_TEST: (PASS|FAIL)' "$CLIENT_LOG" 2>/dev/null || echo "(no result line found)"

# ---- 14. Cleanup ----
if [ "$KEEP_JOB" = false ]; then
  log "Cleaning up stress-test Job(s)..."
  for s in $(seq 0 $((STRESS_SHARDS - 1))); do
    JOB_NAME_S="${JOB_NAME}-${s}"
    kubectl delete job "$JOB_NAME_S" -n "$NAMESPACE" --ignore-not-found --wait=false 2>/dev/null || true
  done
  # Also delete pods (the Job controller may not have deleted them yet)
  kubectl delete pod -l app=nat-stress-test -n "$NAMESPACE" --ignore-not-found --wait=false 2>/dev/null || true
  log "Cleanup complete"
else
  log "Keeping Job(s) (use 'kubectl delete job -l app=nat-stress-test' to clean up later)"
fi

if [ "$JOB_STATUS" = "PASS" ]; then
  log "STRESS TEST: PASS"
  exit 0
else
  log "STRESS TEST: $JOB_STATUS"
  exit 1
fi