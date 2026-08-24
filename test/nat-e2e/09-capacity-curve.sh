#!/usr/bin/env bash
# 09-capacity-curve — graduated true-concurrent capacity curve for NAT mode.
#
# Runs the stress client at increasing concurrent-client counts (200/400/600/800,
# 200 per shard at CORRECT per-shard offsets) against the CURRENTLY deployed
# edge/room, capturing the per-client RTP loss report at each scale. This maps
# where end-to-end downlink loss / connect failures begin as concurrency grows —
# the quantitative anchor for the "loss is host-capacity-bound, not the relay
# protocol" finding.
#
# Unlike 07-stress-test.sh this does NOT rebuild/redeploy servers: it reuses the
# live deployment so every scale sees identical server resources. Scales run
# sequentially so each one gets the full host (a clean solo-capacity curve).
#
#   ./09-capacity-curve.sh [scales...]     default: "200 400 600 800"
#   SKIP_IMG=1 ./09-capacity-curve.sh ...  skip the client-image rebuild
#   LOSS_WINDOW=30s  ...                   loss-report window (default 30s)
#
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"
NAMESPACE="${NAMESPACE:-default}"
LOSS_WINDOW="${LOSS_WINDOW:-30s}"
SCALES="${*:-200 400 600 800}"
ROOM_PREFIX="nat-capacity-$(date +%s)"
LOG="/tmp/nat-capacity-$(date +%s).log"

require go kubectl docker kind

log "=== Capacity Curve ==="
log "scales:      $SCALES"
log "loss window: $LOSS_WINDOW"
log "room prefix: $ROOM_PREFIX"
log "log:         $LOG"

# ---- 1. Rebuild the client image so the source (no debug line) is what runs ----
if [ "${SKIP_IMG:-}" != "1" ]; then
  log "Building stress client image..."
  SB="$(mktemp -d)"
  (cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$SB/nat-client" ./test/nat-e2e/client)
  docker build -q -t livekit-nat-dev:stress -f "$DIR/Dockerfile.stress" "$SB" >/dev/null
  rm -rf "$SB"
  kind load docker-image livekit-nat-dev:stress --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  log "client image loaded"
fi

# ---- 2. Run each scale sequentially ----
declare -A RESULT_LOSS RESULT_CONNECT RESULT_PASS
for N in $SCALES; do
  SHARDS=$(( (N + 199) / 200 ))      # 200 clients per shard
  ROOM="${ROOM_PREFIX}-${N}"
  START=$(date +%s)
  log "--- scale $N (${SHARDS} shards, room ${ROOM}) ---"

  for s in $(seq 0 $((SHARDS - 1))); do
    shard_lo=$(( s * 200 ))
    shard_n=$(( N - shard_lo ))
    if [ "$shard_n" -gt 200 ]; then shard_n=200; fi
    JOB="nat-cap-${N}-${s}"
    cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  namespace: $NAMESPACE
  labels:
    app: nat-capacity
spec:
  backoffLimit: 1
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      affinity:
        # The measured loss is DOMINATED by client-pod CPU starvation when a
        # client pod shares a node with the edge/room servers (observed: shard
        # on the edge node = 63% loss, shard on a server-free node = 0.25% at
        # the same 400-client scale). Prefer nodes without servers so the loss
        # reflects the server, not the harness. NOTE: with servers on
        # worker/worker2 and only worker3 free, this caps clean measurement at
        # ~200 concurrent — a 4-node kind cluster cannot cleanly scale beyond.
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchExpressions:
                    - key: app.kubernetes.io/name
                      operator: In
                      values: [edge, room]
                topologyKey: kubernetes.io/hostname
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
            - "10"
            - "-stress-publish=true"
            - "-stress-offset"
            - "${shard_lo}"
            - "-stress-loss-window"
            - "$LOSS_WINDOW"
          resources:
            requests: {cpu: "1", memory: 512Mi}
            # 8Gi not 6Gi: a 200-client pod with the loss tracker can be
            # OOMKilled (exit 137) at the 6Gi limit under full load, truncating
            # the run mid-connect and faking "connect failures".
            limits: {cpu: "4", memory: 8Gi}
EOF
  done

  # Wait for all shards of this scale
  for s in $(seq 0 $((SHARDS - 1))); do
    JOB="nat-cap-${N}-${s}"
    if ! kubectl wait --for=condition=complete "job/$JOB" -n "$NAMESPACE" --timeout=900s >/dev/null 2>&1; then
      if kubectl get job "$JOB" -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null | grep -q '^[1-9]'; then
        log "  shard $s FAILED"
      else
        log "  shard $s TIMED OUT"
      fi
    fi
  done

  # Collect the loss + connect summary from each shard's pod log
  CONNECT_OK=0
  CONNECT_TOTAL=0
  LOSS_PCT=""
  for s in $(seq 0 $((SHARDS - 1))); do
    JOB="nat-cap-${N}-${s}"
    POD=$(kubectl get pods -n "$NAMESPACE" -l job-name="$JOB" --field-selector=status.phase=Succeeded -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
    [ -z "$POD" ] && POD=$(kubectl get pods -n "$NAMESPACE" -l job-name="$JOB" -o jsonpath='{.items[-1].metadata.name}' 2>/dev/null)
    [ -z "$POD" ] && continue
    CONN=$(kubectl logs -n "$NAMESPACE" "$POD" 2>/dev/null | grep -oE "STRESS_TEST connect: [0-9]+/[0-9]+ succeeded" | head -1)
    AGG=$(kubectl logs -n "$NAMESPACE" "$POD" 2>/dev/null | grep -oE "STRESS_TEST loss aggregate: .*" | head -1)
    log "  shard $s: $CONN | $AGG"
    C=$(echo "$CONN" | grep -oE "[0-9]+/[0-9]+" | head -1); COK=${C%/*}; CTOT=${C#*/}
    CONNECT_OK=$((CONNECT_OK + ${COK:-0})); CONNECT_TOTAL=$((CONNECT_TOTAL + ${CTOT:-0}))
    L=$(echo "$AGG" | grep -oE "\([0-9.]+%\)" | tr -d '()' | head -1)
    LOSS_PCT="${LOSS_PCT} ${L}"
  done
  ELAPSED=$(( $(date +%s) - START ))
  RESULT_CONNECT[$N]="${CONNECT_OK}/${CONNECT_TOTAL}"
  RESULT_LOSS[$N]="${LOSS_PCT}"
  log "  scale $N done in ${ELAPSED}s: connect ${CONNECT_OK}/${CONNECT_TOTAL}${LOSS_PCT}"
  echo "CAPACITY scale=$N connect=${CONNECT_OK}/${CONNECT_TOTAL} loss=${LOSS_PCT} elapsed=${ELAPSED}s" | tee -a "$LOG"

  # Cleanup this scale before the next (sequential = full host for each)
  kubectl delete job -l app=nat-capacity -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
  kubectl wait --for=delete job -l app=nat-capacity -n "$NAMESPACE" --timeout=120s >/dev/null 2>&1 || true
done

# ---- 3. Summary ----
echo ""
echo "============================================"
echo "  CAPACITY CURVE SUMMARY"
echo "============================================"
for N in $SCALES; do
  echo "  ${N} concurrent: connect ${RESULT_CONNECT[$N]}  loss ${RESULT_LOSS[$N]}"
done
echo "============================================"
