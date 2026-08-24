#!/usr/bin/env bash
# 08-browser-e2e — browser-based E2E tests for NAT mode.
#
# Deploys the Playwright + Chromium browser test image as a K8s Job (in-cluster,
# avoiding the host-path bridge flake), monitors its progress, and reports
# per-test results.
#
# Usage:
#   ./08-browser-e2e.sh [options]
#
# Options:
#   -s <scenario>  Run a single scenario (default: all)
#   -k             Keep the Job after completion (default: cleanup)
#   -i <image>     Browser test image tag (default: livekit-nat-dev:browser)
#   -r <room>      Room name (default: nat-browser-e2e)
#   -h             Show help
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"

# ---- parse options ----
SINGLE_SCENARIO=""
KEEP_JOB=false
BROWSER_IMG="${BROWSER_IMG:-livekit-nat-dev:browser}"
ROOM="nat-browser-e2e"

while getopts "s:ki:r:h" opt; do
  case "$opt" in
    s) SINGLE_SCENARIO="$OPTARG" ;;
    k) KEEP_JOB=true ;;
    i) BROWSER_IMG="$OPTARG" ;;
    r) ROOM="$OPTARG" ;;
    h)
      echo "Usage: $0 [options]"
      echo "  -s <scenario>  Run a single scenario (default: all)"
      echo "  -k             Keep the Job after completion (default: cleanup)"
      echo "  -i <image>     Browser test image tag (default: livekit-nat-dev:browser)"
      echo "  -r <room>      Room name (default: nat-browser-e2e)"
      exit 0
      ;;
    *) echo "unknown option -$opt"; exit 2 ;;
  esac
done

JOB_NAME="nat-browser-e2e-$(date +%s)"
METRICS_DIR="${WORK:-/tmp}/nat-browser-metrics-$(date +%s)"

require go kubectl docker kind

log "=== Browser E2E Test Configuration ==="
log "image:        $BROWSER_IMG"
log "room:         $ROOM"
log "scenario:     ${SINGLE_SCENARIO:-all}"
log "metrics dir:  $METRICS_DIR"
mkdir -p "$METRICS_DIR"

# ---- 1. Ensure cluster is running ----
if ! kubectl cluster-info >/dev/null 2>&1; then
  log "Cluster not found — creating..."
  "$DIR/01-cluster.sh"
fi

# ---- 2. Build images ----
log "Building images (server + browser test)..."
SKIP_BROWSER=0 "$DIR/02-build-image.sh"

# ---- 3. Deploy server + edge service ----
log "Deploying server..."
"$DIR/03-deploy.sh"

# ---- 4. Seed the room map ----
log "Seeding room map for $ROOM..."
seed_room_map "$ROOM"

# ---- 5. Create the browser test Job ----
log "Creating browser E2E test Job..."

# Build the Job args: if a single scenario is specified, pass it as --grep
if [ -n "$SINGLE_SCENARIO" ]; then
  JOB_ARGS_LINE="args: [\"-g\", \"${SINGLE_SCENARIO}\"]"
else
  JOB_ARGS_LINE=""
fi

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB_NAME
  namespace: $NAMESPACE
  labels:
    app: nat-browser-e2e
    app.kubernetes.io/name: browser-test
spec:
  ttlSecondsAfterFinished: 3600
  backoffLimit: 0
  activeDeadlineSeconds: 600
  template:
    metadata:
      labels:
        app: nat-browser-e2e
        app.kubernetes.io/name: browser-test
    spec:
      restartPolicy: Never
      containers:
        - name: browser-test
          image: $BROWSER_IMG
          imagePullPolicy: Never
          env:
            - name: API_KEY
              value: "$API_KEY"
            - name: API_SECRET
              value: "$API_SECRET"
            - name: LIVEKIT_URL
              value: "ws://livekit-edge:7880"
            - name: ROOM
              value: "$ROOM"
            - name: TEST_URL
              value: "http://localhost:8081"
          ${JOB_ARGS_LINE}
          resources:
            requests:
              cpu: "2"
              memory: 1Gi
            limits:
              cpu: "4"
              memory: 2Gi
      initContainers:
        - name: wait-for-edge
          image: busybox:1.36
          command:
            - sh
            - -c
            - |
              until nc -zv livekit-edge 7880 2>/dev/null; do
                echo "waiting for livekit-edge:7880..."
                sleep 1
              done
              echo "edge ready"
EOF

# ---- 6. Wait for the Job pod to be created ----
log "Waiting for browser-test pod to start..."
POD_NAME=""
for i in $(seq 1 30); do
  POD_NAME="$(kubectl get pods -n "$NAMESPACE" -l app=nat-browser-e2e -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -n "$POD_NAME" ]; then
    break
  fi
  sleep 1
done
if [ -z "$POD_NAME" ]; then
  echo "  ✗ browser-test pod never created" >&2
  exit 1
fi
log "Pod: $POD_NAME"

# ---- 7. Follow the test logs ----
log "Following browser E2E test logs..."
kubectl logs -n "$NAMESPACE" "$POD_NAME" --follow --tail=10 || true

# ---- 8. Wait for Job completion ----
log "Waiting for Job to complete..."
JOB_STATUS=""
if kubectl wait --for=condition=complete "job/$JOB_NAME" -n "$NAMESPACE" --timeout=600s 2>/dev/null; then
  JOB_STATUS="PASS"
  log "Browser E2E tests completed successfully"
else
  if kubectl get job "$JOB_NAME" -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null | grep -q '^[1-9]'; then
    JOB_STATUS="FAIL"
    log "Browser E2E tests FAILED — showing final logs:"
    kubectl logs -n "$NAMESPACE" "$POD_NAME" --tail=100
  else
    JOB_STATUS="TIMEOUT"
    log "Browser E2E tests timed out — showing final logs:"
    kubectl logs -n "$NAMESPACE" "$POD_NAME" --tail=100
  fi
fi

# ---- 9. Collect final output ----
LOG_FILE="$METRICS_DIR/browser-test.log"
kubectl logs -n "$NAMESPACE" "$POD_NAME" --tail=200 > "$LOG_FILE" 2>/dev/null || true
log "Test log saved to $LOG_FILE"

# Parse test results
echo ""
log "=== Test Results ==="
grep -E '^\s+(✓|✗|PASS|FAIL|passed|failed)' "$LOG_FILE" 2>/dev/null || echo "(no test results found in log)"
grep -E '(passed|failed)' "$LOG_FILE" 2>/dev/null | tail -5 || true

# ---- 10. Summary ----
echo ""
echo "============================================"
echo "  BROWSER E2E TEST SUMMARY"
echo "============================================"
echo "  image:         $BROWSER_IMG"
echo "  room:          $ROOM"
echo "  scenario:      ${SINGLE_SCENARIO:-all}"
echo "  result:        $JOB_STATUS"
echo "  metrics dir:   $METRICS_DIR"
echo "============================================"

# ---- 11. Cleanup ----
if [ "$KEEP_JOB" = false ]; then
  log "Cleaning up browser-test Job..."
  kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found --wait=false 2>/dev/null || true
  kubectl delete pod "$POD_NAME" -n "$NAMESPACE" --ignore-not-found --wait=false 2>/dev/null || true
  log "Cleanup complete"
else
  log "Keeping Job (use 'kubectl delete job $JOB_NAME' to clean up later)"
fi

if [ "$JOB_STATUS" = "PASS" ]; then
  log "BROWSER E2E: PASS"
  exit 0
else
  log "BROWSER E2E: $JOB_STATUS"
  exit 1
fi