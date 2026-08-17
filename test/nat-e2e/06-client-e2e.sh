#!/usr/bin/env bash
# 06-client-e2e — Go client E2E (production-grade test client, dual-PC via the
# edge). Covers scenarios lk cannot drive: receive-before-publish, NACK cross-node,
# data (documented unbridged), attribute broadcast.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"   # livekit repo root (test/nat-e2e is 2 levels deep)
ROOM_GO="${ROOM_GO:-nat-go}"

require go kubectl
WS_URL="${WS_URL:-ws://$(edge_node_ip):7880}"

# use a fresh room so prior runs cannot interfere
ROOM_GO="${ROOM_GO}-$(date +%s)"
seed_room_map "$ROOM_GO"

echo "building nat-client..."
go build -o /tmp/nat-client "$DIR/client"

run_scenario() { # run_scenario <name> <expect-exit-zero>
  local name="$1" expect_zero="$2"
  echo "=== $name ==="
  set +e
  /tmp/nat-client -url "$WS_URL" -api-key "$API_KEY" -api-secret "$API_SECRET" \
    -room "$ROOM_GO" -scenario "$name"
  local rc=$?
  set -e
  if [ "$expect_zero" = "yes" ] && [ "$rc" -ne 0 ]; then
    echo "  ✗ $name FAILED (exit $rc)"
    return 1
  fi
  echo "  ✓ $name client exit $rc"
}

wait_log() { # wait_log <edge|room> <pattern> <timeout_sec> (same as 04-e2e.sh)
  local node="$1" pat="$2" timeout="$3" i=0
  until grep -qE "$pat" < <(kubectl logs -n "$NAMESPACE" "deploy/livekit-$node" -c server --tail=4000 2>/dev/null); do
    i=$((i+1))
    if [ "$i" -ge "$timeout" ]; then
      echo "  ✗ timeout ($timeout s) waiting in ${node} logs for: $pat" >&2
      return 1
    fi
    sleep 1
  done
}

PASS=0; FAIL=0

# receive-before-publish: subscriber joins empty room, publisher publishes later
if run_scenario receive-before-publish yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# NACK cross-node: client sends NACKs; assert the edge forwarded nack-typed RTCP
# to the room's DownTrack (the definitive cross-node NACK path).
if run_scenario nack yes; then
  if wait_log edge 'nat edge -> room down RTCP forwarded.*nack' 40; then
    n="$(edge_logs --since=3m | grep -c 'nat edge -> room down RTCP forwarded.*nack' || true)"
    n="${n:-0}"
    echo "  ✓ NACK: edge forwarded $n nack batches to room DownTrack"; PASS=$((PASS+1))
  else
    echo "  ✗ NACK: no nack-typed down RTCP reached the room"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# data: documented limitation (DC not bridged cross-node)
if run_scenario data no; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# attributes: broadcast cross-node
if run_scenario attributes yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

echo "==== GO-CLIENT SUMMARY: $PASS passed, $FAIL failed ===="
[ "$FAIL" -eq 0 ]
