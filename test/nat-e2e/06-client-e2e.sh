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

run_scenario() { # run_scenario <name> <expect-exit-zero> [room]
  local name="$1" expect_zero="$2" room="${3:-$ROOM_GO}"
  echo "=== $name ==="
  set +e
  /tmp/nat-client -url "$WS_URL" -api-key "$API_KEY" -api-secret "$API_SECRET" \
    -room "$room" -scenario "$name"
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

# metadata: participant metadata broadcast cross-node
if run_scenario metadata yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# mute: track mute broadcast cross-node
if run_scenario mute yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# multitrack: two video tracks from one publisher, subscriber receives both;
# assert the room bridged >=2 down tracks cross-node.
if run_scenario multitrack yes; then
  if wait_log room 'nat down track attached \(room -> edge\)' 40; then
    n="$(room_logs --since=3m | grep 'go-mt-sub' | grep -c 'nat down track attached (room -> edge)' || true)"
    n="${n:-0}"
    if [ "$n" -ge 2 ]; then
      echo "  ✓ MULTITRACK: go-mt-sub bridged $n down tracks cross-node"; PASS=$((PASS+1))
    else
      echo "  ✗ MULTITRACK: only $n down tracks bridged for go-mt-sub (expected >=2)"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ MULTITRACK: no down track bridged"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# single-pc: server MUST run single-PC NAT (one control channel + one gateway
# session per participant). The client asserts media flows; the room logs assert
# UseSinglePeerConnection was active and NO dual-PC marker appeared for this room.
ROOM_SPC="${ROOM_GO}-spc"
seed_room_map "$ROOM_SPC"
if run_scenario single-pc yes "$ROOM_SPC"; then
  if wait_log room 'useSinglePC": true' 40; then
    n="$(room_logs --since=3m | grep "$ROOM_SPC" | grep -c 'remote subscriber peer connection (dual-PC)' || true)"
    n="${n:-0}"
    if [ "$n" -eq 0 ]; then
      echo "  ✓ SINGLE-PC: one session per participant (no dual-PC marker for room)"
      PASS=$((PASS+1))
    else
      echo "  ✗ SINGLE-PC: found $n dual-PC markers, expected 0"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ SINGLE-PC: room logs did not show single-PC mode"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

echo "==== GO-CLIENT SUMMARY: $PASS passed, $FAIL failed ===="
[ "$FAIL" -eq 0 ]
