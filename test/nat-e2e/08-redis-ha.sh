#!/usr/bin/env bash
# 08-redis-ha — Redis Sentinel failover test for the NAT architecture.
#
# Proves the architecture survives a redis MASTER loss:
#   1. Baseline: cross-node media (room pinned to room node) with redis healthy.
#   2. Mid-failover: freeze the redis master's event loop (DEBUG SLEEP — blocks
#      PING responses) while a call is live. The sentinels (quorum 2, down-after
#      5s) promote a replica; the live call's media must keep flowing — media
#      (pion → edge → TCP relay → room) bypasses redis, so an outage must not
#      interrupt it.
#   3. Post-failover: NEW joins + cross-node media work through the promoted
#      master (those DO need redis: PSRPC routing, node registry, room map).
#   4. Redis-state assertions: nodes registry + room_node_map intact, and the old
#      master rejoins as a REPLICA (the dynamic-master-discovery fix — a restarted
#      old master must never come back as master again).
#
# Why DEBUG SLEEP and not SIGSTOP / `kubectl delete pod`: signals to the container
# init are neutralized in the kind/OrbStack runtime (kill -STOP/-KILL on PID 1 are
# silently ignored), and deleting the master pod makes the StatefulSet recreate it
# in ~2-3s — faster than sentinel's 5s down-after — and the recreated pod would
# query sentinel before promotion completes and re-become master, skipping the
# failover. DEBUG SLEEP keeps the pod up but its event loop unresponsive for as
# long as we need, so the sentinels get the full window to promote deterministically.
# Requires enable-debug-command yes in the test redis.conf (test-only; production
# keeps debug commands disabled).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

# abort diagnostics: set -e can exit silently; this pinpoints the failing line
trap 'echo "  ✗ DRIVER ERROR at line $LINENO (rc=$?)" >&2' ERR

require go kubectl docker kind

log "=== Redis HA (sentinel failover) test ==="

# ---- 1. ensure cluster / images / deploy ----
if [ "${SKIP_DEPLOY:-0}" != "1" ]; then
  if ! kubectl cluster-info >/dev/null 2>&1; then
    log "Cluster not found — creating..."
    "$DIR/01-cluster.sh"
  fi
  log "Building images (server + client)..."
  SKIP_BROWSER=1 "$DIR/02-build-image.sh"
  log "Deploying server (sentinel redis)..."
  "$DIR/03-deploy.sh"
  log "Building nat-client..."
  go build -o /tmp/nat-client "$DIR/client"

  # freshly-rolled server pods DTLS-timeout the first ~50 connections (see
  # 07-stress-test.sh); give them a beat before connecting any client.
  log "Warming up server nodes (${WARMUP_WAIT:-20}s)..."
  sleep "${WARMUP_WAIT:-20}"
else
  log "SKIP_DEPLOY=1 — using the already-deployed cluster"
fi

ROOM="nat-redis-ha-$(date +%s)"
log "Seeding room map for $ROOM (room -> room node)..."
seed_room_map "$ROOM"
WS_URL="${WS_URL:-ws://$(edge_node_ip):7880}"

# ---- client runner (foreground) ----
run_client() { # run_client <label> <window_secs>
  local label="$1" window="$2" out
  if out="$(/tmp/nat-client -url "$WS_URL" -api-key "$API_KEY" -api-secret "$API_SECRET" \
      -room "$ROOM" -scenario redis-failover -redis-failover-window "$window" 2>&1)" \
      && grep -q "REDIS_FAILOVER: PASS" <<<"$out"; then
    ok "redis-failover: $label (${window}s window)"
  else
    echo "  --- $label client output ---"
    echo "$out"
    fail "redis-failover: $label"
  fi
}

# ---- 2. baseline: media with redis healthy ----
log "Phase A: baseline media (redis healthy)"
run_client "baseline" 10

# ---- 3. mid-failover: live call across the redis master loss ----
log "Phase B: redis master loss mid-call"
OLD_MASTER="$(redis_master_pod)" || { echo "  ✗ no redis master found"; exit 1; }
log "current redis master: $OLD_MASTER"
NODES_BEFORE="$(redis_cli HLEN nodes || true)"
MAP_BEFORE="$(redis_cli HGET room_node_map "$ROOM" || true)"
log "nodes=$NODES_BEFORE room_node_map[$ROOM]=$MAP_BEFORE"

OUT="$(mktemp)"
/tmp/nat-client -url "$WS_URL" -api-key "$API_KEY" -api-secret "$API_SECRET" \
  -room "$ROOM" -scenario redis-failover -redis-failover-window 60 >"$OUT" 2>&1 &
SCPID=$!
ready=0
for i in $(seq 1 90); do
  if grep -q "REDIS_FAILOVER: READY" "$OUT" 2>/dev/null; then ready=1; break; fi
  if ! kill -0 "$SCPID" 2>/dev/null; then break; fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "  ✗ Phase B: client never reached READY (media not established)"
  kill "$SCPID" 2>/dev/null || true; wait "$SCPID" 2>/dev/null || true
  cat "$OUT" >&2 || true
  rm -f "$OUT"
  fail "redis-failover mid-call"
  exit 1
fi
log "client media established; freezing redis master process ($OLD_MASTER)"
# DEBUG SLEEP blocks the master's event loop for REDIS_FREEZE_SECS (no commands
# answered, including sentinel PING) so the sentinels (quorum 2, down-after 5s)
# promote a replica deterministically. NOT SIGSTOP/kubectl delete pod: signals to
# the container init are neutralized in the kind/OrbStack runtime, and deleting
# the pod makes the StatefulSet recreate it (~2s) before sentinel's 5s
# down-after, so the recreated pod re-becomes master and no failover happens.
# DEBUG SLEEP keeps the pod up but unresponsive for the full window.
kubectl exec "$OLD_MASTER" -n "$NAMESPACE" -c redis -- \
  sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning DEBUG SLEEP '"${REDIS_FREEZE_SECS:-20}"'' \
  >/dev/null 2>&1 &
FREEZE_PID=$!

new_master=""
for i in $(seq 1 90); do
  nm="$(redis_master_pod 2>/dev/null || true)"
  if [ -n "$nm" ] && [ "$nm" != "$OLD_MASTER" ]; then new_master="$nm"; break; fi
  sleep 1
done
# wait for the freeze to lift (the DEBUG SLEEP exec returns when redis resumes)
kill "$FREEZE_PID" 2>/dev/null || true; wait "$FREEZE_PID" 2>/dev/null || true
if [ -z "$new_master" ]; then
  echo "  ✗ Phase B: no new redis master elected within 90s"
  fail "redis failover (no new master)"
else
  ok "sentinel promoted new master: $OLD_MASTER -> $new_master"
fi

# wait for the client's window to finish and check its result. Allow time for a
# disrupted session to rejoin (window + rejoin connect/media ≈ 120-130s).
rc=124
for i in $(seq 1 180); do
  # rc=0 first so a non-zero client exit is captured, not fatal (set -e would
  # otherwise abort the driver before we can report the media result)
  if ! kill -0 "$SCPID" 2>/dev/null; then rc=0; wait "$SCPID" || rc=$?; break; fi
  sleep 1
done
if [ "$rc" -eq 124 ]; then
  echo "  ✗ Phase B: client still running after 180s (hung)"
  kill -9 "$SCPID" 2>/dev/null || true; wait "$SCPID" 2>/dev/null || true
  fail "redis-failover mid-call"
elif grep -q "REDIS_FAILOVER: PASS" "$OUT"; then
  ok "redis-failover mid-call (media survived redis master loss)"
else
  echo "  ✗ Phase B: media did not survive failover"
  cat "$OUT" >&2 || true
  fail "redis-failover mid-call"
fi
rm -f "$OUT"

# ---- 4. post-failover: redis state intact + old master rejoins as replica ----
log "Phase C: post-failover state + new joins"
# Let the registry settle: during the outage the cleanup/keepalive path can
# briefly churn the `nodes` hash (RemoveDeadNodes sees stale keepalives). Read
# the assertions only after all nodes are back, not mid-recovery.
if ! wait_for "node registry settled" 90 nodes_count_eq 3; then
  echo "  ✗ registry never settled to 3 nodes"
  fail "nodes registry intact"
fi
NODES_AFTER="$(redis_cli HLEN nodes || true)"
MAP_AFTER="$(redis_cli HGET room_node_map "$ROOM" || true)"
if [ -n "$NODES_AFTER" ] && [ "$NODES_AFTER" = "$NODES_BEFORE" ]; then
  ok "nodes registry intact after failover ($NODES_AFTER nodes)"
else
  echo "  ✗ nodes registry: before=$NODES_BEFORE after=$NODES_AFTER"
  fail "nodes registry intact"
fi
# The room MAY re-home during the outage: a routing read (GetNodeForRoom) fails
# while redis is down, so the next join re-pins the room to a live node. That is
# the designed resilience behavior — what matters is that the room is STILL
# routable to a REGISTERED node (not lost), not that it stayed on the seed node.
if [ -n "$MAP_AFTER" ]; then
  if [ "$MAP_AFTER" = "$MAP_BEFORE" ]; then
    ok "room_node_map intact after failover ($ROOM -> $MAP_AFTER)"
  elif redis_cli HEXISTS nodes "$MAP_AFTER" >/dev/null 2>&1; then
    ok "room_node_map re-homed to a live node during the outage ($ROOM: $MAP_BEFORE -> $MAP_AFTER)"
  else
    echo "  ✗ room_node_map points at unregistered node $MAP_AFTER"
    fail "room_node_map routable"
  fi
else
  echo "  ✗ room_node_map[$ROOM] empty after failover"
  fail "room_node_map routable"
fi
# the OLD master must rejoin as a REPLICA of the promoted master (dynamic master
# discovery): a stale former master coming back as master would split the brain.
# NOTE: redis INFO replication reports the role as "slave" (Redis 7 terminology),
# not "replica".
old_role=""
for i in $(seq 1 60); do
  # timeout 3: the old master may still be frozen/slow right after the freeze —
  # without it the INFO query (and thus the whole poll) can hang.
  old_role="$(kubectl exec "$OLD_MASTER" -n "$NAMESPACE" -c redis -- sh -c \
    'timeout 3 redis-cli -a "$REDIS_PASSWORD" --no-auth-warning --raw INFO replication' 2>/dev/null \
    | grep '^role:' | cut -d: -f2 | tr -d '\r' || true)"
  [ "$old_role" = "slave" ] && break
  sleep 1
done
if [ "$old_role" = "slave" ]; then
  ok "old master $OLD_MASTER rejoined as REPLICA of $new_master (no split-brain)"
else
  echo "  ✗ old master $OLD_MASTER role after failover: '$old_role' (want slave)"
  fail "old master demoted to replica"
fi

run_client "post-failover" 10

echo ""
summary
