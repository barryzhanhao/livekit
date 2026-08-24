#!/usr/bin/env bash
# NAT multi-node E2E — shared helpers.
# Source this from any step script:  . "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
set -euo pipefail

# ---- network (kind image pulls / Go downloads need the proxy) ----
export https_proxy="${https_proxy:-http://127.0.0.1:7897}"
export http_proxy="${http_proxy:-http://127.0.0.1:7897}"
export all_proxy="${all_proxy:-socks5://127.0.0.1:7897}"

# ---- tunables ----
CLUSTER_NAME="${CLUSTER_NAME:-nat-test}"
NAMESPACE="${NAMESPACE:-default}"
ROOM="${ROOM:-nat-test}"
IMG="${IMG:-livekit-nat:dev}"
EDGE_NODE="${EDGE_NODE:-$CLUSTER_NAME-worker}"
ROOM_NODE="${ROOM_NODE:-$CLUSTER_NAME-worker2}"
EDGE2_NODE="${EDGE2_NODE:-$CLUSTER_NAME-worker3}"
EDGE_LABEL="nat-role=edge"
ROOM_LABEL="nat-role=room"
API_KEY="${API_KEY:-devkey}"
API_SECRET="${API_SECRET:-secret}"
WS_URL="${WS_URL:-}"            # set by 03-deploy (ws://<edge-ip>:7880)

# ---- result accounting ----
PASS=0
FAIL=0

log()  { printf '\033[36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
info() { printf '\033[33m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { PASS=$((PASS+1)); printf '\033[32m  ✓ PASS\033[0m %s\n' "$*"; }
fail() { FAIL=$((FAIL+1)); printf '\033[31m  ✗ FAIL\033[0m %s\n' "$*"; }
summary() {
  printf '\n\033[1m==== SUMMARY: %d passed, %d failed ====\033[0m\n' "$PASS" "$FAIL"
  [ "$FAIL" -eq 0 ]
}

require() {
  local c
  for c in "$@"; do
    if ! command -v "$c" >/dev/null 2>&1; then
      echo "missing required command: $c" >&2
      exit 1
    fi
  done
}

# ---- node / pod helpers ----
edge_node_ip() { kubectl get node "$EDGE_NODE" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
room_node_ip() { kubectl get node "$ROOM_NODE" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
edge2_node_ip() { kubectl get node "$EDGE2_NODE" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }

node_logs()  { kubectl logs -n "$NAMESPACE" "deploy/livekit-$1" -c server "${@:2}"; }
edge_logs()  { node_logs edge; }
room_logs()  { node_logs room; }

# ---- redis (sentinel HA) helpers ----
# The redis master moves on failover (sentinel promotes a replica), so every
# redis-cli access must target the CURRENT master pod. Sentinel reports the
# master as an IP; instead find the master by role across the 3 redis pods.
REDIS_PASSWORD="${REDIS_PASSWORD:-nat-e2e-redis-secret}"

redis_master_pod() { # echo the pod name of the current redis master (redis-0/1/2)
  local pod
  for pod in redis-0 redis-1 redis-2; do
    # timeout: a dead/frozen master accepts TCP but never replies; without it the
    # exec would hang until the client-side redis-cli gives up.
    if kubectl exec -n "$NAMESPACE" "$pod" -- sh -c 'timeout 3 redis-cli -a "$REDIS_PASSWORD" --no-auth-warning --raw INFO replication' 2>/dev/null | grep -q '^role:master'; then
      echo "$pod"; return 0
    fi
  done
  return 1
}

# redis-cli against the current master (binary-safe via --raw)
redis_cli() {
  local pod
  pod="$(redis_master_pod)" || { echo "redis_cli: no redis master pod found" >&2; return 1; }
  kubectl exec -n "$NAMESPACE" "$pod" -- redis-cli -a "$REDIS_PASSWORD" --no-auth-warning --raw "$@"
}

# wait_for-compatible predicates (wait_for runs plain commands in-process)
nodes_count_ok() { # 2 (no edge2) or 3 (with edge2) nodes registered
  local n
  n="$(redis_cli HLEN nodes 2>/dev/null || true)"
  [ "$n" = "2" ] || [ "$n" = "3" ]
}
nodes_count_eq() { # exactly this many nodes registered
  [ "$(redis_cli HLEN nodes 2>/dev/null || true)" = "$1" ]
}

wait_for() { # wait_for <desc> <timeout_sec> <cmd...>
  local desc="$1" timeout="$2"; shift 2
  local i=0
  until "$@" >/dev/null 2>&1; do
    i=$((i+1))
    if [ "$i" -ge "$timeout" ]; then
      echo "  ✗ timeout ($timeout s) waiting for: $desc" >&2
      return 1
    fi
    sleep 1
  done
  log "  ok: $desc"
}

wait_pods_ready() {
  wait_for "pods ready" 180 kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name="$1" -n "$NAMESPACE" --timeout=0
}

# ---- room -> room-node pinning (NAT split determinism) ----
# The edge node must NOT host the room: we pin room_node_map[ROOM] to the room
# node's random NodeID. Both pods use hostNetwork, so pod IP == node IP; the
# registered Node proto (redis `nodes` hash value) contains that IP string.
seed_room_map() {
  local room="${1:-$ROOM}"
  local room_ip; room_ip="$(room_node_ip)"
  local id="" key i
  # nodes re-register after a pod restart; poll until the room node is present
  for i in $(seq 1 60); do
    for key in $(redis_cli HKEYS nodes 2>/dev/null); do
      if redis_cli HGET nodes "$key" | grep -aq "$room_ip"; then
        id="$key"; break
      fi
    done
    [ -n "$id" ] && break
    sleep 1
  done
  if [ -z "$id" ]; then
    echo "  ✗ could not find room node ($room_ip) in redis nodes registry" >&2
    return 1
  fi
  redis_cli HSET room_node_map "$room" "$id" >/dev/null
  log "seeded room_node_map[$room]=$id (room node $room_ip)"
}

room_node_id() {
  local room_ip; room_ip="$(room_node_ip)"
  local id="" key
  for key in $(redis_cli HKEYS nodes); do
    if redis_cli HGET nodes "$key" | grep -aq "$room_ip"; then
      echo "$key"; return 0
    fi
  done
  echo ""
}

# ---- lk (livekit-cli) client ----
lk_join() { # lk_join <identity> [extra lk flags...]
  local ident="$1"; shift
  lk join-room --url "${WS_URL}" --api-key "$API_KEY" --api-secret "$API_SECRET" \
    -r "$ROOM" -i "$ident" "$@"
}

# assertions helpers that grep node logs
edge_has()  { edge_logs | grep -q "$1"; }
room_has()  { room_logs | grep -q "$1"; }

# inject WAN-like loss on the client-facing interface of a node (optional)
#   inject_loss <node> <loss%>   e.g. inject_loss kind-worker 5
inject_loss() {
  local node="$1" loss="$2"
  log "injecting ${loss}% loss on ${node} egress (netem) -> triggers NACK/PLI"
  docker exec "$node" tc qdisc add dev eth0 root netem loss "${loss}%"
}
clear_loss() {
  local node="$1"
  docker exec "$node" tc qdisc del dev eth0 root 2>/dev/null || true
}
