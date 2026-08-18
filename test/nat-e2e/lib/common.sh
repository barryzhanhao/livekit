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

# redis-cli running inside the redis pod (binary-safe via --raw)
redis_cli() { kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli --raw "$@"; }

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
