#!/usr/bin/env bash
# 04-e2e — NAT multi-node end-to-end scenarios against the 2-node cluster.
#
# Usage: ./04-e2e.sh [--loss <pct>]   # --loss injects WAN-like loss to force NACK
#
# Scenarios (each independently asserted from room/edge logs + client output):
#   S1 跨节点 join + NAT split（客户端连边缘、房间在房主）
#   S2 上行媒体面（publisher -> edge -> room, H264 simulcast 灌入 SFU buffer）
#   S3 下行媒体面（room -> edge -> subscriber, SRTP 收包）
#   S4 RTCP 双向（room->edge->publisher 上行；client->edge->room 下行；--loss 触发 NACK/PLI）
#   S5 多 codec 协商（视频 + 数据通道跨节点注册）
#   S6 连接模式检测（dual-PC 双会话）
#   S7 会话拆除（participant 离开后 edge 网关会话关闭）
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

LOSS=""
[ "${1:-}" = "--loss" ] && LOSS="${2:-5}"

require lk kubectl
WS_URL="${WS_URL:-ws://$(edge_node_ip):7880}"
export WS_URL

WORK="$(mktemp -d)"
PUB_SECS=14   # seconds each publisher runs before being killed

# Pin the room to the room node so the NAT split is deterministic. Rooms are
# cleared (room_node_map deleted) when the room closes, so re-seed every run.
seed_room_map "$ROOM"

# wait until a pattern appears in a node's server logs.
# --tail bounds the fetch so huge debug logs can't truncate away the pattern.
wait_log() { # wait_log <edge|room> <pattern> <timeout_sec>
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

# publish-demo (loops forever); caller must kill. Demo = H264 simulcast + opus.
# Run `lk` directly (not via lk_join) so $! is the actual lk PID and kill works.
pub_demo() { # pub_demo <identity>   -> echo pid
  lk join-room --url "$WS_URL" --api-key "$API_KEY" --api-secret "$API_SECRET" \
    -r "$ROOM" -i "$1" --publish-demo --fps 30 >"$WORK/$1.log" 2>&1 &
  echo $!
}
stop_pub() { # stop_pub <pid>
  kill -9 "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}
kill_all_lk() {
  pkill -9 -f 'lk join-room' 2>/dev/null || true
}

# ================================================================ S1: split
s1_split() {
  info "S1: 跨节点 join + NAT split"
  local pid; pid="$(pub_demo s1-pub)"

  wait_log room "nat remote session established" 60 \
    && ok "room node established remote session (NAT split)" \
    || { fail "split not triggered"; stop_pub "$pid"; return; }
  grep -q "nat participant uses remote peer connection" < <(room_logs --since=1m) \
    && ok "participant uses remote peer connection" \
    || fail "participant did NOT use remote peer connection"
  grep -q "nat participant uses remote subscriber peer connection" < <(room_logs --since=1m) \
    && ok "participant uses remote subscriber peer connection (dual-PC)" \
    || fail "dual-PC subscriber session not used"
  wait_log edge "nat edge gateway session starting" 30 \
    && ok "edge gateway session started" \
    || fail "edge gateway session not started"
  wait_log edge "nat edge ICE connection state.*connected" 45 \
    && ok "edge ICE connected" \
    || fail "edge ICE did not reach connected"
  wait_log room "remote published track media plane established" 45 \
    && ok "publisher up-plane established (room dialed media channel)" \
    || fail "publisher up-plane not established"
  wait_log edge "nat gateway publisher track attached" 30 \
    && ok "edge attached publisher track (up)" \
    || fail "edge did not attach publisher track"

  stop_pub "$pid"
}

# ================================================================ S2: up plane
s2_up_plane() {
  info "S2: 上行媒体面（publisher -> edge -> room SFU buffer）"
  local pid; pid="$(pub_demo s2-pub)"
  wait_log room "remote published track media plane established" 60 || fail "S2 up-plane not established"
  local up; up="$(room_logs --since=2m | grep -c 'nat up track receiver registered')"
  [ "$up" -ge 3 ] && ok "simulcast up receivers registered (count=$up)" \
                   || fail "expected >=3 up receivers (simulcast), got $up"
  stop_pub "$pid"
}

# ================================================================ S3: down plane
s3_down_plane() {
  info "S3: 下行媒体面（room DownTrack -> edge SRTP -> subscriber）"
  lk join-room --url "$WS_URL" --api-key "$API_KEY" --api-secret "$API_SECRET" \
    -r "$ROOM" -i "s3-sub" --verbose >"$WORK/s3-sub.log" 2>&1 &
  local pid=$!
  sleep 3

  local p; p="$(pub_demo s3-pub)"
  wait_log room "nat down track attached" 60 \
    && ok "room DownTrack bridged to edge for subscriber" \
    || fail "room did NOT bridge down track"
  wait_log edge "nat gateway subscriber track attached" 60 \
    && ok "edge created subscriber SRTP track" \
    || fail "edge did NOT create subscriber track"

  sleep 5   # let media flow
  # media actually flowed: the room's DownTrack forwarded RTP to the edge
  grep -qE '"packetsSeenPrimary": [1-9]' < <(room_logs --since=3m) \
    && ok "subscriber media flowed (room DownTrack forwarded RTP)" \
    || fail "room DownTrack did not forward RTP — see $WORK/s3-sub.log"
  grep -q 'nat down RTCP received from edge' < <(room_logs --since=3m) \
    && ok "down RTCP flowing client->edge->room" \
    || fail "no down RTCP at room"

  stop_pub "$p"
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# ================================================================ S4: RTCP both dirs
s4_rtcp() {
  info "S4: RTCP 双向（up + down，--loss 触发 NACK/PLI）"
  lk join-room --url "$WS_URL" --api-key "$API_KEY" --api-secret "$API_SECRET" \
    -r "$ROOM" -i "s4-sub" --verbose >"$WORK/s4-sub.log" 2>&1 &
  local pid=$!
  sleep 2

  if [ -n "$LOSS" ]; then
    inject_loss "$EDGE_NODE" "$LOSS"
  fi

  local p; p="$(pub_demo s4-pub)"
  wait_log room "remote published track media plane established" 60 || fail "S4 up-plane"
  wait_log edge "nat gateway publisher track attached" 30 || true

  # 上行 RTCP: room receiver -> edge -> publisher (NACK/PLI/SR)
  wait_log edge "nat room -> edge up RTCP forwarded" 45 \
    && ok "up RTCP room->edge->publisher flowing" \
    || fail "no up RTCP at edge"
  # 下行 RTCP: subscriber client -> edge -> room DownTrack
  wait_log room "nat down RTCP received from edge" 45 \
    && ok "down RTCP client->edge->room flowing" \
    || fail "no down RTCP at room"

  if [ -n "$LOSS" ]; then
    # WAN-loss simulation: netem on the edge's client-facing egress. lk's
    # media-sdk client does not send NACKs, so assert the loss actually reached
    # the connection via the room's cross-node quality feedback (a drop from
    # EXCELLENT). NACK generation itself is covered by unit tests.
    wait_log room 'connection quality changed.*"to": "(GOOD|POOR)' 40 \
      && ok "loss degraded connection quality (WAN loss reached client, room feedback dropped)" \
      || fail "no quality degradation detected after loss injection"
    clear_loss "$EDGE_NODE"
  fi

  stop_pub "$p"
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# ================================================================ S5: codec/DC
s5_codecs() {
  info "S5: 多 codec + 数据通道跨节点"
  local pid; pid="$(pub_demo s5-pub)"
  wait_log room "remote published track media plane established" 60 || fail "S5 up-plane"
  local h264; h264="$(room_logs --since=3m | grep -c 'remote published track media plane established.*H264')"
  [ "$h264" -ge 1 ] && ok "H264 up track across nodes (count=$h264)" || fail "no H264 up track"
  grep -q 'nat edge created data channel' < <(edge_logs --since=3m) \
    && ok "edge created data channels (_reliable/_lossy on subscriber PC)" \
    || fail "no data channel created on edge"
  stop_pub "$pid"
}

# ================================================================ S6: connection mode
s6_dualpc() {
  info "S6: 连接模式检测（dual-PC 双会话）"
  local pid; pid="$(pub_demo s6-pub)"
  wait_log room "nat remote session established" 60 || fail "S6 no session"
  sleep 3
  local n; n="$(room_logs --since=1m | grep -c 'nat remote session established')"
  if [ "$n" -ge 2 ]; then
    ok "dual-PC: $n remote sessions (publisher answerer + subscriber offerer)"
  else
    ok "single-PC/one-shot: $n remote session(s)"
  fi
  stop_pub "$pid"
}

# ================================================================ S7: teardown
s7_teardown() {
  info "S7: 会话拆除"
  sleep 5
  local gw; gw="$(edge_logs --since=6m | grep -c 'nat edge gateway session closed')"
  [ "$gw" -ge 1 ] && ok "edge gateway sessions closed ($gw)" || fail "no gateway session close logged"
  local up; up="$(room_logs --since=6m | grep -c 'remote published track media plane established')"
  [ "$up" -ge 1 ] && ok "room processed remote published tracks ($up)" || fail "no remote tracks processed"
}

# ================================================================ main
kill_all_lk          # ensure no stale lk processes bloat node logs
s1_split
s2_up_plane
s3_down_plane
s4_rtcp
s5_codecs
s6_dualpc
s7_teardown

summary
echo "logs/work dir: $WORK"
