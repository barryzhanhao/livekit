#!/usr/bin/env bash
# 06-client-e2e — Go client E2E (production-grade test client, dual-PC via the
# edge). Covers scenarios lk cannot drive: receive-before-publish, NACK cross-node,
# data cross-node (P0-2), attribute broadcast.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"   # livekit repo root (test/nat-e2e is 2 levels deep)
ROOM_GO="${ROOM_GO:-nat-go}"
WORK="$(mktemp -d)"

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
    # Transient ICE/DTLS flake (documented in README): the client connects from
    # the host through a Docker/VM network path and occasionally times out a
    # 30s connect. Retry ONCE in the SAME room after a short drain — real
    # regressions fail deterministically on both attempts, so this only absorbs
    # the transient environment flake (same pattern as 04-e2e.sh's S12 retry).
    echo "  ! $name failed (exit $rc); waiting 10s and retrying once"
    sleep 10
    set +e
    /tmp/nat-client -url "$WS_URL" -api-key "$API_KEY" -api-secret "$API_SECRET" \
      -room "$room" -scenario "$name"
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      echo "  ✓ $name client exit $rc (after retry)"
      return 0
    fi
    echo "  ✗ $name FAILED (exit $rc, also on retry)"
    return 1
  fi
  echo "  ✓ $name client exit $rc"
  return 0
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

wait_log_rcv() { # wait_log_rcv <pattern> <timeout_sec>  (webhook-receiver pod logs)
  local pat="$1" timeout="$2" i=0
  # process substitution (like wait_log): a `| grep -q` pipeline exits 141 under
  # pipefail because grep -q closes the pipe on its first match (kubectl SIGPIPE),
  # so the until loop would never observe a match.
  until grep -qE "$pat" < <(kubectl logs -n "$NAMESPACE" pod/webhook-receiver --tail=2000 2>/dev/null); do
    i=$((i+1))
    if [ "$i" -ge "$timeout" ]; then
      echo "  ✗ timeout ($timeout s) waiting in webhook-receiver logs for: $pat" >&2
      return 1
    fi
    sleep 1
  done
}

receiver_logs() { kubectl logs -n "$NAMESPACE" pod/webhook-receiver --tail=2000 2>/dev/null; }

PASS=0; FAIL=0

# receive-before-publish: subscriber joins empty room, publisher publishes later
if run_scenario receive-before-publish yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# NACK cross-node: client sends NACKs; assert (a) the edge forwarded nack-typed
# RTCP to the room's DownTrack AND (b) the room's DownTrack RECEIVED + processed
# them (nacks >= 1 in its rtp stats on close) — the definitive cross-node NACK
# round-trip. The actual retransmit (nackAcks) is best-effort: the client's RRs
# may have acked the fake-NACKed packets, so the RTX buffer has already dropped
# them (nackMisses) — nacks>=1 proves the NACK path reached the DownTrack.
# FRESH room (the shared room's accumulated participants disturb the timing).
ROOM_NACK="${ROOM_GO}-nack"
seed_room_map "$ROOM_NACK"
if run_scenario nack yes "$ROOM_NACK"; then
  if wait_log edge 'nat edge -> room down RTCP forwarded.*nack' 40 \
  && wait_log room 'rtp stats.*go-nack-sub' 60; then
    n="$(edge_logs --since=3m | grep -c 'nat edge -> room down RTCP forwarded.*nack' || true)"
    n="${n:-0}"
    nrx="$(room_logs --since=3m | grep 'rtp stats' | grep 'go-nack-sub' | grep -o '"nacks": [0-9]*' | grep -o '[0-9]*' | awk '$1>0' | head -1 || true)"
    nrx="${nrx:-}"
    na="$(room_logs --since=3m | grep 'rtp stats' | grep 'go-nack-sub' | grep -o '"nackAcks": [0-9]*' | grep -o '[0-9]*' | head -1 || true)"
    na="${na:-}"
    if [ "$n" -ge 1 ] && [ -n "$nrx" ]; then
      echo "  ✓ NACK: edge forwarded $n nack batches; room DownTrack processed them (nacks=$nrx, retransmits=$na)"
      PASS=$((PASS+1))
    else
      echo "  ✗ NACK: forwarded=$n processed(nacks)=${nrx:-none}"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ NACK: no nack-typed down RTCP reached the room, or no go-nack-sub DownTrack stats"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# data: cross-node data-channel message (P0-2: edge detaches + pumps into control)
if run_scenario data yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

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

# single-pc: server MUST run single-PC NAT (one edge gateway session per
# participant, no dual-PC publisher/subscriber split). The client asserts media
# flows; the EDGE gateway log asserts exactly one session per participant (the
# 2-client scenario → 2 sessions) with no offerer (dual-PC) marker. Asserted on
# the edge (room-scoped, low-volume, rotation-robust) rather than the room log,
# which kubelet rotates under SFU noise.
ROOM_SPC="${ROOM_GO}-spc"
seed_room_map "$ROOM_SPC"
if run_scenario single-pc yes "$ROOM_SPC"; then
  if wait_log edge "nat edge gateway session starting.*$ROOM_SPC\"" 40; then
    ns="$(edge_logs --since=3m | grep 'nat edge gateway session starting' | grep "$ROOM_SPC\"" | wc -l | tr -d ' ')"
    no="$(edge_logs --since=3m | grep 'nat edge gateway session starting' | grep "$ROOM_SPC\"" | grep -c 'isOfferer": true' || true)"
    no="${no:-0}"
    if [ "$ns" -ge 1 ] && [ "$no" -eq 0 ]; then
      echo "  ✓ SINGLE-PC: edge gateway sessions=$ns for room (no dual-PC offerer marker)"
      PASS=$((PASS+1))
    else
      echo "  ✗ SINGLE-PC: edge gateway sessions=$ns, dual-PC offerer=$no (expected >=1 and 0)"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ SINGLE-PC: edge logs missing gateway session for room"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# manual-subscribe: AutoSubscribe=false + explicit UpdateSubscription
if run_scenario manual-subscribe yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# participant-name: display-name update broadcast cross-node
if run_scenario participant-name yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# room-admin: REST RoomService create/list/kick/delete cross-node
if run_scenario room-admin yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# track-pause: subscriber pauses (UpdateTrackSettings.Disabled) then resumes a track
if run_scenario track-pause yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# room-lifecycle: room deleted after participants leave (short empty/departure
# timeout). Needs a FRESH room (the shared ROOM_GO has lingering participants).
ROOM_LC="${ROOM_GO}-lifecycle"
if run_scenario room-lifecycle yes "$ROOM_LC"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# service-apis: full RoomService admin surface (mute/update/send/kick/etc.)
if run_scenario service-apis yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# subscription-permission: explicit per-track subscription permission declaration
if run_scenario subscription-permission yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# quality-request: subscriber requests max video quality (dynacast layer selection)
if run_scenario quality-request yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# rtc-validate: token-validation HTTP endpoints (validateInternal + room allocation)
if run_scenario rtc-validate yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# update-video-track: publisher updates track dimensions; subscriber sees the
# updated Width/Height via the participant broadcast cross-node.
if run_scenario update-video-track yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# update-audio-track: publisher updates Opus track features; subscriber sees
# AudioFeatures via the participant broadcast cross-node (also exercises the
# Opus up-plane through the Go client).
if run_scenario update-audio-track yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# data-track-publish: PublishDataTrackRequest + UnpublishDataTrackRequest signal
# path (data-channel messages are documented unbridged cross-node).
if run_scenario data-track-publish yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# hidden-participant: hidden grant participant must be invisible to peers but
# visible to the admin RoomService. Fresh room (shared ROOM_GO may have peers).
ROOM_HID="${ROOM_GO}-hidden"
seed_room_map "$ROOM_HID"
if run_scenario hidden-participant yes "$ROOM_HID"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# subscriber-only: CanPublish=false participant (recorder-style) receives media
# from a normal publisher. Fresh room (custom-grant participant).
ROOM_REC="${ROOM_GO}-rec"
seed_room_map "$ROOM_REC"
if run_scenario subscriber-only yes "$ROOM_REC"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# room-move-forward: MoveParticipant/ForwardParticipant routing RPCs (same-room
# rejected; cross-room routed to the RoomManager stub "not implemented").
if run_scenario room-move-forward yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# whip-ice-restart: WHIP session lifecycle — POST + media, PATCH (ICE restart)
# cleanly rejected (upstream SDP-patch panic hardened to a 4xx error; the node
# must NOT crash), DELETE (teardown) still succeeds. Edge logs must show both the
# WHIP.Patch and WHIP.Delete API calls (the PATCH error log has no method suffix).
ROOM_WHIP_RS="${ROOM_GO}-whiprs"
seed_room_map "$ROOM_WHIP_RS"
if run_scenario whip-ice-restart yes "$ROOM_WHIP_RS"; then
  if wait_log edge 'API WHIP.Patch' 40 && wait_log edge 'API WHIP.Delete' 40; then
    np="$(edge_logs --since=3m | grep -c 'API WHIP.Patch' || true)"; np="${np:-0}"
    nd="$(edge_logs --since=3m | grep -c 'API WHIP.Delete' || true)"; nd="${nd:-0}"
    if [ "$np" -ge 1 ] && [ "$nd" -ge 1 ]; then
      echo "  ✓ WHIP-ICE-RESTART: edge handled PATCH(x$np) + DELETE(x$nd); room node survived"
      PASS=$((PASS+1))
    else
      echo "  ✗ WHIP-ICE-RESTART: edge PATCH/DELETE log anchors missing (patch=$np delete=$nd)"
      FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ WHIP-ICE-RESTART: edge logs missing WHIP.Patch/Delete"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# whip: one-shot signalling (RFC 9725) ingest over /whip/v1. The client asserts
# the WS subscriber received media cross-node. One-shot mode is proven by the
# EDGE gateway session for THIS room ("oneShot": true, gateway.go) plus the WHIP
# HTTP create path (API WHIP.Create go-whip-pub): the edge log is room-scoped,
# low-volume, and not subject to the room's SFU-log rotation. The room name is
# anchored to the closing quote so the whiprs room ("...-whiprs") cannot satisfy
# this room's filter.
ROOM_WHIP="${ROOM_GO}-whip"
seed_room_map "$ROOM_WHIP"
if run_scenario whip yes "$ROOM_WHIP"; then
  if wait_log edge "nat edge gateway session starting.*$ROOM_WHIP\"" 40 && wait_log edge 'API WHIP.Create' 40; then
    no="$(edge_logs --since=3m | grep 'nat edge gateway session starting' | grep "$ROOM_WHIP\"" | grep -c 'oneShot": true' || true)"; no="${no:-0}"
    np="$(edge_logs --since=3m | grep 'API WHIP.Create' | grep -c 'go-whip-pub' || true)"; np="${np:-0}"
    if [ "$no" -ge 1 ] && [ "$np" -ge 1 ]; then
      echo "  ✓ WHIP: one-shot session (edge oneShot x$no, API WHIP.Create go-whip-pub x$np)"
      PASS=$((PASS+1))
    else
      echo "  ✗ WHIP: one-shot session but markers incomplete (oneShot=$no create=$np)"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ WHIP: edge logs missing one-shot gateway session / WHIP.Create"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# simulate-speaker: publisher simulates active-speaker activity; the subscriber
# must observe the speaker broadcast cross-node (SpeakersChanged over the relay).
# FRESH room: speaker deltas only reach participants SUBSCRIBED to the speaker,
# and the subscriber must not be distracted by other publishers' tracks.
ROOM_SPK="${ROOM_GO}-speaker"
seed_room_map "$ROOM_SPK"
if run_scenario simulate-speaker yes "$ROOM_SPK"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# simulate-node-failure: server drops the participant (simulated node failure);
# the client must observe the server-driven disconnect.
if run_scenario simulate-node-failure yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# simulate-server-leave: server cleanly closes the participant (simulated server
# leave); the client must observe the server-driven disconnect.
if run_scenario simulate-server-leave yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# sub-perm-revoke: after media flows, the PUBLISHER revokes the subscriber's
# access to its track (SubscriptionPermission deny-all → maybeRevokeSubscriptions
# → RemoveSubscriber); the revoked subscriber's media must stop growing. Uses a
# FRESH room: in the shared room the subscriber auto-subscribes to other
# publishers' tracks, whose media keeps flowing after the revoke.
ROOM_REV="${ROOM_GO}-revoke"
seed_room_map "$ROOM_REV"
if run_scenario sub-perm-revoke yes "$ROOM_REV"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# participant-leave-visible: B leaves cleanly; A must observe the participant
# removal broadcast cross-node. FRESH room: the shared room accumulates a
# participant per scenario (70+ by this point), which slows joins enough to
# trigger the 30s connect timeout.
ROOM_LV="${ROOM_GO}-leave"
seed_room_map "$ROOM_LV"
if run_scenario participant-leave-visible yes "$ROOM_LV"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# sync-state: a connected client declares its published track via SyncState; a
# valid state must NOT trigger a full reconnect.
if run_scenario sync-state yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# connection-quality: A must observe B's per-participant connection quality
# cross-node (SignalResponse_ConnectionQuality over the relay). FRESH room: the
# quality broadcast is per-room and A's 20s observation window is timing-fragile
# once the shared room accumulates tens of participants (same reasoning as
# participant-leave-visible) — a fresh room keeps the assertion deterministic.
ROOM_CQ="${ROOM_GO}-cq"
seed_room_map "$ROOM_CQ"
if run_scenario connection-quality yes "$ROOM_CQ"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# turn-credentials: with the in-process TURN server enabled, the join response
# must carry a TURN server (iceServersForParticipant) with valid credentials.
if run_scenario turn-credentials yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# turn-relay-only: BOTH clients force ICETransportPolicyRelay, so their only
# candidates are TURN relays; full ICE + media through the relay proves the
# COMPLETE TURN data path (allocation → permission → STUN → RTP/RTCP), not just
# credential issuance. Works in-cluster because config.yaml's turn block sets
# allow_restricted_peer_cidrs (the edge's host candidates are private kind IPs);
# real networks with public host candidates need no allow-list.
ROOM_RELAY="${ROOM_GO}-relay"
seed_room_map "$ROOM_RELAY"
if run_scenario turn-relay-only yes "$ROOM_RELAY"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# reconnect: publisher drops only its signal; a same-identity rejoin removes the
# duplicate participant and republishes — the subscriber sees media restored.
# FRESH room: the rejoin must not be disturbed by other publishers' media.
ROOM_RC="${ROOM_GO}-reconnect"
seed_room_map "$ROOM_RC"
if run_scenario reconnect yes "$ROOM_RC"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# perform-rpc: RoomService PerformRpc to a NAT participant must fail CLEANLY and
# BOUNDED. P0-2 bridges the data channel cross-node, so the RPC request goes out
# via the edge data channel; the test client has no RPC responder, so the RPC
# times out cleanly (RpcError 1501 Connection timeout) instead of hanging. The
# client asserts the specific error + bounded latency; this pins the behavior as
# a deterministic regression test (no indefinite hang).
# FRESH room (clean psrpc routing target).
ROOM_RPC="${ROOM_GO}-rpc"
seed_room_map "$ROOM_RPC"
if run_scenario perform-rpc yes "$ROOM_RPC"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# simulcast-switch: P0-3. The Go client now publishes 3-layer VP8 simulcast
# itself (raw-pion, PLI-responsive — simulcast.go), so the forwarder's up-switch
# layer-lock can actually release (the lk demo publisher only keyframes consumed
# layers → up-switch stalled forever). The client drives LOW→MEDIUM→HIGH and the
# deterministic proofs are room-side:
#   (a) the room received all 3 simulcast up planes cross-node — `available
#       layers changed - layer seen` reaching [0,1,2] for go-sim-go-pub.
#   (b) the DownTrack's max subscribed spatial followed the requests — `setting
#       max spatial layer` 0, 1, 2 for go-sim-sub.
#   (c) the forwarder actually up-switched cross-node — `upgrading layer`
#       reaching layer 2 AND `forwarded key frame` with layer 1 AND layer 2 for
#       go-sim-sub (the layer-lock released and each higher layer's keyframe was
#       forwarded). The client asserts media kept flowing (did not freeze).
# Cross-node BITRATE bands are not asserted (allocator/down-plane timing is
# non-deterministic after an up-switch — see README); the room-side anchors are
# the authoritative P0-3 proof.
ROOM_SIM="${ROOM_GO}-simulcast"
seed_room_map "$ROOM_SIM"
if run_scenario simulcast-switch yes "$ROOM_SIM"; then
  if wait_log room 'available layers changed - layer seen' 60 && wait_log room 'upgrading layer' 60; then
    layers3="$(room_logs --since=4m | grep 'available layers changed - layer seen' | grep 'go-sim-go-pub' | grep -o '\[0, 1, 2\]' | head -1)"
    layers="$(room_logs --since=4m | grep 'setting max spatial layer' | grep 'go-sim-sub' | grep -o '"layer": *[0-9]*' | grep -o '[0-9]*' | sort -u | tr '\n' ' ')"
    upgr="$(room_logs --since=4m | grep 'upgrading layer' | grep 'go-sim-sub' | grep -o '"current": "VideoLayer{s: [0-9-]*' | grep -o '[0-9]*' | sort -u | tr '\n' ' ')"
    kf1="$(room_logs --since=4m | grep 'forwarded key frame' | grep 'go-sim-sub' | grep -o '"layer": [0-9]*' | grep -o '[0-9]*' | sort -u | tr '\n' ' ')"
    if [ -n "$layers3" ] && echo " $layers " | grep -q ' 0 ' && echo " $layers " | grep -q ' 1 ' && echo " $layers " | grep -q ' 2 ' \
      && echo " $upgr " | grep -q ' 2 ' && echo " $kf1 " | grep -q ' 1 ' && echo " $kf1 " | grep -q ' 2 '; then
      echo "  ✓ SIMULCAST-SWITCH: 3 up-planes [0,1,2] + applied {0,1,2} + forwarder upgraded to layer 2 (upgraded: $upgr, keyframes on layers: $kf1)"
      PASS=$((PASS+1))
    else
      echo "  ✗ SIMULCAST-SWITCH: layers3=[$layers3] applied=($layers) upgraded=($upgr) keyframes=($kf1)"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ SIMULCAST-SWITCH: room logs missing up-plane/upgrade anchors"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# reconnect-resume: the reconnect=true resume negotiation path. The publisher
# drops its signal WS (PCs alive), then reconnects the signal with reconnect=true
# + its SID within the disconnect-cleanup grace window. The server MUST take the
# resume path (room log "resuming RTC session"); the client then either completes
# a clean resume (SignalResponse_Reconnect) or falls back to a full reconnect when
# the server rejects it (Leave with RECONNECT action). Either way media must be
# restored — the assertion is that the negotiation path is exercised and the
# client recovers, never hangs. FRESH room (the rejoin must not be disturbed).
ROOM_RS="${ROOM_GO}-resume"
seed_room_map "$ROOM_RS"
if run_scenario reconnect-resume yes "$ROOM_RS"; then
  if wait_log room 'resuming RTC session' 60; then
    echo "  ✓ RECONNECT-RESUME: server took the resume path (resuming RTC session)"
    PASS=$((PASS+1))
  else
    echo "  ✗ RECONNECT-RESUME: room log missing resume anchor"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# webhook-events: the server-side webhook (HTTP callback) path cross-node. The
# server POSTs HMAC-signed events (Authorization Bearer JWT carrying sha256 of the
# body) to the receiver pod (webhook-receiver.default.svc:8080/webhook), which
# verifies the signature and logs each event. A client joins + publishes + leaves;
# the receiver must observe participant_joined / track_published / participant_left
# / track_unpublished for go-webhook-pub, with ZERO rejected signatures. FRESH
# room; assertions are scoped to the identity so other scenarios' events cannot
# satisfy them.
ROOM_WEB="${ROOM_GO}-webhook"
seed_room_map "$ROOM_WEB"
if run_scenario webhook-events yes "$ROOM_WEB"; then
  if wait_log_rcv 'WEBHOOK_EVENT participant_joined.*go-webhook-pub' 40 \
  && wait_log_rcv 'WEBHOOK_EVENT track_published.*go-webhook-pub' 40 \
  && wait_log_rcv 'WEBHOOK_EVENT participant_left.*go-webhook-pub' 40 \
  && wait_log_rcv 'WEBHOOK_EVENT track_unpublished.*go-webhook-pub' 40; then
    nj="$(receiver_logs | grep -c 'WEBHOOK_EVENT participant_joined.*go-webhook-pub' || true)"; nj="${nj:-0}"
    np="$(receiver_logs | grep -c 'WEBHOOK_EVENT track_published.*go-webhook-pub' || true)"; np="${np:-0}"
    nl="$(receiver_logs | grep -c 'WEBHOOK_EVENT participant_left.*go-webhook-pub' || true)"; nl="${nl:-0}"
    nu="$(receiver_logs | grep -c 'WEBHOOK_EVENT track_unpublished.*go-webhook-pub' || true)"; nu="${nu:-0}"
    nr="$(receiver_logs | grep -c 'WEBHOOK_REJECTED' || true)"; nr="${nr:-0}"
    if [ "$nj" -ge 1 ] && [ "$np" -ge 1 ] && [ "$nl" -ge 1 ] && [ "$nu" -ge 1 ] && [ "$nr" -eq 0 ]; then
      echo "  ✓ WEBHOOK-EVENTS: receiver verified join(x$nj)/publish(x$np)/left(x$nl)/unpublish(x$nu); 0 rejected signatures"
      PASS=$((PASS+1))
    else
      echo "  ✗ WEBHOOK-EVENTS: counts join=$nj publish=$np left=$nl unpublish=$nu rejected=$nr"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ WEBHOOK-EVENTS: receiver did not observe all four signed events for go-webhook-pub"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# subscriber-pli: the down-direction RTCP PLI path (keyframe-request variant of
# the down-RTCP loop, complementary to NACK). The subscriber sends a PLI; the
# room's DownTrack must process it and request a publisher keyframe (`sending PLI
# RTCP` — this is the SSRC-rewrite fix's PLI proof; before the fix the edge-rewritten
# MediaSSRC was dropped at `p.MediaSSRC == d.ssrc`). FRESH room; scoped to go-pli-sub.
ROOM_PLI="${ROOM_GO}-pli"
seed_room_map "$ROOM_PLI"
if run_scenario subscriber-pli yes "$ROOM_PLI"; then
  if wait_log room 'sending PLI RTCP' 40; then
    npl="$(room_logs --since=3m | grep 'sending PLI RTCP' | grep -c 'go-pli-sub' || true)"; npl="${npl:-0}"
    if [ "$npl" -ge 1 ]; then
      echo "  ✓ SUBSCRIBER-PLI: room DownTrack processed the subscriber PLI (x$npl) and requested a keyframe"
      PASS=$((PASS+1))
    else
      echo "  ✗ SUBSCRIBER-PLI: 'sending PLI RTCP' present but not scoped to go-pli-sub"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ SUBSCRIBER-PLI: room DownTrack did not process the subscriber PLI"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi

# media-follows-signaling: the core boundary — media terminates on the EDGE node
# the client signaled to. The server's PCs run on the edge (advertise_ip), so the
# client's remote ICE candidates must carry the edge IP (= the WS hostname), never
# the room node's. Self-contained client assertion (candidate IP + media flowed).
# FRESH room (shared room slows joins late in the suite).
ROOM_MFS="${ROOM_GO}-mfs"
seed_room_map "$ROOM_MFS"
if run_scenario media-follows-signaling yes "$ROOM_MFS"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# simulate-ice-restart: server-driven ICE restart (SimulateScenario_
# SwitchCandidateProtocol → participant.ICERestart, the same path resume uses).
# The publisher's transports renegotiate cross-node; media must continue on BOTH
# sides (the publisher's restarted subscriber PC + the subscriber's untouched PC).
# FRESH room (self-contained bidirectional media).
ROOM_ICE="${ROOM_GO}-icerestart"
seed_room_map "$ROOM_ICE"
if run_scenario simulate-ice-restart yes "$ROOM_ICE"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# security-auth: the cross-node media_relay + control channels require the shared
# secret (media_relay.secret, P0-1). A wrong-secret dial to the EDGE's relays must
# be rejected by the auth handshake. Self-contained client assertion (needs no room).
if run_scenario security-auth yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# multi-edge: two clients signal to DIFFERENT edge nodes (edge1 + edge2) for the
# SAME room hosted on the room node. Media must follow signaling on BOTH edges —
# pub(edge1)→sub(edge2) AND sub(edge2)→pub(edge1) both flow (each via the room
# node). Only runs when the edge2 worker is deployed; both edges must show a
# gateway session for this room.
if [ -n "$(edge2_node_ip 2>/dev/null || true)" ]; then
  WS2="ws://$(edge2_node_ip):7880"
  ROOM_ME="${ROOM_GO}-multi"
  seed_room_map "$ROOM_ME"
  echo "=== multi-edge (edge1=$(edge_node_ip) edge2=$(edge2_node_ip)) ==="
  set +e
  /tmp/nat-client -url "$WS_URL" -url2 "$WS2" -api-key "$API_KEY" -api-secret "$API_SECRET" \
    -room "$ROOM_ME" -scenario multi-edge
  rc=$?
  set -e
  if [ "$rc" -eq 0 ] \
  && wait_log edge "nat edge gateway session starting.*$ROOM_ME\"" 40 \
  && wait_log edge2 "nat edge gateway session starting.*$ROOM_ME\"" 40; then
    ne1="$(edge_logs --since=3m | grep 'nat edge gateway session starting' | grep -c "$ROOM_ME\"" || true)"; ne1="${ne1:-0}"
    ne2="$(kubectl logs -n "$NAMESPACE" "deploy/livekit-edge2" -c server --since=3m 2>/dev/null | grep -c 'nat edge gateway session starting.*'"$ROOM_ME"'"' || true)"; ne2="${ne2:-0}"
    if [ "$ne1" -ge 1 ] && [ "$ne2" -ge 1 ]; then
      echo "  ✓ MULTI-EDGE: media crossed edge1(x$ne1)↔edge2(x$ne2) via the room node"
      PASS=$((PASS+1))
    else
      echo "  ✗ MULTI-EDGE: gateway sessions edge1=$ne1 edge2=$ne2"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ MULTI-EDGE: client rc=$rc or gateway sessions missing on both edges"; FAIL=$((FAIL+1))
  fi
else
  echo "  - MULTI-EDGE: skipped (no edge2 worker deployed; run 01-cluster.sh to recreate with worker3)"
fi

# scale-stress: two rooms × two edges, all media flowing concurrently. Room A
# spans edge1(edge→edge2(sub); room B spans edge2(pub)→edge1(sub). Both rooms
# pinned to the room node — stresses the room node's dual-edge handling + the
# control-channel establishment under concurrency. Requires edge2.
if [ -n "$(edge2_node_ip 2>/dev/null || true)" ]; then
  WS2="ws://$(edge2_node_ip):7880"
  ROOM_SC_A="${ROOM_GO}-sca"
  ROOM_SC_B="${ROOM_GO}-scb"
  seed_room_map "$ROOM_SC_A"
  seed_room_map "$ROOM_SC_B"
  echo "=== scale-stress (2 rooms × 2 edges) ==="
  set +e
  /tmp/nat-client -url "$WS_URL" -url2 "$WS2" -room2 "$ROOM_SC_B" \
    -api-key "$API_KEY" -api-secret "$API_SECRET" -room "$ROOM_SC_A" -scenario scale-stress
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    # 4 clients connect concurrently across 2 edges; the documented Docker/VM
    # transient flake can occasionally stall one (same as run_scenario's retry).
    echo "  ! scale-stress failed (exit $rc); waiting 10s and retrying once"
    sleep 10
    set +e
    /tmp/nat-client -url "$WS_URL" -url2 "$WS2" -room2 "$ROOM_SC_B" \
      -api-key "$API_KEY" -api-secret "$API_SECRET" -room "$ROOM_SC_A" -scenario scale-stress
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
      echo "  ✓ SCALE-STRESS: 2 rooms × 2 edges media flowed concurrently (after retry)"
      PASS=$((PASS+1))
    else
      echo "  ✗ SCALE-STRESS: client exit $rc (also on retry)"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✓ SCALE-STRESS: 2 rooms × 2 edges media flowed concurrently"
    PASS=$((PASS+1))
  fi
else
  echo "  - SCALE-STRESS: skipped (no edge2 worker)"
fi

# room-node-failure (#51): REAL room-node failure + client-driven migration. The
# Go scenario pins a fresh room to the ROOM node, establishes media cross-node,
# prints READY, then waits for the room node to die. We kill the room node
# deployment, the scenario observes the server-driven disconnect, re-runs the
# join (resume attempt cleanly rejected → full rejoin, same identity), and
# verifies media is restored — proof the room was RE-HOMED onto a live node.
# We then assert room_node_map moved off the dead node and bring the room node
# back so subsequent scenarios keep working.
if kubectl get deploy/livekit-room -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "=== room-node-failure (real node kill + re-home) ==="
  ROOM_RNF="${ROOM_GO}-rnf"
  seed_room_map "$ROOM_RNF"
  OUT="$(mktemp)"
  /tmp/nat-client -url "$WS_URL" -api-key "$API_KEY" -api-secret "$API_SECRET" \
    -room "$ROOM_RNF" -scenario room-node-failure >"$OUT" 2>&1 &
  SCPID=$!
  # wait for the scenario to establish media (READY marker) before killing
  ready=0
  for i in $(seq 1 90); do
    if grep -q "ROOM_NODE_FAILURE: READY" "$OUT" 2>/dev/null; then ready=1; break; fi
    if ! kill -0 "$SCPID" 2>/dev/null; then break; fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "  ✗ ROOM-NODE-FAILURE: scenario did not reach READY (media not established)"
    kill "$SCPID" 2>/dev/null || true
    wait "$SCPID" 2>/dev/null || true
    echo "$(cat "$OUT")" >&2 || true
    FAIL=$((FAIL+1))
  else
    echo "  media established; killing the room node deployment"
    # Force-kill: scale to 0 (no replacement) + force-delete the pod (SIGKILL, so
    # no graceful drain / UnregisterNode). This exercises the real crash path —
    # the edge's media gateway loses its control channel and the RTC service must
    # close the client WS (OnGatewayLost) to trigger reconnection + re-homing.
    kubectl scale deploy/livekit-room -n "$NAMESPACE" --replicas=0
    kubectl delete pod -l app=livekit-room -n "$NAMESPACE" --force --grace-period=0 >/dev/null 2>&1 || true
    # wait for the scenario to finish (disconnect + rejoin + media resume), bounded
    rc=124
    for i in $(seq 1 180); do
      if ! kill -0 "$SCPID" 2>/dev/null; then
        wait "$SCPID"
        rc=$?
        break
      fi
      sleep 1
    done
    if [ "$rc" -ne 0 ]; then
      if [ "$rc" -eq 124 ]; then
        echo "  ✗ ROOM-NODE-FAILURE: scenario TIMED OUT (client never recovered)"
        kill -9 "$SCPID" 2>/dev/null || true
        wait "$SCPID" 2>/dev/null || true
      else
        echo "  ✗ ROOM-NODE-FAILURE: scenario exit $rc"
      fi
      echo "$(cat "$OUT")" >&2 || true
      FAIL=$((FAIL+1))
    else
      echo "  ✓ ROOM-NODE-FAILURE: scenario exit 0"
      PASS=$((PASS+1))
      # the room must have been re-homed off the dead room node onto a live node
      RH="$(redis_cli HGET room_node_map "$ROOM_RNF" || true)"
      if [ -z "$RH" ]; then
        echo "  ✗ ROOM-NODE-FAILURE: room_node_map[$ROOM_RNF] empty after recovery"; FAIL=$((FAIL+1))
      elif [ "$RH" = "node-room" ]; then
        echo "  ✗ ROOM-NODE-FAILURE: room_node_map[$ROOM_RNF] still on the dead room node"; FAIL=$((FAIL+1))
      else
        echo "  ✓ ROOM-NODE-FAILURE: room re-homed room_node_map[$ROOM_RNF]=$RH"
      fi
    fi
  fi
  # bring the room node back for subsequent scenarios; wait until it re-registers
  kubectl scale deploy/livekit-room -n "$NAMESPACE" --replicas=1 >/dev/null 2>&1 || true
  wait_for "room node back up" 180 kubectl get deploy/livekit-room -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' | grep -q 1
  wait_for "room node re-registered" 60 sh -c 'kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli --raw HLEN nodes | grep -q "^3$"'
  rm -f "$OUT"
else
  echo "  - ROOM-NODE-FAILURE: skipped (no livekit-room deployment)"
fi

# health: HTTP / on both nodes (defaultHandler → healthCheck, node-stats
# heartbeat freshness). Client-facing edge + room node must both answer 200 OK.
if curl -s -m 5 "http://$(edge_node_ip):7880/" | grep -q '^OK$' && curl -s -m 5 "http://$(room_node_ip):7880/" | grep -q '^OK$'; then
  echo "  ✓ HEALTH: / returns 200 OK on edge + room nodes"
  PASS=$((PASS+1))
else
  echo "  ✗ HEALTH: / health check failed on a node"; FAIL=$((FAIL+1))
fi

echo "==== GO-CLIENT SUMMARY: $PASS passed, $FAIL failed ===="
[ "$FAIL" -eq 0 ]
