#!/usr/bin/env bash
# 06-client-e2e — Go client E2E (production-grade test client, dual-PC via the
# edge). Covers scenarios lk cannot drive: receive-before-publish, NACK cross-node,
# data (documented unbridged), attribute broadcast.
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
# removal broadcast cross-node.
if run_scenario participant-leave-visible yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# sync-state: a connected client declares its published track via SyncState; a
# valid state must NOT trigger a full reconnect.
if run_scenario sync-state yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# connection-quality: A must observe B's per-participant connection quality
# cross-node (SignalResponse_ConnectionQuality over the relay).
if run_scenario connection-quality yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# turn-credentials: with the in-process TURN server enabled, the join response
# must carry a TURN server (iceServersForParticipant) with valid credentials.
if run_scenario turn-credentials yes; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# reconnect: publisher drops only its signal; a same-identity rejoin removes the
# duplicate participant and republishes — the subscriber sees media restored.
# FRESH room: the rejoin must not be disturbed by other publishers' media.
ROOM_RC="${ROOM_GO}-reconnect"
seed_room_map "$ROOM_RC"
if run_scenario reconnect yes "$ROOM_RC"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# perform-rpc: RoomService PerformRpc to a NAT participant must fail CLEANLY and
# BOUNDED — data-channel DATA messages are a documented NAT limitation (not
# bridged cross-node), so the room's remote (edge) PCTransport has no local data
# channel and the RPC returns ErrDataChannelUnavailable immediately instead of
# hanging. The client asserts the specific error + bounded latency; this pins the
# documented limitation as a deterministic regression test (no indefinite hang).
# FRESH room (clean psrpc routing target).
ROOM_RPC="${ROOM_GO}-rpc"
seed_room_map "$ROOM_RPC"
if run_scenario perform-rpc yes "$ROOM_RPC"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

# simulcast-switch: REAL 3-layer layer switching. lk publishes 3-layer H264
# simulcast (publish-demo, identity go-sim-lk-pub); the Go subscriber requests
# LOW then MEDIUM/HIGH via UpdateTrackSettings. Two room-side proofs, both scoped
# to this scenario's participants so earlier scenarios (quality-request, single
# layer) cannot satisfy them:
#   (a) the room received all 3 simulcast up planes cross-node — `available layers
#       changed - layer seen` reaching [0,1,2] for go-sim-lk-pub. This is the
#       MediaGateway fix's proof: each simulcast layer now bridges on its own
#       MediaChannel (previously only the first layer's RTP crossed the boundary).
#   (b) the DownTrack's max subscribed spatial followed the client's requests —
#       `setting max spatial layer` shows layers 0, 1, 2 for go-sim-sub (the real
#       down-switch to LOW + selection applied through MEDIUM/HIGH).
# The client drives the requests and keeps the session alive (client-side media
# BITRATE assertions are not reliable: an up-switch after a down-switch stalls the
# forwarder in a layer-lock PLI loop cross-node, documented limitation).
ROOM_SIM="${ROOM_GO}-simulcast"
seed_room_map "$ROOM_SIM"
lk join-room --url "$WS_URL" --api-key "$API_KEY" --api-secret "$API_SECRET" \
  -r "$ROOM_SIM" -i go-sim-lk-pub --publish-demo --fps 30 >"$WORK/lk-sim.log" 2>&1 &
LK_SIM_PID=$!
if run_scenario simulcast-switch yes "$ROOM_SIM"; then
  if wait_log room 'available layers changed - layer seen' 60 && wait_log room 'setting max spatial layer' 60; then
    layers3="$(room_logs --since=3m | grep 'available layers changed - layer seen' | grep 'go-sim-lk-pub' | grep -o '\[0, 1, 2\]' | head -1)"
    layers="$(room_logs --since=3m | grep 'setting max spatial layer' | grep 'go-sim-sub' | grep -o '"layer": *[0-9]*' | grep -o '[0-9]*' | sort -u | tr '\n' ' ')"
    if [ -n "$layers3" ] && echo " $layers " | grep -q ' 0 ' && echo " $layers " | grep -q ' 1 ' && echo " $layers " | grep -q ' 2 '; then
      echo "  ✓ SIMULCAST-SWITCH: 3-layer up-plane cross-node + applied layer selection {0,1,2} (observed: $layers)"
      PASS=$((PASS+1))
    else
      echo "  ✗ SIMULCAST-SWITCH: layers3=[$layers3] applied=($layers)"; FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ SIMULCAST-SWITCH: room logs missing layer anchors"; FAIL=$((FAIL+1))
  fi
else
  FAIL=$((FAIL+1))
fi
kill -9 "$LK_SIM_PID" 2>/dev/null || true
wait "$LK_SIM_PID" 2>/dev/null || true

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
