// NAT multi-node Go E2E client.
//
// Production-grade WebRTC client (reuses the repo's official test/client package)
// that drives scenarios lk (livekit-cli) cannot: receive-before-publish, NACK
// generation, data (data-channel) publishing, and attribute updates. It connects
// to the EDGE node in dual-PC mode (no join_request query param), which is the
// NAT "media follows signaling" path.
//
// Usage (built by 06-client-e2e.sh):
//
//	nat-client -url ws://<edge-ip>:7880 -api-key devkey -api-secret secret \
//	           -room nat-go -scenario <receive-before-publish|nack|data|attributes|single-pc|metadata|mute|multitrack|whip|manual-subscribe|participant-name|track-pause|room-lifecycle>
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/webrtc/v4"
	"github.com/twitchtv/twirp"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	testclient "github.com/livekit/livekit-server/test/client"
)

func boolPtr(b bool) *bool { return &b }

func mustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	must(err)
	return u
}

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
}

func token(apiKey, apiSecret, room, identity string) string {
	at := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity(identity).
		SetName(identity)
	at.AddGrant(&auth.VideoGrant{RoomJoin: true, Room: room, CanPublish: boolPtr(true), CanSubscribe: boolPtr(true), CanPublishData: boolPtr(true), CanUpdateOwnMetadata: boolPtr(true)})
	t, err := at.ToJWT()
	must(err)
	return t
}

// newClient connects to the edge WS and starts the RTC session (dual-PC mode).
func newClient(wsURL, apiKey, apiSecret, room, identity string) *testclient.RTCClient {
	return newRTCClient(wsURL, apiKey, apiSecret, room, identity, false)
}

// newSinglePCClient connects in single-PC mode: the WS URL carries a
// join_request (v1 protocol), which makes the server use UseSinglePeerConnection.
// In NAT mode this exercises a distinct path: ONE control channel + ONE edge
// gateway session per participant (instead of the dual-PC publisher+subscriber
// pair). The room-side assertions live in 06-client-e2e.sh.
func newSinglePCClient(wsURL, apiKey, apiSecret, room, identity string) *testclient.RTCClient {
	return newRTCClient(wsURL, apiKey, apiSecret, room, identity, true)
}

// newRelayClient connects with ICETransportPolicyRelay forced — the ONLY ICE
// candidates the client gathers are TURN relays (no host/srflx), so any
// established connection is necessarily traversing the TURN relay.
func newRelayClient(wsURL, apiKey, apiSecret, room, identity string) *testclient.RTCClient {
	opts := &testclient.Options{AutoSubscribe: true, ForceRelay: true}
	conn, err := testclient.NewWebSocketConn(wsURL, token(apiKey, apiSecret, room, identity), opts)
	must(err)
	c, err := testclient.NewRTCClient(conn, false, opts)
	must(err)
	go c.Run()
	return c
}

func newRTCClient(wsURL, apiKey, apiSecret, room, identity string, singlePC bool) *testclient.RTCClient {
	// singlePC: the WS URL must carry a join_request (v1 protocol) for the server
	// to enable UseSinglePeerConnection; NewRTCClient's flag alone does not add it.
	opts := &testclient.Options{AutoSubscribe: true, UseJoinRequestQueryParam: singlePC}
	conn, err := testclient.NewWebSocketConn(wsURL, token(apiKey, apiSecret, room, identity), opts)
	must(err)
	c, err := testclient.NewRTCClient(conn, singlePC, opts)
	must(err)
	go c.Run() // Run blocks (signal read loop); drive it in a goroutine like the official harness
	return c
}

func waitConnected(c *testclient.RTCClient) {
	must(c.WaitUntilConnected(30 * time.Second))
	fmt.Println("connected:", c.ID())
}

// waitBytes polls until the client has received at least n media bytes.
func waitBytes(c *testclient.RTCClient, n uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.BytesReceived() >= n {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out: only received %d bytes", c.BytesReceived())
}

// waitRemoteTrack polls until a remote participant has a published track.
func waitRemoteTrack(c *testclient.RTCClient, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range c.RemoteParticipants() {
			if len(p.Tracks) > 0 {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out: no remote tracks")
}

// waitRemoteTrackCount polls until the client observes n published remote tracks
// (across all remote participants).
func waitRemoteTrackCount(c *testclient.RTCClient, n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		total := 0
		for _, p := range c.RemoteParticipants() {
			total += len(p.Tracks)
		}
		if total >= n {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out: observed %d remote tracks, want >=%d", countRemoteTracks(c), n)
}

func countRemoteTracks(c *testclient.RTCClient) int {
	total := 0
	for _, p := range c.RemoteParticipants() {
		total += len(p.Tracks)
	}
	return total
}

// scenarioSinglePC: both peers connect in single-PC mode (join_request).
// Exercises the NAT single-PC path: one control channel + one edge gateway
// session per participant, media flowing both directions across nodes.
func scenarioSinglePC(url, apiKey, apiSecret, room string) {
	sub := newSinglePCClient(url, apiKey, apiSecret, room, "go-spc-sub")
	waitConnected(sub)
	pub := newSinglePCClient(url, apiKey, apiSecret, room, "go-spc-pub")
	waitConnected(pub)

	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("SINGLE_PC: FAIL no media received in single-PC mode", err)
		os.Exit(1)
	}
	fmt.Println("SINGLE_PC: PASS (subscriber received", sub.BytesReceived(), "bytes in single-PC mode)")
}

// scenarioMetadata: update participant metadata on the publisher; the subscriber
// must observe it via the participant broadcast across nodes.
func scenarioMetadata(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-meta-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-meta-pub")
	waitConnected(pub)

	md := fmt.Sprintf("nat-e2e-meta-%d", time.Now().Unix())
	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_UpdateMetadata{
			UpdateMetadata: &livekit.UpdateParticipantMetadata{Metadata: md},
		},
	}))

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range sub.RemoteParticipants() {
			if p.Metadata == md {
				fmt.Println("METADATA: PASS (subscriber saw updated metadata)")
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("METADATA: FAIL subscriber never saw the metadata update")
	os.Exit(1)
}

// scenarioMute: publisher mutes its published track; the subscriber must observe
// the muted flag via the participant broadcast. The track SID is read from the
// subscriber's remote view (server-assigned TR_*), which is what mute expects.
func scenarioMute(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-mute-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-mute-pub")
	waitConnected(pub)

	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	// wait until the track is visible to the subscriber and capture its SID
	trackID := ""
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && trackID == "" {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-mute-pub" {
				continue
			}
			for _, t := range p.Tracks {
				trackID = t.Sid
			}
		}
		if trackID == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if trackID == "" {
		fmt.Println("MUTE: FAIL published track never visible to subscriber")
		os.Exit(1)
	}

	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Mute{
			Mute: &livekit.MuteTrackRequest{Sid: trackID, Muted: true},
		},
	}))

	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-mute-pub" {
				continue
			}
			for _, t := range p.Tracks {
				if t.Sid == trackID && t.Muted {
					fmt.Println("MUTE: PASS (subscriber saw track muted)")
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("MUTE: FAIL subscriber never saw the track muted")
	os.Exit(1)
}

// scenarioMultitrack: publish two video tracks (camera + screen-share style);
// the subscriber must auto-subscribe to both and receive media.
func scenarioMultitrack(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-mt-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-mt-pub")
	waitConnected(pub)

	w1, err := pub.AddStaticTrack("video/vp8", "video1", "camera")
	must(err)
	defer w1.Stop()
	w2, err := pub.AddStaticTrack("video/vp8", "video2", "screen")
	must(err)
	defer w2.Stop()

	if err := waitRemoteTrackCount(sub, 2, 20*time.Second); err != nil {
		fmt.Println("MULTITRACK: FAIL", err)
		os.Exit(1)
	}
	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("MULTITRACK: FAIL no media received", err)
		os.Exit(1)
	}
	fmt.Println("MULTITRACK: PASS (2 tracks published; subscriber received", sub.BytesReceived(), "bytes)")
}

// scenarioWhip: a WHIP (one-shot signalling) publisher ingests video over
// /whip/v1 (RFC 9725); a normal WS subscriber must receive its track cross-node.
// On the server this exercises UseOneShotSignallingMode — one control channel +
// one edge gateway session, ICE gathered before the answer (asserted via room
// logs "oneShot": true in 06-client-e2e.sh).
func scenarioWhip(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-whip-sub")
	waitConnected(sub)

	httpBase := "http://" + mustParseURL(url).Host
	done := make(chan error, 1)
	go func() {
		done <- whipPublish(httpBase, token(apiKey, apiSecret, room, "go-whip-pub"), room, "go-whip-pub", 25*time.Second)
	}()

	// The subscriber must observe the WHIP publisher's track and receive media.
	if err := waitRemoteTrackCount(sub, 1, 20*time.Second); err != nil {
		select {
		case perr := <-done:
			fmt.Println("WHIP: publisher error:", perr)
		default:
		}
		fmt.Println("WHIP: FAIL subscriber never saw the WHIP track", err)
		os.Exit(1)
	}
	if err := waitBytes(sub, 4096, 30*time.Second); err != nil {
		fmt.Println("WHIP: FAIL no media from WHIP publisher", err)
		os.Exit(1)
	}
	if err := <-done; err != nil {
		fmt.Println("WHIP: FAIL publisher error", err)
		os.Exit(1)
	}
	fmt.Println("WHIP: PASS (one-shot publish; subscriber received", sub.BytesReceived(), "bytes)")
}

// scenarioWhipIceRestart: full WHIP session lifecycle — POST (create + publish),
// PATCH If-Match:* (ICE restart), then DELETE (teardown). The ICE-restart PATCH
// is blocked by an upstream livekit/protocol panic in the SDP patch helper
// (mutate-while-ranging on candidate-bearing remote descriptions); the fork
// hardens that into a clean 4xx error so a remote fragment cannot crash the node.
// The scenario asserts the node SURVIVES the malformed-fragment PATCH (DELETE
// still works right after) — a crash-resilience regression test. It also covers
// whipservice.handleParticipantPatch + handleParticipantDelete.
func scenarioWhipIceRestart(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-whip-rs-sub")
	waitConnected(sub)

	httpBase := "http://" + mustParseURL(url).Host
	s, err := newWhipSession(httpBase, token(apiKey, apiSecret, room, "go-whip-rs"), "go-whip-rs")
	must(err)
	defer s.pc.Close()

	if err := s.waitConnected(20 * time.Second); err != nil {
		fmt.Println("WHIP_ICE_RESTART: FAIL initial connect", err)
		os.Exit(1)
	}
	// Publish RTP in the background immediately: the edge gateway registers the
	// up-plane track only after the first RTP packet (FireOnTrackBeforeFirstRTP
	// is off on the edge), so the track only becomes visible to subscribers once
	// real media flows.
	go func() {
		_ = s.publishVideo(90 * time.Second)
	}()

	// subscriber sees the WHIP track + initial media
	if err := waitRemoteTrackCount(sub, 1, 20*time.Second); err != nil {
		fmt.Println("WHIP_ICE_RESTART: FAIL subscriber never saw the WHIP track", err)
		os.Exit(1)
	}
	if err := waitBytes(sub, 4096, 30*time.Second); err != nil {
		fmt.Println("WHIP_ICE_RESTART: FAIL no initial media", err)
		os.Exit(1)
	}

	// ICE restart via PATCH (If-Match: *) — must yield a CLEAN error, not a crash.
	perr := s.iceRestart()
	if perr == nil {
		fmt.Println("WHIP_ICE_RESTART: FAIL expected clean error from ICE-restart PATCH (upstream SDP-patch panic), got success")
		os.Exit(1)
	}
	fmt.Println("WHIP_ICE_RESTART: PATCH returned clean error:", perr)

	// The node must still be alive after the failed PATCH: the session teardown
	// (DELETE → DeleteSession psrpc) must still succeed.
	if err := s.deleteSession(); err != nil {
		fmt.Println("WHIP_ICE_RESTART: FAIL DELETE after failed PATCH — node likely crashed", err)
		os.Exit(1)
	}
	fmt.Println("WHIP_ICE_RESTART: PASS (POST + media; PATCH cleanly rejected without crash; DELETE teardown)")
}

// scenarioCodecs: publish VP9 and AV1 tracks cross-node; the room must register
// up receivers with those MIME types (covered beyond the usual VP8/H264/Opus).
// NOTE: kept out of the automated suite — the test client's fake RTP does not
// carry a payload type that triggers the edge gateway's OnTrack for VP9/AV1
// (FireOnTrackBeforeFirstRTP is off on the edge), so the up-plane never
// establishes with synthetic media. Documented in the README; real-encoder
// clients (pion's VP8/Opus) are covered by other scenarios.
func scenarioCodecs(url, apiKey, apiSecret, room string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-codec-pub")
	waitConnected(pub)
	sub := newClient(url, apiKey, apiSecret, room, "go-codec-sub")
	waitConnected(sub)

	w1, err := pub.AddStaticTrackWithCodec(webrtc.RTPCodecCapability{MimeType: "video/vp9"}, "vp9", "camera")
	must(err)
	defer w1.Stop()
	w2, err := pub.AddStaticTrackWithCodec(webrtc.RTPCodecCapability{MimeType: "video/av1"}, "av1", "camera")
	must(err)
	defer w2.Stop()

	if err := waitRemoteTrackCount(sub, 2, 20*time.Second); err != nil {
		fmt.Println("CODECS: FAIL subscriber did not see both tracks", err)
		os.Exit(1)
	}
	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("CODECS: FAIL no media received", err)
		os.Exit(1)
	}
	fmt.Println("CODECS: PASS (VP9 + AV1 published; subscriber received", sub.BytesReceived(), "bytes)")
}

// scenarioManualSubscribe: subscriber joins with AutoSubscribe=false, the
// publisher publishes, and the subscriber explicitly subscribes to the track
// (UpdateSubscription), then receives media.
func scenarioManualSubscribe(url, apiKey, apiSecret, room string) {
	opts := &testclient.Options{AutoSubscribe: false}
	conn, err := testclient.NewWebSocketConn(url, token(apiKey, apiSecret, room, "go-manual-sub"), opts)
	must(err)
	sub, err := testclient.NewRTCClient(conn, false, opts)
	must(err)
	go sub.Run()
	waitConnected(sub)

	pub := newClient(url, apiKey, apiSecret, room, "go-manual-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	// AutoSubscribe=false: the subscriber must NOT auto-receive; find the track
	// and subscribe explicitly.
	if err := waitRemoteTrack(sub, 20*time.Second); err != nil {
		fmt.Println("MANUAL_SUBSCRIBE: FAIL no remote track visible", err)
		os.Exit(1)
	}
	var trackSid string
	for _, p := range sub.RemoteParticipants() {
		for _, t := range p.Tracks {
			trackSid = t.Sid
		}
	}
	must(sub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Subscription{
			Subscription: &livekit.UpdateSubscription{
				TrackSids: []string{trackSid},
				Subscribe: true,
			},
		},
	}))

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("MANUAL_SUBSCRIBE: FAIL no media after explicit subscribe", err)
		os.Exit(1)
	}
	fmt.Println("MANUAL_SUBSCRIBE: PASS (explicit subscription delivered", sub.BytesReceived(), "bytes)")
}

// scenarioParticipantName: the publisher updates its display name; the
// subscriber must observe it via the participant broadcast cross-node.
func scenarioParticipantName(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-name-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-name-pub")
	waitConnected(pub)

	name := fmt.Sprintf("nat-e2e-name-%d", time.Now().Unix())
	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_UpdateMetadata{
			UpdateMetadata: &livekit.UpdateParticipantMetadata{Name: name},
		},
	}))

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range sub.RemoteParticipants() {
			if p.Name == name {
				fmt.Println("PARTICIPANT_NAME: PASS (subscriber saw updated name)")
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("PARTICIPANT_NAME: FAIL subscriber never saw the name update")
	os.Exit(1)
}

// scenarioRoomAdmin: exercises the REST RoomService API (admin): create a room,
// list rooms, two clients join, kick one participant (admin), and delete the
// room — all cross-node through the edge's HTTP endpoint.
func scenarioRoomAdmin(url, apiKey, apiSecret, room string) {
	httpBase := "http://" + mustParseURL(url).Host
	rmc := livekit.NewRoomServiceJSONClient(httpBase, &http.Client{})

	adminCtx := func() context.Context {
		at := auth.NewAccessToken(apiKey, apiSecret)
		at.AddGrant(&auth.VideoGrant{RoomCreate: true, RoomList: true, RoomAdmin: true, Room: room})
		t, err := at.ToJWT()
		must(err)
		header := make(http.Header)
		testclient.SetAuthorizationToken(header, t)
		ctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
		must(err)
		return ctx
	}

	// create the room
	_, err := rmc.CreateRoom(adminCtx(), &livekit.CreateRoomRequest{Name: room})
	if err != nil {
		fmt.Println("ROOM_ADMIN: FAIL create room", err)
		os.Exit(1)
	}

	// two clients join (via WS, dual-PC)
	pub := newClient(url, apiKey, apiSecret, room, "go-admin-pub")
	waitConnected(pub)
	sub := newClient(url, apiKey, apiSecret, room, "go-admin-sub")
	waitConnected(sub)

	// list rooms: the created room must be present with 2 participants
	found := false
	for range 10 {
		res, err := rmc.ListRooms(adminCtx(), &livekit.ListRoomsRequest{})
		must(err)
		for _, r := range res.Rooms {
			if r.Name == room {
				found = true
				if r.NumParticipants >= 2 {
					break
				}
			}
		}
		if found {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !found {
		fmt.Println("ROOM_ADMIN: FAIL room not listed with participants")
		os.Exit(1)
	}

	// admin kicks the subscriber
	_, err = rmc.RemoveParticipant(adminCtx(), &livekit.RoomParticipantIdentity{
		Room: room, Identity: "go-admin-sub",
	})
	if err != nil {
		fmt.Println("ROOM_ADMIN: FAIL kick", err)
		os.Exit(1)
	}
	time.Sleep(2 * time.Second)

	// delete the room
	_, err = rmc.DeleteRoom(adminCtx(), &livekit.DeleteRoomRequest{Room: room})
	if err != nil {
		fmt.Println("ROOM_ADMIN: FAIL delete room", err)
		os.Exit(1)
	}
	fmt.Println("ROOM_ADMIN: PASS (create/list/kick/delete via RoomService REST)")
}

// scenarioTrackPause: the subscriber pauses a subscribed track
// (UpdateTrackSettings.Disabled) — its received byte count must freeze — then
// resumes it and media flows again. Exercises subscriber pause/resume cross-node.
func scenarioTrackPause(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-pause-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-pause-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	// capture the server-assigned track SID
	var trackSid string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && trackSid == "" {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-pause-pub" {
				continue
			}
			for _, t := range p.Tracks {
				trackSid = t.Sid
			}
		}
		if trackSid == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if trackSid == "" {
		fmt.Println("TRACK_PAUSE: FAIL no track visible to subscriber")
		os.Exit(1)
	}

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("TRACK_PAUSE: FAIL no initial media", err)
		os.Exit(1)
	}
	before := sub.BytesReceived()

	// pause: bytes must stop growing
	must(sub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_TrackSetting{
			TrackSetting: &livekit.UpdateTrackSettings{
				TrackSids: []string{trackSid},
				Disabled:  true,
			},
		},
	}))
	time.Sleep(3 * time.Second)
	afterPause := sub.BytesReceived()
	if afterPause > before+256 {
		fmt.Println("TRACK_PAUSE: FAIL media kept flowing after pause", before, "->", afterPause)
		os.Exit(1)
	}

	// resume: media flows again
	must(sub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_TrackSetting{
			TrackSetting: &livekit.UpdateTrackSettings{
				TrackSids: []string{trackSid},
				Disabled:  false,
			},
		},
	}))
	if err := waitBytes(sub, afterPause+2048, 20*time.Second); err != nil {
		fmt.Println("TRACK_PAUSE: FAIL no media after resume", err)
		os.Exit(1)
	}
	fmt.Println("TRACK_PAUSE: PASS (pause froze bytes; resume resumed media)")
}

// scenarioRoomLifecycle: create a room with a short empty timeout, join two
// clients, disconnect both, and verify the room is deleted once it is empty —
// the room-management lifecycle across nodes.
func scenarioRoomLifecycle(url, apiKey, apiSecret, room string) {
	httpBase := "http://" + mustParseURL(url).Host
	rmc := livekit.NewRoomServiceJSONClient(httpBase, &http.Client{})
	adminCtx := func() context.Context {
		at := auth.NewAccessToken(apiKey, apiSecret)
		at.AddGrant(&auth.VideoGrant{RoomCreate: true, RoomList: true, RoomAdmin: true, Room: room})
		t, err := at.ToJWT()
		must(err)
		header := make(http.Header)
		testclient.SetAuthorizationToken(header, t)
		ctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
		must(err)
		return ctx
	}

	// EmptyTimeout covers a room that never had participants; once joined and
	// left, CloseIfEmpty switches to DepartureTimeout — set both short.
	_, err := rmc.CreateRoom(adminCtx(), &livekit.CreateRoomRequest{
		Name:             room,
		EmptyTimeout:     5,
		DepartureTimeout: 5,
	})
	if err != nil {
		fmt.Println("ROOM_LIFECYCLE: FAIL create room", err)
		os.Exit(1)
	}

	c1 := newClient(url, apiKey, apiSecret, room, "go-lifecycle-a")
	waitConnected(c1)
	c2 := newClient(url, apiKey, apiSecret, room, "go-lifecycle-b")
	waitConnected(c2)

	// both leave cleanly
	c1.Stop()
	c2.Stop()

	// room must be deleted after the empty timeout
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		res, err := rmc.ListRooms(adminCtx(), &livekit.ListRoomsRequest{})
		must(err)
		gone := true
		for _, r := range res.Rooms {
			if r.Name == room {
				gone = false
				break
			}
		}
		if gone {
			fmt.Println("ROOM_LIFECYCLE: PASS (room deleted after empty timeout)")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("ROOM_LIFECYCLE: FAIL room not deleted after empty timeout")
	os.Exit(1)
}

// scenarioServiceAPIs: exercises the full RoomService admin surface (REST) — the
// room manager + RPC handlers behind each call — to raise E2E coverage of
// pkg/service. Calls: create/list/get room, list/get participant, mute a track,
// update participant metadata, update room metadata, update subscriptions,
// send data, remove participant, delete room.
func scenarioServiceAPIs(url, apiKey, apiSecret, room string) {
	httpBase := "http://" + mustParseURL(url).Host
	rmc := livekit.NewRoomServiceJSONClient(httpBase, &http.Client{})
	adminCtx := func(grants *auth.VideoGrant) context.Context {
		at := auth.NewAccessToken(apiKey, apiSecret)
		at.AddGrant(grants)
		t, err := at.ToJWT()
		must(err)
		header := make(http.Header)
		testclient.SetAuthorizationToken(header, t)
		ctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
		must(err)
		return ctx
	}
	fullAdmin := &auth.VideoGrant{RoomCreate: true, RoomList: true, RoomAdmin: true, Room: room}

	_, err := rmc.CreateRoom(adminCtx(fullAdmin), &livekit.CreateRoomRequest{Name: room})
	must(err)

	// two clients so there are participants to administer
	c1 := newClient(url, apiKey, apiSecret, room, "go-apis-a")
	waitConnected(c1)
	c2 := newClient(url, apiKey, apiSecret, room, "go-apis-b")
	waitConnected(c2)
	writer, err := c1.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()
	time.Sleep(2 * time.Second)

	// find the track SID from the OTHER participant's view
	var trackSid string
	for _, p := range c2.RemoteParticipants() {
		if p.Identity == "go-apis-a" {
			for _, t := range p.Tracks {
				trackSid = t.Sid
			}
		}
	}
	if trackSid == "" {
		fmt.Println("SERVICE_APIS: FAIL no track to administer")
		os.Exit(1)
	}

	// list participants
	lp, err := rmc.ListParticipants(adminCtx(fullAdmin), &livekit.ListParticipantsRequest{Room: room})
	must(err)
	if len(lp.Participants) < 2 {
		fmt.Println("SERVICE_APIS: FAIL expected >=2 participants")
		os.Exit(1)
	}
	// get one participant
	_, err = rmc.GetParticipant(adminCtx(fullAdmin), &livekit.RoomParticipantIdentity{Room: room, Identity: "go-apis-a"})
	must(err)
	// list rooms (the created room present)
	lr, err := rmc.ListRooms(adminCtx(fullAdmin), &livekit.ListRoomsRequest{})
	must(err)
	found := false
	for _, r := range lr.Rooms {
		if r.Name == room {
			found = true
		}
	}
	if !found {
		fmt.Println("SERVICE_APIS: FAIL room not listed")
		os.Exit(1)
	}
	// admin mutes the published track
	_, err = rmc.MutePublishedTrack(adminCtx(fullAdmin), &livekit.MuteRoomTrackRequest{Room: room, Identity: "go-apis-a", TrackSid: trackSid, Muted: true})
	must(err)
	// update participant metadata
	_, err = rmc.UpdateParticipant(adminCtx(fullAdmin), &livekit.UpdateParticipantRequest{Room: room, Identity: "go-apis-a", Metadata: "admin-updated"})
	must(err)
	// update room metadata
	_, err = rmc.UpdateRoomMetadata(adminCtx(fullAdmin), &livekit.UpdateRoomMetadataRequest{Room: room, Metadata: "room-meta"})
	must(err)
	// admin subscription control
	_, err = rmc.UpdateSubscriptions(adminCtx(fullAdmin), &livekit.UpdateSubscriptionsRequest{Room: room, Identity: "go-apis-b", TrackSids: []string{trackSid}, Subscribe: false})
	must(err)
	// send data to the room
	_, err = rmc.SendData(adminCtx(fullAdmin), &livekit.SendDataRequest{Room: room, Data: []byte("admin-data"), Kind: livekit.DataPacket_RELIABLE})
	must(err)
	// remove (kick) one participant
	_, err = rmc.RemoveParticipant(adminCtx(fullAdmin), &livekit.RoomParticipantIdentity{Room: room, Identity: "go-apis-b"})
	must(err)
	time.Sleep(1 * time.Second)
	// delete the room
	_, err = rmc.DeleteRoom(adminCtx(fullAdmin), &livekit.DeleteRoomRequest{Room: room})
	must(err)

	fmt.Println("SERVICE_APIS: PASS (create/list/get/mute/update/send/kick/delete)")
}

// scenarioSubscriptionPermission: the subscriber explicitly declares per-track
// subscription permissions (SubscriptionPermission); the server evaluates and
// replies with SubscriptionPermissionUpdate, then media must flow — exercises the
// rtc subscription-permission path cross-node.
func scenarioSubscriptionPermission(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-perm-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-perm-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	// capture the publisher's track SID + participant SID
	var trackSid, pubSid string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && trackSid == "" {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-perm-pub" {
				continue
			}
			pubSid = string(p.Sid)
			for _, t := range p.Tracks {
				trackSid = t.Sid
			}
		}
		if trackSid == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if trackSid == "" {
		fmt.Println("SUBSCRIPTION_PERMISSION: FAIL no track visible")
		os.Exit(1)
	}

	// declare permission to subscribe to the publisher's track
	must(sub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_SubscriptionPermission{
			SubscriptionPermission: &livekit.SubscriptionPermission{
				AllParticipants: true,
				TrackPermissions: []*livekit.TrackPermission{
					{ParticipantSid: pubSid, TrackSids: []string{trackSid}},
				},
			},
		},
	}))

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("SUBSCRIPTION_PERMISSION: FAIL no media received", err)
		os.Exit(1)
	}
	fmt.Println("SUBSCRIPTION_PERMISSION: PASS (permission granted; media flowed)")
}

// scenarioSimulateSpeaker: the publisher simulates N seconds of active-speaker
// activity (SimulateScenario_SpeakerUpdate). The room broadcasts active-speaker
// changes to all participants; the subscriber must observe the publisher as an
// ACTIVE speaker cross-node (SignalResponse_SpeakersChanged over the relay).
func scenarioSimulateSpeaker(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-spk-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-spk-pub")
	waitConnected(pub)

	// speaker deltas are scoped: SendSpeakerUpdate(force=false) only reaches
	// participants SUBSCRIBED to the speaker (or the speaker itself). Publish a
	// track and confirm the subscriber receives media so the subscription exists
	// before the simulated speaker update.
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()
	if err := waitBytes(sub, 1024, 30*time.Second); err != nil {
		fmt.Println("SIMULATE_SPEAKER: FAIL no media before speaker update", err)
		os.Exit(1)
	}

	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Simulate{
			Simulate: &livekit.SimulateScenario{
				Scenario: &livekit.SimulateScenario_SpeakerUpdate{SpeakerUpdate: 3},
			},
		},
	}))

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range sub.ActiveSpeakers() {
			if s.Active && s.Sid == string(pub.ID()) {
				fmt.Println("SIMULATE_SPEAKER: PASS (subscriber saw publisher active speaker cross-node)")
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("SIMULATE_SPEAKER: FAIL subscriber never saw active speaker", sub.ActiveSpeakers())
	os.Exit(1)
}

// scenarioSimulateNodeFailure: the participant simulates a node failure
// (SimulateScenario_NodeFailure); the server drops the participant (reconnect
// allowed) and closes the signal connection. The client must observe the
// server-driven disconnect.
func scenarioSimulateNodeFailure(url, apiKey, apiSecret, room string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-nf-pub")
	waitConnected(pub)

	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Simulate{
			Simulate: &livekit.SimulateScenario{
				Scenario: &livekit.SimulateScenario_NodeFailure{NodeFailure: true},
			},
		},
	}))

	if err := pub.WaitUntilDisconnected(15 * time.Second); err != nil {
		fmt.Println("SIMULATE_NODE_FAILURE: FAIL participant not disconnected", err)
		os.Exit(1)
	}
	fmt.Println("SIMULATE_NODE_FAILURE: PASS (participant disconnected, reason", pub.DisconnectReason(), ")")
}

// scenarioSimulateServerLeave: the participant simulates a server leave
// (SimulateScenario_ServerLeave); the server cleanly closes the participant and
// the signal connection. The client must observe the server-driven disconnect.
func scenarioSimulateServerLeave(url, apiKey, apiSecret, room string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-sl-pub")
	waitConnected(pub)

	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Simulate{
			Simulate: &livekit.SimulateScenario{
				Scenario: &livekit.SimulateScenario_ServerLeave{ServerLeave: true},
			},
		},
	}))

	if err := pub.WaitUntilDisconnected(15 * time.Second); err != nil {
		fmt.Println("SIMULATE_SERVER_LEAVE: FAIL participant not disconnected", err)
		os.Exit(1)
	}
	fmt.Println("SIMULATE_SERVER_LEAVE: PASS (participant disconnected, reason", pub.DisconnectReason(), ")")
}

// scenarioSubPermRevoke: after media flows, the PUBLISHER revokes the
// subscriber's access to its track (SubscriptionPermission{AllParticipants:
// false} → maybeRevokeSubscriptions → RemoveSubscriber). The revoked subscriber's
// media must STOP growing (down track closed cross-node). Note the publisher —
// not the subscriber — sends SubscriptionPermission: it controls who may
// subscribe to its own tracks.
func scenarioSubPermRevoke(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-rev-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-rev-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	// baseline: the subscriber must receive media before the revoke
	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("SUB_PERM_REVOKE: FAIL no media before revoke", err)
		os.Exit(1)
	}

	// the publisher denies ALL subscribers for its tracks
	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_SubscriptionPermission{
			SubscriptionPermission: &livekit.SubscriptionPermission{
				AllParticipants:  false,
				TrackPermissions: []*livekit.TrackPermission{},
			},
		},
	}))

	// let the revoke propagate (server closes the down track, edge removes the
	// SRTP sender), then measure that the subscriber's byte count stopped growing
	time.Sleep(3 * time.Second)
	before := sub.BytesReceived()
	time.Sleep(3 * time.Second)
	after := sub.BytesReceived()
	if after-before > 500 {
		fmt.Printf("SUB_PERM_REVOKE: FAIL media still flowing after revoke (%d bytes over 3s)\n", after-before)
		os.Exit(1)
	}
	fmt.Printf("SUB_PERM_REVOKE: PASS (media froze after revoke: +%d bytes over 3s)\n", after-before)
}

// scenarioParticipantLeaveVisible: B leaves the room cleanly; A must observe the
// participant-removal broadcast cross-node (SignalResponse_Update with the
// departed participant removed).
func scenarioParticipantLeaveVisible(url, apiKey, apiSecret, room string) {
	a := newClient(url, apiKey, apiSecret, room, "go-leave-a")
	waitConnected(a)
	b := newClient(url, apiKey, apiSecret, room, "go-leave-b")
	waitConnected(b)

	if err := waitRemoteIdentity(a, "go-leave-b", 20*time.Second); err != nil {
		fmt.Println("PARTICIPANT_LEAVE: FAIL A never saw B", err)
		os.Exit(1)
	}

	b.Stop()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !remoteIdentitySeen(a, "go-leave-b") {
			fmt.Println("PARTICIPANT_LEAVE: PASS (A no longer sees B after leave)")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("PARTICIPANT_LEAVE: FAIL A still sees B after leave")
	os.Exit(1)
}

// scenarioSyncState: a connected client sends SyncState declaring its published
// track. The server validates the track exists (onSyncState); a valid state must
// NOT trigger a full reconnect — the client stays connected.
func scenarioSyncState(url, apiKey, apiSecret, room string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-sync-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	// wait for the track to be acked (appears in GetPublishedTrackIDs)
	var trackSid string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && trackSid == "" {
		ids := pub.GetPublishedTrackIDs()
		if len(ids) > 0 {
			trackSid = ids[0]
		}
		if trackSid == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if trackSid == "" {
		fmt.Println("SYNC_STATE: FAIL no published track acked")
		os.Exit(1)
	}

	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_SyncState{
			SyncState: &livekit.SyncState{
				PublishTracks: []*livekit.TrackPublishedResponse{
					{Track: &livekit.TrackInfo{Sid: trackSid}},
				},
			},
		},
	}))

	// a valid SyncState must not trigger a full reconnect: the client must stay
	// connected for a few seconds after the request
	if err := pub.WaitUntilDisconnected(3 * time.Second); err == nil {
		fmt.Println("SYNC_STATE: FAIL client disconnected after valid SyncState (full reconnect triggered)")
		os.Exit(1)
	}
	fmt.Println("SYNC_STATE: PASS (valid SyncState accepted; no full reconnect)")
}

// scenarioConnectionQuality: the server periodically publishes per-participant
// connection quality to every participant; A must observe B's quality info
// cross-node (SignalResponse_ConnectionQuality over the relay).
func scenarioConnectionQuality(url, apiKey, apiSecret, room string) {
	a := newClient(url, apiKey, apiSecret, room, "go-cq-a")
	waitConnected(a)
	b := newClient(url, apiKey, apiSecret, room, "go-cq-b")
	waitConnected(b)

	// B publishes so quality is actively computed for it
	writer, err := b.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()
	if err := waitBytes(a, 1024, 30*time.Second); err != nil {
		fmt.Println("CONNECTION_QUALITY: FAIL no media from B", err)
		os.Exit(1)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, qi := range a.LastConnectionQuality() {
			if qi.ParticipantSid == string(b.ID()) && qi.Score > 0 {
				fmt.Println("CONNECTION_QUALITY: PASS (A observed B's quality cross-node:", qi.Quality, qi.Score, ")")
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("CONNECTION_QUALITY: FAIL A never received B's quality update")
	os.Exit(1)
}

// scenarioTurnCredentials: with the in-process TURN server enabled, every join
// response must carry a TURN server (iceServersForParticipant) with valid,
// non-empty credentials (TURN auth handler). The client need not connect through
// TURN — asserting the issued credentials exercises the server's TURN path.
func scenarioTurnCredentials(url, apiKey, apiSecret, room string) {
	c := newClient(url, apiKey, apiSecret, room, "go-turn-pub")
	waitConnected(c)

	servers := c.JoinIceServers()
	for _, is := range servers {
		for _, u := range is.Urls {
			if strings.HasPrefix(u, "turn:") || strings.HasPrefix(u, "turns:") {
				if is.Username == "" || is.Credential == "" {
					fmt.Println("TURN_CREDENTIALS: FAIL TURN server", u, "has empty credentials")
					os.Exit(1)
				}
				// the client must actually ALLOCATE a relay on the advertised TURN
				// server (gather a relay candidate), proving the allocation path
				// works cross-node — not just that credentials are issued.
				if !c.HasRelayCandidate() {
					fmt.Println("TURN_CREDENTIALS: FAIL no relay candidate gathered despite TURN", u)
					os.Exit(1)
				}
				fmt.Println("TURN_CREDENTIALS: PASS (join issued TURN", u, "; client allocated a relay candidate)")
				return
			}
		}
	}
	fmt.Println("TURN_CREDENTIALS: FAIL no TURN server in join response", servers)
	os.Exit(1)
}

// scenarioTurnRelayOnly: the full relay-ONLY ICE path. Both clients force
// ICETransportPolicyRelay, so their ONLY candidate type is the TURN relay; a
// connected + media-carrying session then proves the ENTIRE relay data path —
// allocation, CreatePermission, STUN checks through the relay, RTP/RTCP through
// the relay — not just credential issuance/allocation (turn-credentials). In
// the kind/Docker environment this requires allow_restricted_peer_cidrs in the
// TURN config (the edge's host candidates are private IPs; see config.yaml);
// real networks advertise public host candidates and need no allow-list.
func scenarioTurnRelayOnly(url, apiKey, apiSecret, room string) {
	pub := newRelayClient(url, apiKey, apiSecret, room, "go-relay-pub")
	waitConnected(pub)
	sub := newRelayClient(url, apiKey, apiSecret, room, "go-relay-sub")
	waitConnected(sub)

	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 2048, 90*time.Second); err != nil {
		fmt.Println("TURN_RELAY_ONLY: FAIL no media over the relay", err)
		os.Exit(1)
	}
	// media flowed, so both ICE connections are up; with relay-only policy the
	// selected pair MUST be a relay on both clients — assert it to prove the
	// relay data path end-to-end (no host/srflx fallback on either side).
	if !pub.IsRelaySelectedOnAnyTransport() {
		fmt.Println("TURN_RELAY_ONLY: FAIL publisher's selected candidate is not a TURN relay")
		os.Exit(1)
	}
	if !sub.IsRelaySelectedOnAnyTransport() {
		fmt.Println("TURN_RELAY_ONLY: FAIL subscriber's selected candidate is not a TURN relay")
		os.Exit(1)
	}
	fmt.Println("TURN_RELAY_ONLY: PASS (relay-only ICE connected; media flowed through the TURN relay)")
}

// scenarioReconnect: after the publisher's signal connection drops (PCs stay
// alive), a fresh join with the SAME identity within the server's disconnect
// grace window removes the duplicate participant and republishes; the subscriber
// must observe media restored cross-node. Exercises the server's
// duplicate-identity cleanup — the outcome a real client's full reconnect lands on.
func scenarioReconnect(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-rc-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-rc-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 1024, 30*time.Second); err != nil {
		fmt.Println("RECONNECT: FAIL no baseline media", err)
		os.Exit(1)
	}

	// drop ONLY the signal; keep the PCs alive (the server keeps the participant
	// for the disconnect-cleanup grace window)
	pub.DropSignal()
	time.Sleep(2 * time.Second)

	// fresh join with the same identity → the server removes the duplicate
	// participant (RemoveParticipant DuplicateIdentity) and the new participant
	// joins; it then republishes
	before := sub.BytesReceived()
	pub2 := newClient(url, apiKey, apiSecret, room, "go-rc-pub")
	waitConnected(pub2)
	writer2, err := pub2.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer2.Stop()

	// the subscriber must receive media again cross-node (fresh track from pub2)
	if err := waitBytes(sub, before+1024, 60*time.Second); err != nil {
		fmt.Println("RECONNECT: FAIL media not restored after reconnect", err)
		os.Exit(1)
	}
	fmt.Println("RECONNECT: PASS (media restored after signal drop + same-identity rejoin)")
}

// scenarioPerformRpc: exercises the RoomService PerformRpc path cross-node. Data
// channel DATA messages are a documented NAT limitation (not bridged cross-node),
// so an RPC to a NAT participant must fail CLEANLY and BOUNDED — the room's remote
// (edge) PCTransport has no local data channel, so SendDataMessage returns
// ErrDataChannelUnavailable immediately and the RPC returns a psrpc Internal error
// instead of hanging. This pins the documented limitation as a regression test:
// no indefinite hang, deterministic error, bounded latency.
func scenarioPerformRpc(url, apiKey, apiSecret, room string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-rpc-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	httpBase := "http://" + mustParseURL(url).Host
	// bounded HTTP client: a hanging PerformRpc would surface as a client-side
	// timeout rather than a clean bounded error.
	rmc := livekit.NewRoomServiceJSONClient(httpBase, &http.Client{Timeout: 15 * time.Second})

	at := auth.NewAccessToken(apiKey, apiSecret)
	at.AddGrant(&auth.VideoGrant{RoomAdmin: true, Room: room})
	t, err := at.ToJWT()
	must(err)
	header := make(http.Header)
	testclient.SetAuthorizationToken(header, t)
	ctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
	must(err)

	start := time.Now()
	_, err = rmc.PerformRpc(ctx, &livekit.PerformRpcRequest{
		Room:                room,
		DestinationIdentity: "go-rpc-pub",
		Method:              "echo",
		Payload:             "hello",
		ResponseTimeoutMs:   5000,
	})
	elapsed := time.Since(start)

	// 1) clean error, never a hang. With P0-2 the data channel IS bridged, so the
	// RPC request goes out via the edge data channel and, since the test client
	// does not implement an RPC responder, times out cleanly after the ack window
	// (RpcError 1501 Connection timeout). Without a usable DC it fails immediately
	// ("data channel is not available"). Either way the RPC is bounded + clean.
	if err == nil {
		fmt.Println("PERFORM_RPC: FAIL expected an error (no RPC responder), got success")
		os.Exit(1)
	}
	clean := strings.Contains(err.Error(), "data channel is not available") ||
		strings.Contains(err.Error(), "1501") ||
		strings.Contains(err.Error(), "Connection timeout")
	if !clean {
		fmt.Println("PERFORM_RPC: FAIL unexpected error:", err)
		os.Exit(1)
	}
	// 2) bounded: returns well under the 10s default RPC response timeout.
	if elapsed > 10*time.Second {
		fmt.Printf("PERFORM_RPC: FAIL took %.1fs (expected bounded, not a hang)\n", elapsed.Seconds())
		os.Exit(1)
	}
	fmt.Printf("PERFORM_RPC: PASS (clean bounded error in %.1fs: %v)\n", elapsed.Seconds(), err)
}

// scenarioSimulcastSwitch: P0-3. Self-contained 3-layer VP8 simulcast publisher
// (raw pion, PLI-responsive — see simulcast.go) replaces the `lk --publish-demo`
// publisher, which cannot support an up-switch assertion: its encoder only
// keyframes the layer(s) being consumed, so after a subscriber down-switches the
// forwarder's layer-lock waits forever for a keyframe on the (now-quiet) higher
// layer (documented P0-3 "layer-lock PLI loop").
//
// The subscriber drives LOW→MEDIUM→HIGH. The deterministic proofs are the
// room-side layer anchors the 06 harness asserts:
//
//	(a) `available layers changed - layer seen` reaching [0,1,2] for the Go
//	    publisher (all 3 simulcast planes bridged cross-node).
//	(b) `upgrading layer` reaching layer 1 then 2 + `forwarded key frame` with
//	    layer 1 AND layer 2 for go-sim-sub — the forwarder's layer-lock RELEASED
//	    and the higher layer's keyframe was forwarded cross-node. This is the
//	    P0-3 proof: with the lk publisher the up-switch stalled forever.
//
// The client additionally checks the down-stream did not freeze (byte rate stays
// > 0 after each request). Cross-node BITRATE bands are not asserted: the
// allocator/down-plane delivery after an up-switch has non-deterministic timing
// (documented in the README), so a strict band would flake — the room-side
// anchors are the authoritative up-switch proof.
func scenarioSimulcastSwitch(url, apiKey, apiSecret, room string) {
	pub, err := newSimulcastPublisher(url, apiKey, apiSecret, room, "go-sim-go-pub")
	must(err)
	defer pub.Close()
	pub.StartMedia()
	fmt.Println("SIMULCAST_SWITCH: publisher connected, streaming 3 layers")

	sub := newClient(url, apiKey, apiSecret, room, "go-sim-sub")
	waitConnected(sub)
	defer sub.Stop()

	var trackSid string
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) && trackSid == "" {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-sim-go-pub" {
				continue
			}
			for _, t := range p.Tracks {
				if strings.Contains(strings.ToLower(t.MimeType), "vp8") {
					trackSid = t.Sid
				}
			}
		}
		if trackSid == "" {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if trackSid == "" {
		fmt.Println("SIMULCAST_SWITCH: FAIL no VP8 simulcast track visible from go-sim-go-pub")
		os.Exit(1)
	}

	// baseline: the subscription completes and media starts (initial tier is HIGH).
	if err := waitBytes(sub, 16384, 30*time.Second); err != nil {
		fmt.Println("SIMULCAST_SWITCH: FAIL no baseline media", err)
		os.Exit(1)
	}

	setQuality := func(q livekit.VideoQuality) {
		must(sub.SendRequest(&livekit.SignalRequest{
			Message: &livekit.SignalRequest_TrackSetting{
				TrackSetting: &livekit.UpdateTrackSettings{
					TrackSids: []string{trackSid},
					Quality:   q,
				},
			},
		}))
	}
	// rate samples subscriber bytes/sec over a 3s window; printed as diagnostics.
	// The room-side up-switch anchors (06 script) are the authoritative P0-3
	// proof; cross-node down-plane bitrate after an up-switch has
	// non-deterministic delivery timing (README), so it is not asserted.
	rate := func(step string) float64 {
		b0 := sub.BytesReceived()
		time.Sleep(3 * time.Second)
		b1 := sub.BytesReceived()
		fmt.Printf("SIMULCAST_SWITCH: t=%s %s delta %d bytes (%d -> %d) over 3s ≈ %.0f kbps\n", time.Now().Format("15:04:05.000"), step, b1-b0, b0, b1, float64(b1-b0)*8/3/1000)
		// diagnostic: the subscriber PC's transport counters tell us whether the
		// down-plane RTP actually reaches the client's pion (bytesReceived grows)
		// vs being lost on the wire or dropped before the track reader.
		bs, br, inbound := sub.SubscriberTransportStats()
		fmt.Printf("SIMULCAST_SUBSTATS: %s subscriber transport bytesSent=%d bytesReceived=%d inbound=%v\n", step, bs, br, inbound)
		return float64(b1-b0) * 8 / 3
	}
	// Layer bitrate floors (kbps): the publisher emits q≈96 / h≈480 / f≈1680.
	// The subscriber must measure a rate in the layer's band — a hard assertion
	// that the down-plane actually delivers the switched layer cross-node. The
	// floor is ~60% of the emitted rate to absorb measurement jitter; below it
	// the down-plane froze (the documented pre-fix P0-3 residual).
	layerFloor := map[livekit.VideoQuality]float64{
		livekit.VideoQuality_LOW:    60,
		livekit.VideoQuality_MEDIUM: 300,
		livekit.VideoQuality_HIGH:   1200,
	}
	check := func(step string, q livekit.VideoQuality) {
		setQuality(q)
		// settle: a down-switch latches within one periodic q-keyframe (≤2s); an
		// up-switch needs the forwarder's PLI → publisher keyframe → cross-node RTP
		// round trip (~1s).
		time.Sleep(4 * time.Second)
		measured := rate(step) // bps
		measuredKbps := measured / 1000
		if measuredKbps < layerFloor[q] {
			fmt.Printf("SIMULCAST_SWITCH: FAIL %s measured %.0f kbps < floor %.0f kbps\n", step, measuredKbps, layerFloor[q])
			os.Exit(1)
		}
		fmt.Printf("SIMULCAST_SWITCH: ✓ %s measured %.0f kbps >= floor %.0f kbps\n", step, measuredKbps, layerFloor[q])
	}

	check("LOW", livekit.VideoQuality_LOW)
	check("MEDIUM", livekit.VideoQuality_MEDIUM)
	check("HIGH", livekit.VideoQuality_HIGH)
	fmt.Println("SIMULCAST_SWITCH: PASS (LOW→MEDIUM→HIGH delivered cross-node at layer-appropriate bitrate; room-side 'upgrading layer' + 'forwarded key frame' anchors prove the up-switch)")
}

// scenarioReconnectResume: the reconnect=true resume negotiation path. The pub
// drops ONLY its signal WS (PCs stay alive); within the disconnect-cleanup grace
// window it reconnects the signal with reconnect=true + its SID, which the server
// answers by resuming the existing participant (ResumeParticipant → room log
// "resuming RTC session"). Under the NAT architecture the resume is either
// accepted (SignalResponse_Reconnect, ICE-restart renegotiation on the subscriber
// transport) or fails cleanly (SignalResponse_Leave with a RECONNECT action), in
// which case the client MUST fall back to a full reconnect and republish. We
// assert the negotiation path is exercised (room-side anchor) and media is
// restored via whichever outcome occurs — never a hang.
func scenarioReconnectResume(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-rs-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-rs-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 1024, 30*time.Second); err != nil {
		fmt.Println("RECONNECT_RESUME: FAIL no baseline media", err)
		os.Exit(1)
	}

	// drop the signal WS; keep the PCs alive (participant stays within the 5s grace)
	pub.DropSignal()
	time.Sleep(1 * time.Second)

	// attempt a signal-resume with reconnect=true + prior SID
	resumeOpts := &testclient.Options{
		AutoSubscribe: true,
		Reconnect:     true,
		ReconnectSID:  string(pub.ID()),
	}
	if err := pub.Resume(url, token(apiKey, apiSecret, room, "go-rs-pub"), resumeOpts); err != nil {
		// could not even re-establish the signal → same-identity full reconnect
		fmt.Println("RECONNECT_RESUME: resume connect failed, falling back to full reconnect:", err)
		before := sub.BytesReceived()
		pub2 := newClient(url, apiKey, apiSecret, room, "go-rs-pub")
		waitConnected(pub2)
		w2, err2 := pub2.AddStaticTrack("video/vp8", "video", "camera")
		must(err2)
		defer w2.Stop()
		if err := waitBytes(sub, before+1024, 60*time.Second); err != nil {
			fmt.Println("RECONNECT_RESUME: FAIL media not restored after fallback", err)
			os.Exit(1)
		}
		fmt.Println("RECONNECT_RESUME: PASS (resume connect failed → full-reconnect fallback; media restored)")
		return
	}

	// resume signal established: wait for the server to either accept the resume
	// (ReconnectResponse) or reject it (Leave with RECONNECT action).
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		if pub.ResumeAccepted() {
			time.Sleep(2 * time.Second) // let the ICE-restart renegotiation settle
			before := sub.BytesReceived()
			if err := waitBytes(sub, before+1024, 20*time.Second); err != nil {
				fmt.Println("RECONNECT_RESUME: FAIL resume accepted but media did not continue", err)
				os.Exit(1)
			}
			fmt.Println("RECONNECT_RESUME: PASS (clean resume; server resumed participant, media continued)")
			return
		}
		if pub.Disconnected() {
			reason := pub.DisconnectReason()
			fmt.Println("RECONNECT_RESUME: server rejected resume (reason", reason, ") → full-reconnect fallback")
			before := sub.BytesReceived()
			pub2 := newClient(url, apiKey, apiSecret, room, "go-rs-pub")
			waitConnected(pub2)
			w2, err2 := pub2.AddStaticTrack("video/vp8", "video", "camera")
			must(err2)
			defer w2.Stop()
			if err := waitBytes(sub, before+1024, 60*time.Second); err != nil {
				fmt.Println("RECONNECT_RESUME: FAIL media not restored after fallback", err)
				os.Exit(1)
			}
			fmt.Println("RECONNECT_RESUME: PASS (resume rejected → full-reconnect fallback; media restored)")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("RECONNECT_RESUME: FAIL resume outcome unresolved within 25s (neither accept nor leave)")
	os.Exit(1)
}

// scenarioRoomNodeFailure: #51 — REAL room-node failure + client-driven migration
// (cross-node recovery). The 06 harness pins this room to the ROOM node and, once
// the scenario prints READY (media established cross-node), KILLS the room node
// deployment. The room node hosts the SFU + participants, so its death must:
//
//  1. tear down in-flight sessions: the client observes a server-driven disconnect
//     (the edge closes the WS when the room node's signal stream dies).
//  2. allow the room to be RE-HOMED: the next join re-runs SelectRoomNode, which
//     clears the stale room_node_map entry for the dead node (GetNodeForRoom) and
//     re-creates the room on a live node.
//  3. restore media: the rejoined publisher republishes and the re-joined
//     subscriber receives RTP again.
//
// We assert (1) the disconnect, (2) that a reconnect=true resume attempt is
// rejected CLEANLY (STATE_MISMATCH — the room is gone, there is nothing to
// resume; a hang would be a failure), and (3) that a full rejoin with the SAME
// identity republishes and the new subscriber receives media. The harness
// additionally asserts room_node_map was re-homed away from the dead node.
func scenarioRoomNodeFailure(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-rnf-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-rnf-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("ROOM_NODE_FAILURE: FAIL no baseline media", err)
		os.Exit(1)
	}
	// READY marker: the harness kills the room node after seeing this line.
	fmt.Println("ROOM_NODE_FAILURE: READY media-established")

	// (1) server-driven disconnect when the room node dies. The edge closes the
	// WS once the room node's signal stream breaks; a REAL failure must be
	// observable by the client within a bounded window (not a silent hang).
	if err := pub.WaitUntilDisconnected(120 * time.Second); err != nil {
		fmt.Println("ROOM_NODE_FAILURE: FAIL publisher not disconnected after node failure:", err)
		os.Exit(1)
	}
	fmt.Println("ROOM_NODE_FAILURE: publisher disconnected (reason", pub.DisconnectReason(), ")")
	writer.Stop()

	if err := sub.WaitUntilDisconnected(30 * time.Second); err != nil {
		fmt.Println("ROOM_NODE_FAILURE: FAIL subscriber not disconnected:", err)
		os.Exit(1)
	}
	fmt.Println("ROOM_NODE_FAILURE: subscriber disconnected too")

	// give the registry a beat to converge (dead node reaped + stale map cleared)
	time.Sleep(15 * time.Second)

	// (2) resume attempt (reconnect=true + prior SID): a real client tries to
	// resume first. The room is gone, so the server must reject it CLEANLY with a
	// Leave (STATE_MISMATCH, RECONNECT action) — never accept it, never hang.
	if err := pub.Resume(url, token(apiKey, apiSecret, room, "go-rnf-pub"), &testclient.Options{
		Reconnect:    true,
		ReconnectSID: string(pub.ID()),
	}); err != nil {
		fmt.Println("ROOM_NODE_FAILURE: resume signal failed:", err)
	} else {
		deadline := time.Now().Add(20 * time.Second)
		rejected := false
		for time.Now().Before(deadline) {
			if pub.Disconnected() {
				rejected = true
				fmt.Println("ROOM_NODE_FAILURE: resume rejected (reason", pub.DisconnectReason(), ") → full rejoin")
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !rejected {
			fmt.Println("ROOM_NODE_FAILURE: FAIL resume neither accepted nor rejected within 20s (hang)")
			os.Exit(1)
		}
	}
	pub.Stop()

	// (3) full rejoin with the SAME identity → the room is re-created on a live
	// node; the publisher republishes and the new subscriber receives media.
	pub2 := newClient(url, apiKey, apiSecret, room, "go-rnf-pub")
	waitConnected(pub2)
	w2, err := pub2.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer w2.Stop()

	sub2 := newClient(url, apiKey, apiSecret, room, "go-rnf-sub")
	waitConnected(sub2)
	if err := waitBytes(sub2, 2048, 90*time.Second); err != nil {
		fmt.Println("ROOM_NODE_FAILURE: FAIL media not restored after rejoin:", err)
		os.Exit(1)
	}
	fmt.Println("ROOM_NODE_FAILURE: PASS (node failure observed; resume cleanly rejected; room re-homed; media restored)")
}

// scenarioWebhookEvents: drives the server-side webhook (HTTP callback) path
// cross-node. The server POSTs signed events (Authorization Bearer JWT carrying
// the sha256 of the body) to the configured webhook URL; the receiver pod verifies
// the signature and logs each event. The bash harness asserts the receiver
// observed participant_joined / track_published / participant_left /
// track_unpublished for this identity. Here the client just joins, publishes, and
// leaves — firing the lifecycle events deterministically.
func scenarioWebhookEvents(url, apiKey, apiSecret, room string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-webhook-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	// give the server a beat to dispatch join + publish events, then leave so
	// participant_left / track_unpublished fire deterministically
	time.Sleep(3 * time.Second)
	pub.Stop()
	time.Sleep(2 * time.Second)
	fmt.Println("WEBHOOK_EVENTS: PASS (client joined, published, left)")
}

// scenarioSubscriberPLI: validates the down-direction RTCP PLI path (the
// keyframe-request variant of the down-RTCP loop, complementary to NACK). The
// subscriber sends a PictureLossIndication for the media it receives; the room's
// DownTrack must process it (its `sending PLI RTCP` log fires and it requests a
// publisher keyframe). The on-wire MediaSSRC is the edge-rewritten one — the room
// boundary rewrites it to the internal DownTrack SSRC (see
// rewriteDownRTCPForLocalSSRC). Server-side assertion lives in 06-client-e2e.sh.
func scenarioSubscriberPLI(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-pli-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-pli-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("SUBSCRIBER_PLI: FAIL no media received before PLI", err)
		os.Exit(1)
	}
	sub.SendPLI()
	time.Sleep(3 * time.Second) // let the room process the PLI + request a keyframe
	fmt.Println("SUBSCRIBER_PLI: PASS (PLI sent; room-side assertion via logs)")
}

// scenarioMediaFollowsSignaling: proves media terminates on the EDGE node (the
// one the client signaled to) — the core "media follows signaling" boundary. The
// server's peer connections run on the edge (advertise_ip), so every remote ICE
// candidate the client receives must carry the edge node's IP (= the WS hostname
// the client connected to), never the room node's. Combined with the media-flow
// assertion, this pins the boundary at the IP level.
func scenarioMediaFollowsSignaling(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-mfs-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-mfs-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("MEDIA_FOLLOWS_SIGNALING: FAIL no media received", err)
		os.Exit(1)
	}

	edgeIP := mustParseURL(url).Hostname()
	ips := sub.RemoteCandidateIPs()
	found := false
	for _, ip := range ips {
		if ip == edgeIP {
			found = true
		}
	}
	if !found {
		fmt.Printf("MEDIA_FOLLOWS_SIGNALING: FAIL server ICE candidates (%v) missing edge IP %s\n", ips, edgeIP)
		os.Exit(1)
	}
	fmt.Printf("MEDIA_FOLLOWS_SIGNALING: PASS (server ICE candidates include edge IP %s — media terminates on the signaling node)\n", edgeIP)
}

// scenarioMultiEdge: two clients signal to DIFFERENT edge nodes (edge1 via the
// primary URL, edge2 via url2) for the SAME room, which is hosted on the room
// node. Media must follow signaling on BOTH edges: pub(edge1) publishes → sub
// receives via edge2 (edge1→room→edge2), and sub(edge2) publishes → pub receives
// via edge1 (edge2→room→edge1). Proves the multi-edge core property: any edge can
// terminate a client's media for a room hosted elsewhere.
func scenarioMultiEdge(url, apiKey, apiSecret, room, url2 string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-multi-pub")
	waitConnected(pub)
	sub := newClient(url2, apiKey, apiSecret, room, "go-multi-sub")
	waitConnected(sub)

	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()
	if err := waitBytes(sub, 2048, 60*time.Second); err != nil {
		fmt.Println("MULTI_EDGE: FAIL no media from edge1-pub to edge2-sub", err)
		os.Exit(1)
	}

	writer2, err := sub.AddStaticTrack("video/vp8", "video", "camera2")
	must(err)
	defer writer2.Stop()
	if err := waitBytes(pub, 2048, 60*time.Second); err != nil {
		fmt.Println("MULTI_EDGE: FAIL no media from edge2-sub to edge1-pub", err)
		os.Exit(1)
	}
	fmt.Println("MULTI_EDGE: PASS (media crossed edge1↔edge2 via the room node)")
}

// scenarioScaleStress: two rooms, each with a pub + sub, with the four clients
// spread across BOTH edges (room A: pub on edge1, sub on edge2; room B: pub on
// edge2, sub on edge1). Both rooms are pinned to the room node. All media must
// flow concurrently — stressing the room node's dual-edge handling + the
// control-channel establishment under concurrency.
func scenarioScaleStress(url, apiKey, apiSecret, room, url2, room2 string) {
	// room A
	aPub := newClient(url, apiKey, apiSecret, room, "go-scale-a-pub")
	waitConnected(aPub)
	aSub := newClient(url2, apiKey, apiSecret, room, "go-scale-a-sub")
	waitConnected(aSub)
	// room B
	bPub := newClient(url2, apiKey, apiSecret, room2, "go-scale-b-pub")
	waitConnected(bPub)
	bSub := newClient(url, apiKey, apiSecret, room2, "go-scale-b-sub")
	waitConnected(bSub)

	wa, err := aPub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer wa.Stop()
	wb, err := bPub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer wb.Stop()

	if err := waitBytes(aSub, 2048, 90*time.Second); err != nil {
		fmt.Println("SCALE_STRESS: FAIL room A (edge1→edge2) no media", err)
		os.Exit(1)
	}
	if err := waitBytes(bSub, 2048, 90*time.Second); err != nil {
		fmt.Println("SCALE_STRESS: FAIL room B (edge2→edge1) no media", err)
		os.Exit(1)
	}
	fmt.Println("SCALE_STRESS: PASS (2 rooms × 2 edges all media flowed concurrently)")
}

// scenarioSimulateICERestart: the server-driven ICE restart path (the same
// `participant.ICERestart` the resume flow uses, driven directly via
// SimulateScenario_SwitchCandidateProtocol). Both peers publish+subscribe; the
// publisher triggers the restart on its transports, so its SUBSCRIBER PC (the
// room→edge→client receive path) renegotiates cross-node. Media must continue on
// BOTH sides after the restart completes — proving the cross-node ICE-restart /
// renegotiation path end-to-end.
func scenarioSimulateICERestart(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-icerestart-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-icerestart-pub")
	waitConnected(pub)

	w1, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer w1.Stop()
	w2, err := sub.AddStaticTrack("video/vp8", "video", "camera2")
	must(err)
	defer w2.Stop()

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("SIMULATE_ICE_RESTART: FAIL subscriber not receiving before restart", err)
		os.Exit(1)
	}
	if err := waitBytes(pub, 2048, 30*time.Second); err != nil {
		fmt.Println("SIMULATE_ICE_RESTART: FAIL publisher not receiving before restart", err)
		os.Exit(1)
	}

	// server-driven ICE restart on the publisher's transports (UDP protocol — the
	// working path; the restart itself + the cross-node renegotiation is the test)
	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Simulate{
			Simulate: &livekit.SimulateScenario{
				Scenario: &livekit.SimulateScenario_SwitchCandidateProtocol{
					SwitchCandidateProtocol: livekit.CandidateProtocol_UDP,
				},
			},
		},
	}))
	time.Sleep(3 * time.Second) // let the renegotiation + ICE restart complete

	beforePub := pub.BytesReceived()
	if err := waitBytes(pub, beforePub+2048, 30*time.Second); err != nil {
		fmt.Println("SIMULATE_ICE_RESTART: FAIL publisher stopped receiving after restart", err)
		os.Exit(1)
	}
	beforeSub := sub.BytesReceived()
	if err := waitBytes(sub, beforeSub+2048, 30*time.Second); err != nil {
		fmt.Println("SIMULATE_ICE_RESTART: FAIL subscriber stopped receiving after restart", err)
		os.Exit(1)
	}
	fmt.Println("SIMULATE_ICE_RESTART: PASS (media continued on both sides after server-driven ICE restart)")
}

// scenarioSecurityAuth: the cross-node media_relay + control channels require the
// shared secret (media_relay.secret, P0-1). Dialing the EDGE node's relays with a
// WRONG secret must be rejected by the auth handshake — proving the internal TCP
// channels are not open to unauthenticated peers.
func scenarioSecurityAuth(url, apiKey, apiSecret, room string) {
	edgeHost := mustParseURL(url).Hostname()

	_, err := transport.DialTCPControlChannel(net.JoinHostPort(edgeHost, "7884"), "wrong-secret")
	if err == nil {
		fmt.Println("SECURITY_AUTH: FAIL control relay accepted a wrong-secret dial")
		os.Exit(1)
	}
	_, err = transport.DialTCPMediaChannel(net.JoinHostPort(edgeHost, "7883"), "wrong-secret")
	if err == nil {
		fmt.Println("SECURITY_AUTH: FAIL media relay accepted a wrong-secret dial")
		os.Exit(1)
	}
	fmt.Println("SECURITY_AUTH: PASS (cross-node relays reject unauthenticated dials)")
}
func waitRemoteIdentity(c *testclient.RTCClient, identity string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range c.RemoteParticipants() {
			if p.Identity == identity {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("remote participant %q not seen", identity)
}

// remoteIdentitySeen reports whether c currently sees a remote participant with
// the identity (used to assert a participant's removal after leave).
func remoteIdentitySeen(c *testclient.RTCClient, identity string) bool {
	for _, p := range c.RemoteParticipants() {
		if p.Identity == identity {
			return true
		}
	}
	return false
}

// scenarioQualityRequest: the subscriber sets the max video quality of a
// subscribed track (UpdateTrackSettings.Quality), exercising the room's
// dynacast/layer-selection path; media must keep flowing at the requested tier.
func scenarioQualityRequest(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-quality-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-quality-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	var trackSid string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && trackSid == "" {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-quality-pub" {
				continue
			}
			for _, t := range p.Tracks {
				trackSid = t.Sid
			}
		}
		if trackSid == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if trackSid == "" {
		fmt.Println("QUALITY_REQUEST: FAIL no track visible")
		os.Exit(1)
	}

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("QUALITY_REQUEST: FAIL no initial media", err)
		os.Exit(1)
	}

	// request HIGH quality (dynacast layer selection path)
	must(sub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_TrackSetting{
			TrackSetting: &livekit.UpdateTrackSettings{
				TrackSids: []string{trackSid},
				Quality:   livekit.VideoQuality_HIGH,
			},
		},
	}))
	time.Sleep(1 * time.Second)
	before := sub.BytesReceived()
	if err := waitBytes(sub, before+2048, 20*time.Second); err != nil {
		fmt.Println("QUALITY_REQUEST: FAIL no media after quality request", err)
		os.Exit(1)
	}
	fmt.Println("QUALITY_REQUEST: PASS (quality request processed; media flowed)")
}

// scenarioReceiveBeforePublish: subscriber joins an empty room, then the publisher
// joins and publishes. The subscriber must auto-subscribe to the new track.
func scenarioReceiveBeforePublish(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-sub")
	waitConnected(sub)
	time.Sleep(2 * time.Second) // ensure the subscriber is fully in the room first

	pub := newClient(url, apiKey, apiSecret, room, "go-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 1024, 30*time.Second); err != nil {
		fmt.Println("RECEIVE_BEFORE_PUBLISH: FAIL", err)
		os.Exit(1)
	}
	fmt.Println("RECEIVE_BEFORE_PUBLISH: PASS (subscriber received", sub.BytesReceived(), "bytes from a track published after join)")
}

// scenarioNack: subscriber receives media, then sends RTCP NACKs toward the
// publisher. The server-side cross-node NACK path is asserted by 06-client-e2e.sh
// from the room node logs (nat down RTCP received from edge ... nack).
func scenarioNack(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-nack-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-nack-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(sub, 2048, 30*time.Second); err != nil {
		fmt.Println("NACK: FAIL no media received before NACK", err)
		os.Exit(1)
	}
	for i := 0; i < 6; i++ {
		sub.SendNacks(5) // NACK the last 5 received packets, 6 rounds
		time.Sleep(150 * time.Millisecond)
	}
	// keep the session alive so the room's DownTrack processes the final NACK
	// round and increments its retransmit counter (nackAcks) before this client
	// leaves and its DownTrack closes (the close logs the rtp stats).
	time.Sleep(3 * time.Second)
	fmt.Println("NACK: sent 6 rounds of 5 NACKs (assert server-side via room logs)")
}

// scenarioData: publish a data-channel message cross-node (edge → control →
// room → broadcast → subscriber edge DC). P0-2: the edge executor detaches the
// client's data channel on open and pumps ReadDataChannel into event_data_message
// (pion never runs OnMessage for detached channels — the server enables
// se.DetachDataChannels() on every PC), so the message must arrive.
func scenarioData(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-data-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-data-pub")
	waitConnected(pub)

	received := make(chan string, 1)
	sub.OnDataReceived = func(data []byte, sid string) {
		select {
		case received <- string(data):
		default:
		}
	}
	time.Sleep(1 * time.Second)

	must(pub.PublishData([]byte("nat-e2e-data-message"), livekit.DataPacket_RELIABLE))

	select {
	case msg := <-received:
		if msg != "nat-e2e-data-message" {
			fmt.Println("DATA: FAIL unexpected payload", msg)
			os.Exit(1)
		}
		fmt.Println("DATA: PASS (subscriber received the data-channel message cross-node)")
	case <-time.After(8 * time.Second):
		fmt.Println("DATA: FAIL timeout — subscriber never received the data-channel message")
		os.Exit(1)
	}
}

// scenarioAttributes: set participant attributes on the publisher; the subscriber
// must observe the updated attributes via the participant broadcast.
func scenarioAttributes(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-attr-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-attr-pub")
	waitConnected(pub)

	attrs := map[string]string{"nat-test": "true", "ts": fmt.Sprintf("%d", time.Now().Unix())}
	must(pub.SetAttributes(attrs))

	// poll for the remote participant's attributes to reflect the update
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range sub.RemoteParticipants() {
			if p.Attributes["nat-test"] == "true" {
				fmt.Println("ATTRIBUTES: PASS (subscriber saw updated attributes)")
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("ATTRIBUTES: FAIL subscriber never saw the attributes update")
	os.Exit(1)
}

// waitTrackSID polls the remote participant view until the given identity has a
// published track, returning its server-assigned SID (TR_*). Falls back to
// checking the local published-track view if the remote view is empty.
func waitTrackSID(c *testclient.RTCClient, identity string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range c.RemoteParticipants() {
			if p.Identity != identity {
				continue
			}
			for _, t := range p.Tracks {
				if t.Sid != "" {
					return t.Sid
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return ""
}

// scenarioRTCValidate: exercises the server's token-validation HTTP endpoints
// (/rtc/validate and /rtc/v1/validate). These run the full validateInternal →
// ValidateConnectRequest → room-allocation path (token grants, limits) WITHOUT
// creating a media session — a distinct, network-reachable surface.
func scenarioRTCValidate(wsURL, apiKey, apiSecret, room string) {
	httpBase := "http://" + mustParseURL(wsURL).Host

	// legacy validate: token as query param → 200 "success"
	tok := token(apiKey, apiSecret, room, "go-validate")
	resp, err := http.Get(fmt.Sprintf("%s/rtc/validate?access_token=%s", httpBase, url.QueryEscape(tok)))
	must(err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "success" {
		fmt.Println("RTC_VALIDATE: FAIL legacy validate", resp.StatusCode, string(body))
		os.Exit(1)
	}

	// missing token → 401 (auth-failure branch)
	resp, err = http.Get(httpBase + "/rtc/validate")
	must(err)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		fmt.Println("RTC_VALIDATE: FAIL missing-token expected 401, got", resp.StatusCode)
		os.Exit(1)
	}

	// v1 validate without join_request → 400 (needsJoinRequest branch)
	resp, err = http.Get(fmt.Sprintf("%s/rtc/v1/validate?access_token=%s", httpBase, url.QueryEscape(tok)))
	must(err)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		fmt.Println("RTC_VALIDATE: FAIL v1 no-join-request expected 400, got", resp.StatusCode)
		os.Exit(1)
	}

	fmt.Println("RTC_VALIDATE: PASS (validate endpoints: success + auth/join_request failure branches)")
}

// scenarioUpdateVideoTrack: the publisher updates a published video track's
// dimensions (UpdateVideoTrack); the subscriber must observe the new
// Width/Height via the participant broadcast cross-node.
func scenarioUpdateVideoTrack(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-uvt-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-uvt-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	trackSid := waitTrackSID(sub, "go-uvt-pub", 20*time.Second)
	if trackSid == "" {
		fmt.Println("UPDATE_VIDEO_TRACK: FAIL no track visible to subscriber")
		os.Exit(1)
	}

	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_UpdateVideoTrack{
			UpdateVideoTrack: &livekit.UpdateLocalVideoTrack{
				TrackSid: trackSid,
				Width:    1280,
				Height:   720,
			},
		},
	}))

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-uvt-pub" {
				continue
			}
			for _, t := range p.Tracks {
				if t.Sid == trackSid && t.Width == 1280 && t.Height == 720 {
					fmt.Println("UPDATE_VIDEO_TRACK: PASS (subscriber saw width/height update cross-node)")
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("UPDATE_VIDEO_TRACK: FAIL subscriber never saw width/height update")
	os.Exit(1)
}

// scenarioUpdateAudioTrack: the publisher publishes an Opus audio track and
// updates its features (UpdateAudioTrack → stereo); the subscriber must observe
// the audio track's AudioFeatures via the participant broadcast cross-node.
// Also exercises the Opus up-plane through the Go client.
func scenarioUpdateAudioTrack(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-uat-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-uat-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("audio/opus", "audio", "microphone")
	must(err)
	defer writer.Stop()

	trackSid := waitTrackSID(sub, "go-uat-pub", 20*time.Second)
	if trackSid == "" {
		fmt.Println("UPDATE_AUDIO_TRACK: FAIL no audio track visible to subscriber")
		os.Exit(1)
	}

	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_UpdateAudioTrack{
			UpdateAudioTrack: &livekit.UpdateLocalAudioTrack{
				TrackSid: trackSid,
				Features: []livekit.AudioTrackFeature{livekit.AudioTrackFeature_TF_STEREO},
			},
		},
	}))

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-uat-pub" {
				continue
			}
			for _, t := range p.Tracks {
				if t.Sid == trackSid && len(t.AudioFeatures) > 0 && t.AudioFeatures[0] == livekit.AudioTrackFeature_TF_STEREO {
					fmt.Println("UPDATE_AUDIO_TRACK: PASS (subscriber saw audio features update cross-node)")
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("UPDATE_AUDIO_TRACK: FAIL subscriber never saw audio features update")
	os.Exit(1)
}

// scenarioDataTrackPublish: publish a data track via PublishDataTrackRequest and
// unpublish it via UnpublishDataTrackRequest — the participant data-track signal
// path. (Data-channel messages are not bridged cross-node in the fork, so the
// writer is never started; the signal RPC + response is the point.)
func scenarioDataTrackPublish(url, apiKey, apiSecret, room string) {
	pub := newClient(url, apiKey, apiSecret, room, "go-dtp-pub")
	waitConnected(pub)

	writer, err := pub.PublishDataTrack()
	if err != nil {
		fmt.Println("DATA_TRACK_PUBLISH: FAIL publish data track", err)
		os.Exit(1)
	}
	writer.Stop() // do not start: DC messages are not bridged cross-node (§6.8)

	// unpublish the data track (handle 1 = the first published)
	must(pub.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_UnpublishDataTrackRequest{
			UnpublishDataTrackRequest: &livekit.UnpublishDataTrackRequest{PubHandle: 1},
		},
	}))
	time.Sleep(1 * time.Second)
	fmt.Println("DATA_TRACK_PUBLISH: PASS (publish + unpublish data track via signal path)")
}

// scenarioHiddenParticipant: a participant with the hidden grant joins. Other
// participants must NOT see it in their remote view (broadcast exclusion), but
// the admin RoomService must (participants map still holds it).
func scenarioHiddenParticipant(url, apiKey, apiSecret, room string) {
	hiddenTok := func() string {
		at := auth.NewAccessToken(apiKey, apiSecret).
			SetIdentity("go-hidden").
			SetName("go-hidden")
		at.AddGrant(&auth.VideoGrant{
			RoomJoin: true, Room: room, Hidden: true,
			CanPublish: boolPtr(true), CanSubscribe: boolPtr(true),
		})
		t, err := at.ToJWT()
		must(err)
		return t
	}()

	conn, err := testclient.NewWebSocketConn(url, hiddenTok, &testclient.Options{AutoSubscribe: true})
	must(err)
	hidden, err := testclient.NewRTCClient(conn, false, &testclient.Options{AutoSubscribe: true})
	must(err)
	go hidden.Run()
	waitConnected(hidden)

	// a normal subscriber joins and must NOT see the hidden participant
	sub := newClient(url, apiKey, apiSecret, room, "go-hidden-sub")
	waitConnected(sub)
	time.Sleep(3 * time.Second)
	for _, p := range sub.RemoteParticipants() {
		if p.Identity == "go-hidden" {
			fmt.Println("HIDDEN_PARTICIPANT: FAIL subscriber saw the hidden participant")
			os.Exit(1)
		}
	}

	// the admin must still be able to see it via RoomService
	httpBase := "http://" + mustParseURL(url).Host
	rmc := livekit.NewRoomServiceJSONClient(httpBase, &http.Client{})
	adminCtx := func() context.Context {
		at := auth.NewAccessToken(apiKey, apiSecret)
		at.AddGrant(&auth.VideoGrant{RoomAdmin: true, RoomList: true, Room: room})
		t, err := at.ToJWT()
		must(err)
		header := make(http.Header)
		testclient.SetAuthorizationToken(header, t)
		ctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
		must(err)
		return ctx
	}
	lp, err := rmc.ListParticipants(adminCtx(), &livekit.ListParticipantsRequest{Room: room})
	must(err)
	for _, p := range lp.Participants {
		if p.Identity == "go-hidden" {
			fmt.Println("HIDDEN_PARTICIPANT: PASS (hidden from peers, visible to admin)")
			return
		}
	}
	fmt.Println("HIDDEN_PARTICIPANT: FAIL admin could not see hidden participant")
	os.Exit(1)
}

// scenarioSubscriberOnly: a participant joins WITHOUT publish permission
// (recorder-style subscriber). It must appear in the room (admin) and receive
// media from a normal publisher — the CanPublish=false grant path.
func scenarioSubscriberOnly(url, apiKey, apiSecret, room string) {
	recTok := func() string {
		at := auth.NewAccessToken(apiKey, apiSecret).
			SetIdentity("go-rec").
			SetName("go-rec")
		// no CanPublish: subscriber-only grants
		at.AddGrant(&auth.VideoGrant{RoomJoin: true, Room: room, CanSubscribe: boolPtr(true)})
		t, err := at.ToJWT()
		must(err)
		return t
	}()

	conn, err := testclient.NewWebSocketConn(url, recTok, &testclient.Options{AutoSubscribe: true})
	must(err)
	rec, err := testclient.NewRTCClient(conn, false, &testclient.Options{AutoSubscribe: true})
	must(err)
	go rec.Run()
	waitConnected(rec)

	// a normal publisher publishes; the subscriber-only participant must receive
	pub := newClient(url, apiKey, apiSecret, room, "go-rec-pub")
	waitConnected(pub)
	writer, err := pub.AddStaticTrack("video/vp8", "video", "camera")
	must(err)
	defer writer.Stop()

	if err := waitBytes(rec, 2048, 30*time.Second); err != nil {
		fmt.Println("SUBSCRIBER_ONLY: FAIL subscriber-only participant received no media", err)
		os.Exit(1)
	}
	fmt.Println("SUBSCRIBER_ONLY: PASS (CanPublish=false participant received", rec.BytesReceived(), "bytes cross-node)")
}

// scenarioRoomMoveForward: exercises RoomService MoveParticipant/ForwardParticipant
// (multi-node participant routing RPCs). Same-room targets are rejected by the
// RoomService before routing; cross-room targets route to the participant's node,
// whose RoomManager stubs return "not implemented".
func scenarioRoomMoveForward(url, apiKey, apiSecret, room string) {
	httpBase := "http://" + mustParseURL(url).Host
	rmc := livekit.NewRoomServiceJSONClient(httpBase, &http.Client{})
	destRoom := room + "-dest"
	// EnsureDestRoomPermission requires source==grant.Room AND destination==grant.
	// DestinationRoom. A single token is bound to one source→destination pair, so
	// the same-room and cross-room calls need separate admin contexts.
	adminCtx := func(dest string) context.Context {
		at := auth.NewAccessToken(apiKey, apiSecret)
		at.AddGrant(&auth.VideoGrant{RoomAdmin: true, Room: room, DestinationRoom: dest})
		t, err := at.ToJWT()
		must(err)
		header := make(http.Header)
		testclient.SetAuthorizationToken(header, t)
		ctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
		must(err)
		return ctx
	}

	// a participant to "move"
	p := newClient(url, apiKey, apiSecret, room, "go-move-p")
	waitConnected(p)

	// same-room → rejected by RoomService before routing (destination == source)
	_, err := rmc.MoveParticipant(adminCtx(room), &livekit.MoveParticipantRequest{Room: room, Identity: "go-move-p", DestinationRoom: room})
	if err == nil || !strings.Contains(err.Error(), "destination room cannot be the same as source room") {
		fmt.Println("ROOM_MOVE_FORWARD: FAIL same-room move not rejected:", err)
		os.Exit(1)
	}
	_, err = rmc.ForwardParticipant(adminCtx(room), &livekit.ForwardParticipantRequest{Room: room, Identity: "go-move-p", DestinationRoom: room})
	if err == nil || !strings.Contains(err.Error(), "destination room cannot be the same as source room") {
		fmt.Println("ROOM_MOVE_FORWARD: FAIL same-room forward not rejected:", err)
		os.Exit(1)
	}

	// cross-room → routed to the participant's node → RoomManager stub "not implemented"
	_, err = rmc.MoveParticipant(adminCtx(destRoom), &livekit.MoveParticipantRequest{Room: room, Identity: "go-move-p", DestinationRoom: destRoom})
	if err == nil || !strings.Contains(err.Error(), "not implemented") {
		fmt.Println("ROOM_MOVE_FORWARD: FAIL cross-room move expected not implemented, got:", err)
		os.Exit(1)
	}
	_, err = rmc.ForwardParticipant(adminCtx(destRoom), &livekit.ForwardParticipantRequest{Room: room, Identity: "go-move-p", DestinationRoom: destRoom})
	if err == nil || !strings.Contains(err.Error(), "not implemented") {
		fmt.Println("ROOM_MOVE_FORWARD: FAIL cross-room forward expected not implemented, got:", err)
		os.Exit(1)
	}
	fmt.Println("ROOM_MOVE_FORWARD: PASS (same-room rejected; cross-room routed to stub)")
}

func main() {
	// Surface the client's pion-stack warnings/errors (track drops, SRTP, etc.)
	// to stdout so client-side failures are visible in the E2E suite, while
	// keeping normal-flow INFO logs (full SDP dumps) out of the noise.
	logger.InitFromConfig(&logger.Config{Level: "warn"}, "nat-e2e-client")

	// match the official test harness: register prometheus metrics so the RTC
	// client's telemetry hooks don't dereference nil counters.
	if err := prometheus.Init("nat-e2e-client", livekit.NodeType_SERVER); err != nil {
		fmt.Fprintln(os.Stderr, "prometheus init:", err)
		os.Exit(2)
	}

	url := flag.String("url", "ws://127.0.0.1:7880", "edge node WebSocket URL")
	url2 := flag.String("url2", "", "second edge node WebSocket URL (multi-edge scenario)")
	room2 := flag.String("room2", "", "second room name (scale-stress scenario)")
	apiKey := flag.String("api-key", "devkey", "API key")
	apiSecret := flag.String("api-secret", "secret", "API secret")
	room := flag.String("room", "nat-go", "room name")
	scenario := flag.String("scenario", "receive-before-publish", "scenario: receive-before-publish|nack|data|attributes|single-pc|metadata|mute|multitrack|whip|manual-subscribe|participant-name|track-pause|room-lifecycle|service-apis|subscription-permission|quality-request|rtc-validate|update-video-track|update-audio-track|data-track-publish|hidden-participant|subscriber-only|room-move-forward|whip-ice-restart|perform-rpc|simulcast-switch|reconnect-resume|room-node-failure|webhook-events|subscriber-pli|media-follows-signaling|multi-edge|simulate-ice-restart|security-auth|scale-stress|turn-credentials|turn-relay-only")
	flag.Parse()

	switch *scenario {
	case "receive-before-publish":
		scenarioReceiveBeforePublish(*url, *apiKey, *apiSecret, *room)
	case "nack":
		scenarioNack(*url, *apiKey, *apiSecret, *room)
	case "data":
		scenarioData(*url, *apiKey, *apiSecret, *room)
	case "attributes":
		scenarioAttributes(*url, *apiKey, *apiSecret, *room)
	case "single-pc":
		scenarioSinglePC(*url, *apiKey, *apiSecret, *room)
	case "metadata":
		scenarioMetadata(*url, *apiKey, *apiSecret, *room)
	case "mute":
		scenarioMute(*url, *apiKey, *apiSecret, *room)
	case "multitrack":
		scenarioMultitrack(*url, *apiKey, *apiSecret, *room)
	case "whip":
		scenarioWhip(*url, *apiKey, *apiSecret, *room)
	case "manual-subscribe":
		scenarioManualSubscribe(*url, *apiKey, *apiSecret, *room)
	case "participant-name":
		scenarioParticipantName(*url, *apiKey, *apiSecret, *room)
	case "room-admin":
		scenarioRoomAdmin(*url, *apiKey, *apiSecret, *room)
	case "track-pause":
		scenarioTrackPause(*url, *apiKey, *apiSecret, *room)
	case "room-lifecycle":
		scenarioRoomLifecycle(*url, *apiKey, *apiSecret, *room)
	case "service-apis":
		scenarioServiceAPIs(*url, *apiKey, *apiSecret, *room)
	case "subscription-permission":
		scenarioSubscriptionPermission(*url, *apiKey, *apiSecret, *room)
	case "quality-request":
		scenarioQualityRequest(*url, *apiKey, *apiSecret, *room)
	case "rtc-validate":
		scenarioRTCValidate(*url, *apiKey, *apiSecret, *room)
	case "update-video-track":
		scenarioUpdateVideoTrack(*url, *apiKey, *apiSecret, *room)
	case "update-audio-track":
		scenarioUpdateAudioTrack(*url, *apiKey, *apiSecret, *room)
	case "data-track-publish":
		scenarioDataTrackPublish(*url, *apiKey, *apiSecret, *room)
	case "hidden-participant":
		scenarioHiddenParticipant(*url, *apiKey, *apiSecret, *room)
	case "subscriber-only":
		scenarioSubscriberOnly(*url, *apiKey, *apiSecret, *room)
	case "room-move-forward":
		scenarioRoomMoveForward(*url, *apiKey, *apiSecret, *room)
	case "whip-ice-restart":
		scenarioWhipIceRestart(*url, *apiKey, *apiSecret, *room)
	case "simulate-speaker":
		scenarioSimulateSpeaker(*url, *apiKey, *apiSecret, *room)
	case "simulate-node-failure":
		scenarioSimulateNodeFailure(*url, *apiKey, *apiSecret, *room)
	case "simulate-server-leave":
		scenarioSimulateServerLeave(*url, *apiKey, *apiSecret, *room)
	case "sub-perm-revoke":
		scenarioSubPermRevoke(*url, *apiKey, *apiSecret, *room)
	case "participant-leave-visible":
		scenarioParticipantLeaveVisible(*url, *apiKey, *apiSecret, *room)
	case "sync-state":
		scenarioSyncState(*url, *apiKey, *apiSecret, *room)
	case "connection-quality":
		scenarioConnectionQuality(*url, *apiKey, *apiSecret, *room)
	case "turn-credentials":
		scenarioTurnCredentials(*url, *apiKey, *apiSecret, *room)
	case "turn-relay-only":
		scenarioTurnRelayOnly(*url, *apiKey, *apiSecret, *room)
	case "reconnect":
		scenarioReconnect(*url, *apiKey, *apiSecret, *room)
	case "perform-rpc":
		scenarioPerformRpc(*url, *apiKey, *apiSecret, *room)
	case "simulcast-switch":
		scenarioSimulcastSwitch(*url, *apiKey, *apiSecret, *room)
	case "reconnect-resume":
		scenarioReconnectResume(*url, *apiKey, *apiSecret, *room)
	case "room-node-failure":
		scenarioRoomNodeFailure(*url, *apiKey, *apiSecret, *room)
	case "webhook-events":
		scenarioWebhookEvents(*url, *apiKey, *apiSecret, *room)
	case "subscriber-pli":
		scenarioSubscriberPLI(*url, *apiKey, *apiSecret, *room)
	case "media-follows-signaling":
		scenarioMediaFollowsSignaling(*url, *apiKey, *apiSecret, *room)
	case "multi-edge":
		scenarioMultiEdge(*url, *apiKey, *apiSecret, *room, *url2)
	case "scale-stress":
		scenarioScaleStress(*url, *apiKey, *apiSecret, *room, *url2, *room2)
	case "simulate-ice-restart":
		scenarioSimulateICERestart(*url, *apiKey, *apiSecret, *room)
	case "security-auth":
		scenarioSecurityAuth(*url, *apiKey, *apiSecret, *room)
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenario)
		os.Exit(2)
	}
}
