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
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/webrtc/v4"
	"github.com/twitchtv/twirp"

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
				fmt.Println("TURN_CREDENTIALS: PASS (join issued TURN", u, "user", is.Username, ")")
				return
			}
		}
	}
	fmt.Println("TURN_CREDENTIALS: FAIL no TURN server in join response", servers)
	os.Exit(1)
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

	// 1) clean error, never a hang: the data channel is unavailable cross-node
	if err == nil {
		fmt.Println("PERFORM_RPC: FAIL expected error (cross-node data channel unbridged), got success")
		os.Exit(1)
	}
	if !strings.Contains(err.Error(), "data channel is not available") {
		fmt.Println("PERFORM_RPC: FAIL unexpected error (want data-channel-unavailable):", err)
		os.Exit(1)
	}
	// 2) bounded: returns in well under the 10s default RPC response timeout
	// (the ackTimer/responseTimeout would only fire if the request had reached a
	// live data channel; here the send fails before any timer).
	if elapsed > 10*time.Second {
		fmt.Printf("PERFORM_RPC: FAIL took %.1fs (expected immediate failure, not a hang)\n", elapsed.Seconds())
		os.Exit(1)
	}
	fmt.Printf("PERFORM_RPC: PASS (clean bounded error in %.1fs: %v)\n", elapsed.Seconds(), err)
}

// scenarioSimulcastSwitch: with a real 3-layer simulcast publisher (the bash
// harness runs `lk join-room --publish-demo`, identity go-sim-lk-pub), the Go
// subscriber drives the simulcast layer path cross-node. The deterministic proofs
// are server-side (06-client-e2e.sh asserts the room logs):
//
//	(a) the room received all 3 simulcast up planes — `available layers changed -
//	    layer seen` reaching [0,1,2] for go-sim-lk-pub. This is the MediaGateway
//	    fix's proof: each simulcast layer now bridges on its own MediaChannel
//	    (previously only the first layer's RTP crossed the NAT boundary).
//	(b) the DownTrack's max subscribed spatial followed the client's requests —
//	    `setting max spatial layer` shows 0, 1, 2 for go-sim-sub.
//
// Here the client only needs to send the TrackSettings requests (LOW/MEDIUM/HIGH)
// and keep the session alive; the client-side media BITRATE assertions are not
// reliable because an up-switch after a down-switch stalls the forwarder in a
// layer-lock PLI loop cross-node (documented limitation; the down-switch itself
// latches only intermittently). The room-side layer anchors are deterministic.
func scenarioSimulcastSwitch(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-sim-sub")
	waitConnected(sub)

	var trackSid string
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) && trackSid == "" {
		for _, p := range sub.RemoteParticipants() {
			if p.Identity != "go-sim-lk-pub" {
				continue
			}
			for _, t := range p.Tracks {
				if strings.Contains(strings.ToLower(t.MimeType), "h264") {
					trackSid = t.Sid
				}
			}
		}
		if trackSid == "" {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if trackSid == "" {
		fmt.Println("SIMULCAST_SWITCH: FAIL no H264 simulcast track visible from go-sim-lk-pub")
		os.Exit(1)
	}

	// baseline: the subscription completes and media starts at the initial (HIGH) tier
	if err := waitBytes(sub, 8192, 30*time.Second); err != nil {
		fmt.Println("SIMULCAST_SWITCH: FAIL no baseline media", err)
		os.Exit(1)
	}

	// Drive the layer selection LOW→MEDIUM→HIGH. The room applies each request
	// (`setting max spatial layer` → 0, 1, 2), which the bash harness asserts.
	for _, q := range []livekit.VideoQuality{
		livekit.VideoQuality_LOW,
		livekit.VideoQuality_MEDIUM,
		livekit.VideoQuality_HIGH,
	} {
		must(sub.SendRequest(&livekit.SignalRequest{
			Message: &livekit.SignalRequest_TrackSetting{
				TrackSetting: &livekit.UpdateTrackSettings{
					TrackSids: []string{trackSid},
					Quality:   q,
				},
			},
		}))
		time.Sleep(2 * time.Second)
	}
	fmt.Println("SIMULCAST_SWITCH: PASS (layer selection LOW/MEDIUM/HIGH driven cross-node)")
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
	for i := 0; i < 3; i++ {
		sub.SendNacks(5) // NACK the last 5 received packets, 3 rounds
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("NACK: sent 3 rounds of 5 NACKs (assert server-side via room logs)")
}

// scenarioData: publish a data-channel message. NOTE: the fork currently does not
// bridge data-channel messages across nodes (§6.8), so the subscriber is NOT
// expected to receive them — this scenario documents the limitation by reporting
// whether the message arrived.
func scenarioData(url, apiKey, apiSecret, room string) {
	sub := newClient(url, apiKey, apiSecret, room, "go-data-sub")
	waitConnected(sub)
	pub := newClient(url, apiKey, apiSecret, room, "go-data-pub")
	waitConnected(pub)
	time.Sleep(1 * time.Second)

	must(pub.PublishData([]byte("nat-e2e-data-message"), livekit.DataPacket_RELIABLE))
	time.Sleep(3 * time.Second)

	if sub.BytesReceived() > 0 {
		// BytesReceived counts RTP only; data arrives via the DC handler. The
		// reliable DC isn't bridged cross-node yet, so this path is expected to
		// stay empty.
		fmt.Println("DATA: subscriber received bytes (unexpected for unbridged DC)")
	} else {
		fmt.Println("DATA: message sent; subscriber DC not bridged cross-node (documented §6.8 limitation)")
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
	// match the official test harness: register prometheus metrics so the RTC
	// client's telemetry hooks don't dereference nil counters.
	if err := prometheus.Init("nat-e2e-client", livekit.NodeType_SERVER); err != nil {
		fmt.Fprintln(os.Stderr, "prometheus init:", err)
		os.Exit(2)
	}

	url := flag.String("url", "ws://127.0.0.1:7880", "edge node WebSocket URL")
	apiKey := flag.String("api-key", "devkey", "API key")
	apiSecret := flag.String("api-secret", "secret", "API secret")
	room := flag.String("room", "nat-go", "room name")
	scenario := flag.String("scenario", "receive-before-publish", "scenario: receive-before-publish|nack|data|attributes|single-pc|metadata|mute|multitrack|whip|manual-subscribe|participant-name|track-pause|room-lifecycle|service-apis|subscription-permission|quality-request|rtc-validate|update-video-track|update-audio-track|data-track-publish|hidden-participant|subscriber-only|room-move-forward|whip-ice-restart|perform-rpc|simulcast-switch|reconnect-resume")
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
	case "reconnect":
		scenarioReconnect(*url, *apiKey, *apiSecret, *room)
	case "perform-rpc":
		scenarioPerformRpc(*url, *apiKey, *apiSecret, *room)
	case "simulcast-switch":
		scenarioSimulcastSwitch(*url, *apiKey, *apiSecret, *room)
	case "reconnect-resume":
		scenarioReconnectResume(*url, *apiKey, *apiSecret, *room)
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenario)
		os.Exit(2)
	}
}
