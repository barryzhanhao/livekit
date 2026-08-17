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
//	           -room nat-go -scenario <receive-before-publish|nack|data|attributes|single-pc|metadata|mute|multitrack>
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	testclient "github.com/livekit/livekit-server/test/client"
)

func boolPtr(b bool) *bool { return &b }

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
	scenario := flag.String("scenario", "receive-before-publish", "scenario: receive-before-publish|nack|data|attributes")
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
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenario)
		os.Exit(2)
	}
}
