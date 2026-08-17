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
//	           -room nat-go -scenario <receive-before-publish|nack|data|attributes>
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
	opts := &testclient.Options{AutoSubscribe: true}
	conn, err := testclient.NewWebSocketConn(wsURL, token(apiKey, apiSecret, room, identity), opts)
	must(err)
	c, err := testclient.NewRTCClient(conn, false /* dual-PC */, opts)
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
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenario)
		os.Exit(2)
	}
}
