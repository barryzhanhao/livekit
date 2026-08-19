// PLI-responsive 3-layer VP8 simulcast publisher for the NAT E2E suite (P0-3).
//
// Why it exists: the `lk --publish-demo` publisher is not a deterministic
// 3-layer simulcast source for an up-switch assertion. Its encoder only
// keyframes the layer(s) being actively consumed, so after a subscriber
// down-switches, the forwarder's layer-lock waits forever for a keyframe on the
// (now-quiet) higher layer — the documented P0-3 "layer-lock PLI loop". This
// publisher behaves like a real encoder: it answers every per-layer RTCP PLI
// with a keyframe on that layer, so a subscriber's LOW→MEDIUM→HIGH drive
// completes every up-switch cross-node:
//
//	room forwarder PLI (layer-lock) → receiver.SendPLI → cross-node up RTCP
//	→ edge gateway WriteRTCP → publisher ReadSimulcastRTCP(rid) → keyframe on
//	that layer → cross-node up RTP → forwarder Select() releases the lock.
//
// It is a raw-pion LiveKit client (dual-PC, publisher side): the WS join reuses
// the shared test/client websocket helper, but the peer connections are driven
// directly so we can (a) publish 3 RID simulcast encodings and (b) read per-RID
// RTCP — the server's own PCTransport has no seam for either.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/codecs"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/signalling"

	testclient "github.com/livekit/livekit-server/test/client"
)

// vp8KeyFrame is a genuine VP8 8x8 keyframe — the exact byte sequence the
// server uses for blank-frame injection (pkg/sfu/downtrack.go VP8KeyFrame8x8) —
// prefixed with the RTP payload descriptor (0x10: S=1, no X, start of
// partition). The room's buffer parses it as a real keyframe:
// codec.VP8.Unmarshal reads descriptor[0]=0x10 (S set) then the VP8 frame tag
// 0x10 (P=0 → keyframe), so the forwarder's layer-lock releases on it.
var vp8KeyFrame = append([]byte{0x10},
	0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x08, 0x00,
	0x08, 0x00, 0x00, 0x47, 0x08, 0x85, 0x85, 0x88,
	0x85, 0x84, 0x88, 0x02, 0x02, 0x00, 0x0c, 0x0d,
	0x60, 0x00, 0xfe, 0xff, 0xab, 0x50, 0x80,
)

// simLayer is one simulcast encoding. Distinct frameBytes → distinct bitrates so
// a subscriber can tell which spatial layer it is receiving by byte rate alone:
//
//	q ~30fps×400B   ≈ 96 kbps
//	h ~30fps×2000B  ≈ 480 kbps
//	f ~30fps×7000B  ≈ 1.7 Mbps
//
// The q layer carries periodic keyframes (so down-switches and the initial
// HIGH subscription latch); h/f only keyframe on PLI — exactly the behavior a
// real simulcast encoder needs for the up-switch to be observable.
type simLayer struct {
	rid             string
	track           *webrtc.TrackLocalStaticRTP
	keyframes       chan struct{} // buffered 1: a PLI for this layer arrived
	frameBytes      int
	packetSize      int
	periodicKeySecs float64 // 0 = keyframes only on PLI

	// negotiated SSRC + running packet/octet counters, used to emit RTCP Sender
	// Reports. The SFU aligns simulcast layer timestamps from these
	// (getRefLayerRTPTimestamp); without SRs, layer up-switches stall.
	ssrc    uint32
	packets uint32
	octets  uint32
}

type simulcastPublisher struct {
	ws     *websocket.Conn
	wsMu   sync.Mutex             // gorilla allows a single concurrent writer
	pc     *webrtc.PeerConnection // publisher PC (client offers)
	subPC  *webrtc.PeerConnection // subscriber PC (server offers; we answer empty)
	sender *webrtc.RTPSender
	layers []*simLayer

	// negotiated header-extension IDs + mid. The edge's pion PC resolves incoming
	// SSRCs to simulcast encodings by probing the RID (and MID) header extensions
	// (handleUnknownRTPPacket), so every outgoing packet must carry them. TrackLocal
	// writers never add them automatically.
	midExtID uint8
	ridExtID uint8
	mid      string

	connected chan struct{}
	cancel    context.CancelFunc
	done      chan struct{}
	once      sync.Once

	// tsClock is the shared RTP timestamp base (90kHz) for ALL simulcast layers.
	// A real encoder derives every layer from the same captured frame, so all
	// layers share one timestamp clock; that is what makes the forwarder's
	// cross-layer timestamp alignment (via RTCP Sender Reports) exact. Independent
	// per-layer clocks drift apart by goroutine jitter, which the SR-based switch
	// check rejects as "switch point too far behind".
	tsClock atomic.Uint32
}

// extmapRe matches `a=extmap:<id> <uri>` lines so the negotiated extension IDs
// can be read from the (post-answer) local description.
var extmapRe = regexp.MustCompile(`a=extmap:([0-9]+)\s+(\S+)`)

// newSimulcastPublisher dials the edge, completes the dual-PC negotiation, and
// returns once the publisher PC is ICE-connected (blocking). Media starts only
// after the caller invokes StartMedia.
func newSimulcastPublisher(wsURL, apiKey, apiSecret, room, identity string) (*simulcastPublisher, error) {
	conn, err := testclient.NewWebSocketConn(wsURL, token(apiKey, apiSecret, room, identity), &testclient.Options{AutoSubscribe: false})
	if err != nil {
		return nil, fmt.Errorf("signal dial: %w", err)
	}

	p := &simulcastPublisher{
		ws:        conn,
		connected: make(chan struct{}),
		done:      make(chan struct{}),
		layers: []*simLayer{
			{rid: "q", frameBytes: 400, packetSize: 400, periodicKeySecs: 2, keyframes: make(chan struct{}, 1)},
			{rid: "h", frameBytes: 2000, packetSize: 700, periodicKeySecs: 0, keyframes: make(chan struct{}, 1)},
			{rid: "f", frameBytes: 7000, packetSize: 1200, periodicKeySecs: 0, keyframes: make(chan struct{}, 1)},
		},
	}

	// -- initial SignalResponse_Join ---------------------------------------
	// Wire format is content-negotiated protobuf (the shared test client sends
	// BinaryMessage; the server answers with protobuf-binary SignalResponses).
	var join *livekit.JoinResponse
	for {
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("waiting for join: %w", err)
		}
		if msgType == websocket.PingMessage {
			_ = conn.WriteMessage(websocket.PongMessage, nil)
			continue
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		res := &livekit.SignalResponse{}
		if err := proto.Unmarshal(payload, res); err != nil {
			conn.Close()
			return nil, fmt.Errorf("decoding signal response: %w", err)
		}
		if j := res.GetJoin(); j != nil {
			join = j
			break
		}
	}
	if join.SubscriberPrimary {
		// This publisher offers on the publisher PC; SubscriberPrimary mode would
		// expect the client to offer on the subscriber PC instead. The shared test
		// clients in this suite are non-SubscriberPrimary, so this is unexpected.
		fmt.Println("SIMCAST publisher: server requested SubscriberPrimary (unexpected); continuing publisher-offer flow")
	}

	var iceServers []webrtc.ICEServer
	for _, is := range join.IceServers {
		iceServers = append(iceServers, webrtc.ICEServer{URLs: is.Urls, Username: is.Username, Credential: is.Credential})
	}

	me := &webrtc.MediaEngine{}
	vp8 := codecs.VP8CodecParameters
	vp8.RTPCodecCapability.RTCPFeedback = []webrtc.RTCPFeedback{
		{Type: webrtc.TypeRTCPFBNACK},
		{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"},
	}
	if err := me.RegisterCodec(vp8, webrtc.RTPCodecTypeVideo); err != nil {
		conn.Close()
		return nil, err
	}
	// The edge resolves our simulcast SSRCs by probing the MID + RID header
	// extensions, so they must be negotiated and present on every packet.
	for _, uri := range []string{
		"urn:ietf:params:rtp-hdrext:sdes:mid",
		"urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
	} {
		if err := me.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: uri}, webrtc.RTPCodecTypeVideo); err != nil {
			conn.Close()
			return nil, err
		}
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me))

	// -- publisher PC: 3-layer simulcast (one m-line, a=simulcast:send q;h;f) --
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: iceServers})
	if err != nil {
		conn.Close()
		return nil, err
	}
	p.pc = pc

	base, err := webrtc.NewTrackLocalStaticRTP(
		codecs.VP8CodecParameters.RTPCodecCapability, "video", "camera",
		webrtc.WithRTPStreamID("q"),
	)
	if err != nil {
		conn.Close()
		return nil, err
	}
	sender, err := pc.AddTrack(base)
	if err != nil {
		conn.Close()
		return nil, err
	}
	p.sender = sender
	p.layers[0].track = base
	for _, l := range p.layers[1:] {
		t, err := webrtc.NewTrackLocalStaticRTP(
			codecs.VP8CodecParameters.RTPCodecCapability, "video", "camera",
			webrtc.WithRTPStreamID(l.rid),
		)
		if err != nil {
			conn.Close()
			return nil, err
		}
		if err := sender.AddEncoding(t); err != nil {
			conn.Close()
			return nil, err
		}
		l.track = t
	}

	// -- subscriber PC: server offers; we answer empty (no subscriptions) ------
	subPC, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: iceServers})
	if err != nil {
		conn.Close()
		return nil, err
	}
	p.subPC = subPC
	subPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			_ = p.sendRequest(&livekit.SignalRequest{Message: &livekit.SignalRequest_Trickle{Trickle: signalling.ToProtoTrickle(webrtc.ICECandidateInit{}, livekit.SignalTarget_SUBSCRIBER, true)}})
			return
		}
		_ = p.sendRequest(&livekit.SignalRequest{Message: &livekit.SignalRequest_Trickle{Trickle: signalling.ToProtoTrickle(c.ToJSON(), livekit.SignalTarget_SUBSCRIBER, false)}})
	})

	// -- publisher PC event wiring -------------------------------------------
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		if st == webrtc.PeerConnectionStateConnected {
			select {
			case p.connected <- struct{}{}:
			default:
			}
		}
	})
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			_ = p.sendRequest(&livekit.SignalRequest{Message: &livekit.SignalRequest_Trickle{Trickle: signalling.ToProtoTrickle(webrtc.ICECandidateInit{}, livekit.SignalTarget_PUBLISHER, true)}})
			return
		}
		_ = p.sendRequest(&livekit.SignalRequest{Message: &livekit.SignalRequest_Trickle{Trickle: signalling.ToProtoTrickle(c.ToJSON(), livekit.SignalTarget_PUBLISHER, false)}})
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		// Drain server-created data channels so their writes never block. This
		// client is not detach-enabled (the server's PC config is), so OnMessage
		// fires normally here.
		dc.OnMessage(func(webrtc.DataChannelMessage) {})
	})
	pc.OnTrack(func(_ *webrtc.TrackRemote, r *webrtc.RTPReceiver) {
		// Publisher PC should not receive media tracks; drain RTCP just in case.
		go func() {
			for {
				if _, _, err := r.ReadRTCP(); err != nil {
					return
				}
			}
		}()
	})

	// -- per-RID RTCP: PLI for a layer → keyframe request --------------------
	for _, l := range p.layers {
		go func(l *simLayer) {
			for {
				pkts, _, err := sender.ReadSimulcastRTCP(l.rid)
				if err != nil {
					return
				}
				for _, pkt := range pkts {
					if _, ok := pkt.(*rtcp.PictureLossIndication); ok {
						select {
						case l.keyframes <- struct{}{}:
						default:
						}
					}
				}
			}
		}(l)
	}

	// -- signal read loop (drives offer/answer/trickle for both PCs) ---------
	go p.readLoop()

	// -- declare the track BEFORE offering ------------------------------------
	// In the standard (dual-PC) publish path the client must send
	// SignalRequest_AddTrack so the room has a pending TrackInfo for the offer's
	// msid; one-shot/WHIP mode synthesizes it from the SDP instead (which is why
	// "remote published track has no pending track" appeared without this). Cid
	// must equal the offer's `a=msid:<stream> <track>` track-id ("video").
	if err := p.sendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_AddTrack{
			AddTrack: &livekit.AddTrackRequest{
				Cid:  "video",
				Name: "camera",
				Type: livekit.TrackType_VIDEO,
				SimulcastCodecs: []*livekit.SimulcastCodec{
					{Cid: "video", Codec: "video/vp8"},
				},
				// The declared quality layers drive the RID→spatial mapping: the
				// room pairs each layer's Quality with the offer's `a=simulcast:send
				// q;h;f` rids (participant.go VideoQualityToRid). Without them the
				// TrackInfo has no layers and every incoming rid maps to spatial 0,
				// so the forwarder can never switch up (all packets look like layer 0).
				Layers: []*livekit.VideoLayer{
					{Quality: livekit.VideoQuality_LOW},
					{Quality: livekit.VideoQuality_MEDIUM},
					{Quality: livekit.VideoQuality_HIGH},
				},
			},
		},
	}); err != nil {
		conn.Close()
		return nil, err
	}

	// -- client is the publisher offerer -------------------------------------
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		conn.Close()
		return nil, err
	}
	offerID := uint32(rand.Intn(255) + 1)
	if err := p.sendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Offer{
			Offer: signalling.ToProtoSessionDescription(offer, offerID, nil),
		},
	}); err != nil {
		conn.Close()
		return nil, err
	}

	select {
	case <-p.connected:
	case <-time.After(20 * time.Second):
		conn.Close()
		return nil, fmt.Errorf("publisher PC did not connect (state %s)", pc.ConnectionState())
	}

	// The answer is in by now; read the negotiated extension IDs + mid from the
	// local description (the answer adopts the offerer's extmap IDs) so the
	// outgoing packets carry the exact values the edge's probe expects.
	if ld := pc.LocalDescription(); ld != nil {
		for _, m := range extmapRe.FindAllStringSubmatch(ld.SDP, -1) {
			id, _ := strconv.Atoi(m[1])
			switch m[2] {
			case "urn:ietf:params:rtp-hdrext:sdes:mid":
				p.midExtID = uint8(id)
			case "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id":
				p.ridExtID = uint8(id)
			}
		}
	}
	for _, tr := range pc.GetTransceivers() {
		if tr.Mid() != "" {
			p.mid = tr.Mid()
			break
		}
	}

	params := sender.GetParameters()
	for _, e := range params.Encodings {
		fmt.Printf("SIMCAST publisher encoding rid=%s ssrc=%d\n", e.RID, e.SSRC)
		for _, l := range p.layers {
			if l.rid == e.RID {
				l.ssrc = uint32(e.SSRC)
			}
		}
	}
	fmt.Printf("SIMCAST publisher negotiated mid=%q midExtID=%d ridExtID=%d\n", p.mid, p.midExtID, p.ridExtID)
	return p, nil
}

// readLoop processes SignalResponses until the websocket closes.
func (p *simulcastPublisher) readLoop() {
	for {
		msgType, payload, err := p.ws.ReadMessage()
		if err != nil {
			p.closePCs()
			return
		}
		if msgType == websocket.PingMessage {
			_ = p.ws.WriteMessage(websocket.PongMessage, nil)
			continue
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		res := &livekit.SignalResponse{}
		if err := proto.Unmarshal(payload, res); err != nil {
			continue
		}
		switch msg := res.Message.(type) {
		case *livekit.SignalResponse_Answer:
			desc, _, _ := signalling.FromProtoSessionDescription(msg.Answer)
			if err := p.pc.SetRemoteDescription(desc); err != nil {
				fmt.Println("SIMCAST publisher: set remote answer:", err)
			}
		case *livekit.SignalResponse_Offer:
			// Server-offered subscriber PC (dual-PC): answer it empty. This client
			// has no subscriptions (auto_subscribe=false) so the offer is minimal.
			desc, offerID, _ := signalling.FromProtoSessionDescription(msg.Offer)
			if err := p.subPC.SetRemoteDescription(desc); err != nil {
				fmt.Println("SIMCAST publisher: set subscriber remote offer:", err)
				continue
			}
			answer, err := p.subPC.CreateAnswer(nil)
			if err != nil {
				fmt.Println("SIMCAST publisher: create subscriber answer:", err)
				continue
			}
			if err := p.subPC.SetLocalDescription(answer); err != nil {
				fmt.Println("SIMCAST publisher: set subscriber local answer:", err)
				continue
			}
			if err := p.sendRequest(&livekit.SignalRequest{
				Message: &livekit.SignalRequest_Answer{
					Answer: signalling.ToProtoSessionDescription(answer, offerID, nil),
				},
			}); err != nil {
				fmt.Println("SIMCAST publisher: send subscriber answer:", err)
			}
		case *livekit.SignalResponse_Trickle:
			candidate, err := signalling.FromProtoTrickle(msg.Trickle)
			if err != nil {
				continue
			}
			if msg.Trickle.Target == livekit.SignalTarget_SUBSCRIBER {
				_ = p.subPC.AddICECandidate(candidate)
			} else {
				_ = p.pc.AddICECandidate(candidate)
			}
		case *livekit.SignalResponse_Reconnect:
			// Server-driven ICE restart; this publisher does not subscribe, so no
			// media is at risk — ignore.
		case *livekit.SignalResponse_Leave:
			p.closePCs()
			return
		}
	}
}

// StartMedia launches the shared media loop and the RTCP Sender Report loop.
// Every frame tick (33.3ms) ALL layers are written with the exact same RTP
// timestamp — a real encoder derives every simulcast layer from the same
// captured frame, and exact cross-layer ts alignment is what lets the SFU's
// stream trackers and forwarder treat them as one synchronized stream. Layers
// h/f emit keyframes only when a PLI arrives; q also every 2s.
func (p *simulcastPublisher) StartMedia() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.tsClock.Store(uint32(rand.Intn(1 << 30)))
	go p.mediaLoop(ctx)
	go p.sendSenderReports(ctx)
}

func (p *simulcastPublisher) mediaLoop(ctx context.Context) {
	seq := make([]uint16, len(p.layers))
	for i := range seq {
		seq[i] = uint16(rand.Intn(1 << 16))
	}
	ts := p.tsClock.Load()
	// pre-allocated P-frame payloads (one per layer, distinct sizes → distinct
	// bitrates): [0x10 VP8 descriptor S=1][0x01 VP8 frame tag P=1][filler]
	pframes := make([][]byte, len(p.layers))
	for i, l := range p.layers {
		pf := make([]byte, l.frameBytes)
		pf[0] = 0x10
		pf[1] = 0x01
		pframes[i] = pf
	}

	frame := 0
	lastReport := time.Now()
	ticker := time.NewTicker(time.Second / 30)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts += 3000 // 90kHz / 30fps
			p.tsClock.Store(ts)
			frame++
			for i, l := range p.layers {
				isKey := false
				select {
				case <-l.keyframes:
					isKey = true
				default:
				}
				if l.periodicKeySecs > 0 && frame%int(l.periodicKeySecs*30) == 0 {
					isKey = true
				}
				frameData := pframes[i]
				if isKey {
					frameData = vp8KeyFrame
				}
				if err := writeVP8Frame(p, l, &seq[i], ts, frameData); err != nil {
					return
				}
			}
			// throughput diagnostic: per-layer write totals every ~3s, so the E2E
			// can compare publisher writes against edge-pump reads / room-buffer
			// packets to locate cross-node RTP loss.
			if time.Since(lastReport) >= 3*time.Second {
				lastReport = time.Now()
				parts := make([]string, 0, len(p.layers))
				for _, l := range p.layers {
					parts = append(parts, fmt.Sprintf("%s=%d", l.rid, l.packets))
				}
				fmt.Printf("SIMCAST_PUBWRITE: %s frame=%d\n", strings.Join(parts, " "), frame)
			}
		}
	}
}

// sendSenderReports emits one RTCP Sender Report per layer every 250ms. The SFU
// uses these to compute the RTP timestamp offset between simulcast layers
// (forwarder.getRefLayerRTPTimestamp); without a fresh report for the reference
// layer at the switch moment, an up-switch fails with "unavailable layer ref"
// and the forwarder never leaves the layer-lock. 250ms keeps that window tiny.
func (p *simulcastPublisher) sendSenderReports(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := ntpNow()
			rtpTime := p.tsClock.Load()
			pkts := make([]rtcp.Packet, 0, len(p.layers))
			for _, l := range p.layers {
				if l.ssrc == 0 {
					continue
				}
				pkts = append(pkts, &rtcp.SenderReport{
					SSRC:        l.ssrc,
					NTPTime:     now,
					RTPTime:     rtpTime,
					PacketCount: l.packets,
					OctetCount:  l.octets,
				})
			}
			if len(pkts) == 0 {
				continue
			}
			if err := p.pc.WriteRTCP(pkts); err != nil {
				return
			}
		}
	}
}

// ntpNow converts the current wall-clock to the 64-bit NTP timestamp used by
// RTCP Sender Reports (seconds since 1900 + 32-bit fraction).
func ntpNow() uint64 {
	t := time.Now()
	seconds := uint64(t.Unix()) + 2208988800 // 1900-01-01 epoch offset
	fraction := uint64(t.Nanosecond()) << 32 / 1e9
	return (seconds << 32) | fraction
}

// writeVP8Frame splits frame into packets (marker on the last), stamping the MID
// + RID header extensions the edge needs to route the SSRC to its encoding, and
// updating the layer's packet/octet counters for Sender Reports. Returns the
// track write error so the writer can stop when the PC closes.
func writeVP8Frame(p *simulcastPublisher, l *simLayer, seq *uint16, ts uint32, frame []byte) error {
	packetSize := l.packetSize
	if packetSize <= 0 {
		packetSize = 1200
	}
	for i := 0; i < len(frame); i += packetSize {
		end := min(i+packetSize, len(frame))
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    96,
				SequenceNumber: *seq,
				Timestamp:      ts,
				Marker:         end == len(frame),
			},
			Payload: frame[i:end],
		}
		if p.mid != "" {
			_ = pkt.Header.SetExtension(p.midExtID, []byte(p.mid))
		}
		if l.rid != "" {
			_ = pkt.Header.SetExtension(p.ridExtID, []byte(l.rid))
		}
		*seq++
		l.packets++
		l.octets += uint32(len(frame[i:end]))
		if err := l.track.WriteRTP(pkt); err != nil {
			return err
		}
	}
	return nil
}

func (p *simulcastPublisher) sendRequest(msg *livekit.SignalRequest) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	return p.ws.WriteMessage(websocket.BinaryMessage, payload)
}

func (p *simulcastPublisher) closePCs() {
	p.once.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		_ = p.pc.Close()
		if p.subPC != nil {
			_ = p.subPC.Close()
		}
	})
}

// Close tears down the publisher; safe to call multiple times.
func (p *simulcastPublisher) Close() {
	p.closePCs()
	_ = p.ws.Close()
}
