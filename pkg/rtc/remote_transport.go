// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rtc

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/protocol/logger"
)

// remotePCRequestTimeout bounds a control-channel round-trip. Pion ops on the
// edge complete in milliseconds; a timeout here only fires when the edge is
// wedged, so the room's negotiation can fail cleanly instead of hanging.
// A var (not const) so tests can shrink it.
var remotePCRequestTimeout = 15 * time.Second

// remotePCMessage is the JSON control envelope exchanged over a ControlChannel
// between the room node (remotePeerConnection) and the edge node (MediaGateway
// executor). Three kinds:
//
//	request  — room -> edge, carries an op and a JSON body (id set)
//	response — edge -> room, carries the result/error for a prior request (id set)
//	event    — edge -> room, async callback (no id)
type remotePCMessage struct {
	ID   int64           `json:"id,omitempty"`
	Kind string          `json:"kind"` // "request" | "response" | "event"
	Op   string          `json:"op"`
	Err  string          `json:"err,omitempty"`
	Body json.RawMessage `json:"body,omitempty"`
}

const (
	remotePCKindRequest  = "request"
	remotePCKindResponse = "response"
	remotePCKindEvent    = "event"

	// ops (room -> edge requests)
	remotePCOpSetRemoteDescription     = "set_remote_description"
	remotePCOpSetLocalDescription      = "set_local_description"
	remotePCOpAddICECandidate          = "add_ice_candidate"
	remotePCOpCreateOffer              = "create_offer"
	remotePCOpCreateAnswer             = "create_answer"
	remotePCOpLocalDescription         = "local_description"
	remotePCOpRemoteDescription        = "remote_description"
	remotePCOpCurrentLocalDescription  = "current_local_description"
	remotePCOpCurrentRemoteDescription = "current_remote_description"
	remotePCOpPendingLocalDescription  = "pending_local_description"
	remotePCOpPendingRemoteDescription = "pending_remote_description"
	remotePCOpICEConnectionState       = "ice_connection_state"
	remotePCOpICEGatheringState        = "ice_gathering_state"
	remotePCOpConnectionState          = "connection_state"
	remotePCOpSignalingState           = "signaling_state"
	remotePCOpCreateDataChannel        = "create_data_channel"
	remotePCOpClose                    = "close"

	// events (edge -> room)
	remotePCOpEventICECandidate             = "event_ice_candidate"
	remotePCOpEventICEGatheringStateChange  = "event_ice_gathering_state_change"
	remotePCOpEventICEConnectionStateChange = "event_ice_connection_state_change"
	remotePCOpEventConnectionStateChange    = "event_connection_state_change"
	remotePCOpEventOnTrack                  = "event_on_track"
)

// remoteTrackEvent is the metadata for a publisher track the edge node received
// via pc.OnTrack. It carries enough for the room node to establish the
// up-direction MediaChannel and pump plaintext RTP into its SFU buffer.
type remoteTrackEvent struct {
	TrackID  string                    `json:"track_id"`
	StreamID string                    `json:"stream_id"`
	SSRC     uint32                    `json:"ssrc"`
	RID      string                    `json:"rid,omitempty"`
	Mid      string                    `json:"mid,omitempty"`
	Codec    webrtc.RTPCodecParameters `json:"codec"`
}

type remotePCResponse struct {
	body json.RawMessage
	err  error
}

// remotePeerConnection implements the peerConnection interface by forwarding
// transport operations to the edge node's MediaGateway over a ControlChannel.
// SDP/ICE/state operations are serialized as JSON; the AddTrack family (which
// requires MediaChannel establishment) is not yet wired and returns an error.
type remotePeerConnection struct {
	ch transport.ControlChannel

	reqID atomic.Int64

	mu      sync.Mutex
	pending map[int64]chan remotePCResponse

	cbMu                       sync.RWMutex
	onICECandidate             func(*webrtc.ICECandidate)
	onICEGatheringStateChange  func(webrtc.ICEGatheringState)
	onICEConnectionStateChange func(webrtc.ICEConnectionState)
	onConnectionStateChange    func(webrtc.PeerConnectionState)
	onTrack                    func(*webrtc.TrackRemote, *webrtc.RTPReceiver)
	onRemoteTrack              func(remoteTrackEvent)
	onDataChannel              func(*webrtc.DataChannel)

	gatheringComplete chan struct{}
	gatheringOnce     sync.Once

	descMu     sync.Mutex
	localDesc  *webrtc.SessionDescription
	remoteDesc *webrtc.SessionDescription
}

var _ peerConnection = (*remotePeerConnection)(nil)

func NewRemotePeerConnection(ch transport.ControlChannel) *remotePeerConnection {
	r := &remotePeerConnection{
		ch:                ch,
		pending:           make(map[int64]chan remotePCResponse),
		gatheringComplete: make(chan struct{}),
	}
	go r.readLoop()
	return r
}

func (r *remotePeerConnection) readLoop() {
	for {
		raw, err := r.ch.Receive()
		if err != nil {
			r.closePending()
			return
		}
		var msg remotePCMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		r.dispatch(msg)
	}
}

func (r *remotePeerConnection) dispatch(msg remotePCMessage) {
	switch msg.Kind {
	case remotePCKindResponse:
		r.mu.Lock()
		respCh, ok := r.pending[msg.ID]
		delete(r.pending, msg.ID)
		r.mu.Unlock()
		if ok {
			respCh <- remotePCResponse{body: msg.Body, err: msgErr(msg.Err)}
		}
	case remotePCKindEvent:
		r.dispatchEvent(msg)
	}
}

func msgErr(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}

func (r *remotePeerConnection) dispatchEvent(msg remotePCMessage) {
	r.cbMu.RLock()
	defer r.cbMu.RUnlock()

	switch msg.Op {
	case remotePCOpEventICECandidate:
		if r.onICECandidate != nil {
			var c webrtc.ICECandidate
			if err := json.Unmarshal(msg.Body, &c); err == nil {
				r.onICECandidate(&c)
			}
		}
	case remotePCOpEventICEGatheringStateChange:
		var v int
		if err := json.Unmarshal(msg.Body, &v); err == nil {
			state := webrtc.ICEGatheringState(v)
			// One-shot mode does not wire onICEGatheringStateChange (the room
			// never trickles), but GetAnswer still waits on gatheringComplete —
			// signal it whenever the edge's gathering completes, independent of
			// the user callback (otherwise a WHIP/one-shot answer hangs forever).
			if r.onICEGatheringStateChange != nil {
				r.onICEGatheringStateChange(state)
			}
			if state == webrtc.ICEGatheringStateComplete {
				r.gatheringOnce.Do(func() { close(r.gatheringComplete) })
			}
		}
	case remotePCOpEventICEConnectionStateChange:
		if r.onICEConnectionStateChange != nil {
			var v int
			if err := json.Unmarshal(msg.Body, &v); err == nil {
				r.onICEConnectionStateChange(webrtc.ICEConnectionState(v))
			}
		}
	case remotePCOpEventConnectionStateChange:
		if r.onConnectionStateChange != nil {
			var v int
			if err := json.Unmarshal(msg.Body, &v); err == nil {
				r.onConnectionStateChange(webrtc.PeerConnectionState(v))
			}
		}
	case remotePCOpEventOnTrack:
		if r.onRemoteTrack != nil {
			var ev remoteTrackEvent
			if err := json.Unmarshal(msg.Body, &ev); err == nil {
				logger.Debugw("nat room received remote track event", "trackID", ev.TrackID, "ssrc", ev.SSRC, "codec", ev.Codec.MimeType, "mid", ev.Mid, "rid", ev.RID)
				r.onRemoteTrack(ev)
			}
		}
	}
}

func (r *remotePeerConnection) closePending() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, respCh := range r.pending {
		respCh <- remotePCResponse{err: transport.ErrMediaChannelClosed}
		delete(r.pending, id)
	}
}

func (r *remotePeerConnection) request(op string, body any) (json.RawMessage, error) {
	var raw json.RawMessage
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		raw = b
	}

	id := r.reqID.Inc()
	respCh := make(chan remotePCResponse, 1)
	r.mu.Lock()
	r.pending[id] = respCh
	r.mu.Unlock()

	msg, err := json.Marshal(remotePCMessage{ID: id, Kind: remotePCKindRequest, Op: op, Body: raw})
	if err != nil {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
		return nil, err
	}
	if err := r.ch.Send(msg); err != nil {
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
		return nil, err
	}

	// Bound the round-trip: a wedged edge (alive control channel but stuck
	// executor) must not block room-side negotiation forever. 15s is far above
	// the sub-second pion ops the edge executes; the room's own negotiation
	// timers fail first in the normal path, so this is purely a backstop.
	timer := time.NewTimer(remotePCRequestTimeout)
	defer timer.Stop()
	select {
	case resp := <-respCh:
		return resp.body, resp.err
	case <-timer.C:
		r.mu.Lock()
		delete(r.pending, id)
		r.mu.Unlock()
		return nil, fmt.Errorf("remote pc op %q timed out", op)
	}
}

// ---- negotiation (fire-and-forget with error) ----
// Local/remote descriptions are cached on Set so the getters never round-trip.
// They must not block the ControlChannel read loop: callers inside the dispatch
// path (e.g. handleRemotePublishedTrack -> LastPublisherOffer) would otherwise
// deadlock waiting for a response that only the read loop can deliver.

func (r *remotePeerConnection) SetRemoteDescription(desc webrtc.SessionDescription) error {
	r.cacheRemoteDesc(desc)
	_, err := r.request(remotePCOpSetRemoteDescription, desc)
	return err
}

func (r *remotePeerConnection) SetLocalDescription(desc webrtc.SessionDescription) error {
	r.cacheLocalDesc(desc)
	_, err := r.request(remotePCOpSetLocalDescription, desc)
	return err
}

func (r *remotePeerConnection) AddICECandidate(candidate webrtc.ICECandidateInit) error {
	_, err := r.request(remotePCOpAddICECandidate, candidate)
	return err
}

// ---- request/response ----

func (r *remotePeerConnection) CreateOffer(options *webrtc.OfferOptions) (webrtc.SessionDescription, error) {
	body, err := r.request(remotePCOpCreateOffer, options)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	var sd webrtc.SessionDescription
	err = json.Unmarshal(body, &sd)
	return sd, err
}

func (r *remotePeerConnection) CreateAnswer(options *webrtc.AnswerOptions) (webrtc.SessionDescription, error) {
	body, err := r.request(remotePCOpCreateAnswer, options)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	var sd webrtc.SessionDescription
	err = json.Unmarshal(body, &sd)
	return sd, err
}

func (r *remotePeerConnection) cacheLocalDesc(sd webrtc.SessionDescription) {
	r.descMu.Lock()
	r.localDesc = &sd
	r.descMu.Unlock()
}

func (r *remotePeerConnection) cacheRemoteDesc(sd webrtc.SessionDescription) {
	r.descMu.Lock()
	r.remoteDesc = &sd
	r.descMu.Unlock()
}

func (r *remotePeerConnection) cachedLocalDesc() *webrtc.SessionDescription {
	r.descMu.Lock()
	defer r.descMu.Unlock()
	return r.localDesc
}

func (r *remotePeerConnection) cachedRemoteDesc() *webrtc.SessionDescription {
	r.descMu.Lock()
	defer r.descMu.Unlock()
	return r.remoteDesc
}

func (r *remotePeerConnection) sdpGetter(op string) *webrtc.SessionDescription {
	body, err := r.request(op, nil)
	if err != nil {
		return nil
	}
	if len(body) == 0 || string(body) == "null" {
		return nil
	}
	var sd webrtc.SessionDescription
	if err := json.Unmarshal(body, &sd); err != nil {
		return nil
	}
	return &sd
}

func (r *remotePeerConnection) LocalDescription() *webrtc.SessionDescription {
	return r.cachedLocalDesc()
}

func (r *remotePeerConnection) RemoteDescription() *webrtc.SessionDescription {
	return r.cachedRemoteDesc()
}

func (r *remotePeerConnection) CurrentLocalDescription() *webrtc.SessionDescription {
	return r.cachedLocalDesc()
}

func (r *remotePeerConnection) CurrentRemoteDescription() *webrtc.SessionDescription {
	return r.cachedRemoteDesc()
}

func (r *remotePeerConnection) PendingLocalDescription() *webrtc.SessionDescription {
	return r.sdpGetter(remotePCOpPendingLocalDescription)
}

func (r *remotePeerConnection) PendingRemoteDescription() *webrtc.SessionDescription {
	return r.sdpGetter(remotePCOpPendingRemoteDescription)
}

func (r *remotePeerConnection) intGetter(op string) int {
	body, err := r.request(op, nil)
	if err != nil {
		return 0
	}
	var v int
	if err := json.Unmarshal(body, &v); err != nil {
		return 0
	}
	return v
}

func (r *remotePeerConnection) ICEConnectionState() webrtc.ICEConnectionState {
	return webrtc.ICEConnectionState(r.intGetter(remotePCOpICEConnectionState))
}

func (r *remotePeerConnection) ICEGatheringState() webrtc.ICEGatheringState {
	return webrtc.ICEGatheringState(r.intGetter(remotePCOpICEGatheringState))
}

func (r *remotePeerConnection) ConnectionState() webrtc.PeerConnectionState {
	return webrtc.PeerConnectionState(r.intGetter(remotePCOpConnectionState))
}

func (r *remotePeerConnection) SignalingState() webrtc.SignalingState {
	return webrtc.SignalingState(r.intGetter(remotePCOpSignalingState))
}

// ---- events ----

func (r *remotePeerConnection) OnICECandidate(f func(*webrtc.ICECandidate)) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.onICECandidate = f
}

func (r *remotePeerConnection) OnICEGatheringStateChange(f func(webrtc.ICEGatheringState)) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.onICEGatheringStateChange = f
}

func (r *remotePeerConnection) OnICEConnectionStateChange(f func(webrtc.ICEConnectionState)) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.onICEConnectionStateChange = f
}

func (r *remotePeerConnection) OnConnectionStateChange(f func(webrtc.PeerConnectionState)) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.onConnectionStateChange = f
}

func (r *remotePeerConnection) OnTrack(f func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.onTrack = f
}

// OnRemoteTrack registers a metadata callback for publisher tracks received by
// the edge node via pc.OnTrack. In NAT mode the room node has no real
// TrackRemote; this carries the track identity/codec needed to establish the
// up-direction MediaChannel and pump RTP into its SFU buffer.
func (r *remotePeerConnection) OnRemoteTrack(f func(remoteTrackEvent)) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.onRemoteTrack = f
}

func (r *remotePeerConnection) OnDataChannel(f func(*webrtc.DataChannel)) {
	r.cbMu.Lock()
	defer r.cbMu.Unlock()
	r.onDataChannel = f
}

func (r *remotePeerConnection) GatheringComplete() <-chan struct{} {
	return r.gatheringComplete
}

func (r *remotePeerConnection) Close() error {
	// Fire-and-forget: send the close request then tear down the channel. Do not
	// block on a response — the edge node may already be gone.
	msg, _ := json.Marshal(remotePCMessage{ID: r.reqID.Inc(), Kind: remotePCKindRequest, Op: remotePCOpClose})
	_ = r.ch.Send(msg)
	return r.ch.Close()
}

// ---- not yet wired (require MediaChannel establishment) ----
// Each stub reports the operation name so a production hit is diagnosable.

func (r *remotePeerConnection) AddTrack(t webrtc.TrackLocal) (*webrtc.RTPSender, error) {
	return nil, fmt.Errorf("remote pc AddTrack(%s) not yet supported", t.ID())
}

func (r *remotePeerConnection) AddTransceiverFromTrack(t webrtc.TrackLocal, init ...webrtc.RTPTransceiverInit) (*webrtc.RTPTransceiver, error) {
	return nil, fmt.Errorf("remote pc AddTransceiverFromTrack(%s) not yet supported", t.ID())
}

func (r *remotePeerConnection) AddTransceiverFromKind(kind webrtc.RTPCodecType, init ...webrtc.RTPTransceiverInit) (*webrtc.RTPTransceiver, error) {
	return nil, fmt.Errorf("remote pc AddTransceiverFromKind(%s) not yet supported", kind)
}

func (r *remotePeerConnection) RemoveTrack(s *webrtc.RTPSender) error {
	return fmt.Errorf("remote pc RemoveTrack not yet supported")
}

func (r *remotePeerConnection) GetTransceivers() []*webrtc.RTPTransceiver {
	return nil
}

func (r *remotePeerConnection) CreateDataChannel(label string, init *webrtc.DataChannelInit) (*webrtc.DataChannel, error) {
	// The edge node owns SCTP in NAT mode: ask it to create the data channel so
	// the negotiated SDP keeps its m=application section. The returned local
	// *webrtc.DataChannel is a facade only (message forwarding is deferred, §6.8).
	if _, err := r.request(remotePCOpCreateDataChannel, remotePCCreateDataChannelRequest{Label: label, Init: init}); err != nil {
		return nil, err
	}
	return nil, nil
}

type remotePCCreateDataChannelRequest struct {
	Label string                  `json:"label"`
	Init  *webrtc.DataChannelInit `json:"init,omitempty"`
}

func (r *remotePeerConnection) WriteRTCP([]rtcp.Packet) error {
	return fmt.Errorf("remote pc WriteRTCP not yet supported")
}

func (r *remotePeerConnection) GetStats() webrtc.StatsReport {
	return nil
}

func (r *remotePeerConnection) SCTP() *webrtc.SCTPTransport {
	return nil
}
