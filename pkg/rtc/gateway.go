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
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// MediaChannelDialer establishes a per-track MediaChannel to the edge node in NAT
// mode. It dials the edge, sends the hello frame (which carries the session and
// track identity), and returns the hello-capable channel. The service layer wires
// this to MediaRelay; the rtc subscription/up-track paths call it to attach tracks.
type MediaChannelDialer func(hello transport.MediaHello) (transport.HelloMediaChannel, error)

// GatewaySetup is the serializable per-session configuration the room node sends
// to the edge node so the edge can build a real pion PeerConnection with the
// correct MediaEngine. Only the per-session fields travel across the wire; the
// edge node applies its own global WebRTCConfig (setting engine, ICE servers,
// interceptor factories) and its own direction config. Both nodes run the same
// binary and config, so those need not be serialized.
type GatewaySetup struct {
	RoomName                 string           `json:"room_name,omitempty"`
	SessionID                string           `json:"session_id,omitempty"`
	PublishCodecs            []*livekit.Codec `json:"publish_codecs"`
	SubscribeCodecs          []*livekit.Codec `json:"subscribe_codecs"`
	IsOfferer                bool             `json:"is_offerer"`
	IsSendSide               bool             `json:"is_send_side"`
	UseOneShotSignallingMode bool             `json:"use_one_shot"`
	FireOnTrackBySdp         bool             `json:"fire_on_track_by_sdp"`
}

// gatewayAck is the handshake reply to a GatewaySetup sent over the ControlChannel
// before the remote-PC protocol begins.
type gatewayAck struct {
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

// NewEdgePeerConnection builds the edge node's real pion PeerConnection for a
// session. cfg is the edge's own WebRTCConfig; direction is the edge's own
// direction config; setup carries the serialized per-session codec lists and role.
func NewEdgePeerConnection(cfg *WebRTCConfig, direction DirectionConfig, setup GatewaySetup) (*webrtc.PeerConnection, error) {
	params := TransportParams{
		Config:                   cfg,
		DirectionConfig:          direction,
		EnabledPublishCodecs:     setup.PublishCodecs,
		EnabledSubscribeCodecs:   setup.SubscribeCodecs,
		IsOfferer:                setup.IsOfferer,
		IsSendSide:               setup.IsSendSide,
		UseOneShotSignallingMode: setup.UseOneShotSignallingMode,
		FireOnTrackBySdp:         setup.FireOnTrackBySdp,
		Logger:                   logger.GetLogger(),
	}
	pc, _, _, err := newPeerConnection(params, nil)
	if err != nil {
		return nil, err
	}
	return pc.(*localPeerConnection).PeerConnection, nil
}

// DialGateway performs the setup handshake on an already-dialed ControlChannel:
// it sends the setup and awaits the edge's ack, leaving the channel ready for
// the remote-PC protocol. Combine with NewRemotePeerConnection to drive the edge
// PC from the room node.
func DialGateway(ch transport.ControlChannel, setup GatewaySetup) error {
	b, err := json.Marshal(setup)
	if err != nil {
		return err
	}
	if err := ch.Send(b); err != nil {
		return err
	}

	ackRaw, err := ch.Receive()
	if err != nil {
		return err
	}
	var ack gatewayAck
	if err := json.Unmarshal(ackRaw, &ack); err != nil {
		return err
	}
	if !ack.OK {
		if ack.Err == "" {
			ack.Err = "edge gateway setup failed"
		}
		return errors.New(ack.Err)
	}
	return nil
}

// DialEdgeGatewaySession establishes a remote peer connection from the room node
// to an edge node over an already-dialed ControlChannel: it performs the setup
// handshake and returns a remotePeerConnection ready to drive the edge's real
// pion PC. The ControlChannel must be a fresh connection dedicated to this
// session.
func DialEdgeGatewaySession(ch transport.ControlChannel, setup GatewaySetup) (*remotePeerConnection, error) {
	if err := DialGateway(ch, setup); err != nil {
		return nil, err
	}
	return NewRemotePeerConnection(ch), nil
}

// RunEdgeGatewaySession runs the edge-node side of a NAT-mode session on a fresh
// ControlChannel. It reads the setup, builds a real pion PeerConnection wrapped
// in a MediaGateway, replies with an ack, and returns the gateway and the setup
// (so the caller can register the gateway by setup.SessionID). The direction
// config is derived from setup.IsOfferer (offerer → subscriber direction,
// answerer → publisher direction), matching the room node's transport setup. The
// remote-PC executor runs in the background and tears down the PC and gateway
// when the channel closes; onClose (if set) is invoked with the session ID
// immediately before teardown so the caller can unregister the gateway.
func RunEdgeGatewaySession(ch transport.ControlChannel, cfg *WebRTCConfig, onClose func(sessionID string, isOfferer bool)) (*transport.MediaGateway, GatewaySetup, error) {
	raw, err := ch.Receive()
	if err != nil {
		return nil, GatewaySetup{}, err
	}
	var setup GatewaySetup
	if err := json.Unmarshal(raw, &setup); err != nil {
		sendGatewayAck(ch, err)
		return nil, GatewaySetup{}, err
	}

	direction := cfg.Publisher
	if setup.IsOfferer {
		direction = cfg.Subscriber
	}

	pc, err := NewEdgePeerConnection(cfg, direction, setup)
	if err != nil {
		sendGatewayAck(ch, err)
		return nil, GatewaySetup{}, err
	}
	sendGatewayAck(ch, nil)

	gw := transport.NewMediaGateway(pc)
	gw.SetSessionID(setup.SessionID)
	logger.Debugw("nat edge gateway session starting", "room", setup.RoomName, "sessionID", setup.SessionID, "isOfferer", setup.IsOfferer, "isSendSide", setup.IsSendSide, "oneShot", setup.UseOneShotSignallingMode)
	prometheus.AddNATGatewaySession()
	sessionStart := time.Now()
	// Forward published tracks (publisher, up direction) to the room node as
	// control events; the room node then establishes the up-direction MediaChannel
	// and pumps RTP into its SFU buffer.
	gw.OnPublishedTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		mid := ""
		if tr := receiver.RTPTransceiver(); tr != nil {
			mid = tr.Mid()
		}
		// The edge's OnTrack fires before first RTP (FireOnTrackBySdp), so
		// track.Codec() reports an empty MimeType / ClockRate / PayloadType. The
		// room needs the full negotiated codec (ClockRate, PayloadType, fmtp) to
		// bind the SFU buffer — resolve it from the transceiver's negotiated
		// parameters (the same source the local room path uses via
		// rtpReceiver.GetParameters), falling back to track.Codec().
		codec := resolveEdgeOnTrackCodec(track.Codec(), receiver.GetParameters())
		body, _ := json.Marshal(remoteTrackEvent{
			TrackID:  track.ID(),
			StreamID: track.StreamID(),
			SSRC:     uint32(track.SSRC()),
			RID:      track.RID(),
			Mid:      mid,
			Codec:    codec,
		})
		msg, _ := json.Marshal(remotePCMessage{Kind: remotePCKindEvent, Op: remotePCOpEventOnTrack, Body: body})
		_ = ch.Send(msg)
	})
	go func() {
		RunRemotePCExecutor(ch, pc)
		gw.Close()
		_ = pc.Close()
		prometheus.SubNATGatewaySession(time.Since(sessionStart))
		if onClose != nil {
			onClose(setup.SessionID, setup.IsOfferer)
		}
		logger.Infow("nat edge gateway session closed", "sessionID", setup.SessionID)
	}()
	return gw, setup, nil
}

func sendGatewayAck(ch transport.ControlChannel, err error) {
	ack := gatewayAck{OK: err == nil}
	if err != nil {
		ack.Err = err.Error()
	}
	b, _ := json.Marshal(ack)
	_ = ch.Send(b)
}

// resolveEdgeOnTrackCodec returns the full negotiated codec for a publisher track
// received on the edge node. The edge's OnTrack fires before first RTP
// (FireOnTrackBySdp), so trackCodec may report an empty MimeType / ClockRate /
// PayloadType. The room needs the complete codec (ClockRate, PayloadType, fmtp)
// to bind the SFU buffer — without it, the buffer bind fails with "invalid
// codec" and the published track is immediately unpublished. When the track's
// own codec is incomplete, resolve it from the transceiver's negotiated
// parameters (the same source the local room path uses via
// rtpReceiver.GetParameters).
func resolveEdgeOnTrackCodec(trackCodec webrtc.RTPCodecParameters, params webrtc.RTPParameters) webrtc.RTPCodecParameters {
	if trackCodec.MimeType != "" && trackCodec.ClockRate != 0 {
		return trackCodec
	}
	for _, c := range params.Codecs {
		if c.MimeType == "" || c.ClockRate == 0 {
			continue
		}
		return c
	}
	return trackCodec
}
