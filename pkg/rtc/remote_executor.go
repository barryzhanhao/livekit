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
	"fmt"
	"strings"

	"github.com/pion/webrtc/v4"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/protocol/logger"
)

// RunRemotePCExecutor is the edge-node side of the remote control plane. It
// reads control requests from ch, applies them to pc (the real pion
// PeerConnection owned by the edge MediaGateway), sends responses, and forwards
// the pc's events back over ch. It blocks until the channel closes.
func RunRemotePCExecutor(ch transport.ControlChannel, pc *webrtc.PeerConnection) {
	e := &remotePCExecutor{ch: ch, pc: &localPeerConnection{pc}}
	e.registerEvents()
	e.loop()
}

type remotePCExecutor struct {
	ch transport.ControlChannel
	pc peerConnection
}

func (e *remotePCExecutor) registerEvents() {
	e.pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		// pion signals the end of gathering with a nil candidate; skip it (the
		// room learns about completion via the gathering-state event).
		if c == nil {
			return
		}
		body, _ := json.Marshal(c)
		e.sendEvent(remotePCOpEventICECandidate, body)
		logger.Debugw("nat edge ICE candidate", "candidate", c.String())
	})
	e.pc.OnICEGatheringStateChange(func(s webrtc.ICEGatheringState) {
		body, _ := json.Marshal(int(s))
		e.sendEvent(remotePCOpEventICEGatheringStateChange, body)
		logger.Debugw("nat edge ICE gathering state", "state", s)
	})
	e.pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		body, _ := json.Marshal(int(s))
		e.sendEvent(remotePCOpEventICEConnectionStateChange, body)
		logger.Infow("nat edge ICE connection state", "state", s)
	})
	e.pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		body, _ := json.Marshal(int(s))
		e.sendEvent(remotePCOpEventConnectionStateChange, body)
		logger.Infow("nat edge peer connection state", "state", s)
	})
}

// hasMediaKind reports whether the SDP contains a media section of the given
// kind (used for concise negotiation logging).
func hasMediaKind(sd webrtc.SessionDescription, kind string) bool {
	return strings.Contains(sd.SDP, "\nm="+kind+" ") || strings.Contains(sd.SDP, "\rm="+kind+" ") || strings.HasPrefix(sd.SDP, "m="+kind+" ")
}

func (e *remotePCExecutor) sendEvent(op string, body json.RawMessage) {
	msg, _ := json.Marshal(remotePCMessage{Kind: remotePCKindEvent, Op: op, Body: body})
	_ = e.ch.Send(msg)
}

func (e *remotePCExecutor) loop() {
	for {
		raw, err := e.ch.Receive()
		if err != nil {
			return
		}
		var msg remotePCMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg.Kind != remotePCKindRequest {
			continue
		}
		e.handleRequest(msg)
	}
}

func (e *remotePCExecutor) handleRequest(msg remotePCMessage) {
	body, err := e.apply(msg)
	respErr := ""
	if err != nil {
		respErr = err.Error()
	}
	resp, _ := json.Marshal(remotePCMessage{ID: msg.ID, Kind: remotePCKindResponse, Op: msg.Op, Err: respErr, Body: body})
	_ = e.ch.Send(resp)
}

func (e *remotePCExecutor) apply(msg remotePCMessage) (json.RawMessage, error) {
	switch msg.Op {
	case remotePCOpSetRemoteDescription:
		var sd webrtc.SessionDescription
		if err := json.Unmarshal(msg.Body, &sd); err != nil {
			return nil, err
		}
		logger.Debugw("nat edge SetRemoteDescription", "type", sd.Type, "hasVideo", hasMediaKind(sd, "video"), "hasAudio", hasMediaKind(sd, "audio"))
		return nil, e.pc.SetRemoteDescription(sd)

	case remotePCOpSetLocalDescription:
		var sd webrtc.SessionDescription
		if err := json.Unmarshal(msg.Body, &sd); err != nil {
			return nil, err
		}
		logger.Debugw("nat edge SetLocalDescription", "type", sd.Type)
		return nil, e.pc.SetLocalDescription(sd)

	case remotePCOpAddICECandidate:
		var c webrtc.ICECandidateInit
		if err := json.Unmarshal(msg.Body, &c); err != nil {
			return nil, err
		}
		logger.Debugw("nat edge AddICECandidate", "candidate", c.Candidate)
		return nil, e.pc.AddICECandidate(c)

	case remotePCOpCreateOffer:
		sd, err := e.pc.CreateOffer(nil)
		if err != nil {
			return nil, err
		}
		logger.Debugw("nat edge CreateOffer", "hasVideo", hasMediaKind(sd, "video"))
		return json.Marshal(sd)

	case remotePCOpCreateAnswer:
		sd, err := e.pc.CreateAnswer(nil)
		if err != nil {
			return nil, err
		}
		logger.Debugw("nat edge CreateAnswer", "hasVideo", hasMediaKind(sd, "video"), "hasAudio", hasMediaKind(sd, "audio"))
		return json.Marshal(sd)

	case remotePCOpLocalDescription:
		return marshalSDP(e.pc.LocalDescription())
	case remotePCOpRemoteDescription:
		return marshalSDP(e.pc.RemoteDescription())
	case remotePCOpCurrentLocalDescription:
		return marshalSDP(e.pc.CurrentLocalDescription())
	case remotePCOpCurrentRemoteDescription:
		return marshalSDP(e.pc.CurrentRemoteDescription())
	case remotePCOpPendingLocalDescription:
		return marshalSDP(e.pc.PendingLocalDescription())
	case remotePCOpPendingRemoteDescription:
		return marshalSDP(e.pc.PendingRemoteDescription())

	case remotePCOpICEConnectionState:
		return json.Marshal(int(e.pc.ICEConnectionState()))
	case remotePCOpICEGatheringState:
		return json.Marshal(int(e.pc.ICEGatheringState()))
	case remotePCOpConnectionState:
		return json.Marshal(int(e.pc.ConnectionState()))
	case remotePCOpSignalingState:
		return json.Marshal(int(e.pc.SignalingState()))

	case remotePCOpCreateDataChannel:
		var req remotePCCreateDataChannelRequest
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return nil, err
		}
		init := req.Init
		if init == nil {
			init = &webrtc.DataChannelInit{}
		}
		dc, err := e.pc.CreateDataChannel(req.Label, init)
		if err != nil {
			return nil, err
		}
		logger.Infow("nat edge created data channel", "label", req.Label, "id", dc.ID())
		return json.Marshal(dc.ID())

	case remotePCOpClose:
		return nil, e.pc.Close()
	}

	return nil, fmt.Errorf("remote pc op %q not supported by edge executor", msg.Op)
}

func marshalSDP(sd *webrtc.SessionDescription) (json.RawMessage, error) {
	if sd == nil {
		return json.RawMessage("null"), nil
	}
	return json.Marshal(sd)
}
