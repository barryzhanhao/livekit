// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sfu

import "github.com/pion/webrtc/v4"

type TrackRemote interface {
	ID() string
	RID() string
	Msid() string
	SSRC() webrtc.SSRC
	RtxSSRC() webrtc.SSRC
	StreamID() string
	Kind() webrtc.RTPCodecType
	Codec() webrtc.RTPCodecParameters
	RTCTrack() *webrtc.TrackRemote
}

// TrackRemoteFromSdp represents a remote track that could be created by the sdp.
// It is a wrapper around the webrtc.TrackRemote and return the Codec from sdp
// before the first RTP packet is received.
type TrackRemoteFromSdp struct {
	*webrtc.TrackRemote
	sdpCodec webrtc.RTPCodecParameters
}

func NewTrackRemoteFromSdp(track *webrtc.TrackRemote, codec webrtc.RTPCodecParameters) *TrackRemoteFromSdp {
	return &TrackRemoteFromSdp{
		TrackRemote: track,
		sdpCodec:    codec,
	}
}

func (t *TrackRemoteFromSdp) Codec() webrtc.RTPCodecParameters {
	return t.sdpCodec
}

func (t *TrackRemoteFromSdp) RTCTrack() *webrtc.TrackRemote {
	return t.TrackRemote
}

// TrackRemoteFromMetadata is a TrackRemote for NAT mode ("media follows
// signaling"), where the room node has no pion TrackRemote (it lives on the edge
// node). It carries the published track's metadata received from the edge's
// pc.OnTrack event, and RTCTrack() returns nil.
type TrackRemoteFromMetadata struct {
	trackID  string
	streamID string
	rid      string
	msid     string
	ssrc     webrtc.SSRC
	codec    webrtc.RTPCodecParameters
	kind     webrtc.RTPCodecType
}

func NewTrackRemoteFromMetadata(id, streamID, rid, msid string, ssrc webrtc.SSRC, codec webrtc.RTPCodecParameters, kind webrtc.RTPCodecType) *TrackRemoteFromMetadata {
	return &TrackRemoteFromMetadata{
		trackID:  id,
		streamID: streamID,
		rid:      rid,
		msid:     msid,
		ssrc:     ssrc,
		codec:    codec,
		kind:     kind,
	}
}

func (t *TrackRemoteFromMetadata) ID() string           { return t.trackID }
func (t *TrackRemoteFromMetadata) RID() string          { return t.rid }
func (t *TrackRemoteFromMetadata) Msid() string         { return t.msid }
func (t *TrackRemoteFromMetadata) SSRC() webrtc.SSRC    { return t.ssrc }
func (t *TrackRemoteFromMetadata) RtxSSRC() webrtc.SSRC { return 0 }
func (t *TrackRemoteFromMetadata) StreamID() string     { return t.streamID }
func (t *TrackRemoteFromMetadata) Kind() webrtc.RTPCodecType {
	return t.kind
}
func (t *TrackRemoteFromMetadata) Codec() webrtc.RTPCodecParameters { return t.codec }
func (t *TrackRemoteFromMetadata) RTCTrack() *webrtc.TrackRemote    { return nil }
