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

package transport

import "github.com/pion/webrtc/v4"

// Media track direction constants used for hello-frame correlation between a
// MediaChannel and the track it carries (NAT mode, "media follows signaling").
const (
	// MediaDirectionDown is a subscriber track: media flows room node -> edge
	// node, where the edge writes it into a local TrackLocalStaticRTP.
	MediaDirectionDown = "down"
	// MediaDirectionUp is a publisher track: media flows edge node -> room node,
	// where the room pumps it into the SFU buffer.
	MediaDirectionUp = "up"
)

// MediaHello is the one-time setup frame sent as the first frame on a
// MediaChannel. It identifies the session and track the channel carries (session
// ID, track ID, direction, codec, and optional SSRC) so the receiving
// MediaGateway can attach the channel to the right local track or reader without
// any out-of-band correlation.
type MediaHello struct {
	SessionID string                    `json:"session_id,omitempty"`
	TrackID   string                    `json:"track_id"`
	Direction string                    `json:"direction"` // MediaDirectionDown | MediaDirectionUp
	Codec     webrtc.RTPCodecCapability `json:"codec"`
	SSRC      uint32                    `json:"ssrc,omitempty"`
}

// HelloMediaChannel is a MediaChannel that additionally supports exchanging the
// one-time hello frame used for track correlation. Only networked channels need
// this; in-process channels are paired directly and skip the hello.
type HelloMediaChannel interface {
	MediaChannel
	SendHello(hello MediaHello) error
	ReadHello() (MediaHello, error)
}
