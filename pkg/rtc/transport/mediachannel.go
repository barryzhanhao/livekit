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

import "errors"

// ErrMediaChannelClosed is returned by reads after Close and by writes on a
// closed channel.
var ErrMediaChannelClosed = errors.New("media channel closed")

// MediaChannel carries plaintext RTP and RTCP for a single track between two
// nodes in NAT mode ("media follows signaling"). It is the transport-agnostic
// boundary between the SFU (room node) and the WebRTC transport (edge node).
//
// Both endpoints hold one end of the same channel. Media (RTP) flows in a
// single direction per track: the media source writes RTP and the media sink
// reads RTP. RTCP is bidirectional. Payloads are raw RTP/RTCP packets (SRTP
// is applied/removed by the edge transport, never here).
//
// Implementations are free to be in-process (see NewLocalMediaChannelPair) or
// networked (see the TCP implementation). The contract is the same either way,
// which is what lets the SFU be split across nodes without changing its logic.
type MediaChannel interface {
	// WriteRTP delivers a plaintext RTP packet to the peer.
	WriteRTP(payload []byte) error
	// WriteRTCP delivers a plaintext RTCP packet to the peer.
	WriteRTCP(payload []byte) error
	// ReadRTP blocks until an RTP packet is available. It returns
	// ErrMediaChannelClosed after Close.
	ReadRTP() ([]byte, error)
	// ReadRTCP blocks until an RTCP packet is available. It returns
	// ErrMediaChannelClosed after Close.
	ReadRTCP() ([]byte, error)
	// Close terminates the channel. Pending reads return ErrMediaChannelClosed
	// and subsequent writes fail with ErrMediaChannelClosed.
	Close() error
}
