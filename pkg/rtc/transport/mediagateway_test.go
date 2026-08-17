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

import (
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

func TestMediaGatewayAddSubscriberTrack(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()

	g := NewMediaGateway(pc)
	defer g.Close()

	ch, _ := NewLocalMediaChannelPair(10)

	codec := webrtc.RTPCodecCapability{MimeType: "video/vp8", ClockRate: 90000}
	require.NoError(t, g.AddSubscriberTrack("track-1", codec, ch))

	// The track is registered and a sender added to the PC.
	require.Len(t, pc.GetSenders(), 1)
	g.mu.RLock()
	_, ok := g.downTracks["track-1"]
	g.mu.RUnlock()
	require.True(t, ok)
}

func TestMediaGatewayAddPublisherTrack(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()

	g := NewMediaGateway(pc)
	defer g.Close()

	ch, peer := NewLocalMediaChannelPair(10)
	defer peer.Close()

	pkt := testPacket(t, 300, 0x1234abcd, []byte{0xaa, 0xbb})
	r := &fakeRTPReader{packets: []*rtp.Packet{pkt}}
	require.NoError(t, g.AddPublisherTrack("track-2", r, ch))

	raw, err := peer.ReadRTP()
	require.NoError(t, err)

	var got rtp.Packet
	require.NoError(t, got.Unmarshal(raw))
	require.Equal(t, pkt.SequenceNumber, got.SequenceNumber)
	require.Equal(t, pkt.Payload, got.Payload)
}

func TestMediaGatewayRemoveAndClose(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()

	g := NewMediaGateway(pc)

	ch, _ := NewLocalMediaChannelPair(10)
	codec := webrtc.RTPCodecCapability{MimeType: "audio/opus", ClockRate: 48000}
	require.NoError(t, g.AddSubscriberTrack("track-3", codec, ch))

	g.RemoveTrack("track-3")
	g.mu.RLock()
	_, ok := g.downTracks["track-3"]
	g.mu.RUnlock()
	require.False(t, ok)

	// Close is idempotent and leaves the gateway unusable.
	g.Close()
	g.Close()

	ch2, _ := NewLocalMediaChannelPair(10)
	require.ErrorIs(t, g.AddSubscriberTrack("track-4", codec, ch2), ErrMediaChannelClosed)
}
