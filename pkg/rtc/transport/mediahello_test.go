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

// TestMediaHelloRoundTrip verifies the hello frame survives a TCP round trip.
func TestMediaHelloRoundTrip(t *testing.T) {
	ln, err := ListenTCPMediaChannel("127.0.0.1:0", "test-relay-secret")
	require.NoError(t, err)
	defer ln.Close()

	hello := MediaHello{
		TrackID:   "track-hello",
		Direction: MediaDirectionDown,
		Codec:     webrtc.RTPCodecCapability{MimeType: "video/vp8", ClockRate: 90000},
		SSRC:      0xdeadbeef,
	}

	dialedCh := make(chan HelloMediaChannel, 1)
	go func() {
		ch, err := DialTCPMediaChannelHello(ln.Addr().String(), hello, "test-relay-secret")
		if err != nil {
			dialedCh <- nil
			return
		}
		dialedCh <- ch
	}()

	accepted, err := ln.AcceptHello()
	require.NoError(t, err)
	defer accepted.Close()

	got, err := accepted.ReadHello()
	require.NoError(t, err)
	require.Equal(t, hello, got)

	dialed := <-dialedCh
	require.NotNil(t, dialed)
	defer dialed.Close()
}

// TestMediaGatewayAttachHelloUp proves an "up" (publisher) track is attached via
// the hello frame and RTP flows across the TCP media channel to the room side.
func TestMediaGatewayAttachHelloUp(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()

	g := NewMediaGateway(pc)
	defer g.Close()

	ln, err := ListenTCPMediaChannel("127.0.0.1:0", "test-relay-secret")
	require.NoError(t, err)
	defer ln.Close()

	dialedCh := make(chan HelloMediaChannel, 1)
	go func() {
		ch, err := DialTCPMediaChannelHello(ln.Addr().String(), MediaHello{
			TrackID:   "track-up",
			Direction: MediaDirectionUp,
			Codec:     webrtc.RTPCodecCapability{MimeType: "audio/opus", ClockRate: 48000},
		}, "test-relay-secret")
		if err != nil {
			dialedCh <- nil
			return
		}
		dialedCh <- ch
	}()

	accepted, err := ln.AcceptHello()
	require.NoError(t, err)
	defer accepted.Close()

	pkt := testPacket(t, 7, 0x11223344, []byte{0xde, 0xad})
	require.NoError(t, g.AttachHello(accepted, &fakeRTPReader{packets: []*rtp.Packet{pkt}}))

	dialed := <-dialedCh
	require.NotNil(t, dialed)
	defer dialed.Close()

	raw, err := dialed.ReadRTP()
	require.NoError(t, err)

	var got rtp.Packet
	require.NoError(t, got.Unmarshal(raw))
	require.Equal(t, pkt.SequenceNumber, got.SequenceNumber)
	require.Equal(t, pkt.SSRC, got.SSRC)
	require.Equal(t, pkt.Payload, got.Payload)
}

// TestMediaGatewayAttachHelloDown proves a "down" (subscriber) track is attached
// via the hello frame: the gateway creates a local TrackLocalStaticRTP and adds a
// sender to the pion PC.
func TestMediaGatewayAttachHelloDown(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()

	g := NewMediaGateway(pc)
	defer g.Close()

	ln, err := ListenTCPMediaChannel("127.0.0.1:0", "test-relay-secret")
	require.NoError(t, err)
	defer ln.Close()

	dialedCh := make(chan HelloMediaChannel, 1)
	go func() {
		ch, err := DialTCPMediaChannelHello(ln.Addr().String(), MediaHello{
			TrackID:   "track-down",
			Direction: MediaDirectionDown,
			Codec:     webrtc.RTPCodecCapability{MimeType: "video/vp8", ClockRate: 90000},
		}, "test-relay-secret")
		if err != nil {
			dialedCh <- nil
			return
		}
		dialedCh <- ch
	}()

	accepted, err := ln.AcceptHello()
	require.NoError(t, err)
	defer accepted.Close()

	require.NoError(t, g.AttachHello(accepted, nil))

	dialed := <-dialedCh
	require.NotNil(t, dialed)
	defer dialed.Close()

	require.Len(t, pc.GetSenders(), 1)
	g.mu.RLock()
	_, ok := g.downTracks["track-down"]
	g.mu.RUnlock()
	require.True(t, ok)
}
