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
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// TestDownDirectionRTPFlow verifies the NAT-mode down direction end-to-end with a
// real ICE/DTLS/SRTP connection: the "room node" writes plaintext RTP through a
// MediaChannelRTPWriter into a MediaChannel, the "edge node" MediaGateway pumps
// it into a bound TrackLocalStaticRTP, and the client's pion PeerConnection
// receives the SRTP-decrypted packet. This is the ground truth for 3d-9's
// down-direction wiring.
func TestDownDirectionRTPFlow(t *testing.T) {
	// --- edge node: real PC + gateway, subscriber track pumped from a MediaChannel ---
	edgePC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer edgePC.Close()

	gw := NewMediaGateway(edgePC)
	defer gw.Close()

	codec := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}
	edgeCh, roomCh := NewLocalMediaChannelPair(10)
	defer roomCh.Close()

	require.NoError(t, gw.AddSubscriberTrack("track-1", codec, edgeCh))

	// --- client node ---
	clientPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer clientPC.Close()

	gotRTP := make(chan *rtp.Packet, 1)
	clientPC.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			pkt, _, err := tr.ReadRTP()
			if err == nil {
				gotRTP <- pkt
			}
		}()
	})

	// --- negotiate: edge offers (it owns the track), client answers ---
	offer, err := edgePC.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, edgePC.SetLocalDescription(offer))
	<-webrtc.GatheringCompletePromise(edgePC)

	require.NoError(t, clientPC.SetRemoteDescription(*edgePC.LocalDescription()))
	answer, err := clientPC.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, clientPC.SetLocalDescription(answer))
	<-webrtc.GatheringCompletePromise(clientPC)

	require.NoError(t, edgePC.SetRemoteDescription(*clientPC.LocalDescription()))

	// --- wait for ICE + DTLS to connect ---
	require.Eventually(t, func() bool {
		return edgePC.ConnectionState() == webrtc.PeerConnectionStateConnected &&
			clientPC.ConnectionState() == webrtc.PeerConnectionStateConnected
	}, 10*time.Second, 50*time.Millisecond)

	// --- room node writes plaintext RTP through the MediaChannel ---
	writer := NewMediaChannelRTPWriter(roomCh)
	pkt := testPacket(t, 100, 0x12345678, []byte{0xde, 0xad, 0xbe, 0xef})
	_, err = writer.WriteRTP(&pkt.Header, pkt.Payload)
	require.NoError(t, err)

	select {
	case got := <-gotRTP:
		// SSRC is rewritten by the edge's TrackLocalStaticRTP; seq number and
		// payload must survive the round trip intact.
		require.Equal(t, pkt.SequenceNumber, got.SequenceNumber)
		require.Equal(t, pkt.Payload, got.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not receive RTP")
	}
}

// TestDownDirectionRTPFlowTCP is TestDownDirectionRTPFlow but over a real TCP
// MediaChannel (listener + dial + hello frame), matching the cross-node path.
func TestDownDirectionRTPFlowTCP(t *testing.T) {
	edgePC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer edgePC.Close()

	gw := NewMediaGateway(edgePC)
	defer gw.Close()

	mediaLn, err := ListenTCPMediaChannel("127.0.0.1:0")
	require.NoError(t, err)
	defer mediaLn.Close()

	codec := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}

	// Room dials a per-track media channel and sends the hello frame.
	dialedCh := make(chan HelloMediaChannel, 1)
	go func() {
		ch, err := DialTCPMediaChannelHello(mediaLn.Addr().String(), MediaHello{
			TrackID:   "track-1",
			Direction: MediaDirectionDown,
			Codec:     codec,
		})
		if err != nil {
			dialedCh <- nil
			return
		}
		dialedCh <- ch
	}()

	accepted, err := mediaLn.AcceptHello()
	require.NoError(t, err)
	defer accepted.Close()
	require.NoError(t, gw.AttachHello(accepted, nil))

	roomCh := <-dialedCh
	require.NotNil(t, roomCh)
	defer roomCh.Close()

	// Client + negotiation (edge offers).
	clientPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer clientPC.Close()

	gotRTP := make(chan *rtp.Packet, 1)
	clientPC.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			pkt, _, err := tr.ReadRTP()
			if err == nil {
				gotRTP <- pkt
			}
		}()
	})

	offer, err := edgePC.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, edgePC.SetLocalDescription(offer))
	<-webrtc.GatheringCompletePromise(edgePC)

	require.NoError(t, clientPC.SetRemoteDescription(*edgePC.LocalDescription()))
	answer, err := clientPC.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, clientPC.SetLocalDescription(answer))
	<-webrtc.GatheringCompletePromise(clientPC)

	require.NoError(t, edgePC.SetRemoteDescription(*clientPC.LocalDescription()))

	require.Eventually(t, func() bool {
		return edgePC.ConnectionState() == webrtc.PeerConnectionStateConnected &&
			clientPC.ConnectionState() == webrtc.PeerConnectionStateConnected
	}, 10*time.Second, 50*time.Millisecond)

	writer := NewMediaChannelRTPWriter(roomCh)
	pkt := testPacket(t, 200, 0x22334455, []byte{0x01, 0x02, 0x03})
	_, err = writer.WriteRTP(&pkt.Header, pkt.Payload)
	require.NoError(t, err)

	select {
	case got := <-gotRTP:
		require.Equal(t, pkt.SequenceNumber, got.SequenceNumber)
		require.Equal(t, pkt.Payload, got.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not receive RTP")
	}
}
