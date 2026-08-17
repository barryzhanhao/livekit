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
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
)

// TestRemotePCSplitOverTCP is the vertical slice for NAT mode: a room node's
// remotePeerConnection drives a real pion PeerConnection living on an "edge"
// node, with the control plane (SDP/ICE) flowing over a TCP ControlChannel and
// the media plane (a down track) attached over a TCP MediaChannel via a hello
// frame. It proves the full split end-to-end without any node-discovery wiring.
func TestRemotePCSplitOverTCP(t *testing.T) {
	controlLn, err := transport.ListenTCPControlChannel("127.0.0.1:0")
	require.NoError(t, err)
	defer controlLn.Close()

	mediaLn, err := transport.ListenTCPMediaChannel("127.0.0.1:0")
	require.NoError(t, err)
	defer mediaLn.Close()

	// --- edge node ---
	edgePC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer edgePC.Close()

	gw := transport.NewMediaGateway(edgePC)
	defer gw.Close()

	controlAccepted := make(chan transport.ControlChannel, 1)
	go func() {
		ch, err := controlLn.Accept()
		if err != nil {
			controlAccepted <- nil
			return
		}
		go RunRemotePCExecutor(ch, edgePC)
		controlAccepted <- ch
	}()

	// --- room node ---
	roomCh, err := transport.DialTCPControlChannel(controlLn.Addr().String())
	require.NoError(t, err)
	defer roomCh.Close()

	remote := NewRemotePeerConnection(roomCh)

	edgeCh := <-controlAccepted
	require.NotNil(t, edgeCh)

	// Control plane: a full offer/answer exchange, driven from the room side,
	// executes against the real edge PC over TCP.
	clientPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer clientPC.Close()

	// A media section is required for pion to create an ICE agent (and thus emit
	// ice-ufrag) in the offer.
	_, err = clientPC.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo)
	require.NoError(t, err)

	offer, err := clientPC.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, clientPC.SetLocalDescription(offer))
	// Gather ICE so the offer carries ice-ufrag/ice-pwd and candidates.
	<-webrtc.GatheringCompletePromise(clientPC)
	offer = *clientPC.LocalDescription()

	require.NoError(t, remote.SetRemoteDescription(offer))

	answer, err := remote.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, remote.SetLocalDescription(answer))

	require.NotNil(t, remote.LocalDescription())
	require.NotNil(t, remote.RemoteDescription())

	// Media plane: the room dials a per-track media channel, sends a hello frame,
	// and the edge gateway attaches it as a subscriber (down) track.
	dialedMedia := make(chan transport.HelloMediaChannel, 1)
	go func() {
		ch, err := transport.DialTCPMediaChannelHello(mediaLn.Addr().String(), transport.MediaHello{
			TrackID:   "track-down",
			Direction: transport.MediaDirectionDown,
			Codec:     webrtc.RTPCodecCapability{MimeType: "video/vp8", ClockRate: 90000},
		})
		if err != nil {
			dialedMedia <- nil
			return
		}
		dialedMedia <- ch
	}()

	acceptedMedia, err := mediaLn.AcceptHello()
	require.NoError(t, err)
	defer acceptedMedia.Close()

	require.NoError(t, gw.AttachHello(acceptedMedia, nil))

	roomMedia := <-dialedMedia
	require.NotNil(t, roomMedia)
	defer roomMedia.Close()

	// The gateway registered the down track and added a sender to the edge PC.
	require.Len(t, edgePC.GetSenders(), 1)

	// Close propagates over TCP to the real edge PC.
	require.NoError(t, remote.Close())
	require.Eventually(t, func() bool {
		return edgePC.ConnectionState() == webrtc.PeerConnectionStateClosed
	}, 2*time.Second, 10*time.Millisecond)
}
