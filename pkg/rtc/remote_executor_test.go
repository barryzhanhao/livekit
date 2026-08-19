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
	"github.com/livekit/protocol/livekit"
)

// TestRemotePCExecutorLoopback drives a remotePeerConnection against a real pion
// PC via RunRemotePCExecutor over an in-process ControlChannel, verifying the
// full control-plane loop (request -> executor -> real PC -> response).
func TestRemotePCExecutorLoopback(t *testing.T) {
	realPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer realPC.Close()
	local := &localPeerConnection{realPC}

	a, b := transport.NewLocalControlChannelPair(16)
	defer b.Close()

	go RunRemotePCExecutor(a, realPC)

	remote := NewRemotePeerConnection(b)
	defer remote.Close()

	// State getters round-trip to the real PC.
	require.Equal(t, local.ICEConnectionState(), remote.ICEConnectionState())
	require.Equal(t, local.SignalingState(), remote.SignalingState())
	require.Equal(t, local.ConnectionState(), remote.ConnectionState())

	// A description getter returns nil (unset) rather than erroring.
	require.Nil(t, remote.LocalDescription())

	// Close is fire-and-forget on the room side; wait for the executor to
	// propagate it to the real PC.
	require.NoError(t, remote.Close())
	require.Eventually(t, func() bool {
		return local.ConnectionState() == webrtc.PeerConnectionStateClosed
	}, 2*time.Second, 10*time.Millisecond)
}

// TestRemotePCExecutorDataChannelWire verifies the client->room data-channel
// bridging with REAL pion PCs configured with detach enabled (as the server
// does: transport.go sets se.DetachDataChannels() on every PC). Detached
// channels never run pion's OnMessage read loop, so the edge executor must
// detach on open and pump ReadDataChannel into event_data_message. This test
// guards that wiring against regression.
func TestRemotePCExecutorDataChannelWire(t *testing.T) {
	se := webrtc.SettingEngine{}
	se.DetachDataChannels()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	edgePC, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer edgePC.Close()

	a, b := transport.NewLocalControlChannelPair(16)
	defer b.Close()
	go RunRemotePCExecutor(a, edgePC)

	remote := NewRemotePeerConnection(b)
	defer remote.Close()

	// room side: register the inbound callback
	gotMsg := make(chan string, 1)
	remote.OnDataMessage(func(kind livekit.DataPacket_Kind, data []byte) {
		select {
		case gotMsg <- string(data):
		default:
		}
	})

	// client side: offerer with the livekit data channels
	client, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer client.Close()

	// loopback ICE between the two in-process PCs
	edgePC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = client.AddICECandidate(c.ToJSON())
		}
	})
	client.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = edgePC.AddICECandidate(c.ToJSON())
		}
	})

	ordered := true
	dc, err := client.CreateDataChannel(ReliableDataChannel, &webrtc.DataChannelInit{Ordered: &ordered})
	require.NoError(t, err)

	offer, err := client.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, client.SetLocalDescription(offer))
	require.NoError(t, edgePC.SetRemoteDescription(offer))
	answer, err := edgePC.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, edgePC.SetLocalDescription(answer))
	require.NoError(t, client.SetRemoteDescription(answer))

	// client writes once the channel is open
	dc.OnOpen(func() {
		_ = dc.SendText("nat-e2e-data-message")
	})

	select {
	case m := <-gotMsg:
		require.Equal(t, "nat-e2e-data-message", m)
	case <-time.After(10 * time.Second):
		t.Fatal("room never received the client's data-channel message")
	}
}
