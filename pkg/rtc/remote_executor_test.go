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
