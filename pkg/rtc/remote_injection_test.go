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

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/livekit-server/pkg/rtc/transport/transportfakes"
)

// TestPCTransportUsesRemotePeerConnection verifies the NAT-mode injection seam:
// when RemotePeerConnection is set, PCTransport uses it instead of a local pion PC.
func TestPCTransportUsesRemotePeerConnection(t *testing.T) {
	_, b := transport.NewLocalControlChannelPair(10)
	defer b.Close()

	remote := NewRemotePeerConnection(b)

	params := TransportParams{
		Config:               &WebRTCConfig{},
		IsOfferer:            true,
		Handler:              &transportfakes.FakeHandler{},
		RemotePeerConnection: remote,
	}
	tr, err := NewPCTransport(params)
	require.NoError(t, err)
	defer tr.Close()

	require.Equal(t, remote, tr.pc)
}
