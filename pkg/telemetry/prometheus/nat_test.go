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

package prometheus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"
)

func initNATForTest(t *testing.T) {
	t.Helper()
	require.NoError(t, Init("test-node", livekit.NodeType_SERVER))
}

func TestNATRelayConnectionMetrics(t *testing.T) {
	initNATForTest(t)

	// ---- Initial state via atomic gauges (used by GetNodeStats) ----
	require.Equal(t, uint64(0), natRelayConnActive.Load())

	// ---- Inbound media connection ----
	IncrementNATRelayConnection(NATRelayDirectionInbound, NATRelayTypeMedia)
	require.Equal(t, uint64(1), natRelayConnActive.Load())

	// ---- Outbound control connection ----
	IncrementNATRelayConnection(NATRelayDirectionOutbound, NATRelayTypeControl)
	require.Equal(t, uint64(2), natRelayConnActive.Load())

	// ---- Close one ----
	DecrementNATRelayConnection()
	require.Equal(t, uint64(1), natRelayConnActive.Load())

	// ---- Close second ----
	DecrementNATRelayConnection()
	require.Equal(t, uint64(0), natRelayConnActive.Load())

	// ---- Decrement below zero -- atomic uint64 wraps, but this is harmless
	// because the gauge is never decremented more than incremented in practice.
	DecrementNATRelayConnection()
	require.Equal(t, uint64(0xffffffffffffffff), natRelayConnActive.Load())
}

func TestNATRelayAuthMetrics(t *testing.T) {
	initNATForTest(t)

	// ---- Auth failure ----
	IncrementNATRelayAuthFailure()

	// ---- Auth success with latency ----
	IncrementNATRelayAuthSuccess(10 * time.Millisecond)

	// ---- Multiple failures ----
	IncrementNATRelayAuthFailure()
	IncrementNATRelayAuthFailure()

	// We can't easily read CounterVec values from tests without a full gather,
	// but we can verify the functions don't panic (they are exercised by the
	// transport-level auth handshake tests which call via authHandshake).
}

func TestNATGatewaySessionMetrics(t *testing.T) {
	initNATForTest(t)

	require.Equal(t, uint64(0), natGatewaySessActive.Load())

	// ---- Session create ----
	AddNATGatewaySession()
	require.Equal(t, uint64(1), natGatewaySessActive.Load())

	// ---- Session close with duration ----
	SubNATGatewaySession(30 * time.Second)
	require.Equal(t, uint64(0), natGatewaySessActive.Load())

	// ---- Session close with zero duration (should not observe histogram) ----
	AddNATGatewaySession()
	SubNATGatewaySession(0)
	require.Equal(t, uint64(0), natGatewaySessActive.Load())
}

func TestNATGatewayTrackMetrics(t *testing.T) {
	initNATForTest(t)

	require.Equal(t, uint64(0), natGatewayTrackUpActive.Load())
	require.Equal(t, uint64(0), natGatewayTrackDownActive.Load())

	// ---- Add down track ----
	AddNATGatewayTrack(NATGatewayTrackDown)
	require.Equal(t, uint64(1), natGatewayTrackDownActive.Load())
	require.Equal(t, uint64(0), natGatewayTrackUpActive.Load())

	// ---- Add up track ----
	AddNATGatewayTrack(NATGatewayTrackUp)
	require.Equal(t, uint64(1), natGatewayTrackDownActive.Load())
	require.Equal(t, uint64(1), natGatewayTrackUpActive.Load())

	// ---- Remove down track ----
	SubNATGatewayTrack(NATGatewayTrackDown)
	require.Equal(t, uint64(0), natGatewayTrackDownActive.Load())
	require.Equal(t, uint64(1), natGatewayTrackUpActive.Load())

	// ---- Remove up track ----
	SubNATGatewayTrack(NATGatewayTrackUp)
	require.Equal(t, uint64(0), natGatewayTrackDownActive.Load())
	require.Equal(t, uint64(0), natGatewayTrackUpActive.Load())

	// ---- Down/up ordering (regression: SubNATGatewayTrack once decremented
	// NATGatewayTrackUpActive when NATGatewayTrackDown was passed) ----
	AddNATGatewayTrack(NATGatewayTrackUp)
	AddNATGatewayTrack(NATGatewayTrackDown)
	require.Equal(t, uint64(1), natGatewayTrackUpActive.Load())
	require.Equal(t, uint64(1), natGatewayTrackDownActive.Load())
	SubNATGatewayTrack(NATGatewayTrackDown)
	require.Equal(t, uint64(1), natGatewayTrackUpActive.Load(), "removing down must not affect up")
	require.Equal(t, uint64(0), natGatewayTrackDownActive.Load())
	SubNATGatewayTrack(NATGatewayTrackUp)
	require.Equal(t, uint64(0), natGatewayTrackUpActive.Load())
}