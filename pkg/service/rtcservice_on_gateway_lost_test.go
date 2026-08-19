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

package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
)

// fakeWSClient is a minimal types.WebsocketClient that records Close and never
// completes a read — enough to observe whether OnGatewayLost tears the WS down.
type fakeWSClient struct {
	closed atomic.Bool
}

func (f *fakeWSClient) ReadMessage() (int, []byte, error)         { select {} }
func (f *fakeWSClient) WriteMessage(int, []byte) error            { return nil }
func (f *fakeWSClient) WriteControl(int, []byte, time.Time) error { return nil }
func (f *fakeWSClient) SetReadDeadline(time.Time) error           { return nil }
func (f *fakeWSClient) Close() error {
	f.closed.Store(true)
	return nil
}

func newTestRTCService(router *routingfakes.FakeRouter) *RTCService {
	conf, err := config.NewConfig("", true, nil, nil)
	if err != nil {
		panic(err)
	}
	return NewRTCService(conf, nil, router, nil)
}

// TestOnGatewayLost covers the room-node failure migration trigger: when a media
// gateway session loses its control channel, the WS must be closed ONLY when the
// anchored room node is actually dead (so the client reconnects and the room is
// re-homed), never on a normal teardown against a live node.
func TestOnGatewayLost(t *testing.T) {
	origDelay := nodeFailureConfirmDelay
	nodeFailureConfirmDelay = 50 * time.Millisecond
	defer func() { nodeFailureConfirmDelay = origDelay }()

	t.Run("dead room node closes the WS", func(t *testing.T) {
		router := &routingfakes.FakeRouter{}
		// stale stats -> selector.IsAvailable == false -> treated as dead
		router.GetNodeReturns(&livekit.Node{Id: "node-room", Stats: &livekit.NodeStats{UpdatedAt: 1}}, nil)

		s := newTestRTCService(router)
		ws := &fakeWSClient{}
		s.mu.Lock()
		s.connsBySID["PA_dead"] = &rtcSignalConn{sigConn: NewWSSignalConnection(ws), roomNodeID: "node-room"}
		s.mu.Unlock()

		s.OnGatewayLost("PA_dead")
		require.Eventually(t, func() bool { return ws.closed.Load() }, 2*time.Second, 10*time.Millisecond,
			"WS must close when the room node is dead")
	})

	t.Run("unregistered room node closes the WS", func(t *testing.T) {
		router := &routingfakes.FakeRouter{}
		router.GetNodeReturns(nil, routing.ErrNotFound) // node removed from the registry

		s := newTestRTCService(router)
		ws := &fakeWSClient{}
		s.mu.Lock()
		s.connsBySID["PA_gone"] = &rtcSignalConn{sigConn: NewWSSignalConnection(ws), roomNodeID: "node-room"}
		s.mu.Unlock()

		s.OnGatewayLost("PA_gone")
		require.Eventually(t, func() bool { return ws.closed.Load() }, 2*time.Second, 10*time.Millisecond,
			"WS must close when the room node is unregistered")
	})

	t.Run("alive room node does not close the WS", func(t *testing.T) {
		router := &routingfakes.FakeRouter{}
		router.GetNodeReturns(&livekit.Node{Id: "node-room", Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()}}, nil)

		s := newTestRTCService(router)
		ws := &fakeWSClient{}
		s.mu.Lock()
		s.connsBySID["PA_alive"] = &rtcSignalConn{sigConn: NewWSSignalConnection(ws), roomNodeID: "node-room"}
		s.mu.Unlock()

		s.OnGatewayLost("PA_alive")
		// the confirmation delay elapses; a live node must keep the WS open (a
		// normal participant teardown, handled by the psrpc stream close instead).
		time.Sleep(4 * nodeFailureConfirmDelay)
		require.False(t, ws.closed.Load(), "WS must stay open when the room node is alive")
	})

	t.Run("no connection is a no-op", func(t *testing.T) {
		s := newTestRTCService(&routingfakes.FakeRouter{})
		s.OnGatewayLost("PA_unknown") // must not panic
	})
}
