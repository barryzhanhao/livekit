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
	"encoding/json"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/rtc/transport"
)

// mockRemoteExecutor drives the edge side of a remotePeerConnection over an
// in-process ControlChannel: it answers requests and can emit events.
func mockRemoteExecutor(t *testing.T, ch transport.ControlChannel, received chan string) {
	t.Helper()
	go func() {
		for {
			raw, err := ch.Receive()
			if err != nil {
				return
			}
			var msg remotePCMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			if msg.Kind != remotePCKindRequest {
				continue
			}
			received <- msg.Op

			var body json.RawMessage
			switch msg.Op {
			case remotePCOpCreateAnswer:
				body, _ = json.Marshal(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: "v=0\r\nmock"})
			case remotePCOpICEConnectionState:
				body, _ = json.Marshal(int(webrtc.ICEConnectionStateConnected))
			}

			resp, _ := json.Marshal(remotePCMessage{ID: msg.ID, Kind: remotePCKindResponse, Op: msg.Op, Body: body})
			_ = ch.Send(resp)

			// Emit a connection-state event after SetRemoteDescription so the
			// event-dispatch path is exercised.
			if msg.Op == remotePCOpSetRemoteDescription {
				evBody, _ := json.Marshal(int(webrtc.ICEConnectionStateConnected))
				ev, _ := json.Marshal(remotePCMessage{Kind: remotePCKindEvent, Op: remotePCOpEventICEConnectionStateChange, Body: evBody})
				_ = ch.Send(ev)
			}
		}
	}()
}

func TestRemotePeerConnectionNegotiation(t *testing.T) {
	a, b := transport.NewLocalControlChannelPair(10)
	defer b.Close()

	received := make(chan string, 10)
	mockRemoteExecutor(t, b, received)

	r := NewRemotePeerConnection(a)
	defer r.Close()

	require.NoError(t, r.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: "v=0\r\noffer"}))
	select {
	case op := <-received:
		require.Equal(t, remotePCOpSetRemoteDescription, op)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for set_remote_description")
	}

	ans, err := r.CreateAnswer(nil)
	require.NoError(t, err)
	require.Equal(t, webrtc.SDPTypeAnswer, ans.Type)
	require.Equal(t, "v=0\r\nmock", ans.SDP)

	state := r.ICEConnectionState()
	require.Equal(t, webrtc.ICEConnectionStateConnected, state)
}

func TestRemotePeerConnectionEventDispatch(t *testing.T) {
	a, b := transport.NewLocalControlChannelPair(10)
	defer b.Close()

	received := make(chan string, 10)
	mockRemoteExecutor(t, b, received)

	r := NewRemotePeerConnection(a)
	defer r.Close()

	eventCh := make(chan webrtc.ICEConnectionState, 1)
	r.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		eventCh <- s
	})

	require.NoError(t, r.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: "v=0\r\noffer"}))

	select {
	case s := <-eventCh:
		require.Equal(t, webrtc.ICEConnectionStateConnected, s)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection state event")
	}
}

// TestRemotePeerConnectionRequestTimeout verifies the round-trip backstop: a
// wedged edge (no response on the control channel) must fail the request after
// the timeout instead of blocking the room-side negotiation forever.
func TestRemotePeerConnectionRequestTimeout(t *testing.T) {
	a, _ := transport.NewLocalControlChannelPair(10)
	defer a.Close()

	// No executor on the peer end: requests are never answered.
	r := NewRemotePeerConnection(a)
	defer r.Close()

	old := remotePCRequestTimeout
	remotePCRequestTimeout = 100 * time.Millisecond
	defer func() { remotePCRequestTimeout = old }()

	start := time.Now()
	_, err := r.CreateAnswer(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out")
	// Fails promptly, far below the production 15s ceiling.
	require.Less(t, time.Since(start), 5*time.Second)

	// The pending entry is cleaned up so no stale state accumulates.
	r.mu.Lock()
	require.Len(t, r.pending, 0)
	r.mu.Unlock()
}

// TestRemotePeerConnectionDisconnectDetection verifies that OnDisconnected fires
// when the control channel breaks unexpectedly (edge node restart), and that
// intentional Close() suppresses the callback.
func TestRemotePeerConnectionDisconnectDetection(t *testing.T) {
	// ---- unexpected close fires OnDisconnected ----
	t.Run("unexpected channel close fires callback", func(t *testing.T) {
		a, b := transport.NewLocalControlChannelPair(10)
		defer b.Close()

		r := NewRemotePeerConnection(a)
		defer r.Close()

		disconnected := make(chan struct{}, 1)
		r.OnDisconnected(func() {
			disconnected <- struct{}{}
		})

		// Close the peer end — simulates edge node restart.
		_ = b.Close()

		select {
		case <-disconnected:
		case <-time.After(2 * time.Second):
			t.Fatal("OnDisconnected was not called after unexpected channel close")
		}
	})

	// ---- intentional Close() suppresses OnDisconnected ----
	t.Run("intentional close does not fire callback", func(t *testing.T) {
		a, b := transport.NewLocalControlChannelPair(10)
		defer b.Close()

		r := NewRemotePeerConnection(a)

		disconnected := make(chan struct{}, 1)
		r.OnDisconnected(func() {
			close(disconnected) // use close so it stays closed even if fired
		})

		// Intentional Close() — must NOT fire the callback.
		_ = r.Close()

		// Give the readLoop time to exit and fire the callback if it were going to.
		time.Sleep(200 * time.Millisecond)

		select {
		case <-disconnected:
			t.Fatal("OnDisconnected was called after intentional Close()")
		default:
			// OK — callback was suppressed.
		}
	})

	// ---- pending request unblocks on unexpected close ----
	t.Run("pending request unblocks on unexpected close", func(t *testing.T) {
		a, b := transport.NewLocalControlChannelPair(10)

		r := NewRemotePeerConnection(a)
		defer r.Close()

		done := make(chan error, 1)
		go func() {
			_, err := r.CreateAnswer(nil)
			done <- err
		}()
		time.Sleep(100 * time.Millisecond) // let the request send and block
		_ = b.Close() // kill the channel — simulates edge restart

		select {
		case err := <-done:
			require.Error(t, err)
			require.ErrorIs(t, err, transport.ErrMediaChannelClosed)
		case <-time.After(3 * time.Second):
			t.Fatal("request did not unblock after unexpected channel close")
		}
	})
}
