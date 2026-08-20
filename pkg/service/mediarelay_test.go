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
	"net"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/livekit"
)

func reserveFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

func testRTCConfig(t *testing.T) (*rtc.WebRTCConfig, []*livekit.Codec) {
	t.Helper()
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	conf.RTC.TCPPort = 0
	rtcConf, err := rtc.NewWebRTCConfig(conf)
	require.NoError(t, err)
	ff := buffer.NewFactoryOfBufferFactory(500, 200)
	rtcConf.SetBufferFactory(ff.CreateBufferFactory())

	codecs := make([]*livekit.Codec, 0, len(conf.Room.EnabledCodecs))
	for _, c := range conf.Room.EnabledCodecs {
		codecs = append(codecs, &livekit.Codec{Mime: c.Mime, FmtpLine: c.FmtpLine})
	}
	return rtcConf, codecs
}

func TestMediaRelayDialNode(t *testing.T) {
	relay := NewMediaRelay("127.0.0.1", reserveFreePort(t), 0, "test-relay-secret")
	require.NoError(t, relay.Start())
	t.Cleanup(func() { _ = relay.Stop() })
	require.NotNil(t, relay.Addr())

	// Accept in a goroutine: with auth enabled the dial blocks until the server
	// side of the handshake accepts.
	acceptedCh := make(chan transport.MediaChannel, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := relay.Accept()
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptedCh <- accepted
	}()

	// DialNode resolves node.Ip:<port> and dials it.
	dialed, err := relay.DialNode(&livekit.Node{Ip: "127.0.0.1"})
	require.NoError(t, err)
	defer dialed.Close()

	var accepted transport.MediaChannel
	select {
	case accepted = <-acceptedCh:
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	}
	defer accepted.Close()

	// RTP flows dialed → accepted.
	require.NoError(t, dialed.WriteRTP([]byte("rtp")))
	p, err := accepted.ReadRTP()
	require.NoError(t, err)
	require.Equal(t, []byte("rtp"), p)
}

// TestMediaRelayControlSession drives the edge role end-to-end: the relay's
// control accept loop runs a gateway session, and a dialing room node completes
// the setup handshake and drives the edge PC remotely.
func TestMediaRelayControlSession(t *testing.T) {
	rtcConf, codecs := testRTCConfig(t)

	relay := NewMediaRelay("127.0.0.1", reserveFreePort(t), reserveFreePort(t), "test-relay-secret")
	relay.SetRTCConfig(rtcConf)
	require.NoError(t, relay.Start())
	t.Cleanup(func() { _ = relay.Stop() })

	node := &livekit.Node{Ip: "127.0.0.1"}

	ch, err := relay.DialNodeControl(node)
	require.NoError(t, err)

	remote, err := rtc.DialEdgeGatewaySession(ch, rtc.GatewaySetup{
		SessionID:       "sess-1",
		PublishCodecs:   codecs,
		SubscribeCodecs: codecs,
	})
	require.NoError(t, err)

	// The control plane is live: state getters round-trip to the edge PC.
	require.Equal(t, webrtc.PeerConnectionStateNew, remote.ConnectionState())

	// The gateway is registered under the session ID for media routing.
	require.Eventually(t, func() bool {
		return relay.gatewayFor("sess-1", transport.MediaDirectionUp) != nil
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, remote.Close())
}

