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

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/livekit"
)

func newTestWebRTCConfig(t *testing.T) *WebRTCConfig {
	t.Helper()
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	conf.RTC.TCPPort = 0
	rtcConf, err := NewWebRTCConfig(conf)
	require.NoError(t, err)
	ff := buffer.NewFactoryOfBufferFactory(500, 200)
	rtcConf.SetBufferFactory(ff.CreateBufferFactory())
	return rtcConf
}

func defaultTestCodecs(t *testing.T) []*livekit.Codec {
	t.Helper()
	conf, err := config.NewConfig("", true, nil, nil)
	require.NoError(t, err)
	codecs := make([]*livekit.Codec, 0, len(conf.Room.EnabledCodecs))
	for _, c := range conf.Room.EnabledCodecs {
		codecs = append(codecs, &livekit.Codec{Mime: c.Mime, FmtpLine: c.FmtpLine})
	}
	return codecs
}

// TestEdgeGatewaySessionHandshake drives the room->edge session establishment
// over TCP: the room sends a GatewaySetup, the edge builds a real pion PC +
// MediaGateway, and the returned remotePeerConnection drives the edge PC. It also
// attaches a down track over a hello-framed media channel.
func TestEdgeGatewaySessionHandshake(t *testing.T) {
	rtcConf := newTestWebRTCConfig(t)
	codecs := defaultTestCodecs(t)

	controlLn, err := transport.ListenTCPControlChannel("127.0.0.1:0", "test-relay-secret")
	require.NoError(t, err)
	defer controlLn.Close()

	mediaLn, err := transport.ListenTCPMediaChannel("127.0.0.1:0", "test-relay-secret")
	require.NoError(t, err)
	defer mediaLn.Close()

	gwCh := make(chan *transport.MediaGateway, 1)
	go func() {
		ch, err := controlLn.Accept()
		if err != nil {
			gwCh <- nil
			return
		}
		gw, _, err := RunEdgeGatewaySession(ch, rtcConf, nil)
		gwCh <- gw

		mch, err := mediaLn.AcceptHello()
		if err != nil {
			return
		}
		if gw != nil {
			_ = gw.AttachHello(mch, nil)
		}
	}()

	// --- room node ---
	roomCh, err := transport.DialTCPControlChannel(controlLn.Addr().String(), "test-relay-secret")
	require.NoError(t, err)

	remote, err := DialEdgeGatewaySession(roomCh, GatewaySetup{
		PublishCodecs:   codecs,
		SubscribeCodecs: codecs,
	})
	require.NoError(t, err)

	// The control plane is live: state getters round-trip to the edge PC.
	require.Equal(t, webrtc.PeerConnectionStateNew, remote.ConnectionState())
	require.Equal(t, webrtc.SignalingStateStable, remote.SignalingState())

	// Media plane: dial a down track and attach it via the hello frame.
	mediaCh, err := transport.DialTCPMediaChannelHello(mediaLn.Addr().String(), transport.MediaHello{
		TrackID:   "track-down",
		Direction: transport.MediaDirectionDown,
		Codec:     webrtc.RTPCodecCapability{MimeType: "video/vp8", ClockRate: 90000},
	}, "test-relay-secret")
	require.NoError(t, err)
	defer mediaCh.Close()

	gw := <-gwCh
	require.NotNil(t, gw)
	require.Eventually(t, func() bool {
		return len(gw.PeerConnection().GetSenders()) == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Close tears down the edge PC over TCP.
	require.NoError(t, remote.Close())
	require.Eventually(t, func() bool {
		return gw.PeerConnection().ConnectionState() == webrtc.PeerConnectionStateClosed
	}, 2*time.Second, 10*time.Millisecond)
}
