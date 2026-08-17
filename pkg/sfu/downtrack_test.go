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

package sfu

import (
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/livekit/mediatransportutil/pkg/codec"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu/pacer"
)

type fakeDownTrackListener struct {
	codecNegotiated bool
}

func (f *fakeDownTrackListener) OnBindAndConnected()                    {}
func (f *fakeDownTrackListener) OnStatsUpdate(_ *livekit.AnalyticsStat) {}
func (f *fakeDownTrackListener) OnMaxSubscribedLayerChanged(_ int32)    {}
func (f *fakeDownTrackListener) OnRttUpdate(_ uint32)                   {}
func (f *fakeDownTrackListener) OnCodecNegotiated(webrtc.RTPCodecCapability) {
	f.codecNegotiated = true
}
func (f *fakeDownTrackListener) OnDownTrackClose(bool) {}
func (f *fakeDownTrackListener) OnStreamStarted()      {}

type fakeTrackReceiver struct {
	trackID livekit.TrackID
	codec   webrtc.RTPCodecParameters
}

func (f *fakeTrackReceiver) TrackID() livekit.TrackID         { return f.trackID }
func (f *fakeTrackReceiver) StreamID() string                 { return "" }
func (f *fakeTrackReceiver) Codec() webrtc.RTPCodecParameters { return f.codec }
func (f *fakeTrackReceiver) Mime() mime.MimeType              { return mime.MimeTypeOpus }
func (f *fakeTrackReceiver) VideoLayerMode() livekit.VideoLayer_Mode {
	return livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM
}
func (f *fakeTrackReceiver) HeaderExtensions() []webrtc.RTPHeaderExtensionParameter {
	return nil
}
func (f *fakeTrackReceiver) IsClosed() bool { return false }
func (f *fakeTrackReceiver) ReadRTP([]byte, uint8, uint64) (int, error) {
	return 0, nil
}
func (f *fakeTrackReceiver) GetLayeredBitrate() ([]int32, Bitrates)  { return nil, Bitrates{} }
func (f *fakeTrackReceiver) GetAudioLevel() (float64, bool)          { return 0, false }
func (f *fakeTrackReceiver) SendPLI(int32, bool)                     {}
func (f *fakeTrackReceiver) SetMaxExpectedSpatialLayer(int32)        {}
func (f *fakeTrackReceiver) AddDownTrack(TrackSender) error          { return nil }
func (f *fakeTrackReceiver) DeleteDownTrack(livekit.ParticipantID)   {}
func (f *fakeTrackReceiver) GetDownTracks() []TrackSender            { return nil }
func (f *fakeTrackReceiver) DebugInfo() map[string]any               { return nil }
func (f *fakeTrackReceiver) TrackInfo() *livekit.TrackInfo           { return nil }
func (f *fakeTrackReceiver) UpdateTrackInfo(*livekit.TrackInfo)      {}
func (f *fakeTrackReceiver) GetPrimaryReceiverForRed() TrackReceiver { return f }
func (f *fakeTrackReceiver) GetRedReceiver() TrackReceiver           { return f }
func (f *fakeTrackReceiver) GetTemporalLayerFpsForSpatial(int32) []float32 {
	return nil
}
func (f *fakeTrackReceiver) GetTrackStats() *livekit.RTPStats { return nil }
func (f *fakeTrackReceiver) AddOnReady(fn func())             { fn() }
func (f *fakeTrackReceiver) AddOnCodecStateChange(func(webrtc.RTPCodecParameters, ReceiverCodecState)) {
}
func (f *fakeTrackReceiver) CodecState() ReceiverCodecState { return ReceiverCodecState(0) }
func (f *fakeTrackReceiver) VideoSizes() []codec.VideoSize  { return nil }
func (f *fakeTrackReceiver) Restart(string)                 {}

type fakeRTPWriteStream struct {
	wrote []*rtp.Packet
}

func (f *fakeRTPWriteStream) WriteRTP(header *rtp.Header, payload []byte) (int, error) {
	f.wrote = append(f.wrote, &rtp.Packet{Header: *header, Payload: payload})
	return len(payload), nil
}

var _ pacer.RTPWriteStream = (*fakeRTPWriteStream)(nil)

func TestDownTrackBindRemote(t *testing.T) {
	codecParam := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000},
		PayloadType:        111,
	}
	receiver := &fakeTrackReceiver{trackID: "track-1", codec: codecParam}
	listener := &fakeDownTrackListener{}

	dt, err := NewDownTrack(DownTrackParams{
		Codecs:   []webrtc.RTPCodecParameters{codecParam},
		Receiver: receiver,
		Logger:   logger.GetLogger(),
		Listener: listener,
	})
	require.NoError(t, err)

	ws := &fakeRTPWriteStream{}
	_, err = dt.BindRemote(RemoteBindContext{
		NegotiatedCodecParameters: []webrtc.RTPCodecParameters{codecParam},
		SSRC:                      1234,
		SSRCRTX:                   5678,
		WriteStream:               ws,
	})
	require.NoError(t, err)

	require.Equal(t, uint32(1234), dt.ssrc)
	require.Equal(t, uint32(5678), dt.ssrcRTX)
	require.Equal(t, uint32(111), dt.payloadType.Load())
	require.Equal(t, bindStateBound, dt.bindState.Load())
	require.Same(t, ws, dt.writeStream)
	require.True(t, listener.codecNegotiated)
}
