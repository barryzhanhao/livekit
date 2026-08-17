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

package transport

import (
	"io"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

type fakeRTPWriter struct {
	packets chan *rtp.Packet
}

func (f *fakeRTPWriter) WriteRTP(p *rtp.Packet) error {
	f.packets <- p
	return nil
}

type fakeRTPReader struct {
	packets []*rtp.Packet
	idx     int
}

func (f *fakeRTPReader) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	if f.idx >= len(f.packets) {
		return nil, nil, io.EOF
	}
	p := f.packets[f.idx]
	f.idx++
	return p, nil, nil
}

func testPacket(t *testing.T, seq uint16, ssrc uint32, payload []byte) *rtp.Packet {
	t.Helper()
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      0x12345678,
			SSRC:           ssrc,
		},
		Payload: payload,
	}
}

func TestPumpMediaChannelToTrackLocal(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)
	defer a.Close()
	defer b.Close()

	w := &fakeRTPWriter{packets: make(chan *rtp.Packet, 1)}
	go pumpMediaChannelToTrackLocal(a, w)

	pkt := testPacket(t, 100, 0xdeadbeef, []byte{0xde, 0xad})
	raw, err := pkt.Marshal()
	require.NoError(t, err)
	require.NoError(t, b.WriteRTP(raw))

	select {
	case got := <-w.packets:
		require.Equal(t, pkt.SequenceNumber, got.SequenceNumber)
		require.Equal(t, pkt.SSRC, got.SSRC)
		require.Equal(t, pkt.Payload, got.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RTP packet")
	}
}

func TestPumpTrackToMediaChannel(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)
	defer a.Close()
	defer b.Close()

	pkt := testPacket(t, 200, 0xbeefdead, []byte{0x01, 0x02, 0x03})
	r := &fakeRTPReader{packets: []*rtp.Packet{pkt}}
	go pumpTrackToMediaChannel(r, a)

	raw, err := b.ReadRTP()
	require.NoError(t, err)

	var got rtp.Packet
	require.NoError(t, got.Unmarshal(raw))
	require.Equal(t, pkt.SequenceNumber, got.SequenceNumber)
	require.Equal(t, pkt.SSRC, got.SSRC)
	require.Equal(t, pkt.Payload, got.Payload)
}
