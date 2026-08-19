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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
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

func TestPumpReceiverRTCPToMediaChannel(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)
	defer a.Close()
	defer b.Close()

	sr := &rtcp.SenderReport{
		SSRC:        0xbeefdead,
		NTPTime:     0x12345678,
		RTPTime:     0x1000,
		PacketCount: 42,
		OctetCount:  9000,
	}
	marshalled, err := rtcp.Marshal([]rtcp.Packet{sr})
	require.NoError(t, err)
	rcv := &fakeSimulcastRTCPReader{rid: "h", batches: [][]byte{marshalled}}
	go pumpReceiverRTCPToMediaChannel(rcv, "h", a)

	raw, err := b.ReadRTCP()
	require.NoError(t, err)

	pkts, err := rtcp.Unmarshal(raw)
	require.NoError(t, err)
	require.Len(t, pkts, 1)
	got, ok := pkts[0].(*rtcp.SenderReport)
	require.True(t, ok)
	require.Equal(t, sr.SSRC, got.SSRC)
	require.Equal(t, sr.NTPTime, got.NTPTime)
	require.Equal(t, sr.RTPTime, got.RTPTime)
	require.Equal(t, sr.PacketCount, got.PacketCount)
	require.Equal(t, sr.OctetCount, got.OctetCount)
}

// fakeSimulcastRTCPReader delivers fixed RTCP batches for a rid then EOF,
// satisfying rtcpSimulcastReader.
type fakeSimulcastRTCPReader struct {
	rid     string
	batches [][]byte
	idx     int
}

func (f *fakeSimulcastRTCPReader) ReadSimulcast(b []byte, rid string) (int, interceptor.Attributes, error) {
	if rid != f.rid {
		return 0, nil, fmt.Errorf("wrong rid %q", rid)
	}
	if f.idx >= len(f.batches) {
		return 0, nil, io.EOF
	}
	bb := f.batches[f.idx]
	f.idx++
	return copy(b, bb), nil, nil
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
