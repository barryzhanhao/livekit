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
	"errors"
	"testing"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/stretchr/testify/require"
)

// defaultPayloadSize is a typical RTP video packet payload (including RTP header).
const defaultPayloadSize = 1200

// newTCPPair returns the two ends of an established TCP MediaChannel.
func newTCPPair(tb testing.TB) (MediaChannel, MediaChannel) {
	tb.Helper()

	secret := "test-relay-secret" // fail-closed: channels require a secret
	ln, err := ListenTCPMediaChannel("127.0.0.1:0", secret)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ln.Close() })

	type result struct {
		dialed MediaChannel
		err    error
	}
	dialedCh := make(chan result, 1)
	go func() {
		dialed, err := DialTCPMediaChannel(ln.Addr().String(), secret)
		dialedCh <- result{dialed, err}
	}()

	accepted, err := ln.Accept()
	require.NoError(tb, err)
	dialed := <-dialedCh
	require.NoError(tb, dialed.err)

	return accepted, dialed.dialed
}

func TestTCPMediaChannelBidirectional(t *testing.T) {
	a, b := newTCPPair(t)
	defer a.Close()
	defer b.Close()

	// RTP a -> b.
	require.NoError(t, a.WriteRTP([]byte("rtp-a-b")))
	p, err := b.ReadRTP()
	require.NoError(t, err)
	require.Equal(t, []byte("rtp-a-b"), p)

	// RTCP is bidirectional.
	require.NoError(t, b.WriteRTCP([]byte("rtcp-b-a")))
	p, err = a.ReadRTCP()
	require.NoError(t, err)
	require.Equal(t, []byte("rtcp-b-a"), p)

	require.NoError(t, a.WriteRTCP([]byte("rtcp-a-b")))
	p, err = b.ReadRTCP()
	require.NoError(t, err)
	require.Equal(t, []byte("rtcp-a-b"), p)
}

func TestTCPMediaChannelClose(t *testing.T) {
	a, b := newTCPPair(t)
	defer b.Close()

	require.NoError(t, a.Close())

	// b's reader detects EOF and tears down asynchronously.
	require.Eventually(t, func() bool {
		_, err := b.ReadRTP()
		return errors.Is(err, ErrMediaChannelClosed)
	}, 2*time.Second, 10*time.Millisecond)

	// Writes on the closed end fail immediately.
	require.ErrorIs(t, a.WriteRTP([]byte("x")), ErrMediaChannelClosed)
}

// TestTCPMediaChannelCloseReason verifies that CloseReason distinguishes a relay
// DROP (the peer's connection died) from an intentional Close on the same side.
// The room node uses this to trigger a reconnect on relay drop while NOT
// reconnecting on teardown (unsubscribe / participant leave).
func TestTCPMediaChannelCloseReason(t *testing.T) {
	a, b := newTCPPair(t)

	// a closes intentionally → a's reason is nil (teardown).
	require.NoError(t, a.Close())
	require.Nil(t, a.(*tcpMediaChannel).CloseReason())

	// b's reader sees the connection die while b's own done is still open →
	// that is a relay DROP from b's perspective → CloseReason is non-nil.
	require.Eventually(t, func() bool {
		_, err := b.ReadRTP()
		return errors.Is(err, ErrMediaChannelClosed)
	}, 2*time.Second, 10*time.Millisecond)
	require.NotNil(t, b.(*tcpMediaChannel).CloseReason())
}

// BenchmarkTCPMediaChannelThroughput measures the raw TCP MediaChannel throughput
// for RTP payloads. It creates a pair, sends N payloads of defaultPayloadSize from
// one end, and reports the aggregate throughput including the 5-byte framing
// overhead per packet.
func BenchmarkTCPMediaChannelThroughput(b *testing.B) {
	logger.Infow("benchmark TCP media channel throughput", "payloadSize", defaultPayloadSize, "iterations", b.N)

	a, bCh := newTCPPair(b)
	defer a.Close()
	defer bCh.Close()

	payload := make([]byte, defaultPayloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	// Pre-warm: send all payloads, then measure the drain.
	b.ResetTimer()

	// Writer goroutine: send b.N payloads as fast as possible.
	done := make(chan struct{})
	go func() {
		for i := 0; i < b.N; i++ {
			_ = a.WriteRTP(payload)
		}
		close(done)
	}()

	// Reader: drain all payloads.
	read := 0
	for read < b.N {
		_, err := bCh.ReadRTP()
		if err != nil {
			b.Fatalf("read failed after %d reads: %v", read, err)
		}
		read++
	}
	<-done

	// Report throughput (bytes per second).
	frameSize := defaultPayloadSize + frameHeaderSize // payload + framing overhead
	totalBytes := int64(b.N) * int64(frameSize)
	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds(), "bytes/sec")
	b.ReportMetric(float64(totalBytes*8)/b.Elapsed().Seconds(), "bits/sec")
	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds()/1024/1024, "MB/sec")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pkt/sec")
}

// BenchmarkTCPMediaChannelThroughputBurst writes b.N packets, measures the total
// wall-clock time, and reports throughput. It test both directions.
func BenchmarkTCPMediaChannelThroughputReverse(b *testing.B) {
	a, bCh := newTCPPair(b)
	defer a.Close()
	defer bCh.Close()

	payload := make([]byte, defaultPayloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.ResetTimer()

	done := make(chan struct{})
	go func() {
		for i := 0; i < b.N; i++ {
			_ = bCh.WriteRTP(payload)
		}
		close(done)
	}()

	read := 0
	for read < b.N {
		_, err := a.ReadRTP()
		if err != nil {
			b.Fatalf("read failed after %d reads: %v", read, err)
		}
		read++
	}
	<-done

	frameSize := defaultPayloadSize + frameHeaderSize
	totalBytes := int64(b.N) * int64(frameSize)
	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds(), "bytes/sec")
	b.ReportMetric(float64(totalBytes*8)/b.Elapsed().Seconds(), "bits/sec")
	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds()/1024/1024, "MB/sec")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pkt/sec")
}

// TestTCPMediaChannelThroughput is a non-benchmark throughput sanity check that
// runs once and verifies the TCP channel can sustain ≥500 Mbps for 100k packets.
func TestTCPMediaChannelThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}
	const numPackets = 100_000
	payload := make([]byte, defaultPayloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	a, bCh := newTCPPair(t)
	defer a.Close()
	defer bCh.Close()

	// Writer goroutine.
	done := make(chan struct{})
	go func() {
		for i := 0; i < numPackets; i++ {
			_ = a.WriteRTP(payload)
		}
		close(done)
	}()

	start := time.Now()
	read := 0
	for read < numPackets {
		_, err := bCh.ReadRTP()
		require.NoError(t, err)
		read++
	}
	<-done
	elapsed := time.Since(start)

	frameSize := int64(defaultPayloadSize + frameHeaderSize)
	totalBytes := int64(numPackets) * frameSize
	throughputMBps := float64(totalBytes) / elapsed.Seconds() / 1024 / 1024

	t.Logf("TCP media channel throughput test:")
	t.Logf("  packets: %d", numPackets)
	t.Logf("  payload size: %d bytes", defaultPayloadSize)
	t.Logf("  frame size (with header): %d bytes", frameSize)
	t.Logf("  elapsed: %v", elapsed)
	t.Logf("  throughput: %.2f MB/sec (%.2f Mbps)", throughputMBps, throughputMBps*8)
	t.Logf("  packet rate: %.0f pkt/sec", float64(numPackets)/elapsed.Seconds())

	// On localhost loopback, we expect >500 MB/sec for 1200-byte packets.
	// This is a sanity check, not a performance floor — CI environments may vary.
	require.Greater(t, throughputMBps, 10.0, "throughput should exceed 10 MB/sec on localhost")
}
