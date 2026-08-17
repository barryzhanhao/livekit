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

	"github.com/stretchr/testify/require"
)

// newTCPPair returns the two ends of an established TCP MediaChannel.
func newTCPPair(t *testing.T) (MediaChannel, MediaChannel) {
	t.Helper()

	ln, err := ListenTCPMediaChannel("127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	type result struct {
		dialed MediaChannel
		err    error
	}
	dialedCh := make(chan result, 1)
	go func() {
		dialed, err := DialTCPMediaChannel(ln.Addr().String())
		dialedCh <- result{dialed, err}
	}()

	accepted, err := ln.Accept()
	require.NoError(t, err)
	dialed := <-dialedCh
	require.NoError(t, dialed.err)

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
