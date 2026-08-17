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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalMediaChannelBidirectional(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)

	// RTP flows from the media source (a) to the sink (b).
	require.NoError(t, a.WriteRTP([]byte("rtp1")))
	p, err := b.ReadRTP()
	require.NoError(t, err)
	require.Equal(t, []byte("rtp1"), p)

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

func TestLocalMediaChannelClose(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)

	require.NoError(t, a.Close())

	// Reads on the peer fail after close.
	_, err := b.ReadRTP()
	require.ErrorIs(t, err, ErrMediaChannelClosed)
	_, err = b.ReadRTCP()
	require.ErrorIs(t, err, ErrMediaChannelClosed)

	// Writes on both ends fail after close.
	require.ErrorIs(t, a.WriteRTP([]byte("x")), ErrMediaChannelClosed)
	require.ErrorIs(t, b.WriteRTCP([]byte("y")), ErrMediaChannelClosed)
}
