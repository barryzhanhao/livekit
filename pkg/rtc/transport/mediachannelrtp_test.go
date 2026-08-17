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

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func TestMediaChannelRTPWriterRoundTrip(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)
	defer a.Close()
	defer b.Close()

	w := NewMediaChannelRTPWriter(a)
	header := &rtp.Header{
		Version:        2,
		PayloadType:    96,
		SequenceNumber: 12345,
		Timestamp:      67890,
		SSRC:           0xdeadbeef,
	}
	payload := []byte{0xde, 0xad, 0xbe, 0xef}

	n, err := w.WriteRTP(header, payload)
	require.NoError(t, err)
	require.Greater(t, n, 0)

	raw, err := b.ReadRTP()
	require.NoError(t, err)

	var got rtp.Packet
	require.NoError(t, got.Unmarshal(raw))
	require.Equal(t, header.PayloadType, got.PayloadType)
	require.Equal(t, header.SequenceNumber, got.SequenceNumber)
	require.Equal(t, header.Timestamp, got.Timestamp)
	require.Equal(t, header.SSRC, got.SSRC)
	require.Equal(t, payload, got.Payload)
}

// TestMediaChannelRTPWriterPadding verifies the SFU-pacer padding translation:
// Padding=true with the padding already embedded in the payload (last byte =
// padding size) must be rewritten so pion Marshal re-adds it cleanly.
func TestMediaChannelRTPWriterPadding(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)
	defer a.Close()
	defer b.Close()

	w := NewMediaChannelRTPWriter(a)
	header := &rtp.Header{
		Version:        2,
		PayloadType:    96,
		SequenceNumber: 12345,
		Timestamp:      67890,
		SSRC:           0xdeadbeef,
		Padding:        true,
	}
	// 4 bytes of padding already embedded (0x00 0x00 0x00 0x04 = size byte last).
	payload := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x04}

	_, err := w.WriteRTP(header, payload)
	require.NoError(t, err)

	raw, err := b.ReadRTP()
	require.NoError(t, err)

	var got rtp.Packet
	require.NoError(t, got.Unmarshal(raw))
	require.True(t, got.Padding)
	require.Equal(t, uint8(4), got.PaddingSize)
	require.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, got.Payload)
}

// TestMediaChannelRTPWriterMalformedPadding verifies the malformed-padding guard:
// a size byte larger than the payload must clear the Padding flag so Marshal
// succeeds instead of failing with errInvalidRTPPadding for every such packet.
func TestMediaChannelRTPWriterMalformedPadding(t *testing.T) {
	a, b := NewLocalMediaChannelPair(10)
	defer a.Close()
	defer b.Close()

	w := NewMediaChannelRTPWriter(a)
	header := &rtp.Header{
		Version:        2,
		PayloadType:    96,
		SequenceNumber: 12345,
		Timestamp:      67890,
		SSRC:           0xdeadbeef,
		Padding:        true,
	}
	// Corrupt: the trailing size byte (0xff) claims more padding than exists.
	payload := []byte{0xde, 0xad, 0xff}

	_, err := w.WriteRTP(header, payload)
	require.NoError(t, err)

	raw, err := b.ReadRTP()
	require.NoError(t, err)

	var got rtp.Packet
	require.NoError(t, got.Unmarshal(raw))
	require.False(t, got.Padding)
	require.Equal(t, []byte{0xde, 0xad, 0xff}, got.Payload)
}
