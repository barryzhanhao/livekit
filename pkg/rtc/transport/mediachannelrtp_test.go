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
