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

	"github.com/pion/rtp"

	"github.com/livekit/livekit-server/pkg/sfu/pacer"
)

// MediaChannelRTPWriter adapts a MediaChannel to the pacer.RTPWriteStream
// interface used by the SFU's DownTrack. It marshals the RTP header and payload
// into raw bytes and forwards them over the channel to the edge node, where
// they are written into a local pion TrackLocal and SRTP-encrypted for the
// subscriber. This is the NAT-mode (media follows signaling) down direction.
type MediaChannelRTPWriter struct {
	ch MediaChannel
}

var _ pacer.RTPWriteStream = (*MediaChannelRTPWriter)(nil)

func NewMediaChannelRTPWriter(ch MediaChannel) *MediaChannelRTPWriter {
	return &MediaChannelRTPWriter{ch: ch}
}

func (w *MediaChannelRTPWriter) WriteRTP(header *rtp.Header, payload []byte) (int, error) {
	pkt := rtp.Packet{
		Header:  *header,
		Payload: payload,
	}
	buf := make([]byte, pkt.MarshalSize())
	n, err := pkt.MarshalTo(buf)
	if err != nil {
		return 0, err
	}

	if err := w.ch.WriteRTP(buf[:n]); err != nil {
		return 0, err
	}
	return n, nil
}

// PumpRTP reads plaintext RTP from src and writes it into dst until src closes
// or dst fails. It is the NAT-mode up direction: on the room node, src is the
// MediaChannel carrying a published track from the edge node and dst is the
// SFU buffer.Buffer for that track.
func PumpRTP(src MediaChannel, dst io.Writer) {
	for {
		pkt, err := src.ReadRTP()
		if err != nil {
			return
		}
		if _, err := dst.Write(pkt); err != nil {
			return
		}
	}
}
