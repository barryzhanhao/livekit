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

package utils

import (
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

func TestExtractHeaderExtensionsFromSDP(t *testing.T) {
	sdpStr := `v=0
o=- 1 2 IN IP4 127.0.0.1
s=-
t=0 0
a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level
a=extmap:3/sendrecv http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time
m=audio 9 UDP/TLS/RTP/SAVPF 111
a=extmap:2 urn:ietf:params:rtp-hdrext:sdes:mid
`
	sd := &webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdpStr}
	exts := ExtractHeaderExtensionsFromSDP(sd)

	byURI := make(map[string]webrtc.RTPHeaderExtensionParameter)
	for _, e := range exts {
		byURI[e.URI] = e
	}
	require.Len(t, exts, 3)
	require.Equal(t, 1, byURI["urn:ietf:params:rtp-hdrext:ssrc-audio-level"].ID)
	require.Equal(t, 3, byURI["http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time"].ID)
	require.Equal(t, 2, byURI["urn:ietf:params:rtp-hdrext:sdes:mid"].ID)

	require.Nil(t, ExtractHeaderExtensionsFromSDP(nil))
}
