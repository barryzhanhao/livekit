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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
)

// Do a fuzzy find for a codec in the list of codecs
// Used for lookup up a codec in an existing list to find a match
func CodecParametersFuzzySearch(needle webrtc.RTPCodecParameters, haystack []webrtc.RTPCodecParameters) (webrtc.RTPCodecParameters, error) {
	// First attempt to match on MimeType + SDPFmtpLine
	for _, c := range haystack {
		if mime.IsMimeTypeStringEqual(c.RTPCodecCapability.MimeType, needle.RTPCodecCapability.MimeType) &&
			c.RTPCodecCapability.SDPFmtpLine == needle.RTPCodecCapability.SDPFmtpLine {
			return c, nil
		}
	}

	// Fallback to just MimeType
	for _, c := range haystack {
		if mime.IsMimeTypeStringEqual(c.RTPCodecCapability.MimeType, needle.RTPCodecCapability.MimeType) {
			return c, nil
		}
	}

	return webrtc.RTPCodecParameters{}, webrtc.ErrCodecNotFound
}

// Given a CodecParameters find the RTX CodecParameters if one exists
func FindRTXPayloadType(needle webrtc.PayloadType, haystack []webrtc.RTPCodecParameters) webrtc.PayloadType {
	aptStr := fmt.Sprintf("apt=%d", needle)
	for _, c := range haystack {
		if aptStr == c.SDPFmtpLine {
			return c.PayloadType
		}
	}

	return webrtc.PayloadType(0)
}

// GetHeaderExtensionID returns the ID of a header extension, or 0 if not found
func GetHeaderExtensionID(extensions []interceptor.RTPHeaderExtension, extension webrtc.RTPHeaderExtensionCapability) int {
	for _, h := range extensions {
		if extension.URI == h.URI {
			return h.ID
		}
	}
	return 0
}

// ExtractHeaderExtensionsFromSDP parses a=extmap attributes from an SDP
// (session and media level) into negotiated RTP header extension parameters. It
// is used in NAT mode where the room node has no pion RTPReceiver to query, so
// it reconstructs the negotiated extensions from its own remote description.
func ExtractHeaderExtensionsFromSDP(sd *webrtc.SessionDescription) []webrtc.RTPHeaderExtensionParameter {
	if sd == nil {
		return nil
	}
	parsed, err := sd.Unmarshal()
	if err != nil {
		return nil
	}

	byID := make(map[int]webrtc.RTPHeaderExtensionParameter)
	add := func(attrs []sdp.Attribute) {
		for _, a := range attrs {
			if a.Key != "extmap" {
				continue
			}
			// value is "<id> <URI>" or "<id>/<direction> <URI>"
			parts := strings.SplitN(a.Value, " ", 2)
			if len(parts) != 2 {
				continue
			}
			idStr := parts[0]
			if idx := strings.IndexByte(idStr, '/'); idx >= 0 {
				idStr = idStr[:idx]
			}
			id, err := strconv.Atoi(idStr)
			if err != nil {
				continue
			}
			byID[id] = webrtc.RTPHeaderExtensionParameter{URI: parts[1], ID: id}
		}
	}
	add(parsed.Attributes)
	for _, m := range parsed.MediaDescriptions {
		add(m.Attributes)
	}

	extensions := make([]webrtc.RTPHeaderExtensionParameter, 0, len(byID))
	for _, ext := range byID {
		extensions = append(extensions, ext)
	}
	return extensions
}

var (
	ErrInvalidRTPVersion      = errors.New("invalid RTP version")
	ErrRTPPayloadTypeMismatch = errors.New("RTP payload type mismatch")
	ErrRTPSSRCMismatch        = errors.New("RTP SSRC mismatch")
)

// ValidateRTPPacket checks for a valid RTP packet and returns an error if fields are incorrect
func ValidateRTPPacket(pkt *rtp.Packet, expectedPayloadType uint8, expectedSSRC uint32) error {
	if pkt.Version != 2 {
		return fmt.Errorf("%w, expected: 2, actual: %d", ErrInvalidRTPVersion, pkt.Version)
	}

	if expectedPayloadType != 0 && pkt.PayloadType != expectedPayloadType {
		return fmt.Errorf("%w, expected: %d, actual: %d", ErrRTPPayloadTypeMismatch, expectedPayloadType, pkt.PayloadType)
	}

	if expectedSSRC != 0 && pkt.SSRC != expectedSSRC {
		return fmt.Errorf("%w, expected: %d, actual: %d", ErrRTPSSRCMismatch, expectedSSRC, pkt.SSRC)
	}

	return nil
}

func IsSimulcastMode(m livekit.VideoLayer_Mode) bool {
	return m == livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM || m == livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM_INCOMPLETE_RTCP_SR
}
