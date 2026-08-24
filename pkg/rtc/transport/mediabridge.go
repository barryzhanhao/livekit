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

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/logger"
)

// rtpPacketWriter is the narrow sink for outbound RTP packets on the edge node.
// webrtc.TrackLocalStaticRTP satisfies it: a MediaChannel-backed track receives
// RTP from the room node's DownTrack and writes it into the local pion PC.
type rtpPacketWriter interface {
	WriteRTP(p *rtp.Packet) error
}

// rtpPacketReader is the narrow source for inbound RTP packets on the edge node.
// webrtc.TrackRemote satisfies it.
type rtpPacketReader interface {
	ReadRTP() (*rtp.Packet, interceptor.Attributes, error)
}

// pumpMediaChannelToTrackLocal reads plaintext RTP from src (a MediaChannel carrying
// a subscribed track from the room node) and writes it into dst (a TrackLocalStaticRTP
// bound to the edge node's pion PeerConnection). This is the NAT-mode down direction
// on the edge node: media flows SFU → MediaChannel → edge pion PC → subscriber.
func pumpMediaChannelToTrackLocal(src MediaChannel, dst rtpPacketWriter) {
	for {
		raw, err := src.ReadRTP()
		if err != nil {
			return
		}

		prometheus.AddNATRelayBytes(prometheus.NATRelayDirectionInbound, prometheus.NATRelayFlowRTP, len(raw))

		var pkt rtp.Packet
		if err := pkt.Unmarshal(raw); err != nil {
			continue
		}
		if err := dst.WriteRTP(&pkt); err != nil {
			logger.Warnw("nat edge down write rtp failed", err, "ssrc", pkt.SSRC, "ts", pkt.Timestamp, "sn", pkt.SequenceNumber, "len", len(raw))
			return
		}
	}
}

// pumpTrackToMediaChannel reads plaintext RTP from src (a TrackRemote on the edge
// node's pion PeerConnection) and writes it into dst (a MediaChannel to the room node).
// This is the NAT-mode up direction on the edge node: media flows publisher → edge
// pion PC → MediaChannel → SFU.
func pumpTrackToMediaChannel(src rtpPacketReader, dst MediaChannel) {
	for {
		pkt, _, err := src.ReadRTP()
		if err != nil {
			return
		}

		buf := make([]byte, pkt.MarshalSize())
		n, err := pkt.MarshalTo(buf)
		if err != nil {
			continue
		}
		prometheus.AddNATRelayBytes(prometheus.NATRelayDirectionOutbound, prometheus.NATRelayFlowRTP, n)
		if err := dst.WriteRTP(buf[:n]); err != nil {
			return
		}
	}
}

// pumpSenderRTCPToMediaChannel reads RTCP feedback (NACK/PLI/SR/RR) for a down
// (subscriber) sender from the edge node's pion PC and writes it into the
// MediaChannel's RTCP direction, so the room node's DownTrack can process it.
// This is the NAT-mode down-direction RTCP path (client → edge → room).
func pumpSenderRTCPToMediaChannel(sender *webrtc.RTPSender, dst MediaChannel) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		data, err := rtcp.Marshal(pkts)
		if err != nil {
			continue
		}
		prometheus.AddNATRelayBytes(prometheus.NATRelayDirectionOutbound, prometheus.NATRelayFlowRTCP, len(data))
		logger.Debugw("nat edge -> room down RTCP forwarded", "pkts", len(pkts), "types", rtcpTypes(pkts))
		if err := dst.WriteRTCP(data); err != nil {
			return
		}
	}
}

// rtcpSimulcastReader is the narrow source for inbound RTCP on the edge node.
// webrtc.RTPReceiver satisfies it. ReadSimulcast (not ReadRTCP) is required:
// simulcast layers share one RTPReceiver and pion's Read()/ReadRTCP() panics on
// a multi-track receiver.
type rtcpSimulcastReader interface {
	ReadSimulcast(b []byte, rid string) (n int, a interceptor.Attributes, err error)
}

// pumpReceiverRTCPToMediaChannel reads RTCP for a publisher's layer (SR/RR/NACK
// sent by the client, demuxed by RID) and writes it into the up-direction
// MediaChannel bound for the room node. This is the NAT-mode up-direction RTCP
// path (publisher → edge → room). It mirrors the down-direction sender pump
// (pumpSenderRTCPToMediaChannel) and is what lets the SFU learn each simulcast
// layer's RTCP Sender Report for cross-layer timestamp alignment.
func pumpReceiverRTCPToMediaChannel(receiver rtcpSimulcastReader, rid string, dst MediaChannel) {
	buf := make([]byte, 65535)
	for {
		n, _, err := receiver.ReadSimulcast(buf, rid)
		if err != nil {
			return
		}
		prometheus.AddNATRelayBytes(prometheus.NATRelayDirectionOutbound, prometheus.NATRelayFlowRTCP, n)
		if pkts, uerr := rtcp.Unmarshal(buf[:n]); uerr == nil {
			logger.Debugw("nat edge -> room up RTCP forwarded", "pkts", len(pkts), "types", rtcpTypes(pkts))
		}
		if err := dst.WriteRTCP(buf[:n]); err != nil {
			return
		}
	}
}

// pumpMediaChannelRTCPToPC reads RTCP feedback from the room node's receiver
// (up direction) and writes it to the edge node's pion PC, which sends it to the
// publisher (NACK/PLI to request retransmits/keyframes).
func pumpMediaChannelRTCPToPC(src MediaChannel, pc *webrtc.PeerConnection) {
	for {
		data, err := src.ReadRTCP()
		if err != nil {
			return
		}
		prometheus.AddNATRelayBytes(prometheus.NATRelayDirectionInbound, prometheus.NATRelayFlowRTCP, len(data))
		pkts, err := rtcp.Unmarshal(data)
		if err != nil {
			continue
		}
		logger.Debugw("nat room -> edge up RTCP forwarded", "pkts", len(pkts), "types", rtcpTypes(pkts))
		if err := pc.WriteRTCP(pkts); err != nil {
			return
		}
	}
}

// rtcpTypes summarizes the packet type names of an RTCP batch for logging.
func rtcpTypes(pkts []rtcp.Packet) []string {
	types := make([]string, 0, len(pkts))
	for _, p := range pkts {
		types = append(types, rtcpTypeName(p))
	}
	return types
}

func rtcpTypeName(p rtcp.Packet) string {
	switch p.(type) {
	case *rtcp.PictureLossIndication:
		return "pli"
	case *rtcp.FullIntraRequest:
		return "fir"
	case *rtcp.ReceiverEstimatedMaximumBitrate:
		return "remb"
	case *rtcp.TransportLayerNack:
		return "nack"
	case *rtcp.ReceiverReport:
		return "rr"
	case *rtcp.SenderReport:
		return "sr"
	case *rtcp.SourceDescription:
		return "sdes"
	case *rtcp.Goodbye:
		return "bye"
	case *rtcp.ExtendedReport:
		return "xr"
	case *rtcp.TransportLayerCC:
		return "twcc"
	default:
		return fmt.Sprintf("type%T", p)
	}
}
