package rtc

import (
	"testing"

	"github.com/pion/rtcp"
	"github.com/stretchr/testify/require"
)

// TestRewriteDownRTCPForLocalSSRC verifies the NAT-mode down-direction RTCP
// boundary rewrite: the edge's pion sender rewrites the RTP SSRC before SRTP, so
// the subscriber's NACK/PLI/RR target the EDGE SSRC; the room rewrites them to
// the internal DownTrack SSRC (whose per-track MediaChannel carries only this
// track's RTCP) so the DownTrack's `MediaSSRC == d.ssrc` filters fire.
func TestRewriteDownRTCPForLocalSSRC(t *testing.T) {
	const (
		edgeSSRC  = uint32(1111)
		localSSRC = uint32(2222)
	)

	// NACK (the critical retransmit path)
	nackData, err := rtcp.Marshal([]rtcp.Packet{&rtcp.TransportLayerNack{
		SenderSSRC: 3333,
		MediaSSRC:  edgeSSRC,
		Nacks:      []rtcp.NackPair{{PacketID: 100, LostPackets: 0x3}},
	}})
	require.NoError(t, err)

	out := rewriteDownRTCPForLocalSSRC(nackData, localSSRC)
	pkts, err := rtcp.Unmarshal(out)
	require.NoError(t, err)
	require.Len(t, pkts, 1)
	nack, ok := pkts[0].(*rtcp.TransportLayerNack)
	require.True(t, ok)
	require.Equal(t, localSSRC, nack.MediaSSRC, "NACK MediaSSRC must be rewritten to the local (room) SSRC")
	require.Equal(t, uint16(100), nack.Nacks[0].PacketID, "NACK payload must be preserved")

	// PLI + FIR (keyframe requests)
	pliData, err := rtcp.Marshal([]rtcp.Packet{&rtcp.PictureLossIndication{
		SenderSSRC: 3333,
		MediaSSRC:  edgeSSRC,
	}})
	require.NoError(t, err)
	pliPkts, err := rtcp.Unmarshal(rewriteDownRTCPForLocalSSRC(pliData, localSSRC))
	require.NoError(t, err)
	require.Equal(t, localSSRC, pliPkts[0].(*rtcp.PictureLossIndication).MediaSSRC)

	firData, err := rtcp.Marshal([]rtcp.Packet{&rtcp.FullIntraRequest{
		SenderSSRC: 3333,
		MediaSSRC:  edgeSSRC,
		FIR:        []rtcp.FIREntry{{SSRC: edgeSSRC, SequenceNumber: 1}},
	}})
	require.NoError(t, err)
	firPkts, err := rtcp.Unmarshal(rewriteDownRTCPForLocalSSRC(firData, localSSRC))
	require.NoError(t, err)
	require.Equal(t, localSSRC, firPkts[0].(*rtcp.FullIntraRequest).MediaSSRC)

	// Receiver report: the reported source SSRC is rewritten to the local SSRC
	rrData, err := rtcp.Marshal([]rtcp.Packet{&rtcp.ReceiverReport{
		SSRC: 3333,
		Reports: []rtcp.ReceptionReport{{
			SSRC:               edgeSSRC,
			FractionLost:       0,
			TotalLost:          0,
			LastSequenceNumber: 500,
			Jitter:             5,
		}},
	}})
	require.NoError(t, err)
	rrPkts, err := rtcp.Unmarshal(rewriteDownRTCPForLocalSSRC(rrData, localSSRC))
	require.NoError(t, err)
	rr := rrPkts[0].(*rtcp.ReceiverReport)
	require.Len(t, rr.Reports, 1)
	require.Equal(t, localSSRC, rr.Reports[0].SSRC, "receiver-report source SSRC must be rewritten")
	require.Equal(t, uint32(500), rr.Reports[0].LastSequenceNumber, "report payload must be preserved")

	// Malformed input must not panic (unmarshal may accept or reject it; either
	// way the function returns without crashing).
	bad := []byte{0x80, 0x01, 0xff, 0xff, 0x00}
	require.NotEmpty(t, rewriteDownRTCPForLocalSSRC(bad, localSSRC))
}
