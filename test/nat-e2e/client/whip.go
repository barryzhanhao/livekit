// WHIP (WebRTC-HTTP Ingestion Protocol) one-shot publish client.
//
// Drives the fork's one-shot signalling NAT path (UseOneShotSignallingMode):
// the client gathers ICE, POSTs a single SDP offer to /whip/v1 (RFC 9725),
// receives the answer, connects ICE/DTLS/SRTP, and publishes plaintext VP8.
// On the server side the edge node terminates the pion PC while the room node
// holds the SFU — one control channel + one gateway session per participant,
// the same "media follows signaling" split as the WS paths.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// whipPublish connects to the edge's WHIP endpoint, publishes fake VP8 RTP for
// duration, and returns. It errors if the offer/answer or ICE/DTLS fails.
func whipPublish(httpBase, token, room, identity string, duration time.Duration) error {
	pc, videoTrack, err := newWhipPC()
	if err != nil {
		return err
	}
	defer pc.Close()

	// One-shot signalling: gather ICE fully before sending the offer.
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	<-webrtc.GatheringCompletePromise(pc)
	// Use the gathered local description (not the pre-gather offer), so the SDP
	// sent to the server includes the ICE candidates.
	offer = *pc.LocalDescription()

	// POST the (candidate-complete) offer to the WHIP resource.
	req, err := http.NewRequest(http.MethodPost, httpBase+"/whip/v1", bytes.NewBufferString(offer.SDP))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-type", "application/sdp")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("whip POST failed: %s: %s", resp.Status, string(body))
	}
	answerSDP, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answerSDP)}); err != nil {
		return err
	}

	// Wait for the media transport to establish.
	connected := make(chan struct{})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		fmt.Printf("WHIP %s peer connection state: %s\n", identity, s)
		switch s {
		case webrtc.PeerConnectionStateConnected:
			close(connected)
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			select {
			case <-connected:
			default:
			}
		}
	})
	select {
	case <-connected:
	case <-time.After(20 * time.Second):
		return fmt.Errorf("whip peer connection did not connect (state %s)", pc.ConnectionState().String())
	}

	// Publish a few seconds of fake VP8 video (like the test client's writer).
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	seq := uint16(0)
	ts := uint32(0)
	ticker := time.NewTicker(20 * time.Millisecond) // 50fps
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			seq++
			ts += 3000 // 90kHz / 50fps
			pkt := &rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    96,
					SequenceNumber: seq,
					Timestamp:      ts,
					SSRC:           0xdeadbeef,
				},
				Payload: make([]byte, 100),
			}
			if err := videoTrack.WriteRTP(pkt); err != nil {
				return fmt.Errorf("whip write RTP: %w", err)
			}
		}
	}
}

// newWhipPC builds a pion PeerConnection preloaded with a fake VP8 video track.
func newWhipPC() (*webrtc.PeerConnection, *webrtc.TrackLocalStaticRTP, error) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, nil, err
	}
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: "video/vp8"}, "video", "video")
	if err != nil {
		return nil, nil, err
	}
	if _, err := pc.AddTrack(track); err != nil {
		return nil, nil, err
	}
	return pc, track, nil
}
