// WHIP (WebRTC-HTTP Ingestion Protocol) one-shot publish client.
//
// Drives the fork's one-shot signalling NAT path (UseOneShotSignallingMode):
// the client gathers ICE, POSTs a single SDP offer to /whip/v1 (RFC 9725),
// receives the answer, connects ICE/DTLS/SRTP, and publishes plaintext VP8.
// On the server side the edge node terminates the pion PC while the room node
// holds the SFU — one control channel + one gateway session per participant,
// the same "media follows signaling" split as the WS paths.
//
// Beyond the basic POST (scenarioWhip), this file also drives the WHIP session
// lifecycle endpoints: PATCH (ICE restart, If-Match: *) and DELETE (teardown) —
// exercising whipservice.handleParticipantPatch / handleParticipantDelete.
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

// whipSession wraps a connected WHIP ingest session (one-shot signalling) with
// the HTTP resource path and ICE ETag from the POST response, so the PATCH
// (ICE restart) and DELETE (teardown) lifecycle endpoints can be driven.
type whipSession struct {
	httpBase string
	token    string
	identity string
	pc       *webrtc.PeerConnection
	video    *webrtc.TrackLocalStaticRTP
	resource string // e.g. /whip/v1/PA_xxx (from the POST Location header)
	etag     string // ICE session id (from the POST ETag header)
}

// newWhipSession performs the one-shot POST and waits for the answer; the caller
// must close s.pc when done.
func newWhipSession(httpBase, token, identity string) (*whipSession, error) {
	pc, videoTrack, err := newWhipPC()
	if err != nil {
		return nil, err
	}

	// One-shot signalling: gather ICE fully before sending the offer.
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		pc.Close()
		return nil, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		pc.Close()
		return nil, err
	}
	<-webrtc.GatheringCompletePromise(pc)
	// Use the gathered local description (not the pre-gather offer), so the SDP
	// sent to the server includes the ICE candidates.
	offer = *pc.LocalDescription()

	// POST the (candidate-complete) offer to the WHIP resource.
	req, err := http.NewRequest(http.MethodPost, httpBase+"/whip/v1", bytes.NewBufferString(offer.SDP))
	if err != nil {
		pc.Close()
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-type", "application/sdp")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		pc.Close()
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		pc.Close()
		return nil, fmt.Errorf("whip POST failed: %s: %s", resp.Status, string(body))
	}
	answerSDP, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		pc.Close()
		return nil, err
	}

	session := &whipSession{
		httpBase: httpBase,
		token:    token,
		identity: identity,
		pc:       pc,
		video:    videoTrack,
		resource: resp.Header.Get("Location"),
		etag:     resp.Header.Get("ETag"),
	}

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answerSDP)}); err != nil {
		pc.Close()
		return nil, err
	}
	return session, nil
}

// waitConnected blocks until the peer connection reaches Connected (or fails).
func (s *whipSession) waitConnected(timeout time.Duration) error {
	connected := make(chan struct{}, 1)
	s.pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		fmt.Printf("WHIP %s peer connection state: %s\n", s.identity, st)
		switch st {
		case webrtc.PeerConnectionStateConnected:
			select {
			case connected <- struct{}{}:
			default:
			}
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			select {
			case connected <- struct{}{}:
			default:
			}
		}
	})
	select {
	case <-connected:
		if s.pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
			return fmt.Errorf("whip peer connection failed (state %s)", s.pc.ConnectionState().String())
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("whip peer connection did not connect (state %s)", s.pc.ConnectionState().String())
	}
}

// publishVideo writes fake VP8 RTP on the session's track for duration.
func (s *whipSession) publishVideo(duration time.Duration) error {
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
			if err := s.video.WriteRTP(pkt); err != nil {
				return fmt.Errorf("whip write RTP: %w", err)
			}
		}
	}
}

// iceRestart performs an HTTP PATCH with If-Match: * (ICE restart per RFC 9725):
// the client re-negotiates with fresh ICE credentials, the server applies the
// restart on the edge PC, and answers with its own SDP fragment.
func (s *whipSession) iceRestart() error {
	offer, err := s.pc.CreateOffer(&webrtc.OfferOptions{ICERestart: true})
	if err != nil {
		return err
	}
	if err := s.pc.SetLocalDescription(offer); err != nil {
		return err
	}
	<-webrtc.GatheringCompletePromise(s.pc)
	offer = *s.pc.LocalDescription()

	req, err := http.NewRequest(http.MethodPatch, s.httpBase+s.resource, bytes.NewBufferString(offer.SDP))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-type", "application/trickle-ice-sdpfrag")
	req.Header.Set("If-Match", "*")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("whip PATCH failed: %s: %s", resp.Status, string(body))
	}
	frag, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(frag) == 0 {
		return nil // no fragment: the server accepted the restart without a reply
	}
	if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(frag)}); err != nil {
		return err
	}
	return nil
}

// deleteSession tears the WHIP session down via HTTP DELETE.
func (s *whipSession) deleteSession() error {
	req, err := http.NewRequest(http.MethodDelete, s.httpBase+s.resource, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("whip DELETE failed: %s: %s", resp.Status, string(body))
	}
	return nil
}

// whipPublish connects to the edge's WHIP endpoint, publishes fake VP8 RTP for
// duration, and returns. It errors if the offer/answer or ICE/DTLS fails.
func whipPublish(httpBase, token, room, identity string, duration time.Duration) error {
	_ = room // unused: the token carries the room
	s, err := newWhipSession(httpBase, token, identity)
	if err != nil {
		return err
	}
	defer s.pc.Close()
	if err := s.waitConnected(20 * time.Second); err != nil {
		return err
	}
	return s.publishVideo(duration)
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
