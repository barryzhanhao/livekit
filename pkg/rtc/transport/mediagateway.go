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
	"errors"
	"fmt"
	"sync"

	"github.com/pion/webrtc/v4"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// MediaGateway is the edge-node component of NAT mode ("media follows
// signaling"). It owns the actual pion PeerConnection (ICE/DTLS/SRTP/UDP) that
// a client's media terminates on, and bridges plaintext RTP between that PC and
// the room node's SFU over MediaChannels.
//
//   - Down (subscriber): MediaChannel -> TrackLocalStaticRTP -> pion PC
//   - Up (publisher):    pion PC -> TrackRemote -> MediaChannel
//
// It is deliberately transport-only: SDP negotiation, subscription management,
// and all SFU logic stay on the room node. The gateway just executes the pion
// PC and moves RTP/RTCP.
type MediaGateway struct {
	pc        *webrtc.PeerConnection
	sessionID string

	mu         sync.RWMutex
	downTracks map[livekit.TrackID]*gatewayDownTrack
	upTracks   map[livekit.TrackID]map[uint32]*gatewayUpTrack // trackID -> SSRC -> bridge (simulcast layers)
	publishers map[uint32]rtpPacketReader                     // SSRC -> published TrackRemote
	closed     bool

	onTrackMu sync.RWMutex
	onTrack   func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver)
}

type gatewayDownTrack struct {
	local *webrtc.TrackLocalStaticRTP
	ch    MediaChannel
}

type gatewayUpTrack struct {
	remote rtpPacketReader
	ch     MediaChannel
}

// NewMediaGateway wraps an already-configured pion PeerConnection. The caller
// retains responsibility for creating/closing the PC (the gateway does not close
// it; Close only tears down the bridges).
func NewMediaGateway(pc *webrtc.PeerConnection) *MediaGateway {
	g := &MediaGateway{
		pc:         pc,
		downTracks: make(map[livekit.TrackID]*gatewayDownTrack),
		upTracks:   make(map[livekit.TrackID]map[uint32]*gatewayUpTrack),
		publishers: make(map[uint32]rtpPacketReader),
	}
	// Register the publisher track when the pion PC receives it. The gateway
	// stores the TrackRemote (keyed by SSRC) so an up-direction MediaChannel can
	// find it, and notifies the edge service via OnPublishedTrack.
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		g.mu.Lock()
		if !g.closed {
			g.publishers[uint32(track.SSRC())] = track
		}
		g.mu.Unlock()

		g.onTrackMu.RLock()
		f := g.onTrack
		g.onTrackMu.RUnlock()
		if f != nil {
			f(track, receiver)
		}
	})
	return g
}

// SetSessionID tags this gateway with the participant session it serves, for
// logging and diagnostics on the edge node (which hosts many sessions).
func (g *MediaGateway) SetSessionID(sessionID string) {
	g.sessionID = sessionID
}

// OnPublishedTrack registers a callback invoked for each publisher track the
// pion PC receives. The edge service uses it to notify the room node (via the
// control channel) so it can establish the up-direction MediaChannel.
func (g *MediaGateway) OnPublishedTrack(f func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver)) {
	g.onTrackMu.Lock()
	g.onTrack = f
	g.onTrackMu.Unlock()
}

// PeerConnection returns the underlying pion PeerConnection.
func (g *MediaGateway) PeerConnection() *webrtc.PeerConnection {
	return g.pc
}

// AddSubscriberTrack registers a subscriber (down) track: it creates a
// TrackLocalStaticRTP, adds it to the pion PC (so pion SRTP-encrypts and sends
// to the subscriber), and starts pumping RTP from the MediaChannel into it.
func (g *MediaGateway) AddSubscriberTrack(trackID livekit.TrackID, codec webrtc.RTPCodecCapability, ch MediaChannel) error {
	local, err := webrtc.NewTrackLocalStaticRTP(codec, string(trackID), string(trackID))
	if err != nil {
		return err
	}

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrMediaChannelClosed
	}
	if _, exists := g.downTracks[trackID]; exists {
		g.mu.Unlock()
		// Duplicate attach (e.g. renegotiation): the first channel owns the bridge;
		// close the redundant channel so the room side's pump unblocks and the TCP
		// connection is not leaked.
		_ = ch.Close()
		return nil
	}
	g.downTracks[trackID] = &gatewayDownTrack{local: local, ch: ch}
	g.mu.Unlock()

	sender, err := g.pc.AddTrack(local)
	if err != nil {
		g.removeDownTrack(trackID)
		return err
	}

	go pumpMediaChannelToTrackLocal(ch, local)
	go pumpSenderRTCPToMediaChannel(sender, ch)
	logger.Debugw("nat gateway subscriber track attached", "trackID", trackID, "codec", codec.MimeType, "sessionID", g.sessionID)
	return nil
}

// AddPublisherTrack registers a publisher (up) track: it starts pumping RTP from
// the rtpPacketReader (typically a pion TrackRemote received via OnTrack) into
// the MediaChannel bound for the room node. The bridge is keyed by trackID+SSRC:
// a simulcast track publishes one TrackRemote per layer, all sharing the trackID,
// so each SSRC gets its own bridge and its own MediaChannel; only a re-attach of
// the SAME SSRC is treated as a duplicate (redundant channel closed).
func (g *MediaGateway) AddPublisherTrack(trackID livekit.TrackID, ssrc uint32, remote rtpPacketReader, ch MediaChannel) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrMediaChannelClosed
	}
	layers := g.upTracks[trackID]
	if layers == nil {
		layers = make(map[uint32]*gatewayUpTrack)
		g.upTracks[trackID] = layers
	}
	if _, exists := layers[ssrc]; exists {
		g.mu.Unlock()
		// Duplicate attach of the SAME layer (e.g. renegotiation): the first
		// channel owns the bridge; close the redundant channel so the room side's
		// pump unblocks and the TCP connection is not leaked. Distinct SSRCs are
		// simulcast layers sharing this trackID and must be bridged separately.
		_ = ch.Close()
		return nil
	}
	layers[ssrc] = &gatewayUpTrack{remote: remote, ch: ch}
	g.mu.Unlock()

	go pumpTrackToMediaChannel(remote, ch)
	go pumpMediaChannelRTCPToPC(ch, g.pc)
	logger.Debugw("nat gateway publisher track attached", "trackID", trackID, "ssrc", ssrc, "sessionID", g.sessionID)
	return nil
}

// RemoveTrack tears down the bridge for a track in either direction.
func (g *MediaGateway) RemoveTrack(trackID livekit.TrackID) {
	g.removeDownTrack(trackID)
	g.removeUpTrack(trackID)
}

// AttachHello reads the hello frame from ch and attaches the channel to the
// matching track. For MediaDirectionDown (subscriber), it creates a
// TrackLocalStaticRTP and pumps the channel into it. For MediaDirectionUp
// (publisher), reader must be the TrackRemote producing the track's RTP.
func (g *MediaGateway) AttachHello(ch HelloMediaChannel, reader rtpPacketReader) error {
	hello, err := ch.ReadHello()
	if err != nil {
		return err
	}
	return g.Attach(hello, ch, reader)
}

// Attach attaches an already-read MediaHello to a MediaChannel. It is the
// session-aware entry point used by the edge relay, which reads the hello frame
// first to resolve the session (via hello.SessionID) before dispatching here.
func (g *MediaGateway) Attach(hello MediaHello, ch MediaChannel, reader rtpPacketReader) error {
	trackID := livekit.TrackID(hello.TrackID)
	switch hello.Direction {
	case MediaDirectionDown:
		return g.AddSubscriberTrack(trackID, hello.Codec, ch)
	case MediaDirectionUp:
		if reader == nil {
			g.mu.RLock()
			reader = g.publishers[hello.SSRC]
			g.mu.RUnlock()
		}
		if reader == nil {
			return errors.New("publisher track requires an rtpPacketReader")
		}
		return g.AddPublisherTrack(trackID, hello.SSRC, reader, ch)
	default:
		return fmt.Errorf("unknown media direction %q", hello.Direction)
	}
}

func (g *MediaGateway) removeDownTrack(trackID livekit.TrackID) {
	g.mu.Lock()
	dt := g.downTracks[trackID]
	delete(g.downTracks, trackID)
	g.mu.Unlock()
	if dt != nil {
		_ = dt.ch.Close()
	}
}

func (g *MediaGateway) removeUpTrack(trackID livekit.TrackID) {
	g.mu.Lock()
	layers := g.upTracks[trackID]
	delete(g.upTracks, trackID)
	g.mu.Unlock()
	for _, ut := range layers {
		_ = ut.ch.Close()
	}
}

// Close tears down all bridges and releases the track registry. It does not
// close the underlying pion PeerConnection (owned by the caller).
func (g *MediaGateway) Close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	down := g.downTracks
	up := g.upTracks
	g.downTracks = make(map[livekit.TrackID]*gatewayDownTrack)
	g.upTracks = make(map[livekit.TrackID]map[uint32]*gatewayUpTrack)
	g.publishers = make(map[uint32]rtpPacketReader)
	g.mu.Unlock()

	for _, dt := range down {
		_ = dt.ch.Close()
	}
	for _, layers := range up {
		for _, ut := range layers {
			_ = ut.ch.Close()
		}
	}
}
