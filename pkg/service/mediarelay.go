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

package service

import (
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// MediaRelay is the cross-node endpoint for NAT mode ("media follows
// signaling"). Each node runs one: it listens for inbound TCP media connections
// (plaintext RTP/RTCP) and inbound TCP control connections (pion PeerConnection
// operations) on node_ip, and can dial peer nodes by their routing address.
//
// All nodes in a cluster use the same configured ports, so a peer's relay
// address is node.Ip:<port> (media) / node.Ip:<control_port> (control). The
// dialed node.Ip is the internal pod IP (see routing.NewLocalNode), so control
// and media flow over the internal network.
type MediaRelay struct {
	nodeIP      string
	port        int
	controlPort int

	cfgMu sync.RWMutex
	cfg   *rtc.WebRTCConfig

	mediaListener   *transport.TCPMediaChannelListener
	controlListener *transport.TCPControlChannelListener

	mu       sync.RWMutex
	gateways map[string]*transport.MediaGateway // session ID -> edge gateway

	done chan struct{}
	wg   sync.WaitGroup
}

func NewMediaRelay(nodeIP string, port, controlPort int) *MediaRelay {
	return &MediaRelay{
		nodeIP:      nodeIP,
		port:        port,
		controlPort: controlPort,
		gateways:    make(map[string]*transport.MediaGateway),
		done:        make(chan struct{}),
	}
}

// SetRTCConfig provides the node's WebRTC config, used to build edge gateway
// peer connections. It must be called before Start.
func (m *MediaRelay) SetRTCConfig(cfg *rtc.WebRTCConfig) {
	m.cfgMu.Lock()
	m.cfg = cfg
	m.cfgMu.Unlock()
}

func (m *MediaRelay) rtcConfig() *rtc.WebRTCConfig {
	m.cfgMu.RLock()
	defer m.cfgMu.RUnlock()
	return m.cfg
}

// Start binds the media and control listeners and starts their accept loops. It
// is a no-op if already started.
func (m *MediaRelay) Start() error {
	if m.mediaListener != nil {
		return nil
	}

	ln, err := transport.ListenTCPMediaChannel(net.JoinHostPort(m.nodeIP, strconv.Itoa(m.port)))
	if err != nil {
		return err
	}
	m.mediaListener = ln

	cln, err := transport.ListenTCPControlChannel(net.JoinHostPort(m.nodeIP, strconv.Itoa(m.controlPort)))
	if err != nil {
		_ = m.mediaListener.Close()
		m.mediaListener = nil
		return err
	}
	m.controlListener = cln

	// The accept loops serve the edge role: they need the node's WebRTC config to
	// build gateway PCs (control) and route media to gateways. Without a config
	// the relay is dial-only (e.g. a room node with no edge responsibilities yet),
	// and callers may Accept() the raw listeners directly.
	if m.rtcConfig() != nil {
		m.wg.Add(2)
		go m.mediaAcceptLoop()
		go m.controlAcceptLoop()
	}
	return nil
}

// Stop closes the listeners and all gateways. It is a no-op if not started.
func (m *MediaRelay) Stop() error {
	if m.mediaListener == nil {
		return nil
	}
	_ = m.mediaListener.Close()
	m.mediaListener = nil
	_ = m.controlListener.Close()
	m.controlListener = nil
	close(m.done)

	m.mu.Lock()
	for _, gw := range m.gateways {
		gw.Close()
	}
	m.gateways = make(map[string]*transport.MediaGateway)
	m.mu.Unlock()

	m.wg.Wait()
	return nil
}

// Addr returns the bound media listener address (nil until started).
func (m *MediaRelay) Addr() net.Addr {
	if m.mediaListener == nil {
		return nil
	}
	return m.mediaListener.Addr()
}

// Accept blocks until an inbound media connection arrives and returns it as a
// MediaChannel.
func (m *MediaRelay) Accept() (transport.MediaChannel, error) {
	return m.mediaListener.Accept()
}

// AcceptHello blocks until an inbound media connection arrives and returns it as
// a HelloMediaChannel so the caller can ReadHello to learn its session/track.
func (m *MediaRelay) AcceptHello() (transport.HelloMediaChannel, error) {
	return m.mediaListener.AcceptHello()
}

// DialNode dials a peer node's media relay (node.Ip:<port>) and returns the
// connection as a MediaChannel.
func (m *MediaRelay) DialNode(node *livekit.Node) (transport.MediaChannel, error) {
	return transport.DialTCPMediaChannel(net.JoinHostPort(node.Ip, strconv.Itoa(m.port)))
}

// DialNodeControl dials a peer node's control relay (node.Ip:<control_port>) and
// returns the connection as a ControlChannel.
func (m *MediaRelay) DialNodeControl(node *livekit.Node) (transport.ControlChannel, error) {
	return transport.DialTCPControlChannel(net.JoinHostPort(node.Ip, strconv.Itoa(m.controlPort)))
}

// DialNodeHello dials a peer node's media relay (node.Ip:<port>), sends the hello
// frame, and returns the hello-capable channel. This is the per-track media
// establishment primitive used by the room node's subscription/up-track paths.
func (m *MediaRelay) DialNodeHello(node *livekit.Node, hello transport.MediaHello) (transport.HelloMediaChannel, error) {
	return transport.DialTCPMediaChannelHello(net.JoinHostPort(node.Ip, strconv.Itoa(m.port)), hello)
}

func (m *MediaRelay) registerGateway(sessionID string, gw *transport.MediaGateway, isOfferer bool) {
	m.mu.Lock()
	m.gateways[gatewayKey(sessionID, isOfferer)] = gw
	m.mu.Unlock()
}

func (m *MediaRelay) unregisterGateway(sessionID string, isOfferer bool) {
	m.mu.Lock()
	delete(m.gateways, gatewayKey(sessionID, isOfferer))
	m.mu.Unlock()
}

// gatewayFor resolves the edge gateway for a media hello. In dual-PC mode the
// publisher and subscriber PCs are separate gateways under the same sessionID:
// the up (publisher) hello targets the answerer (!isOfferer) gateway, the down
// (subscriber) hello the offerer one. In single-PC mode there is only one
// gateway for the session, so fall back to it regardless of role.
func (m *MediaRelay) gatewayFor(sessionID, direction string) *transport.MediaGateway {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if gw := m.gateways[gatewayKey(sessionID, direction == transport.MediaDirectionDown)]; gw != nil {
		return gw
	}
	// single-PC fallback: the only gateway registered for this session
	prefix := sessionID + "|"
	for k, gw := range m.gateways {
		if strings.HasPrefix(k, prefix) {
			return gw
		}
	}
	return nil
}

func gatewayKey(sessionID string, isOfferer bool) string {
	role := "subscriber" // offerer = subscriber PC
	if !isOfferer {
		role = "publisher" // answerer = publisher PC
	}
	return sessionID + "|" + role
}

func (m *MediaRelay) controlAcceptLoop() {
	defer m.wg.Done()
	for {
		ch, err := m.controlListener.Accept()
		if err != nil {
			return
		}
		go m.handleControlSession(ch)
	}
}

func (m *MediaRelay) handleControlSession(ch transport.ControlChannel) {
	cfg := m.rtcConfig()
	if cfg == nil {
		_ = ch.Close()
		return
	}
	gw, setup, err := rtc.RunEdgeGatewaySession(ch, cfg, m.unregisterGateway)
	if err != nil {
		logger.Warnw("nat control session failed", err)
		_ = ch.Close()
		return
	}
	if setup.SessionID == "" {
		// A session without an ID cannot be routed for media; tear it down.
		gw.Close()
		_ = ch.Close()
		return
	}
	m.registerGateway(setup.SessionID, gw, setup.IsOfferer)
	logger.Infow("nat control session established", "sessionID", setup.SessionID, "nodeIP", m.nodeIP)
}

func (m *MediaRelay) mediaAcceptLoop() {
	defer m.wg.Done()
	for {
		ch, err := m.mediaListener.AcceptHello()
		if err != nil {
			return
		}
		go m.handleMediaSession(ch)
	}
}

func (m *MediaRelay) handleMediaSession(ch transport.HelloMediaChannel) {
	hello, err := ch.ReadHello()
	if err != nil {
		logger.Warnw("nat media session: failed to read hello", err)
		_ = ch.Close()
		return
	}
	logger.Debugw("nat media session hello",
		"sessionID", hello.SessionID, "trackID", hello.TrackID,
		"direction", hello.Direction, "ssrc", hello.SSRC, "codec", hello.Codec.MimeType)
	gw := m.gatewayFor(hello.SessionID, hello.Direction)
	if gw == nil {
		// Unknown session: no gateway to attach to.
		logger.Warnw("nat media session: unknown session", nil, "sessionID", hello.SessionID, "trackID", hello.TrackID)
		_ = ch.Close()
		return
	}
	// Down direction is routed by session+track; up direction (publisher) needs
	// the TrackRemote, which the gateway discovers via OnTrack (wired in a later
	// step), so pass nil for now.
	if err := gw.Attach(hello, ch, nil); err != nil {
		logger.Warnw("nat media session: attach failed", err, "sessionID", hello.SessionID, "trackID", hello.TrackID, "direction", hello.Direction)
		_ = ch.Close()
	}
}
