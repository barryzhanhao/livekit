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
	"sync"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/transport"
	"github.com/livekit/protocol/livekit"
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

func (m *MediaRelay) registerGateway(sessionID string, gw *transport.MediaGateway) {
	m.mu.Lock()
	m.gateways[sessionID] = gw
	m.mu.Unlock()
}

func (m *MediaRelay) unregisterGateway(sessionID string) {
	m.mu.Lock()
	delete(m.gateways, sessionID)
	m.mu.Unlock()
}

func (m *MediaRelay) gateway(sessionID string) *transport.MediaGateway {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gateways[sessionID]
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
		_ = ch.Close()
		return
	}
	if setup.SessionID == "" {
		// A session without an ID cannot be routed for media; tear it down.
		gw.Close()
		_ = ch.Close()
		return
	}
	m.registerGateway(setup.SessionID, gw)
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
		_ = ch.Close()
		return
	}
	gw := m.gateway(hello.SessionID)
	if gw == nil {
		// Unknown session: no gateway to attach to.
		_ = ch.Close()
		return
	}
	// Down direction is routed by session+track; up direction (publisher) needs
	// the TrackRemote, which the gateway discovers via OnTrack (wired in a later
	// step), so pass nil for now.
	if err := gw.Attach(hello, ch, nil); err != nil {
		_ = ch.Close()
	}
}
