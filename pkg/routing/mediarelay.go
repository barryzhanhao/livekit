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

package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// ---------------------------------------------------------------------------

// RTPRelaySignal is the signaling message exchanged via Redis pub/sub
// when setting up cross-node RTP forwarding.
type RTPRelaySignal struct {
	Type        string `json:"type"`        // "subscribe", "accept", "reject"
	RoomName    string `json:"room_name,omitempty"`
	TrackID     string `json:"track_id,omitempty"`
	SourceNode  string `json:"source_node,omitempty"`
	TargetNode  string `json:"target_node,omitempty"`
	RelayAddr   string `json:"relay_addr,omitempty"`  // IP:port for the relay data connection
	ParticipantID string `json:"participant_id,omitempty"`
}

// MediaRelayManager manages cross-node RTP forwarding using Redis pub/sub
// for signaling and direct TCP connections for RTP data.
type MediaRelayManager struct {
	rc      redis.UniversalClient
	nodeID  livekit.NodeID
	nodeIP  string

	mu          sync.RWMutex
	relays      map[string]*relayConnection // key: "remoteNodeID:trackID"
	handlers    map[string]RelayTrackHandler
	listener    net.Listener
	listenerPort int
	started     bool
	ctx         context.Context
	cancel      context.CancelFunc
}

// RelayTrackHandler is called when a remote node wants to subscribe to a track.
type RelayTrackHandler func(trackID livekit.TrackID, remoteNodeID livekit.NodeID) (RTPRelay, error)

type relayConnection struct {
	targetID livekit.NodeID
	trackID  livekit.TrackID
	conn     net.Conn
	enc      *rtpEncoder
	dec      *rtpDecoder
	done     chan struct{}
	mu       sync.Mutex
	closed   bool
}

func NewMediaRelayManager(rc redis.UniversalClient, nodeID livekit.NodeID, nodeIP string) *MediaRelayManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &MediaRelayManager{
		rc:       rc,
		nodeID:   nodeID,
		nodeIP:   nodeIP,
		relays:   make(map[string]*relayConnection),
		handlers: make(map[string]RelayTrackHandler),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// RegisterHandler registers a handler for incoming relay subscription requests.
func (m *MediaRelayManager) RegisterHandler(trackID string, handler RelayTrackHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[trackID] = handler
}

// UnregisterHandler removes a handler.
func (m *MediaRelayManager) UnregisterHandler(trackID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handlers, trackID)
}

// Start begins listening for incoming relay requests via Redis pub/sub
// and starts a TCP listener for RTP data connections.
func (m *MediaRelayManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return nil
	}

	// Subscribe to relay request channel for this node
	relayChan := "relay:" + string(m.nodeID)
	pubSub := m.rc.Subscribe(m.ctx, relayChan)
	ch := pubSub.Channel()

	// Start a TCP listener for RTP data connections
	listener, err := net.Listen("tcp", ":0") // random available port
	if err != nil {
		pubSub.Close()
		return fmt.Errorf("could not start RTP relay listener: %w", err)
	}
	m.listener = listener
	m.listenerPort = listener.Addr().(*net.TCPAddr).Port

	go m.acceptRelayConnections(listener)
	go m.handleRelaySignals(ch)

	m.started = true
	logger.Infow("media relay manager started",
		"nodeID", m.nodeID,
		"relayPort", m.listenerPort,
	)
	return nil
}

func (m *MediaRelayManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		return
	}
	m.cancel()
	if m.listener != nil {
		m.listener.Close()
	}
	// Close all active relays
	for key, r := range m.relays {
		r.Close()
		delete(m.relays, key)
	}
	m.started = false
}

// GetRelayPort returns the port this node listens on for relay data connections.
func (m *MediaRelayManager) GetRelayPort() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.listenerPort
}

// SubscribeRemoteTrack initiates a subscription to a track on a remote node.
// It sends a relay request via Redis pub/sub and waits for the remote node
// to accept, then establishes a data connection.
func (m *MediaRelayManager) SubscribeRemoteTrack(
	ctx context.Context,
	roomName livekit.RoomName,
	trackID livekit.TrackID,
	remoteNodeID livekit.NodeID,
	participantID livekit.ParticipantID,
) (RTPRelay, error) {

	relayKey := string(remoteNodeID) + ":" + string(trackID)

	m.mu.RLock()
	if existing, ok := m.relays[relayKey]; ok && !existing.closed {
		m.mu.RUnlock()
		return existing, nil
	}
	m.mu.RUnlock()

	// Get remote node info to know its relay port
	node, err := m.getRemoteNode(ctx, remoteNodeID)
	if err != nil {
		return nil, fmt.Errorf("could not find remote node %s: %w", remoteNodeID, err)
	}

	// Send subscription request via Redis pub/sub
	signal := RTPRelaySignal{
		Type:          "subscribe",
		RoomName:      string(roomName),
		TrackID:       string(trackID),
		SourceNode:    string(m.nodeID),
		TargetNode:    string(remoteNodeID),
		ParticipantID: string(participantID),
	}
	data, _ := json.Marshal(signal)
	err = m.rc.Publish(ctx, "relay:"+string(remoteNodeID), data).Err()
	if err != nil {
		return nil, fmt.Errorf("could not send relay subscribe request: %w", err)
	}

	// Wait for acceptance by connecting to the remote node's relay listener
	relayAddr := fmt.Sprintf("%s:%d", node.Ip, m.GetRelayPort())
	conn, err := net.DialTimeout("tcp", relayAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("could not connect to remote relay at %s: %w", relayAddr, err)
	}

	// Send the subscription request over the connection
	req := RTPRelaySignal{
		Type:          "subscribe",
		TrackID:       string(trackID),
		SourceNode:    string(m.nodeID),
	}
	reqData, _ := json.Marshal(req)
	reqData = append(reqData, '\n')
	if _, err := conn.Write(reqData); err != nil {
		conn.Close()
		return nil, fmt.Errorf("could not send relay subscription: %w", err)
	}

	rc := &relayConnection{
		targetID: remoteNodeID,
		trackID:  trackID,
		conn:     conn,
		enc:      newRTPEncoder(),
		dec:      newRTPDecoder(conn),
		done:     make(chan struct{}),
	}

	m.mu.Lock()
	m.relays[relayKey] = rc
	m.mu.Unlock()

	go rc.dec.run()

	logger.Infow("remote track relay established",
		"trackID", trackID,
		"remoteNode", remoteNodeID,
	)
	return rc, nil
}

func (m *MediaRelayManager) handleRelaySignals(ch <-chan *redis.Message) {
	for msg := range ch {
		var signal RTPRelaySignal
		if err := json.Unmarshal([]byte(msg.Payload), &signal); err != nil {
			logger.Errorw("could not unmarshal relay signal", err)
			continue
		}

		switch signal.Type {
		case "subscribe":
			m.handleIncomingSubscribe(signal)
		}
	}
}

func (m *MediaRelayManager) handleIncomingSubscribe(signal RTPRelaySignal) {
	m.mu.RLock()
	handler, ok := m.handlers[signal.TrackID]
	m.mu.RUnlock()

	if !ok {
		logger.Warnw("no handler for relay subscription", nil,
			"trackID", signal.TrackID,
			"sourceNode", signal.SourceNode,
		)
		return
	}

	remoteNodeID := livekit.NodeID(signal.SourceNode)
	relay, err := handler(livekit.TrackID(signal.TrackID), remoteNodeID)
	if err != nil {
		logger.Errorw("could not create relay for subscription", err,
			"trackID", signal.TrackID,
		)
		return
	}
	_ = relay
}

// acceptRelayConnections accepts incoming TCP connections for RTP relay data.
func (m *MediaRelayManager) acceptRelayConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-m.ctx.Done():
				return
			default:
				logger.Errorw("could not accept relay connection", err)
				continue
			}
		}
		go m.handleRelayConnection(conn)
	}
}

func (m *MediaRelayManager) handleRelayConnection(conn net.Conn) {
	defer conn.Close()

	// Read the subscription request
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	var signal RTPRelaySignal
	if err := json.Unmarshal(buf[:n], &signal); err != nil {
		return
	}

	m.mu.RLock()
	handler, ok := m.handlers[signal.TrackID]
	m.mu.RUnlock()

	if !ok {
		return
	}

	// Create a relay that writes to this connection
	relay := &tcpRTPRelay{
		conn:    conn,
		trackID: livekit.TrackID(signal.TrackID),
		enc:     newRTPEncoder(),
		done:    make(chan struct{}),
	}

	if _, err := handler(livekit.TrackID(signal.TrackID), livekit.NodeID(signal.SourceNode)); err != nil {
		relay.Close()
		return
	}

	// Keep the connection alive for incoming RTP data
	<-relay.done
}

func (m *MediaRelayManager) getRemoteNode(ctx context.Context, nodeID livekit.NodeID) (*livekit.Node, error) {
	data, err := m.rc.HGet(ctx, NodesKey, string(nodeID)).Result()
	if err != nil {
		return nil, err
	}
	n := &livekit.Node{}
	if err := proto.Unmarshal([]byte(data), n); err != nil {
		return nil, err
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// Method implementations for the relayConnection type.
// (type definition is above at line 67)

func (r *relayConnection) WriteRTP(trackID livekit.TrackID, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return io.ErrClosedPipe
	}
	return r.enc.write(r.conn, payload)
}

func (r *relayConnection) WriteRTCP(trackID livekit.TrackID, payload []byte) error {
	// RTCP forwarded the same way as RTP for now
	return r.WriteRTP(trackID, payload)
}

func (r *relayConnection) ReadRTP() ([]byte, error) {
	return r.dec.read()
}

func (r *relayConnection) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.conn.Close()
		close(r.done)
	}
}

// ---------------------------------------------------------------------------
// tcpRTPRelay is a relay that writes RTP to a TCP connection (server side).

type tcpRTPRelay struct {
	conn    net.Conn
	trackID livekit.TrackID
	enc     *rtpEncoder
	done    chan struct{}
	mu      sync.Mutex
	closed  bool
}

func (r *tcpRTPRelay) WriteRTP(_ livekit.TrackID, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return io.ErrClosedPipe
	}
	return r.enc.write(r.conn, payload)
}

func (r *tcpRTPRelay) WriteRTCP(_ livekit.TrackID, payload []byte) error {
	return r.WriteRTP("", payload)
}

func (r *tcpRTPRelay) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.conn.Close()
		close(r.done)
	}
}

// ---------------------------------------------------------------------------
// RTP packet encoding/decoding for relay transport.

type rtpEncoder struct{}

func newRTPEncoder() *rtpEncoder { return &rtpEncoder{} }

func (e *rtpEncoder) write(w io.Writer, payload []byte) error {
	// Simple framing: 4-byte length prefix + payload
	length := len(payload)
	header := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

type rtpDecoder struct {
	r      io.Reader
	pktCh  chan []byte
	done   chan struct{}
	mu     sync.Mutex
	closed bool
}

func newRTPDecoder(r io.Reader) *rtpDecoder {
	return &rtpDecoder{
		r:     r,
		pktCh: make(chan []byte, 200),
		done:  make(chan struct{}),
	}
}

func (d *rtpDecoder) run() {
	defer close(d.done)
	header := make([]byte, 4)
	for {
		_, err := io.ReadFull(d.r, header)
		if err != nil {
			return
		}
		length := int(header[0])<<24 | int(header[1])<<16 | int(header[2])<<8 | int(header[3])
		if length <= 0 || length > 65535 {
			continue
		}
		payload := make([]byte, length)
		_, err = io.ReadFull(d.r, payload)
		if err != nil {
			return
		}
		select {
		case d.pktCh <- payload:
		default:
			// drop if channel full
		}
	}
}

func (d *rtpDecoder) read() ([]byte, error) {
	pkt, ok := <-d.pktCh
	if !ok {
		return nil, io.EOF
	}
	return pkt, nil
}

// ---------------------------------------------------------------------------

// Ensure interface compliance
var _ RTPRelay = (*relayConnection)(nil)
var _ RTPRelay = (*tcpRTPRelay)(nil)
