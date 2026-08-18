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
	"net"
	"sync"
)

// ControlChannel is a reliable, ordered, framed channel for control-plane
// messages between the room node (RemotePCTransport) and the edge node
// (MediaGateway) in NAT mode. It carries the serialized pion PeerConnection
// operations and events (SDP/ICE/track metadata); the message payload is opaque
// here and (de)serialized by the callers.
type ControlChannel interface {
	// Send delivers one message to the peer.
	Send(msg []byte) error
	// Receive blocks until a message is available. Returns ErrMediaChannelClosed
	// after Close.
	Receive() ([]byte, error)
	// Close terminates the channel.
	Close() error
}

// localControlChannel is an in-process ControlChannel used in tests and
// single-node mode. A channel is created as a pair of ends.
type localControlChannel struct {
	done      chan struct{}
	closeOnce *sync.Once

	in   chan []byte
	peer chan []byte
}

// NewLocalControlChannelPair returns the two ends of an in-process ControlChannel.
func NewLocalControlChannelPair(bufferSize int) (endA ControlChannel, endB ControlChannel) {
	if bufferSize <= 0 {
		bufferSize = 1
	}
	done := make(chan struct{})
	once := &sync.Once{}
	aIn := make(chan []byte, bufferSize)
	bIn := make(chan []byte, bufferSize)
	a := &localControlChannel{done: done, closeOnce: once, in: aIn, peer: bIn}
	b := &localControlChannel{done: done, closeOnce: once, in: bIn, peer: aIn}
	return a, b
}

func (c *localControlChannel) Send(msg []byte) error {
	select {
	case <-c.done:
		return ErrMediaChannelClosed
	default:
	}
	select {
	case <-c.done:
		return ErrMediaChannelClosed
	case c.peer <- msg:
		return nil
	}
}

func (c *localControlChannel) Receive() ([]byte, error) {
	select {
	case <-c.done:
		return nil, ErrMediaChannelClosed
	case m := <-c.in:
		return m, nil
	}
}

func (c *localControlChannel) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

// tcpControlChannel is a networked ControlChannel over a single TCP connection,
// using the same length-prefixed framing as tcpMediaChannel.
type tcpControlChannel struct {
	conn net.Conn

	writeMu sync.Mutex
	msgCh   chan []byte

	done      chan struct{}
	closeOnce sync.Once
}

// NewTCPControlChannel wraps an established TCP connection as a ControlChannel.
func NewTCPControlChannel(conn net.Conn) ControlChannel {
	c := &tcpControlChannel{
		conn:  conn,
		msgCh: make(chan []byte, defaultMediaChannelBuffer),
		done:  make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *tcpControlChannel) readLoop() {
	for {
		_, payload, err := readFrame(c.conn)
		if err != nil {
			c.Close()
			return
		}
		select {
		case <-c.done:
			return
		case c.msgCh <- payload:
		}
	}
}

func (c *tcpControlChannel) Send(msg []byte) error {
	select {
	case <-c.done:
		return ErrMediaChannelClosed
	default:
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeFrame(c.conn, frameRTP, msg) // frame type is ignored by the peer reader
}

func (c *tcpControlChannel) Receive() ([]byte, error) {
	select {
	case <-c.done:
		return nil, ErrMediaChannelClosed
	case m := <-c.msgCh:
		return m, nil
	}
}

func (c *tcpControlChannel) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
	return nil
}

// TCPControlChannelListener accepts TCP connections and wraps each as a
// ControlChannel. When created with a non-empty shared secret, every accepted
// connection must complete the node-to-node auth handshake before use.
type TCPControlChannelListener struct {
	ln     net.Listener
	secret string
}

// ListenTCPControlChannel binds a TCP listener on addr for inbound control
// connections. Pass an optional shared secret to require cross-node auth.
func ListenTCPControlChannel(addr string, secret ...string) (*TCPControlChannelListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	sec := ""
	if len(secret) > 0 {
		sec = secret[0]
	}
	return &TCPControlChannelListener{ln: ln, secret: sec}, nil
}

// Addr returns the listener's bound address.
func (l *TCPControlChannelListener) Addr() net.Addr {
	return l.ln.Addr()
}

// Accept blocks until a connection arrives and returns it as a ControlChannel.
func (l *TCPControlChannelListener) Accept() (ControlChannel, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	if err := authHandshake(conn, l.secret, true); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return NewTCPControlChannel(conn), nil
}

// Close closes the listener.
func (l *TCPControlChannelListener) Close() error {
	return l.ln.Close()
}

// DialTCPControlChannel dials addr and returns the connection as a
// ControlChannel. Pass the same shared secret the peer's listener was created
// with to complete the cross-node auth handshake.
func DialTCPControlChannel(addr string, secret ...string) (ControlChannel, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	sec := ""
	if len(secret) > 0 {
		sec = secret[0]
	}
	if err := authHandshake(conn, sec, false); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return NewTCPControlChannel(conn), nil
}
