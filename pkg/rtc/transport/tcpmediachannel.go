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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// tcpMediaChannel is a networked MediaChannel that carries plaintext RTP/RTCP
// over a single TCP connection using a simple length-prefixed framing:
//
//	[1 byte type][4 byte length (big-endian)][payload]
//
// A reader goroutine demultiplexes the two frame types into separate queues so
// ReadRTP and ReadRTCP can be consumed independently without racing on the conn.
// Writes are serialized with a mutex so each frame is written atomically.
//
// This is the self-contained cross-node transport for NAT mode; it depends only
// on the standard library (no external RPC/protobuf schema), matching the
// "media follows signaling" design.
type tcpMediaChannel struct {
	conn net.Conn

	writeMu sync.Mutex

	rtpCh   chan []byte
	rtcpCh  chan []byte
	helloCh chan []byte

	done      chan struct{}
	closeOnce sync.Once
}

const (
	frameRTP   byte = 0
	frameRTCP  byte = 1
	frameHello byte = 2

	// frameHeaderSize is the fixed framing overhead: type (1) + length (4).
	frameHeaderSize = 5

	// maxFramePayload caps a single frame to guard against a corrupt length field.
	maxFramePayload = 1 << 20 // 1 MiB, far above any RTP/RTCP packet
)

// defaultMediaChannelBuffer is the per-direction queue depth.
const defaultMediaChannelBuffer = 512

// NewTCPMediaChannel wraps an established TCP connection as a MediaChannel and
// starts its demultiplexing reader. The caller retains ownership of conn and
// must not use it further.
func NewTCPMediaChannel(conn net.Conn) MediaChannel {
	return newTCPHelloMediaChannel(conn)
}

// newTCPHelloMediaChannel is NewTCPMediaChannel but preserves the hello-capable
// concrete type so the caller can ReadHello/SendHello.
func newTCPHelloMediaChannel(conn net.Conn) *tcpMediaChannel {
	c := &tcpMediaChannel{
		conn:    conn,
		rtpCh:   make(chan []byte, defaultMediaChannelBuffer),
		rtcpCh:  make(chan []byte, defaultMediaChannelBuffer),
		helloCh: make(chan []byte, 1),
		done:    make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *tcpMediaChannel) readLoop() {
	for {
		typ, payload, err := readFrame(c.conn)
		if err != nil {
			c.Close()
			return
		}

		var out chan []byte
		switch typ {
		case frameRTP:
			out = c.rtpCh
		case frameRTCP:
			out = c.rtcpCh
		case frameHello:
			out = c.helloCh
		default:
			// unknown frame type: drop the payload and continue
			continue
		}

		select {
		case <-c.done:
			return
		case out <- payload:
		}
	}
}

func (c *tcpMediaChannel) WriteRTP(payload []byte) error {
	return c.writeFrame(frameRTP, payload)
}

func (c *tcpMediaChannel) WriteRTCP(payload []byte) error {
	return c.writeFrame(frameRTCP, payload)
}

// SendHello writes the one-time hello frame identifying the track this channel
// carries. It must be sent before any RTP/RTCP and is consumed by the peer via
// ReadHello.
func (c *tcpMediaChannel) SendHello(hello MediaHello) error {
	b, err := json.Marshal(hello)
	if err != nil {
		return err
	}
	return c.writeFrame(frameHello, b)
}

// ReadHello blocks until the peer's hello frame arrives. It returns
// ErrMediaChannelClosed if the channel closes first.
func (c *tcpMediaChannel) ReadHello() (MediaHello, error) {
	select {
	case <-c.done:
		return MediaHello{}, ErrMediaChannelClosed
	case b := <-c.helloCh:
		var hello MediaHello
		if err := json.Unmarshal(b, &hello); err != nil {
			return MediaHello{}, err
		}
		return hello, nil
	}
}

func (c *tcpMediaChannel) writeFrame(typ byte, payload []byte) error {
	select {
	case <-c.done:
		return ErrMediaChannelClosed
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeFrame(c.conn, typ, payload)
}

func (c *tcpMediaChannel) ReadRTP() ([]byte, error) {
	select {
	case <-c.done:
		return nil, ErrMediaChannelClosed
	case p := <-c.rtpCh:
		return p, nil
	}
}

func (c *tcpMediaChannel) ReadRTCP() ([]byte, error) {
	select {
	case <-c.done:
		return nil, ErrMediaChannelClosed
	case p := <-c.rtcpCh:
		return p, nil
	}
}

func (c *tcpMediaChannel) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
	return nil
}

// writeFrame writes a type-tagged, length-prefixed frame.
func writeFrame(w io.Writer, typ byte, payload []byte) error {
	if len(payload) > maxFramePayload {
		return fmt.Errorf("media frame too large: %d", len(payload))
	}

	header := [frameHeaderSize]byte{typ}
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

// readFrame reads and returns a single frame's type and payload.
func readFrame(r io.Reader) (byte, []byte, error) {
	header := [frameHeaderSize]byte{}
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(header[1:])
	if length > maxFramePayload {
		return 0, nil, errors.New("media frame length exceeds limit")
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}

// TCPMediaChannelListener accepts TCP connections and wraps each as a MediaChannel.
type TCPMediaChannelListener struct {
	ln net.Listener
}

// ListenTCPMediaChannel binds a TCP listener on addr (e.g. ":7883").
func ListenTCPMediaChannel(addr string) (*TCPMediaChannelListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TCPMediaChannelListener{ln: ln}, nil
}

// Addr returns the listener's bound address.
func (l *TCPMediaChannelListener) Addr() net.Addr {
	return l.ln.Addr()
}

// Accept blocks until a connection arrives and returns it as a MediaChannel.
func (l *TCPMediaChannelListener) Accept() (MediaChannel, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	return NewTCPMediaChannel(conn), nil
}

// AcceptHello blocks until a connection arrives and returns it as a
// HelloMediaChannel so the caller can ReadHello to learn which track it carries.
func (l *TCPMediaChannelListener) AcceptHello() (HelloMediaChannel, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	return newTCPHelloMediaChannel(conn), nil
}

// Close closes the listener.
func (l *TCPMediaChannelListener) Close() error {
	return l.ln.Close()
}

// DialTCPMediaChannel dials addr and returns the connection as a MediaChannel.
func DialTCPMediaChannel(addr string) (MediaChannel, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewTCPMediaChannel(conn), nil
}

// DialTCPMediaChannelHello dials addr, sends the hello frame, and returns the
// hello-capable channel. The peer must accept with AcceptHello and call ReadHello.
func DialTCPMediaChannelHello(addr string, hello MediaHello) (HelloMediaChannel, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := newTCPHelloMediaChannel(conn)
	if err := c.SendHello(hello); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

var _ HelloMediaChannel = (*tcpMediaChannel)(nil)
