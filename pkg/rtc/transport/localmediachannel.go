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

import "sync"

// localMediaChannel is an in-process MediaChannel used in single-node mode and
// in tests. It carries packets between two goroutines sharing the same process.
//
// A channel is always created as a pair of ends (see NewLocalMediaChannelPair);
// each end writes to its own outbound queues and reads from the peer's. RTP and
// RTCP use separate queues so media never head-of-line blocks feedback.
type localMediaChannel struct {
	done      chan struct{} // shared across the pair; closed once on Close
	closeOnce *sync.Once    // shared across the pair

	rtpIn    chan []byte // this end reads from here
	rtcpIn   chan []byte
	peerRTP  chan []byte // this end writes to here
	peerRTCP chan []byte
}

// NewLocalMediaChannelPair returns the two ends of an in-process MediaChannel.
// endA and endB are peers: RTP/RTCP written to one is read from the other, in
// both directions. Closing either end closes the whole channel.
func NewLocalMediaChannelPair(bufferSize int) (endA MediaChannel, endB MediaChannel) {
	if bufferSize <= 0 {
		bufferSize = 1
	}

	done := make(chan struct{})
	once := &sync.Once{}

	aRTP := make(chan []byte, bufferSize)
	aRTCP := make(chan []byte, bufferSize)
	bRTP := make(chan []byte, bufferSize)
	bRTCP := make(chan []byte, bufferSize)

	a := &localMediaChannel{
		done:      done,
		closeOnce: once,
		rtpIn:     aRTP,
		rtcpIn:    aRTCP,
		peerRTP:   bRTP,
		peerRTCP:  bRTCP,
	}
	b := &localMediaChannel{
		done:      done,
		closeOnce: once,
		rtpIn:     bRTP,
		rtcpIn:    bRTCP,
		peerRTP:   aRTP,
		peerRTCP:  aRTCP,
	}
	return a, b
}

func (c *localMediaChannel) WriteRTP(payload []byte) error {
	return c.write(c.peerRTP, payload)
}

func (c *localMediaChannel) WriteRTCP(payload []byte) error {
	return c.write(c.peerRTCP, payload)
}

// write sends to out unless the channel is already closed. The first select is
// a cheap closed-check; the second blocks until either the send succeeds or the
// channel closes. This avoids the select race where a buffered send and a closed
// done channel are both ready.
func (c *localMediaChannel) write(out chan []byte, payload []byte) error {
	select {
	case <-c.done:
		return ErrMediaChannelClosed
	default:
	}
	select {
	case <-c.done:
		return ErrMediaChannelClosed
	case out <- payload:
		return nil
	}
}

func (c *localMediaChannel) ReadRTP() ([]byte, error) {
	select {
	case <-c.done:
		return nil, ErrMediaChannelClosed
	case p := <-c.rtpIn:
		return p, nil
	}
}

func (c *localMediaChannel) ReadRTCP() ([]byte, error) {
	select {
	case <-c.done:
		return nil, ErrMediaChannelClosed
	case p := <-c.rtcpIn:
		return p, nil
	}
}

func (c *localMediaChannel) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}
