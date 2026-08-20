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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalControlChannel(t *testing.T) {
	a, b := NewLocalControlChannelPair(10)

	require.NoError(t, a.Send([]byte("a-b")))
	m, err := b.Receive()
	require.NoError(t, err)
	require.Equal(t, []byte("a-b"), m)

	require.NoError(t, b.Send([]byte("b-a")))
	m, err = a.Receive()
	require.NoError(t, err)
	require.Equal(t, []byte("b-a"), m)

	require.NoError(t, a.Close())
	_, err = b.Receive()
	require.ErrorIs(t, err, ErrMediaChannelClosed)
}

func TestTCPControlChannel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	dialedCh := make(chan ControlChannel, 1)
	go func() {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			dialedCh <- nil
			return
		}
		dialedCh <- NewTCPControlChannel(conn)
	}()

	acceptedConn, err := ln.Accept()
	require.NoError(t, err)
	accepted := NewTCPControlChannel(acceptedConn)
	defer accepted.Close()

	dialed := <-dialedCh
	require.NotNil(t, dialed)
	defer dialed.Close()

	require.NoError(t, dialed.Send([]byte("hello")))
	m, err := accepted.Receive()
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), m)

	require.NoError(t, accepted.Send([]byte("world")))
	m, err = dialed.Receive()
	require.NoError(t, err)
	require.Equal(t, []byte("world"), m)

	// Close propagates: dialed's reader detects EOF and tears down.
	require.NoError(t, accepted.Close())
	require.Eventually(t, func() bool {
		_, err := dialed.Receive()
		return errors.Is(err, ErrMediaChannelClosed)
	}, 2*time.Second, 10*time.Millisecond)
}

func TestTCPControlChannelListenerDial(t *testing.T) {
	secret := "ctl-test-secret" // fail-closed: channels require a secret
	ln, err := ListenTCPControlChannel("127.0.0.1:0", secret)
	require.NoError(t, err)
	defer ln.Close()

	dialedCh := make(chan ControlChannel, 1)
	go func() {
		dialed, err := DialTCPControlChannel(ln.Addr().String(), secret)
		if err != nil {
			dialedCh <- nil
			return
		}
		dialedCh <- dialed
	}()

	accepted, err := ln.Accept()
	require.NoError(t, err)
	defer accepted.Close()

	dialed := <-dialedCh
	require.NotNil(t, dialed)
	defer dialed.Close()

	require.NoError(t, dialed.Send([]byte("ping")))
	m, err := accepted.Receive()
	require.NoError(t, err)
	require.Equal(t, []byte("ping"), m)
}
