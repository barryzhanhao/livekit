package transport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuthHandshakeMediaChannel(t *testing.T) {
	ln, err := ListenTCPMediaChannel("127.0.0.1:0", "test-secret")
	require.NoError(t, err)
	defer ln.Close()

	// correct secret: handshake succeeds, channel works
	go func() {
		ch, err := DialTCPMediaChannel(ln.Addr().String(), "test-secret")
		require.NoError(t, err)
		require.NoError(t, ch.WriteRTP([]byte{0x01}))
	}()
	ch, err := ln.Accept()
	require.NoError(t, err)
	raw, err := ch.ReadRTP()
	require.NoError(t, err)
	require.Equal(t, []byte{0x01}, raw)
}

func TestAuthHandshakeRejectsWrongSecret(t *testing.T) {
	ln, err := ListenTCPMediaChannel("127.0.0.1:0", "test-secret")
	require.NoError(t, err)
	defer ln.Close()

	// server accept loop: rejects the unauthenticated conn
	rejErr := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		rejErr <- err
	}()

	// wrong secret: the client handshake fails (server replies DENY)
	dialErr := make(chan error, 1)
	go func() {
		_, err := DialTCPMediaChannel(ln.Addr().String(), "wrong-secret")
		dialErr <- err
	}()
	select {
	case err := <-dialErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dial with wrong secret did not fail")
	}

	// the server rejected the same conn (handshake validation failed)
	select {
	case err := <-rejErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not reject the unauthenticated conn")
	}
}

func TestAuthHandshakeFailClosedWithoutSecret(t *testing.T) {
	// fail-closed: no secret on either side → the dial aborts and the server
	// rejects, so NAT mode cannot accidentally run unauthenticated.
	ln, err := ListenTCPMediaChannel("127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	rejErr := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		rejErr <- err
	}()

	dialErr := make(chan error, 1)
	go func() {
		_, err := DialTCPMediaChannel(ln.Addr().String())
		dialErr <- err
	}()
	select {
	case err := <-dialErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dial without secret did not fail (fail-closed)")
	}
	select {
	case err := <-rejErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not reject the unauthenticated conn (fail-closed)")
	}
}

func TestAuthHandshakeControlChannel(t *testing.T) {
	ln, err := ListenTCPControlChannel("127.0.0.1:0", "ctl-secret")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		ch, err := DialTCPControlChannel(ln.Addr().String(), "ctl-secret")
		require.NoError(t, err)
		require.NoError(t, ch.Send([]byte(`{"op":"ping"}`)))
	}()
	ch, err := ln.Accept()
	require.NoError(t, err)
	raw, err := ch.Receive()
	require.NoError(t, err)
	require.JSONEq(t, `{"op":"ping"}`, string(raw))
}
