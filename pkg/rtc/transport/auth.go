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
	"crypto/subtle"
	"errors"
	"net"
	"time"
)

// frameAuth is a dedicated frame type used only for the node-to-node auth
// handshake that runs before any media/control frames are exchanged on a
// cross-node TCP channel. It is distinct from RTP/RTCP/hello so a stray auth
// frame can never be mistaken for media.
const frameAuth byte = 3

// authReject is the server's response when the presented secret is invalid.
var authReject = []byte("DENY")

var errAuthFailed = errors.New("cross-node channel auth failed")

// authHandshake performs a shared-secret handshake on a raw TCP connection before
// it is wrapped as a MediaChannel/ControlChannel. The dialing node sends an AUTH
// frame carrying the shared secret; the accepting node validates it in constant
// time and replies with an AUTH frame ("OK" or "DENY"). A nil/empty secret
// disables auth entirely (single-node dev mode).
//
// The handshake is bounded (5s deadline) so a peer that never presents a secret
// cannot hold the accept loop's goroutine open.
func authHandshake(conn net.Conn, secret string, isServer bool) error {
	if secret == "" {
		return nil // no auth configured — dev mode
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	if isServer {
		typ, payload, err := readFrame(conn)
		if err != nil {
			return err
		}
		if typ != frameAuth {
			return errAuthFailed
		}
		if subtle.ConstantTimeCompare(payload, []byte(secret)) != 1 {
			_ = writeFrame(conn, frameAuth, authReject)
			return errAuthFailed
		}
		return writeFrame(conn, frameAuth, []byte("OK"))
	}

	if err := writeFrame(conn, frameAuth, []byte(secret)); err != nil {
		return err
	}
	typ, resp, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typ != frameAuth || string(resp) != "OK" {
		return errAuthFailed
	}
	return nil
}
