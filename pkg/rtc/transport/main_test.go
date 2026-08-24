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
	"os"
	"testing"

	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/livekit"
)

func TestMain(m *testing.M) {
	// Initialize Prometheus metrics for all test cases that exercise NAT-mode
	// instrumentation (auth handshake, relay connections, gateway sessions, etc.).
	// The node ID and type are arbitrary test values; the metrics get registered
	// with global prometheus.MustRegister so transport-level unit tests can safely
	// call the package-level helper functions (IncrementNATRelayConnection, etc.)
	// without panicking on nil metric pointers.
	if err := prometheus.Init("test-node", livekit.NodeType_SERVER); err != nil {
		os.Exit(1)
	}
	os.Exit(m.Run())
}
