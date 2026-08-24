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

package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/livekit/protocol/livekit"
)

// NAT mode ("media follows signaling") metrics for the cross-node relay and
// edge gateway. These are exported alongside the standard LiveKit metrics and
// give operators visibility into the internal media plane: connection counts,
// throughput, auth failures, gateway session lifecycle, and track attachment.
//
// All metrics carry node_id / node_type labels to distinguish metrics from
// different cluster roles (room vs edge) in a multi-pod Grafana dashboard.

var (
	// --- Relay-level metrics ---

	// natRelayConnectionActive is the current number of open TCP connections
	// for cross-node media (RTP/RTCP) and control signalling.
	natRelayConnectionActive prometheus.Gauge
	// natRelayConnectionsTotal counts TCP connections established (not
	// currently open). Use together with `active` to derive churn.
	natRelayConnectionsTotal *prometheus.CounterVec
	// natRelayBytesTotal counts bytes transferred over cross-node TCP
	// connections, broken down by direction (inbound/outbound) and flow
	// type (rtp/rtcp).
	natRelayBytesTotal *prometheus.CounterVec

	// --- Auth / handshake metrics ---

	// natRelayAuthErrorsTotal counts auth failures (wrong/absent secret)
	// during the cross-node TCP handshake.
	natRelayAuthErrorsTotal *prometheus.CounterVec
	// natRelayHandshakeLatency measures the wall-clock time of the full
	// auth handshake (TCP connect + frame exchange) for successful
	// connections.
	natRelayHandshakeLatency prometheus.Histogram

	// --- Gateway session metrics ---

	// natGatewaySessionActive is the current number of active gateway
	// sessions (one per participant PC pair on the edge node).
	natGatewaySessionActive prometheus.Gauge
	// natGatewaySessionsTotal counts gateway sessions created and closed.
	natGatewaySessionsTotal *prometheus.CounterVec
	// natGatewaySessionDurationSeconds measures how long a gateway session
	// lives (from control channel setup to teardown).
	natGatewaySessionDurationSeconds prometheus.Histogram

	// --- Track attachment metrics ---

	// natGatewayTrackActive is the current number of attached tracks on
	// gateway sessions, labelled by direction (up = publisher, down =
	// subscriber).
	natGatewayTrackActive *prometheus.GaugeVec
	// natGatewayTracksTotal counts tracks attached over the lifetime of a
	// gateway, labelled by direction.
	natGatewayTracksTotal *prometheus.CounterVec

	// atomic gauges for GetNodeStats integration
	natRelayConnActive atomic.Uint64
	natGatewaySessActive atomic.Uint64
	natGatewayTrackUpActive atomic.Uint64
	natGatewayTrackDownActive atomic.Uint64
)

const (
	NATRelayDirectionInbound  = "inbound"
	NATRelayDirectionOutbound = "outbound"
	NATRelayFlowRTP           = "rtp"
	NATRelayFlowRTCP          = "rtcp"
	NATRelayTypeMedia         = "media"
	NATRelayTypeControl       = "control"
	NATGatewayTrackUp         = "up"
	NATGatewayTrackDown       = "down"
	NATRelayAuthResultSuccess = "success"
	NATRelayAuthResultFailure = "failure"
	NATSessionEventCreate     = "create"
	NATSessionEventClose      = "close"
)

func initNATStats(nodeID string, nodeType livekit.NodeType) {
	natRelayConnectionActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_relay",
		Name:        "connection_active",
		Help:        "Current number of open cross-node TCP connections (media + control).",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	})
	natRelayConnectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_relay",
		Name:        "connections_total",
		Help:        "Total cross-node TCP connections established.",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	}, []string{"direction", "type"})
	natRelayBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_relay",
		Name:        "bytes_total",
		Help:        "Bytes transferred over cross-node TCP connections.",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	}, []string{"direction", "flow"})
	natRelayAuthErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_relay",
		Name:        "auth_errors_total",
		Help:        "Auth handshake failures (wrong/absent secret).",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	}, []string{"result"})
	natRelayHandshakeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_relay",
		Name:        "handshake_latency_seconds",
		Help:        "Auth handshake latency for successful connections.",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
		Buckets:     prometheus.ExponentialBucketsRange(0.001, 5.0, 10),
	})

	natGatewaySessionActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_gateway",
		Name:        "session_active",
		Help:        "Current number of active gateway sessions (participant PCs on edge).",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	})
	natGatewaySessionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_gateway",
		Name:        "sessions_total",
		Help:        "Gateway session lifecycle events.",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	}, []string{"event"})
	natGatewaySessionDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_gateway",
		Name:        "session_duration_seconds",
		Help:        "Gateway session duration.",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
		Buckets:     []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
	})

	natGatewayTrackActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_gateway",
		Name:        "track_active",
		Help:        "Current number of attached tracks on gateway sessions.",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	}, []string{"direction"})
	natGatewayTracksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   livekitNamespace,
		Subsystem:   "nat_gateway",
		Name:        "tracks_total",
		Help:        "Total tracks attached over gateway lifetime.",
		ConstLabels: prometheus.Labels{"node_id": nodeID, "node_type": nodeType.String()},
	}, []string{"direction"})

	prometheus.MustRegister(natRelayConnectionActive)
	prometheus.MustRegister(natRelayConnectionsTotal)
	prometheus.MustRegister(natRelayBytesTotal)
	prometheus.MustRegister(natRelayAuthErrorsTotal)
	prometheus.MustRegister(natRelayHandshakeLatency)
	prometheus.MustRegister(natGatewaySessionActive)
	prometheus.MustRegister(natGatewaySessionsTotal)
	prometheus.MustRegister(natGatewaySessionDurationSeconds)
	prometheus.MustRegister(natGatewayTrackActive)
	prometheus.MustRegister(natGatewayTracksTotal)
}

// ---- Relay connection helpers ----

// IncrementNATRelayConnection records a new cross-node TCP connection.
func IncrementNATRelayConnection(direction, connType string) {
	natRelayConnectionActive.Add(1)
	natRelayConnActive.Inc()
	natRelayConnectionsTotal.WithLabelValues(direction, connType).Add(1)
}

// DecrementNATRelayConnection records a closed cross-node TCP connection.
func DecrementNATRelayConnection() {
	natRelayConnectionActive.Sub(1)
	natRelayConnActive.Dec()
}

// AddNATRelayBytes records bytes transferred over a cross-node TCP connection.
func AddNATRelayBytes(direction, flow string, n int) {
	natRelayBytesTotal.WithLabelValues(direction, flow).Add(float64(n))
}

// ---- Auth helpers ----

// IncrementNATRelayAuthSuccess records a successful auth handshake with
// its latency.
func IncrementNATRelayAuthSuccess(d time.Duration) {
	natRelayAuthErrorsTotal.WithLabelValues(NATRelayAuthResultSuccess).Add(1)
	natRelayHandshakeLatency.Observe(d.Seconds())
}

// IncrementNATRelayAuthFailure records a failed auth handshake.
func IncrementNATRelayAuthFailure() {
	natRelayAuthErrorsTotal.WithLabelValues(NATRelayAuthResultFailure).Add(1)
}

// ---- Gateway session helpers ----

// AddNATGatewaySession records a new gateway session.
func AddNATGatewaySession() {
	natGatewaySessionActive.Add(1)
	natGatewaySessActive.Inc()
	natGatewaySessionsTotal.WithLabelValues(NATSessionEventCreate).Add(1)
}

// SubNATGatewaySession records a closed gateway session with its duration.
func SubNATGatewaySession(duration time.Duration) {
	natGatewaySessionActive.Sub(1)
	natGatewaySessActive.Dec()
	natGatewaySessionsTotal.WithLabelValues(NATSessionEventClose).Add(1)
	if duration > 0 {
		natGatewaySessionDurationSeconds.Observe(duration.Seconds())
	}
}

// ---- Track attachment helpers ----

// AddNATGatewayTrack records a new track attachment.
func AddNATGatewayTrack(direction string) {
	natGatewayTrackActive.WithLabelValues(direction).Add(1)
	natGatewayTracksTotal.WithLabelValues(direction).Add(1)
	switch direction {
	case NATGatewayTrackUp:
		natGatewayTrackUpActive.Inc()
	case NATGatewayTrackDown:
		natGatewayTrackDownActive.Inc()
	}
}

// SubNATGatewayTrack records a removed track attachment.
func SubNATGatewayTrack(direction string) {
	natGatewayTrackActive.WithLabelValues(direction).Sub(1)
	switch direction {
	case NATGatewayTrackUp:
		natGatewayTrackUpActive.Dec()
	case NATGatewayTrackDown:
		natGatewayTrackDownActive.Dec()
	}
}