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

package pacer

import (
	"sync"
	"time"

	"github.com/livekit/livekit-server/pkg/sfu/ccutils"
	"github.com/pion/rtp"
)

var (
	PacketFactory = &sync.Pool{
		New: func() any {
			return &Packet{}
		},
	}
)

// --------------------------------------

// RTPWriteStream is the narrow sink for outbound RTP packets. It is satisfied by
// webrtc.TrackLocalWriter in single-node mode, and by a MediaChannel-backed
// writer in NAT mode (media follows signaling), where the packet is marshalled
// and forwarded to the edge node.
type RTPWriteStream interface {
	WriteRTP(header *rtp.Header, payload []byte) (int, error)
}

type PacerBehavior string

const (
	PacerBehaviorPassThrough PacerBehavior = "pass-through"
	PacerBehaviorNoQueue     PacerBehavior = "no-queue"
	PacerBehaviorLeakybucket PacerBehavior = "leaky-bucket"
)

type Packet struct {
	Header             *rtp.Header
	HeaderPool         *sync.Pool
	HeaderSize         int
	Payload            []byte
	IsRTX              bool
	ProbeClusterId     ccutils.ProbeClusterId
	IsProbe            bool
	AbsSendTimeExtID   uint8
	TransportWideExtID uint8
	WriteStream        RTPWriteStream
	Pool               *sync.Pool
	PoolEntity         *[]byte
}

type Pacer interface {
	Enqueue(p *Packet)
	Stop()

	SetInterval(interval time.Duration)
	SetBitrate(bitrate int)

	TimeSinceLastSentPacket() time.Duration

	SetPacerProbeObserverListener(listener PacerProbeObserverListener)
	StartProbeCluster(pci ccutils.ProbeClusterInfo)
	EndProbeCluster(probeClusterId ccutils.ProbeClusterId) ccutils.ProbeClusterInfo
}

type PacerProbeObserverListener interface {
	OnPacerProbeObserverClusterComplete(probeClusterId ccutils.ProbeClusterId)
}

// ------------------------------------------------
