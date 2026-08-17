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

package rtc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
)

func TestNewWebRTCConfig_AdvertiseIPDecouplesNodeIP(t *testing.T) {
	const content = `rtc:
  use_external_ip: false
  tcp_port: 0
  node_ip: 10.0.0.10
  advertise_ip: 203.0.113.5`
	conf, err := config.NewConfig(content, true, nil, nil)
	require.NoError(t, err)

	_, err = NewWebRTCConfig(conf)
	require.NoError(t, err)

	// node_ip must remain the internal routing IP; NewWebRTCConfig must not
	// leak the externally advertised IP back into the routing IP.
	require.Equal(t, "10.0.0.10", conf.RTC.NodeIP.PrimaryIP())
	require.Equal(t, "203.0.113.5", conf.RTC.AdvertiseIP.PrimaryIP())
}

func TestNewWebRTCConfig_NoAdvertiseIP(t *testing.T) {
	const content = `rtc:
  use_external_ip: false
  tcp_port: 0
  node_ip: 10.0.0.10`
	conf, err := config.NewConfig(content, true, nil, nil)
	require.NoError(t, err)

	_, err = NewWebRTCConfig(conf)
	require.NoError(t, err)

	require.Equal(t, "10.0.0.10", conf.RTC.NodeIP.PrimaryIP())
	require.True(t, conf.RTC.AdvertiseIP.IsEmpty())
}
