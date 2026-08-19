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

package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/livekit-server/pkg/service/servicefakes"
)

func TestCreateRoom(t *testing.T) {
	t.Run("ensure default room settings are applied", func(t *testing.T) {
		conf, err := config.NewConfig("", true, nil, nil)
		require.NoError(t, err)

		node, err := routing.NewLocalNode(conf)
		require.NoError(t, err)

		ra, conf := newTestRoomAllocator(t, conf, node.Clone())

		room, _, _, err := ra.CreateRoom(context.Background(), &livekit.CreateRoomRequest{Name: "myroom"}, true)
		require.NoError(t, err)
		require.Equal(t, conf.Room.EmptyTimeout, room.EmptyTimeout)
		require.Equal(t, conf.Room.DepartureTimeout, room.DepartureTimeout)
		require.NotEmpty(t, room.EnabledCodecs)
	})
}

func TestSelectRoomNode(t *testing.T) {
	t.Run("reject new participants when track limit has been reached", func(t *testing.T) {
		conf, err := config.NewConfig("", true, nil, nil)
		require.NoError(t, err)
		conf.Limit.NumTracks = 10

		node, err := routing.NewLocalNode(conf)
		require.NoError(t, err)
		node.SetStats(&livekit.NodeStats{
			NumTracksIn:  100,
			NumTracksOut: 100,
		})

		ra, _ := newTestRoomAllocator(t, conf, node.Clone())

		err = ra.SelectRoomNode(context.Background(), "low-limit-room", "")
		require.ErrorIs(t, err, routing.ErrNodeLimitReached)
	})

	t.Run("reject new participants when bandwidth limit has been reached", func(t *testing.T) {
		conf, err := config.NewConfig("", true, nil, nil)
		require.NoError(t, err)
		conf.Limit.BytesPerSec = 100

		node, err := routing.NewLocalNode(conf)
		require.NoError(t, err)
		node.SetStats(&livekit.NodeStats{
			Rates: []*livekit.NodeStatsRate{
				{BytesIn: 1000, BytesOut: 1000},
			},
		})

		ra, _ := newTestRoomAllocator(t, conf, node.Clone())

		err = ra.SelectRoomNode(context.Background(), "low-limit-room", "")
		require.ErrorIs(t, err, routing.ErrNodeLimitReached)
	})

	t.Run("re-homes the room when the pinned node is dead", func(t *testing.T) {
		// a room_node_map entry that still points at a node whose keepalive has
		// died (crash without graceful unregister) must NOT keep the room pinned
		// there: SelectRoomNode sees IsAvailable == false and moves the room.
		conf, err := config.NewConfig("", true, nil, nil)
		require.NoError(t, err)

		dead := &livekit.Node{
			Id:    "dead-room-node",
			State: livekit.NodeState_SERVING,
			Stats: &livekit.NodeStats{UpdatedAt: 1}, // stale far beyond AvailableSeconds
		}
		alive := &livekit.Node{
			Id:    "live-node",
			State: livekit.NodeState_SERVING,
			Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()},
		}

		store := &servicefakes.FakeObjectStore{}
		router := &routingfakes.FakeRouter{}
		router.GetNodeForRoomReturns(dead, nil)
		router.ListNodesReturns([]*livekit.Node{alive}, nil)

		ra, err := service.NewRoomAllocator(conf, router, store)
		require.NoError(t, err)

		require.NoError(t, ra.SelectRoomNode(context.Background(), "room", ""))
		require.Equal(t, 1, router.SetNodeForRoomCallCount())
		_, _, nodeID := router.SetNodeForRoomArgsForCall(0)
		require.Equal(t, livekit.NodeID("live-node"), nodeID)
	})

	t.Run("re-homes the room when the pinned node is unregistered", func(t *testing.T) {
		// a room_node_map entry whose node was removed from the registry (graceful
		// shutdown or dead-node reaping) surfaces as ErrNotFound — the room must
		// be re-homed on a live node rather than failing the join.
		conf, err := config.NewConfig("", true, nil, nil)
		require.NoError(t, err)

		alive := &livekit.Node{
			Id:    "live-node",
			State: livekit.NodeState_SERVING,
			Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()},
		}

		store := &servicefakes.FakeObjectStore{}
		router := &routingfakes.FakeRouter{}
		router.GetNodeForRoomReturns(nil, routing.ErrNotFound)
		router.ListNodesReturns([]*livekit.Node{alive}, nil)

		ra, err := service.NewRoomAllocator(conf, router, store)
		require.NoError(t, err)

		require.NoError(t, ra.SelectRoomNode(context.Background(), "room", ""))
		require.Equal(t, 1, router.SetNodeForRoomCallCount())
		_, _, nodeID := router.SetNodeForRoomArgsForCall(0)
		require.Equal(t, livekit.NodeID("live-node"), nodeID)
	})

	t.Run("keeps the room pinned to a live node", func(t *testing.T) {
		// a healthy pinned node is retained (no re-select, no SetNodeForRoom).
		conf, err := config.NewConfig("", true, nil, nil)
		require.NoError(t, err)

		alive := &livekit.Node{
			Id:    "live-node",
			State: livekit.NodeState_SERVING,
			Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix()},
		}

		store := &servicefakes.FakeObjectStore{}
		router := &routingfakes.FakeRouter{}
		router.GetNodeForRoomReturns(alive, nil)

		ra, err := service.NewRoomAllocator(conf, router, store)
		require.NoError(t, err)

		require.NoError(t, ra.SelectRoomNode(context.Background(), "room", ""))
		require.Equal(t, 0, router.SetNodeForRoomCallCount())
	})
}

func newTestRoomAllocator(t *testing.T, conf *config.Config, node *livekit.Node) (service.RoomAllocator, *config.Config) {
	store := &servicefakes.FakeObjectStore{}
	store.LoadRoomReturns(nil, nil, service.ErrRoomNotFound)
	router := &routingfakes.FakeRouter{}

	// the pinned node must look alive or SelectRoomNode re-selects (and the fake
	// router has no other nodes). Ensure the stats are fresh by default so
	// selector.IsAvailable holds; tests that want a dead node override it.
	node.State = livekit.NodeState_SERVING
	if node.Stats == nil {
		node.Stats = &livekit.NodeStats{}
	}
	if node.Stats.UpdatedAt == 0 {
		node.Stats.UpdatedAt = time.Now().Unix()
	}

	router.GetNodeForRoomReturns(node, nil)

	ra, err := service.NewRoomAllocator(conf, router, store)
	require.NoError(t, err)
	return ra, conf
}
