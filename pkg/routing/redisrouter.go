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

package routing

import (
	"bytes"
	"context"
	"runtime/pprof"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"

	"github.com/livekit/livekit-server/pkg/routing/selector"
)

const (
	// hash of node_id => Node proto
	NodesKey = "nodes"

	// hash of room_name => node_id
	NodeRoomKey = "room_node_map"

	// hash of room_name => participant_id => node_id
	RoomParticipantNodesPrefix = "room_participant_nodes"
)

var _ Router = (*RedisRouter)(nil)

// RedisRouter uses Redis pub/sub to route signaling messages across different nodes
// It relies on the RTC node to be the primary driver of the participant connection.
// Because
type RedisRouter struct {
	*LocalRouter

	rc        redis.UniversalClient
	kps       rpc.KeepalivePubSub
	ctx       context.Context
	isStarted atomic.Bool

	cancel func()
}

func NewRedisRouter(lr *LocalRouter, rc redis.UniversalClient, kps rpc.KeepalivePubSub) *RedisRouter {
	rr := &RedisRouter{
		LocalRouter: lr,
		rc:          rc,
		kps:         kps,
	}
	rr.ctx, rr.cancel = context.WithCancel(context.Background())
	return rr
}

func (r *RedisRouter) RegisterNode() error {
	data, err := proto.Marshal(r.currentNode.Clone())
	if err != nil {
		return err
	}
	if err := r.rc.HSet(r.ctx, NodesKey, string(r.currentNode.NodeID()), data).Err(); err != nil {
		return errors.Wrap(err, "could not register node")
	}
	return nil
}

func (r *RedisRouter) UnregisterNode() error {
	// could be called after Stop(), so we'd want to use an unrelated context
	return r.rc.HDel(context.Background(), NodesKey, string(r.currentNode.NodeID())).Err()
}

func (r *RedisRouter) RemoveDeadNodes() error {
	nodes, err := r.ListNodes()
	if err != nil {
		return err
	}
	for _, n := range nodes {
		// never reap the current node: its own stats can lag under load, and a
		// self-removal would immediately re-home its rooms elsewhere.
		if livekit.NodeID(n.Id) == r.currentNode.NodeID() {
			continue
		}
		if !selector.IsAvailable(n) {
			if err := r.rc.HDel(context.Background(), NodesKey, n.Id).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetNodeForRoom finds the node where the room is hosted at
func (r *RedisRouter) GetNodeForRoom(_ context.Context, roomName livekit.RoomName) (*livekit.Node, error) {
	nodeID, err := r.rc.HGet(r.ctx, NodeRoomKey, string(roomName)).Result()
	if err == redis.Nil {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, errors.Wrap(err, "could not get node for room")
	}

	node, err := r.GetNode(livekit.NodeID(nodeID))
	if errors.Is(err, ErrNotFound) {
		// the node the room was pinned to is no longer registered (room-node
		// shutdown/crash followed by dead-node reaping). Release the stale
		// mapping so the next join re-homes the room instead of routing into a
		// void, and so GetNodeForRoom callers observe the room as unassigned.
		r.clearStaleRoomMapping(roomName, livekit.NodeID(nodeID), "unregistered node")
		return nil, ErrNotFound
	} else if err != nil {
		return nil, errors.Wrap(err, "could not get node for room")
	}

	// The pinned node is still registered but no longer reporting stats — its
	// keepalive died without a clean unregister (e.g. a crashed room node).
	// Treat it as gone so SelectRoomNode re-homes the room on a live node.
	// The current node is by definition alive (it is executing this request);
	// never re-home a room off the local node due to its own stats lag.
	if livekit.NodeID(nodeID) != r.currentNode.NodeID() && !selector.IsAvailable(node) {
		r.clearStaleRoomMapping(roomName, livekit.NodeID(nodeID), "dead node")
		return nil, ErrNotFound
	}

	return node, nil
}

// clearStaleRoomMapping removes a room_node_map entry that points to a node that
// is no longer serving, so the room can be re-homed by the next join.
func (r *RedisRouter) clearStaleRoomMapping(roomName livekit.RoomName, nodeID livekit.NodeID, why string) {
	if err := r.rc.HDel(r.ctx, NodeRoomKey, string(roomName)).Err(); err != nil {
		logger.Warnw("could not clear stale room node mapping", err, "room", roomName, "nodeID", nodeID, "why", why)
		return
	}
	logger.Infow("cleared stale room node mapping", "room", roomName, "nodeID", nodeID, "why", why)
}

// clearStaleRoomMappings reaps every room_node_map entry whose pinned node is
// dead or unregistered. Run periodically so rooms hosted on a failed node are
// released even if nobody joins them for a while.
func (r *RedisRouter) clearStaleRoomMappings() {
	entries, err := r.rc.HGetAll(r.ctx, NodeRoomKey).Result()
	if err != nil {
		logger.Warnw("could not list room node mappings", err)
		return
	}
	for room, nodeID := range entries {
		// never re-home a room mapped to the current node based on its own stats
		if livekit.NodeID(nodeID) == r.currentNode.NodeID() {
			continue
		}
		node, err := r.GetNode(livekit.NodeID(nodeID))
		if err == nil && selector.IsAvailable(node) {
			continue
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			logger.Warnw("could not check node for room mapping", err, "room", room, "nodeID", nodeID)
			continue
		}
		r.clearStaleRoomMapping(livekit.RoomName(room), livekit.NodeID(nodeID), "reap")
	}
}

func (r *RedisRouter) SetNodeForRoom(_ context.Context, roomName livekit.RoomName, nodeID livekit.NodeID) error {
	return r.rc.HSet(r.ctx, NodeRoomKey, string(roomName), string(nodeID)).Err()
}

func (r *RedisRouter) ClearRoomState(_ context.Context, roomName livekit.RoomName) error {
	pipe := r.rc.Pipeline()
	pipe.HDel(context.Background(), NodeRoomKey, string(roomName))
	pipe.Del(context.Background(), RoomParticipantNodesPrefix+":"+string(roomName))
	_, err := pipe.Exec(context.Background())
	if err != nil {
		return errors.Wrap(err, "could not clear room state")
	}
	return nil
}

// ---- Participant-level routing (NAT mode) ----

func (r *RedisRouter) SetParticipantNode(_ context.Context, roomName livekit.RoomName, participantID livekit.ParticipantID, nodeID livekit.NodeID) error {
	key := RoomParticipantNodesPrefix + ":" + string(roomName)
	return r.rc.HSet(r.ctx, key, string(participantID), string(nodeID)).Err()
}

func (r *RedisRouter) RemoveParticipantNode(_ context.Context, roomName livekit.RoomName, participantID livekit.ParticipantID) error {
	key := RoomParticipantNodesPrefix + ":" + string(roomName)
	return r.rc.HDel(r.ctx, key, string(participantID)).Err()
}

func (r *RedisRouter) GetRoomParticipantNodes(_ context.Context, roomName livekit.RoomName) (map[livekit.ParticipantID]livekit.NodeID, error) {
	key := RoomParticipantNodesPrefix + ":" + string(roomName)
	result, err := r.rc.HGetAll(r.ctx, key).Result()
	if err != nil {
		return nil, errors.Wrap(err, "could not get room participant nodes")
	}
	nodes := make(map[livekit.ParticipantID]livekit.NodeID, len(result))
	for pid, nid := range result {
		nodes[livekit.ParticipantID(pid)] = livekit.NodeID(nid)
	}
	return nodes, nil
}

func (r *RedisRouter) GetNode(nodeID livekit.NodeID) (*livekit.Node, error) {
	data, err := r.rc.HGet(r.ctx, NodesKey, string(nodeID)).Result()
	if err == redis.Nil {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	n := livekit.Node{}
	if err = proto.Unmarshal([]byte(data), &n); err != nil {
		return nil, err
	}
	return &n, nil
}

func (r *RedisRouter) ListNodes() ([]*livekit.Node, error) {
	items, err := r.rc.HVals(r.ctx, NodesKey).Result()
	if err != nil {
		return nil, errors.Wrap(err, "could not list nodes")
	}
	nodes := make([]*livekit.Node, 0, len(items))
	for _, item := range items {
		n := livekit.Node{}
		if err := proto.Unmarshal([]byte(item), &n); err != nil {
			return nil, err
		}
		nodes = append(nodes, &n)
	}
	return nodes, nil
}

func (r *RedisRouter) CreateRoom(ctx context.Context, req *livekit.CreateRoomRequest) (res *livekit.Room, err error) {
	rtcNode, err := r.GetNodeForRoom(ctx, livekit.RoomName(req.Name))
	if err != nil {
		return
	}

	return r.CreateRoomWithNodeID(ctx, req, livekit.NodeID(rtcNode.Id))
}

// StartParticipantSignal signal connection sets up paths to the RTC node, and starts to route messages to that message queue
func (r *RedisRouter) StartParticipantSignal(ctx context.Context, roomName livekit.RoomName, pi ParticipantInit) (res StartParticipantSignalResults, err error) {
	rtcNode, err := r.GetNodeForRoom(ctx, roomName)
	if err != nil {
		return
	}

	return r.StartParticipantSignalWithNodeID(ctx, roomName, pi, livekit.NodeID(rtcNode.Id))
}

func (r *RedisRouter) Start() error {
	if r.isStarted.Swap(true) {
		return nil
	}

	workerStarted := make(chan error)
	go r.statsWorker()
	go r.keepaliveWorker(workerStarted)
	go r.cleanupWorker()

	// wait until worker is running
	return <-workerStarted
}

// cleanupWorker periodically reaps dead nodes and releases room_node_map
// entries that point to them. Node liveness is derived from the keepalive that
// each node publishes to its own stats topic (see keepaliveWorker); a node that
// stops reporting — a crashed/failed room node — must be removed from the
// registry and have its rooms released so subsequent joins re-home them onto a
// live node. Without this the `nodes` hash and `room_node_map` would hold
// failed nodes indefinitely (RemoveDeadNodes alone only runs once at startup).
func (r *RedisRouter) cleanupWorker() {
	interval := r.nodeStatsConfig.StatsUpdateInterval * 5
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := r.RemoveDeadNodes(); err != nil {
				logger.Warnw("could not remove dead nodes", err)
			}
			r.clearStaleRoomMappings()
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *RedisRouter) Drain() {
	r.currentNode.SetState(livekit.NodeState_SHUTTING_DOWN)
	if err := r.RegisterNode(); err != nil {
		logger.Errorw("failed to mark as draining", err, "nodeID", r.currentNode.NodeID())
	}
}

func (r *RedisRouter) Stop() {
	if !r.isStarted.Swap(false) {
		return
	}
	logger.Debugw("stopping RedisRouter")
	_ = r.UnregisterNode()
	r.cancel()
}

// update node stats and cleanup
func (r *RedisRouter) statsWorker() {
	goroutineDumped := false
	for r.ctx.Err() == nil {
		// update periodically
		select {
		case <-time.After(r.nodeStatsConfig.StatsUpdateInterval):
			r.kps.PublishPing(r.ctx, r.currentNode.NodeID(), &rpc.KeepalivePing{Timestamp: time.Now().Unix()})

			delaySeconds := r.currentNode.SecondsSinceNodeStatsUpdate()
			if delaySeconds > r.nodeStatsConfig.StatsMaxDelay.Seconds() {
				if !goroutineDumped {
					goroutineDumped = true
					buf := bytes.NewBuffer(nil)
					_ = pprof.Lookup("goroutine").WriteTo(buf, 2)
					logger.Errorw("status update delayed, possible deadlock", nil,
						"delay", delaySeconds,
						"goroutines", buf.String())
				}
			} else {
				goroutineDumped = false
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *RedisRouter) keepaliveWorker(startedChan chan error) {
	pings, err := r.kps.SubscribePing(r.ctx, r.currentNode.NodeID())
	if err != nil {
		startedChan <- err
		return
	}
	close(startedChan)

	for ping := range pings.Channel() {
		if time.Since(time.Unix(ping.Timestamp, 0)) > r.nodeStatsConfig.StatsUpdateInterval {
			logger.Infow("keep alive too old, skipping", "timestamp", ping.Timestamp)
			continue
		}

		if !r.currentNode.UpdateNodeStats() {
			continue
		}

		// TODO: check stats against config.Limit values
		if err := r.RegisterNode(); err != nil {
			logger.Errorw("could not update node", err)
		}
	}
}
