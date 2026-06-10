# LiveKit NAT Mode 底层重构计划

## 背景

### 问题

在 Kubernetes 环境中，LiveKit Pod 使用 Pod IP（如 `10.0.x.x`）运行，外部客户端无法直接访问这些 IP。当前架构将 `NodeIP` 同时用于三个角色：

1. **节点间路由地址**（Redis 注册 + PSRPC 通信）— 需要 Pod IP
2. **ICE Host Candidate**（客户端直连媒体）— 需要外部可达的公网/LB IP
3. **TURN URL 生成** — 需要外部可达的 IP

这三者耦合在一起，导致 NAT 场景下无法正常工作。

### 方案 C 失败的原因

前期尝试了 NAT1To1 （方案 C）：在 pions/webrtc 中通过 `ICEAddressRewriteRule` 将 ICE candidate 中的 Pod IP 替换为外部 IP。失败根因：

- 即使 ICE candidate 改写了外部 IP，外部 IP:PORT 的 UDP 流量不一定能路由到该 Pod
- K8s LoadBalancer/NodePort 通常不转发 WebRTC 大范围 UDP 端口（50000-60000）
- 多 Pod 场景下端口映射冲突

---

## 架构设计：媒体面跟随信令面（Media Follows Signaling）

### 核心思想

> 客户端媒体连到它 WS 连到的那个 Pod（由 LB 分配），而非连到"房主节点"。Pod 之间通过内部网络转发 RTP。

### 架构变化示意图

```
                    ┌────────── LB (TCP:7880 + UDP:7882) ──────────┐
                    │                                               │
               ┌────▼────┐                                   ┌────▼────┐
               │  Pod A  │                                   │  Pod B  │
               │         │                                   │         │
  Client A ───►│ WS ──┐ │                                   │ ┌── WS │◄─── Client B
  (Pub)        │      │ │                                   │ │      │      (Sub)
               │ ◄────┘ │                                   │ └──────│
               │  ICE   │                                   │  ICE   │
               │ 直连   │                                   │ 直连   │
               │         │                                   │         │
               │ ┌───────┼────────── RTP Relay ─────────────┼─►┐     │
               │ │ RTP   │         (Pod 内部网络)           │ │     │
               │ │ Send  ├───────────────────────────────────► Recv  │
               │ └───────┤                                   ├───┘   │
               └─────────┘                                   └───────┘
```

### 关键变化

| 方面 | 当前架构 | 新架构 |
|------|---------|--------|
| **Room-Node 绑定** | 1:1，room 绑定到单个 node | N:M，参与者可跨节点 |
| **NodeIP 用途** | 路由 + ICE + TURN | 仅用于内部 PSRPC + RTP 转发 |
| **ICE Candidate** | 用 Pod IP → 外部不可达 | 用客户端连到的那个 Pod 的 IP（LB 可达） |
| **UDP 端口** | 全范围 50000-60000 | 单端口 7882（UDPMux） |
| **客户端可达性** | 必须能直连房主节点 | 只需能连到自己 WS 的节点 |
| **跨节点媒体** | ❌ 不支持 | ✅ 通过 Pod 内部网络 RTP 转发 |

---

## 变更清单

### Phase 1：Redis 数据结构 + Router 接口扩展

#### 1.1 Redis 数据模型变更

**新增 Redis Hash：`room_participant_nodes`**

```
key: "room_participant_nodes:{roomName}"
field: participantID → value: nodeID
```

**现有 Key 调整：**
- `room_node_map`：保留作为快速入口查询，但不再作为媒体路由的唯一依据
- `nodes`：不变

**受影响文件：**

| 文件 | 变更 |
|------|------|
| `pkg/routing/interfaces.go` | `Router` 接口新增参与者级路由方法 |
| `pkg/routing/redisrouter.go` | 实现新增 Redis 操作 |
| `pkg/routing/localrouter.go` | LocalRouter 桩实现 |

#### 1.2 Router 接口扩展

```go
// pkg/routing/interfaces.go — Router 接口新增

type Router interface {
    MessageRouter
    RegisterNode() error
    UnregisterNode() error
    RemoveDeadNodes() error
    ListNodes() ([]*livekit.Node, error)
    GetNodeForRoom(ctx context.Context, roomName livekit.RoomName) (*livekit.Node, error)
    SetNodeForRoom(ctx context.Context, roomName livekit.RoomName, nodeId livekit.NodeID) error
    ClearRoomState(ctx context.Context, roomName livekit.RoomName) error
    GetRegion() string
    Start() error
    Drain()
    Stop()

    // === 新增：参与者级路由 ===
    SetParticipantNode(ctx context.Context, roomName livekit.RoomName,
        participantID livekit.ParticipantID, nodeID livekit.NodeID) error
    RemoveParticipantNode(ctx context.Context, roomName livekit.RoomName,
        participantID livekit.ParticipantID) error
    GetRoomParticipantNodes(ctx context.Context, roomName livekit.RoomName)
        (map[livekit.ParticipantID]livekit.NodeID, error)

    // === 新增：MediaRouter ===
    MediaRouter
}

// === 新增：媒体面路由 ===
type MediaRouter interface {
    CreateRTPRelay(ctx context.Context, targetNodeID livekit.NodeID) (RTPRelay, error)
    CloseRTPRelay(targetNodeID livekit.NodeID) error
}

type RTPRelay interface {
    WriteRTP(trackID livekit.TrackID, payload []byte) error
    WriteRTCP(trackID livekit.TrackID, payload []byte) error
    Close()
}
```

#### 1.3 RedisRouter 实现

```go
// pkg/routing/redisrouter.go — 新增方法

const (
    NodesKey               = "nodes"
    NodeRoomKey            = "room_node_map"
    RoomParticipantNodes   = "room_participant_nodes"  // 新增
)

func (r *RedisRouter) SetParticipantNode(ctx context.Context,
    roomName livekit.RoomName, participantID livekit.ParticipantID,
    nodeID livekit.NodeID) error {

    key := RoomParticipantNodes + ":" + string(roomName)
    return r.rc.HSet(ctx, key, string(participantID), string(nodeID)).Err()
}

func (r *RedisRouter) RemoveParticipantNode(ctx context.Context,
    roomName livekit.RoomName, participantID livekit.ParticipantID) error {

    key := RoomParticipantNodes + ":" + string(roomName)
    return r.rc.HDel(ctx, key, string(participantID)).Err()
}

func (r *RedisRouter) GetRoomParticipantNodes(ctx context.Context,
    roomName livekit.RoomName) (map[livekit.ParticipantID]livekit.NodeID, error) {

    key := RoomParticipantNodes + ":" + string(roomName)
    result, err := r.rc.HGetAll(ctx, key).Result()
    if err != nil {
        return nil, err
    }
    nodes := make(map[livekit.ParticipantID]livekit.NodeID, len(result))
    for pid, nid := range result {
        nodes[livekit.ParticipantID(pid)] = livekit.NodeID(nid)
    }
    return nodes, nil
}
```

#### 1.4 LocalRouter 桩实现

```go
// pkg/routing/localrouter.go — 新增桩方法

func (r *LocalRouter) SetParticipantNode(_ context.Context,
    _ livekit.RoomName, _ livekit.ParticipantID, _ livekit.NodeID) error {
    return nil
}

func (r *LocalRouter) RemoveParticipantNode(_ context.Context,
    _ livekit.RoomName, _ livekit.ParticipantID) error {
    return nil
}

func (r *LocalRouter) GetRoomParticipantNodes(_ context.Context,
    roomName livekit.RoomName) (map[livekit.ParticipantID]livekit.NodeID, error) {
    // 单节点模式下所有参与者都在本地
    return map[livekit.ParticipantID]livekit.NodeID{}, nil
}

func (r *LocalRouter) CreateRTPRelay(_ context.Context,
    _ livekit.NodeID) (RTPRelay, error) {
    return NewLocalRTPRelay(), nil
}

func (r *LocalRouter) CloseRTPRelay(_ livekit.NodeID) error {
    return nil
}
```

---

### Phase 2：RoomAllocator 参与者级分配

#### 2.1 RoomAllocator 重构

```go
// pkg/service/roomallocator.go
// 当前: SelectRoomNode — 房间级别分配
// 新增: SelectParticipantNode — 参与者级别分配

func (r *StandardRoomAllocator) SelectParticipantNode(
    ctx context.Context,
    roomName livekit.RoomName,
) (livekit.NodeID, string, error) {

    // 查询房间已有参与者的节点分布
    participantNodes, err := r.router.GetRoomParticipantNodes(ctx, roomName)
    if err != nil {
        return "", "", err
    }

    // 获取所有可用节点
    nodes, err := r.router.ListNodes()
    if err != nil {
        return "", "", err
    }

    // 优先选参与者数最少的节点（负载均衡）
    // 或使用 selector 策略
    node, err := r.selector.SelectNode(nodes)
    if err != nil {
        return "", "", err
    }

    return livekit.NodeID(node.Id),
        fmt.Sprintf("assigned by %s selector", r.config.NodeSelector.Kind), nil
}
```

**受影响文件：**

| 文件 | 变更 |
|------|------|
| `pkg/service/roomallocator.go` | 新增 `SelectParticipantNode`，调整 `CreateRoom` 逻辑 |
| `pkg/service/roomallocator.go` | 参与者离开时清理 `room_participant_nodes` |
| `pkg/service/roomservice.go` | 处理参与者加入时调用新分配逻辑 |

---

### Phase 3：跨节点 RTP 转发（核心）

#### 3.1 PSRPC 服务定义

新增 RPC 服务 `MediaRelayService`（在 protocol 层或本地定义）：

```protobuf
service MediaRelay {
    // 远端节点拉取指定 track 的 RTP 流
    rpc PullTrack(PullTrackRequest) returns (stream RTPPacket);
}

message PullTrackRequest {
    string track_id = 1;
    string subscriber_node_id = 2;
}

message RTPPacket {
    bytes payload = 1;
    uint32 timestamp = 2;
    // ... 其他 RTP header 字段
}
```

#### 3.2 新增文件：pkg/rtc/mediarelay.go

```go
// pkg/rtc/mediarelay.go — 新增

package rtc

import (
    "context"
    "sync"

    "github.com/livekit/protocol/livekit"
    "github.com/livekit/protocol/logger"
    "github.com/livekit/protocol/rpc"
    "github.com/livekit/psrpc"
)

// ---- 发布端（track 所在节点） ----

type MediaRelayServer struct {
    nodeID  livekit.NodeID
    server  rpc.TypedMediaRelayServer

    mu       sync.RWMutex
    tracks   map[livekit.TrackID]*relayedTrack
}

type relayedTrack struct {
    track    MediaTrack
    streams  map[livekit.NodeID]*streamWriter
}

// 当远端节点订阅时，建立 relay stream
func (s *MediaRelayServer) PullTrack(stream psrpc.ServerStream) error {
    req := <-stream.Channel()
    if req == nil {
        return nil
    }
    trackID := livekit.TrackID(req.TrackId)

    s.mu.RLock()
    rt, ok := s.tracks[trackID]
    s.mu.RUnlock()
    if !ok {
        return ErrTrackNotFound
    }

    // 将 stream 注册到 track 的转发列表中
    sw := &streamWriter{stream: stream}
    rt.streams[req.SubscriberNodeId] = sw

    // 等待 stream 结束
    <-stream.Context().Done()
    delete(rt.streams, req.SubscriberNodeId)
    return nil
}

// 收到本地 publish 的 RTP 时，转发到所有远端 relay
func (rt *relayedTrack) OnRTP(pkt []byte) {
    for _, sw := range rt.streams {
        sw.stream.Send(&RTPPacket{Payload: pkt})
    }
}

// ---- 订阅端（client 所在节点） ----

type MediaRelayClient struct {
    client rpc.TypedMediaRelayClient
}

func (c *MediaRelayClient) SubscribeRemoteTrack(
    ctx context.Context,
    remoteNodeID livekit.NodeID,
    trackID livekit.TrackID,
) (*remoteTrackReader, error) {

    stream, err := c.client.PullTrack(ctx, remoteNodeID)
    if err != nil {
        return nil, err
    }

    // 发送订阅请求
    err = stream.Send(&PullTrackRequest{
        TrackId:          string(trackID),
        SubscriberNodeId: c.localNodeID,
    })
    if err != nil {
        return nil, err
    }

    reader := &remoteTrackReader{
        stream: stream,
        ch:     make(chan []byte, 100),
    }
    go reader.run()
    return reader, nil
}

type remoteTrackReader struct {
    stream psrpc.Stream
    ch     chan []byte
}

func (r *remoteTrackReader) run() {
    for pkt := range r.stream.Channel() {
        select {
        case r.ch <- pkt.Payload:
        default:
            // channel full, drop
        }
    }
}

func (r *remoteTrackReader) ReadRTP() ([]byte, error) {
    pkt, ok := <-r.ch
    if !ok {
        return nil, io.EOF
    }
    return pkt, nil
}
```

**受影响文件：**

| 文件 | 变更 |
|------|------|
| `pkg/rtc/mediarelay.go` | **新增** — 跨节点 RTP 转发核心 |
| `pkg/rtc/room.go` | `ResolveMediaTrackForSubscriber` 支持远端 track |
| `pkg/rtc/mediatrack.go` | 新增 `OnRTP` 回调注册，支持 relay sender |

#### 3.3 Room 中集成远端 Track

```go
// pkg/rtc/room.go — ResolveMediaTrackForSubscriber 扩展

func (r *Room) ResolveMediaTrackForSubscriber(sub types.LocalParticipant,
    trackID livekit.TrackID) types.MediaResolverResult {

    // 1. 先查本地 track
    info := r.trackManager.GetTrackInfo(trackID)
    if info != nil {
        // 本地有 → 直接返回
        return localTrackResult(info)
    }

    // 2. 本地没有 → 查远端 track
    remoteTrack := r.mediaRelayClient.GetRemoteTrack(trackID)
    if remoteTrack != nil {
        // 创建 RemoteMediaTrack 代理
        return types.MediaResolverResult{
            Track: remoteTrack,
            // ...
        }
    }

    return types.MediaResolverResult{}
}
```

#### 3.4 Router 集成

```go
// pkg/routing/redisrouter.go — MediaRouter 实现

func (r *RedisRouter) CreateRTPRelay(ctx context.Context,
    targetNodeID livekit.NodeID) (RTPRelay, error) {

    // 通过 PSRPC 建立到目标节点的 relay
    stream, err := r.mediaRelayClient.CreateRelay(ctx, targetNodeID)
    if err != nil {
        return nil, err
    }
    return &psrpcRTPRelay{stream: stream}, nil
}
```

---

### Phase 4：Config 简化

#### 4.1 废弃配置项

```go
// pkg/config/config.go — RTCConfig 调整

type RTCConfig struct {
    rtcconfig.RTCConfig    `yaml:",inline"`

    // 保留
    TURNServers []TURNServer `yaml:"turn_servers,omitempty"`
    PacketBufferSize int     `yaml:"packet_buffer_size,omitempty"`
    // ... 其他非 NAT 相关配置

    // === 新增 ===
    MediaRelay MediaRelayConfig `yaml:"media_relay,omitempty"`
}

type MediaRelayConfig struct {
    Enabled    bool          `yaml:"enabled,omitempty"`
    Timeout    time.Duration `yaml:"timeout,omitempty"`
    BufferSize int           `yaml:"buffer_size,omitempty"`
}
```

#### 4.2 mediatransportutil 层调整

```go
// mediatransportutil/pkg/rtcconfig/webrtc_config.go

func NewWebRTCConfig(rtcConf *RTCConfig, development bool) (*WebRTCConfig, error) {
    // ...
    // 废弃: NAT1To1 地址重写逻辑 (line 89-116)
    // if !rtcConf.NodeIP.IsEmpty() && (rtcConf.UseExternalIP || !rtcConf.NodeIPAutoGenerated) { ... }

    // 新增: 始终启用 ICE Lite
    s.SetLite(true)

    // 新增: 默认使用单端口模式
    if !rtcConf.UDPPort.Valid() {
        rtcConf.UDPPort = PortRange{Start: 7882}
    }
    // ...
}
```

#### 4.3 NodeIP 获取优化

```go
// pkg/routing/node.go — NewLocalNode

func NewLocalNode(conf *config.Config) (*LocalNodeImpl, error) {
    nodeID := guid.New(utils.NodePrefix)

    // 优先使用 Pod IP（K8s）
    nodeIP := os.Getenv("POD_IP")
    if nodeIP == "" && conf != nil && !conf.RTC.NodeIP.IsEmpty() {
        // 回退到配置
        nodeIP = conf.RTC.NodeIP.PrimaryIP()
    }
    if nodeIP == "" {
        // 最终回退：本地接口
        ips, _ := rtcconfig.GetLocalIPAddresses(false, false, nil, nil)
        if len(ips) > 0 {
            nodeIP = ips[0]
        }
    }
    if nodeIP == "" {
        return nil, ErrIPNotSet
    }
    // ...
}
```

#### 4.4 TURN URL 生成调整

```go
// pkg/service/roommanager.go — iceServersForParticipant

// 当前：使用 NodeIP 生成 TURN URL
for _, ip := range r.config.RTC.NodeIP.ToStringSlice() {
    urls = append(urls, fmt.Sprintf("turn:%s?transport=udp",
        net.JoinHostPort(ip, strconv.Itoa(int(r.config.TURN.UDPPort)))))
}

// 新：使用 Domain 或 ExternalHost
if r.config.TURN.Domain != "" {
    urls = append(urls, fmt.Sprintf("turn:%s:%d?transport=udp",
        r.config.TURN.Domain, r.config.TURN.UDPPort))
} else if host := os.Getenv("EXTERNAL_HOST"); host != "" {
    urls = append(urls, fmt.Sprintf("turn:%s:%d?transport=udp", host, port))
}
```

---

### Phase 5：清理与兼容

#### 5.1 config-sample.yaml 更新

```yaml
# WebRTC configuration
rtc:
  # UDP port for WebRTC media — single port mode via UDPMux
  udp_port: 7882
  # TCP port for ICE TCP fallback
  tcp_port: 7881
  # ICE Lite mode — recommended for NAT/K8s deployments
  use_ice_lite: true

# Cross-node media relay (NAT mode)
media_relay:
  enabled: true
  timeout: 10s
  buffer_size: 4096

# Deprecated (no longer needed in NAT mode):
# port_range_start: 50000
# port_range_end: 60000
# use_external_ip: false
# node_ip: <external-ip>
# stun_servers: []
# advertise_internal_ip: false
# external_ip_only: false
```

#### 5.2 兼容处理

保留旧配置项的兼容解析，输出 deprecation warning：

```go
if conf.RTC.UseExternalIP {
    logger.Warnw("use_external_ip is deprecated in NAT mode, see docs for new config", nil)
}
if conf.RTC.ICEPortRangeStart != 0 || conf.RTC.ICEPortRangeEnd != 0 {
    logger.Warnw("port_range_start/end is deprecated, use udp_port instead", nil)
}
```

---

## 配置变更总览

| 配置项 | 当前 | 新架构 | 说明 |
|--------|------|--------|------|
| `rtc.node_ip` | 强制 | 废弃 | 改用 POD_IP env var |
| `rtc.port_range_start/end` | 默认 50000-60000 | 废弃 | 改 `udp_port: 7882` |
| `rtc.udp_port` | 可选 | 默认 7882 | 单端口模式 |
| `rtc.use_external_ip` | false | 废弃 | 不再需要 |
| `rtc.stun_servers` | 可选 | 废弃 | 不再需要 |
| `rtc.advertise_internal_ip` | false | 废弃 | 不再需要 |
| `rtc.external_ip_only` | false | 废弃 | 不再需要 |
| `rtc.use_ice_lite` | false | 默认 true | ICE Lite 适用单端口 |
| `rtc.interfaces` | 可选 | 保留 | ICE interface 过滤 |
| `rtc.ips` | 可选 | 保留 | ICE IP 过滤 |
| `rtc.tcp_port` | 7881 | 保留 | TCP fallback |
| `turn.*` | 可选 | 保留简化 | 使用 Domain 替代 NodeIP |
| `media_relay.*` | 不存在 | **新增** | 跨节点 RTP 转发 |

---

## K8s Service 配置参考

```yaml
apiVersion: v1
kind: Service
metadata:
  name: livekit
spec:
  type: LoadBalancer
  ports:
    - name: signaling     # WS 信令
      port: 7880
      targetPort: 7880
      protocol: TCP
    - name: media-udp     # WebRTC 媒体（单端口）
      port: 7882
      targetPort: 7882
      protocol: UDP
    - name: media-tcp     # ICE TCP fallback
      port: 7881
      targetPort: 7881
      protocol: TCP
  selector:
    app: livekit
```

---

## 实现路线图

```
Phase 1: Redis 数据结构 + Router 接口扩展
  ├── 新增 room_participant_nodes Redis hash
  ├── Router interface 扩展 (SetParticipantNode / GetRoomParticipantNodes)
  ├── RedisRouter 实现
  └── LocalRouter 桩实现

Phase 2: RoomAllocator 参与者级分配
  ├── 新增 SelectParticipantNode
  ├── 参与者加入/离开时维护 participant→node 映射
  └── 清理 room_node_map 的媒体相关依赖

Phase 3: 跨节点 RTP 转发
  ├── MediaRelayServer (发布端)
  ├── MediaRelayClient (订阅端)
  ├── Room 集成远端 Track 解析
  └── MediaRouter 接口集成到 Router

Phase 4: 配置简化
  ├── 废弃 NAT 相关配置
  ├── 默认单端口 + ICE Lite
  ├── NodeIP 获取逻辑优化
  └── TURN URL 生成移除 NodeIP 依赖

Phase 5: 清理与兼容
  ├── config-sample.yaml 更新
  ├── 旧配置 deprecation warning
  └── 文档更新
```
