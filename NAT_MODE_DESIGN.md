# NAT 多节点（方案 1）实现设计 — 边缘 transport + 房主 SFU

> 状态：Phase 1 ✅、3a ✅、3b ✅、3c（基础通道）✅。本文档定义拆分层与接口，并记录实现进展。
> 前置结论：旧 `pkg/routing/mediarelay.go`（Redis pub/sub + 裸 TCP）接错了层且握手是坏的，**已废弃删除**。

## 实现进展（截至当前）

| 阶段 | 内容 | 状态 |
|------|------|------|
| Phase 1 | `advertise_ip` 三权分立 | ✅ |
| 3a | `MediaChannel` 接口 + `localMediaChannel` + 删坏 relay | ✅ |
| 3b-down | `DownTrack.writeStream` 收窄为 `pacer.RTPWriteStream` | ✅ |
| 3b-up | 确认上行无需重构 SFU（见 5.2 修正） | ✅（分析结论） |
| 3c 基础 | `tcpMediaChannel`（framing + 双端）+ `MediaChannelRTPWriter` + `PumpRTP` | ✅ |
| 3c 接线 | `MediaRelay`（TCP listener 生命周期 + `DialNode` 节点发现）接入 `LivekitServer` | ✅ |
| 3d-1 | 信号节点 ID 传递：`metadata.IncomingHeader(ctx).RemoteID` → `ParticipantParams.SignalNodeID`（无 proto fork） | ✅ |
| 3d-2 | 控制面序列化：`ControlChannel` + `remotePeerConnection` + `RunRemotePCExecutor`（回环单测绿） | ✅ |
| 3d-3 | 媒体面 hello 帧关联：`MediaHello`/`HelloMediaChannel` + `MediaGateway.AttachHello`（TCP 回环单测绿） | ✅ |
| 3d-4 | 端到端垂直切片：房主 `remotePeerConnection` 经 TCP 控制面驱动边缘真实 pion PC（offer/answer）+ hello 帧 attach 下行 track（`TestRemotePCSplitOverTCP` 绿） | ✅ |
| 3d-5 | 网关会话握手：`GatewaySetup` + `NewEdgePeerConnection` + `RunEdgeGatewaySession`/`DialEdgeGatewaySession`（`TestEdgeGatewaySessionHandshake` 绿，PC 配置序列化选型已落地） | ✅ |
| 3d-6 | 边缘控制 accept loop + 会话注册表（`MediaRelay` 控制 listener + gateway registry）+ `StartSession` split 接线（节点发现/拨号/注入 `RemoteControlChannel`） | ✅ |
| 3d-7 | `DownTrack.BindRemote` seam（`RemoteBindContext` + `resolveCodec`/`finishBind` 抽取，纯重构 + 单测绿） | ✅ |
| 3d-8 | 注入 `MediaChannelDialer`（`rtc` 接口 + `MediaRelay.DialNodeHello` + `StartSession` 闭包 + `ParticipantParams`/`ParticipantImpl` seam） | ✅ |
| 验证 | 下行媒体面端到端（真实 ICE/DTLS/SRTP）：`MediaChannelRTPWriter` → `MediaChannel`(内存/TCP) → `MediaGateway` → `TrackLocalStaticRTP` → 客户端 pion PC 收包（`TestDownDirectionRTPFlow`/`...TCP` 绿） | ✅ |
| 3d-9 | 下行接通：`DownTrack.BindRemoteSelf` + `ParticipantImpl.addTrackLocalRemote`（dialer 建 MediaChannel + hello + `MediaChannelRTPWriter`，返回 nil sender/transceiver） | ✅ 接线（媒体面已验证，完整 participant 集成未端到端测） |
| 3d-10 | 上行控制面：`event_on_track` 协议 + 边缘 `MediaGateway.OnPublishedTrack`（SSRC→TrackRemote 注册）+ `RunEdgeGatewaySession` 事件转发 + 房主 `OnRemoteTrack` 分发 | ✅ 接线 |
| 3d-11 | 上行媒体面房主侧：`TrackRemoteFromMetadata`（sfu.TrackRemote 元数据实现）+ `handleRemotePublishedTrack`（dial 上行 MediaChannel + `PumpRTP` 灌 buffer） | ✅ 媒体面接线 |
| 3d-12 | `NewWebRTCReceiver` 解耦：签名改为接收 `headerExtensions` 而非 `*webrtc.RTPReceiver`（去掉 vestigial `receiver` 字段，唯一用途即 `GetParameters().HeaderExtensions`） | ✅ |
| 3d-13 | `AddReceiver` 远程等价：`mediaTrackReceivedRemote` + `AddReceiver(parameters)` + `handleRemotePublishedTrack` 注册 receiver/buffer | ✅ 接线 |
| 3e-1 | 下行 RTCP 跨节点：`pumpSenderRTCPToMediaChannel`（边缘 sender.ReadRTCP→MediaChannel）+ `DownTrack.ProcessRTCP` + `addTrackLocalRemote` RTCP 读回 | ✅ 接线 |
| 3e-2 | 上行 RTCP 跨节点：房主 `AddReceiver` 接受 `onRTCP` 覆盖 → `ch.WriteRTCP`；边缘 `pumpMediaChannelRTCPToPC`（`ch.ReadRTCP`→`pc.WriteRTCP`） | ✅ 接线 |
| 3e-3 | header extensions 解析：`sfuutils.ExtractHeaderExtensionsFromSDP`（解析 `a=extmap`，从房主 remote description 重建协商扩展，单测绿）+ `handleRemotePublishedTrack` 接入 | ✅ |
| 3e-4 | dual-PC 双会话：`SubscriberRemoteControlChannel`/`SubscriberRemotePeerConnection` + `StartSession` 为 dual-PC 建 publisher/subscriber 两个远程会话（`IsOfferer` 区分方向） | ✅ 接线 |
| 验证 | 真节点端到端验证：双节点 kind K8s 复现套件 `test/nat-e2e/`（20/20 断言绿，含 `--loss 5%` WAN 丢包）。覆盖 NAT split、上下行媒体面（H264 simulcast 30fps 实测）、RTCP 双向、dual-PC、数据通道、质量反馈跨节点 | ✅ |
| 3e | 订阅/选层/拥塞控制跨节点同步：本架构 SubscriptionManager/dynacast/stream-allocator/pacer 全在房主节点，选层/订阅本就本地决策，无需跨节点同步；跨节点反馈（REMB/NACK/PLI/RR/TWCC）已由 3e-1/3e-2 的 RTCP 通道覆盖 | ✅（剩余即真节点验证） |
| 3e-5 | **publisher 上行 RTCP 桥（P0-3 根因修复）**：边缘网关 `pumpReceiverRTCPToMediaChannel`（读 publisher RTPReceiver 的 SR/RR 上行写 MediaChannel）+ 房主 `handleRemotePublishedTrack` 把 up MediaChannel RTCP 灌入 buffer rtcpReader（`SetSenderReportData`→`OnRtcpSenderReport`→forwarder `SetRefSenderReport`）。此前 publisher 的 RTCP（尤其 SR）从不跨节点 → `getRefLayerRTPTimestamp` 无 sender report → 层切换（上切/下切）失败 | ✅ 接线 + E2E |
| 验证 | P0-3 simulcast 上切端到端：`test/nat-e2e/client/simulcast.go`（raw-pion 3-RID VP8 + per-layer PLI→keyframe + 250ms SR）→ forwarder `upgrading layer` 到 1/2 + 各高层 `forwarded key frame`（06 脚本断言）+ **客户端下行 bitrate 断言**（LOW/MEDIUM/HIGH ≥ 60/300/1200 kbps，实测 98/470/1560；根因 `sfu/buffer.Buffer` pending 队列溢出 bug 已修） | ✅ |
| 3f-1 | 房主节点故障迁移触发（#51）：边缘 `MediaRelay.unregisterGateway` → `RTCService.OnGatewayLost`（延迟 7s 确认房主节点 keepalive 过期/已被摘除，避免误判正常 teardown）→ 向客户端发 `Leave(RECONNECT)` + 关 WS，客户端重连并驱动房间重归 | ✅ 接线 + E2E |
| 3f-2 | 死节点周期摘除：`RedisRouter.cleanupWorker`（每 10s：`RemoveDeadNodes` + `room_node_map` 指向死节点的条目清理）+ `GetNodeForRoom` 对 stale 映射惰性清理（返回 `ErrNotFound`，join 立即重归到存活节点） | ✅ 接线 + E2E |
| 验证 | 房主节点故障迁移端到端（#51）：`room-node-failure` 场景**强杀房主节点 pod**（scale 0 + force-delete）→ 客户端 `SIGNAL_CLOSE` 断开 → 死节点被摘除 + 房间重归（`room_node_map` 落到存活节点）→ resume 干净拒绝（`STATE_MISMATCH`）→ 同身份全量重连后房间在新节点重建（跨节点，signalNodeID≠nodeID）+ 媒体恢复 | ✅ |
| 3g-1 | TURN relay-only 全路径（#50）：双客户端强制 `ICETransportPolicyRelay`（唯一候选 = relay）→ **完整 ICE 经 TURN relay 建立** + 媒体经 relay 端到端流通 + 双端选中候选对均为 relay。根因是 `turn.go` `permissionHandler` 默认拒绝私有 IP peer（relayed socket 的 STUN check 到私有边缘 host candidate 失败），**配置问题非服务器 bug**：真网公网 host candidate 默认放行；私有网（含 kind）需 `allow_restricted_peer_cidrs` | ✅ 集群内修复 + E2E |
| 验证 | TURN relay-only 端到端（#50）：`turn-relay-only` 场景（`configs/config.yaml` 加 `allow_restricted_peer_cidrs: [192.168.0.0/16, 10.0.0.0/8, 172.16.0.0/12]`）→ 双端 relay-only ICE connected + `IsRelaySelectedOnAnyTransport` 双端 true + 媒体经 relay 流通（`waitBytes`）。真网公网 host candidate 无需 allow-list，relay-only 直连开箱即用 | ✅ |
| 3h-1 | 生产安全（#A1）：`media_relay` 默认关闭（`Enabled=false`，NAT 显式开启）+ **fail-closed 鉴权**（`Secret` 空 → 服务端拒绝入站、拨号端报错，NAT 无法在无 secret 下运行）+ 启动校验（`enabled && secret==空` 启动失败）。修复"fork 默认监听两个无鉴权内网端口"的安全回归 | ✅ 单测 + E2E |
| 3h-2 | 优雅排空迁移（#A2）：`Stop(false)` 在 NAT 模式下**主动** `CloseAllRooms`（participant `Close(true, MigrationRequested, true)` → `Leave(RESUME)`）→ 客户端迁移 → 房间重归 → 参与者退出 → pod 干净退出（远早于 SIGKILL）。修复滚动更新"等 participant 永不退出 → 30s SIGKILL 崩溃式迁移" | ✅ 手动验证 + E2E `room-node-drain` |
| 3h-3 | 并发建立（#A3）：16 客户端并发加入**集群内 16/16 ×4 全过**——服务端并发处理可靠（网关会话 50ms 内全部建立）；历史 "4+4 只建 2/8" 为 **host 测试路径伪影**（宿主机经 VM 桥，N=16 偶发 ICE 超时）。`concurrent-join` 场景（N=8 host 确定性）固化回归守卫 | ✅ 集群内证明 + 回归守卫 |

## 1. 目标与范围

- `Participant` / `Room` / `SubscriptionManager` / SFU（UpTrack receiver、DownTrack forwarder、dynacast、拥塞控制）**全部留在房主节点**，逻辑不变。
- 只有 **WebRTC transport**（ICE / DTLS / SRTP / UDP 收发）放在客户端 WS 连到的**边缘节点**。
- 跨节点只传**明文 RTP/RTCP**（SRTP 已在边缘节点解过密），走 Pod 内网。

## 2. 数据流

```
客户端 ──SRTP/UDP──► 边缘节点 PCTransport（ICE/DTLS/SRTP 加解密）
                         │ 明文 RTP/RTCP（内部通道，逐 track，双向）
                         ▼
房主节点 TransportManager + SFU（receiver / DownTrack / dynacast / congestion）
```

信令（SDP offer/answer、ICE candidate）**继续复用现有 PSRPC 信令中继**
（`routing/signal.go` 的 `signalClient.RelaySignal` → `service/signal.go` 的 `RelaySignal`），
只是 ICE candidate 从边缘节点发出（用 Phase 1 的 `advertise_ip`）。

## 3. 三个拆分层

### 3.1 信令（无需改动）
现有中继已把 `StartSession`/offer/answer/candidate 在边缘↔房主之间转发。
唯一新增：媒体 candidate 来自边缘节点 PC（Phase 1 `advertise_ip` 已覆盖）。

### 3.2 上行（publisher → SFU）
- 边缘节点 `PCTransport` 的 `Handler.OnTrack(track *webrtc.TrackRemote, rtpReceiver)`
  （`pkg/rtc/transport/handler.go:41`）触发后，读明文 RTP → 经 `MediaChannel` 发给房主。
- 房主节点的 `WebRTCReceiver`（`pkg/sfu/receiver.go`）从 `MediaChannel` 读明文 RTP，
  替代本地 `webrtc.TrackRemote.ReadRTP()`。

### 3.3 下行（SFU → subscriber）
- 房主节点 `DownTrack`（实现 `webrtc.TrackLocal`，`pkg/sfu/downtrack.go:318`）写明文 RTP
  → 经 `MediaChannel` 发给边缘。
- 边缘节点把收到的明文 RTP 写入本地 pion `TrackLocal`，由边缘 PC 做 SRTP 加密发送。

## 4. 核心抽象：`MediaChannel`

```go
// pkg/rtc/transport/mediachannel.go（新）
// 跨节点明文 RTP/RTCP 通道，逐 track，双向。房主与边缘各持一端。
type MediaChannel interface {
    // 下行：房主写明文 RTP → 边缘
    WriteRTP(payload []byte, ssrc uint32) error
    WriteRTCP(payload []byte) error
    // 上行：边缘读明文 RTP → 房主
    ReadRTP() (payload []byte, err error)
    ReadRTCP() (payload []byte, err error)
    Close()
}
```

两个实现，同一接口：
- `localMediaChannel`：进程内（单节点），直接 goroutine + channel，零拷贝；单节点模式走这里，**行为与现状完全一致**。
- `tcpMediaChannel`：跨节点自包含 TCP 通道（1 字节类型 + 4 字节长度帧化 + hello 帧），`MediaRelay` 监听
  `media_relay.port`（7883），节点发现后 `DialNodeHello` 拨号（§11）。首版采用 TCP（简单、无需复用消息总线）；
  接口不变，后续如需更低延迟可替换为专用 UDP 或 QUIC。

> 备注（与早期设计的差异）：最初曾计划首版用 PSRPC 流复用现有消息总线；实现时改用了自包含 TCP
> （`pkg/rtc/transport/tcpmediachannel.go`），避免 RTP 大流量压 PSRPC/Redis 通道。§7 文件清单与 §11 配置均已按 TCP 落地。

## 5. SFU 边界收窄（关键，分两步走，都是纯重构、可先合入）

### 5.1 DownTrack 解耦
`DownTrack` 当前通过 `writeStream webrtc.TrackLocalWriter`（`pkg/sfu/downtrack.go:358`）写包。
把写入抽象成窄接口 `rtpWriter`：
- 本地模式：现有 `webrtc.TrackLocalWriter`。
- 远端模式：`MediaChannel` 的 `WriteRTP`。

`Bind(t webrtc.TrackLocalContext)`（`downtrack.go:502`）只在本地模式走到 pion；远端模式不 bind pion，
改为持有 `MediaChannel`。

### 5.2 上行读入口（已修正结论，无需重构 SFU）

**修正**：上行 RTP 并非由 `WebRTCReceiver` 显式 `ReadRTP`，而是 pion 通过
`packetio.BufferFactory` 机制把 RTP **写入** `buffer.Buffer.Write([]byte)`（`pkg/sfu/buffer/buffer.go:141`）。
因此 `buffer.Buffer` 的 `[]byte` 接口本身就是干净边界，`WebRTCReceiver`/`ReceiverBase` 无需改动。

远端模式只需一个泵 goroutine：`PumpRTP(src MediaChannel, dst io.Writer)`（已实现，
`pkg/rtc/transport/mediachannelrtp.go`），把 `MediaChannel.ReadRTP()` 的明文 RTP 写入房主节点的
`buffer.Buffer`。本地模式无变化（pion 仍直接写入 buffer）。


## 6. Transport 层拆出（3d，最大的一步）

目标：让 pion PC（ICE/DTLS/SRTP/UDP）跑在**边缘节点**，`Participant`/SFU 留在**房主节点**，
中间经 `MediaChannel`（明文 RTP/RTCP）+ 现有 PSRPC 信令中继串起来。

### 6.1 前置：把「信号节点(边缘节点)ID」传到房主节点 ✅ 已完成

**结论**：走 **B（PSRPC 元数据，无 proto fork）**。psrpc 的 `metadata.IncomingHeader(ctx).RemoteID`
即调用方节点 ID（`pkg/server/stream.go:163` 从 stream open 的 `NodeId` 读取），且
`signalService.RelaySignal` 已把它拷进 `ctx` 传给 `HandleSession` → `StartSession`。

实现：`RoomManager.StartSession` 里
`signalNodeID := metadata.IncomingHeader(ctx).RemoteID`（缺省回退 `currentNode.NodeID()`），
存入 `ParticipantParams.SignalNodeID`，`ParticipantImpl.SignalNodeID()` 可读。

### 6.2 总体架构（transport split）

核心思想：**把 pion `PeerConnection` 的「传输面」从房主节点搬到边缘节点**，而
`PCTransport` 的「协商状态机 + SFU 集成」留在房主节点。二者之间用 PSRPC 序列化控制面、
`MediaChannel` 传媒体面。

```
客户端 ──WS/SDP/ICE/UDP──► 边缘节点 A                        房主节点 B
                          ┌─────────────────┐              ┌──────────────────────┐
                          │ MediaGateway    │   PSRPC      │ RemotePCTransport    │
                          │  (真 pion PC)   │◄────────────►│  (协商状态机+SFU)     │
                          │  TrackLocalStatic│  控制面     │   DownTrack/buffer    │
                          └───┬─────────▲───┘              └───┬──────────▲────────┘
                              │MediaCh. │(明文RTP/RTCP)        │MediaCh.  │
                              └─────────┴──────────────────────┘          │
```

### 6.3 拆分点：pion `PeerConnection`（`t.pc`）的「传输面」接口 ✅ 已确认

**实测结论**：`PCTransport` 内部对 `t.pc` 有 **~75 个调用点 / ~26 个方法**，全部是纯传输
（SDP/ICE/track/data channel/状态/RTCP）。而 `TransportManager`→`PCTransport` 的 ~50 个方法
**混杂 SFU**（`AddTrackToStreamAllocator`/`GetPacer`/`GetRTPReceiver`/`WriteRTCP`）。

**所以拆分点是 `t.pc`，不是 `TransportManager` 边界**：`PCTransport`（含 SFU stream allocator/
pacer）整体留在房主节点，只把 `t.pc` 抽象成接口，本地=真实 pion PC、远端=PSRPC 代理到边缘网关。

房主节点要序列化到边缘节点的 **pion PC 方法**（经 PSRPC 控制通道）：

| 房主调用 | 方向 | 边缘执行 |
|---------|------|---------|
| `SetRemoteDescription(offer/answer)` | B→A | `pc.SetRemoteDescription(sd)` |
| `SetLocalDescription` / `CreateAnswer` | B→A | 生成 answer，回传 B |
| `AddICECandidate(candidate)` | B→A | `pc.AddICECandidate(c)` |
| `AddTrack(trackLocal)`（每个订阅 track） | B→A | 边缘建 `TrackLocalStaticRTP`，RTP 走 `MediaChannel` |
| `Close()` | B→A | `pc.Close()` |

**pion PC 事件**（A→B 经 PSRPC）：

| 边缘事件 | 方向 | 房主处理 |
|---------|------|---------|
| `OnICECandidate(c)` | A→B | 经信号中继回客户端 |
| `OnICEConnectionStateChange` / `OnConnectionStateChange` | A→B | 触发 `Handler.OnFailed`/`OnInitialConnected` |
| `OnTrack(track, receiver)` | A→B | 元数据（SSRC/RID/codec）回 B，RTP 走 `MediaChannel` |

### 6.4 组件 1：`MediaGateway`（边缘节点，新组件）

位置：`pkg/rtc/transport/mediagateway.go`。

职责：持有一个「裸」pion PC（复用 `transport.go` 的 `newPeerConnection` 配置逻辑，但不带 SFU
stream allocator/buffer factory），并提供双向 RTP 桥。

```go
type MediaGateway struct {
    pc *webrtc.PeerConnection

    mu         sync.RWMutex
    downTracks map[livekit.TrackID]*gatewayDownTrack // 订阅：MediaChannel → TrackLocalStaticRTP
    upTracks   map[livekit.TrackID]*gatewayUpTrack   // 发布：TrackRemote → MediaChannel
}

type gatewayDownTrack struct {
    local *webrtc.TrackLocalStaticRTP // pc.AddTrack(local) 后，WriteRTP 即加密发送
    ch    MediaChannel
}

type gatewayUpTrack struct {
    remote *webrtc.TrackRemote
    ch     MediaChannel
}
```

- **下行桥**：`go pumpMediaChannelToTrackLocal(ch, local)` ——
  `ch.ReadRTP()` → `rtp.Packet.Unmarshal` → `local.WriteRTP(pkt)`（pion SRTP 加密发送）。
- **上行桥**：`go pumpTrackToMediaChannel(remote, ch)` ——
  `remote.ReadRTP()` → `pkt.Marshal` → `ch.WriteRTP(bytes)`。
- **track 关联**：`MediaChannel` 建立后，首帧是「hello」帧（trackID + 方向 + SSRC/RID/codec），
  网关据此把通道挂到对应 track（见 6.6）。

### 6.5 组件 2：`RemotePCTransport`（房主节点）

位置：`pkg/rtc/transport.go`（或新文件 `pkg/rtc/remote_transport.go`）。

职责：实现与 `PCTransport` 相同的「传输面」窄接口（`SetRemoteDescription`/`AddICECandidate`/
`AddTrack`/事件回调），但内部：
- 控制面：经 PSRPC 发给边缘 `MediaGateway`。
- 媒体面：`DownTrack` 的 `writeStream` = `MediaChannelRTPWriter`（已实现）；buffer 由
  `PumpRTP(MediaChannel, buffer)` 灌入（已实现）。

关键：`TransportManager`（`pkg/rtc/transportmanager.go`）当前 `publisher`/`subscriber` 字段是
`*PCTransport`。需把它们的「传输面」抽象成窄接口 `transport.Transport`（本地 `PCTransport` /
远端 `RemotePCTransport` 都实现），`TransportManager` 按
`params.SignalNodeID == 本地节点` 选择本地或远端实现。这是「本地 vs 远端分支」的落点。

### 6.6 信令两路拆分 + MediaChannel 建立

1. 客户端 WS → 边缘 A。A 调 `GetNodeForRoom` 得知房主 B。
2. 若 A == B：**单节点**，走现有路径（零变化）。
3. 若 A != B（NAT split）：
   - A 建 `MediaGateway`（pion PC，本地处理 offer/answer/ICE，不再经信号中继把 SDP 发给 B）。
   - A 经 PSRPC 把 `StartSession`（含 `SignalNodeID=A`，3d-1 已实现）发给 B。
   - B 建 `Participant` + `RemotePCTransport`（不建本地 pion PC）。
   - B 需要媒体时（下行订阅 / 上行发布），`MediaRelay.DialNode(A)` 建 `MediaChannel`，
     发 hello 帧（trackID + 方向），A 网关 `Accept` 后按 hello 帧挂载 track。
   - SDP 协商结果（mid↔trackID/ssrc 映射、negotiation 状态）由 B 的协商状态机持有，
     通过 PSRPC 把 `SetRemoteDescription`/`AddICECandidate` 下发 A 执行，A 的事件回传 B。

> 注意：SDP/ICE 的**逻辑**仍在房主 B（复用现有 `PCTransport` 的协商状态机），
> 只有 pion PC 的**执行**在边缘 A。这样 `getMidToTrackIDMapping`/`OnOffer`/`OnAnswer`
> 等 SFU 集成逻辑不用重写。

### 6.7 goroutine 模型与生命周期

- 每个 `MediaChannel` 一个读 goroutine（`tcpMediaChannel.readLoop`，已实现）。
- 每个下行 track：`pumpMediaChannelToTrackLocal` goroutine（边缘）。
- 每个上行 track：`pumpTrackToMediaChannel` goroutine（边缘）+ `PumpRTP` goroutine（房主）。
- 任一方向 `Close` → 关 `MediaChannel` → 对端读返回 `ErrMediaChannelClosed` → 级联关闭 track。

### 6.8 失败/重连/迁移语义（待细化实现时处理）

- **ICE 失败**：边缘 pion PC 事件回传 B，B 复用现有 `handleConnectionFailed`/`OnFailed`。
- **客户端重连**：WS 可能落到不同边缘节点 → 需重建设 `MediaGateway` 或复用，
  走现有 `ResumeParticipant` 语义（transport 重建）。
- **房主迁移**：`Migration` 现有语义下，媒体面经 `MediaChannel` 重新拨号新边缘节点。
- **数据通道(SCTP)**：已跨节点打通（P0-2）：边缘 executor 在 DC open 时 `DetachWithDeadline` +
  `ReadDataChannel` 泵入 `event_data_message`（服务端全局 `se.DetachDataChannels()` 使 pion 不跑
  OnMessage 读循环，故不能依赖 `dc.OnMessage`）。下行经 `send_data_message` → 边缘 `SendText`。
- **simulcast 上切（P0-3）**：已打通——PLI 响应式 3 层发布者（`test/nat-e2e/client/simulcast.go`）
  在 per-layer PLI 时发该层关键帧 + 250ms 周期 RTCP Sender Report；forwarder 层锁释放并上切
  （房主日志 `upgrading layer`→1/2 + 各高层 `forwarded key frame`）。**关键产品修复**：publisher
  上行 RTCP 桥（见进度表末行 + 3e）——此前 publisher 的 SR/RR 不跨节点，`getRefLayerRTPTimestamp`
  拿不到 sender report，任何层切换（含下切）都失败。**下行冻结根因（已解决）**：逐跳计数证明
  跨节点桥 100% 无损（发布者写入 = 边缘 pump 读取、房主 TCP 写入 = 边缘 down-pump 读取、边缘
  bytesSent 与客户端 transport bytesReceived 持续增长），冻结在**测试客户端** `track.ReadRTP()`
  经 `sfu/buffer.Buffer.Read` 读 pending 队列处——入队突发（加入时初始 HIGH 层）使 reader 落后
  >500 包触发 `MaxVideoPkts` 溢出截断，`lastPacketRead` 未随截断前移 → 永久阻塞。修复：`buffer.go`
  溢出截断时同步调整 `lastPacketRead`。客户端下行 bitrate 断言已复开（LOW/MEDIUM/HIGH ≥ 60/300/1200
  kbps，实测 98/470/1560）。

### 6.9 分步实现（每步可独立合入、单节点零回归）

1. ✅ 6.1 信号节点 ID 传递（已做）。
2. ✅ **边缘桥原语**：`pumpMediaChannelToTrackLocal` + `pumpTrackToMediaChannel`
   （`pkg/rtc/transport/mediabridge.go`），对称于已实现的 `MediaChannelRTPWriter`/`PumpRTP`，用 fake 单测。
3. ✅ **`MediaGateway`**（`pkg/rtc/transport/mediagateway.go`）：裸 pion PC + 双向桥
   （`AddSubscriberTrack`/`AddPublisherTrack`）+ track 注册表 + Close，fake/真实 PC 单测。
   （hello 帧自动关联留到步骤 6 接线时。）
4. ✅ **抽象 `t.pc`**：定义 `peerConnection` 接口（~31 方法）+ `localPeerConnection` 包装
   （补 `GatheringComplete`），`PCTransport.pc` 改用该接口，~75 调用点机械替换。纯重构、
   单节点零回归（`go test ./pkg/rtc` 全绿）。
5. ✅ **控制面序列化 + `remotePeerConnection` + 边缘 executor**：`ControlChannel`、`remotePeerConnection`
   （`pkg/rtc/remote_transport.go`，实现 `peerConnection`，SDP/ICE/状态走 request/response + 事件回调）、
   `RunRemotePCExecutor`（`pkg/rtc/remote_executor.go`，收到请求应用到真实 pion PC + 转发事件）。
   进程内回环测试（真实 pion PC 走完整控制面）全绿。`AddTrack` 族等 11 方法留 stub（待步骤 6 接线）。
6. 🔶 **join 接线**：`TransportParams.RemotePeerConnection` 注入 seam ✅（`createPeerConnection`
   分支 `setupRemotePeerConnection`，本地零回归；`Close()` 改 fire-and-forget）。
   hello 帧关联 ✅（`MediaHello`/`HelloMediaChannel` + `MediaGateway.AttachHello`，TCP 回环单测绿）。
   端到端垂直切片 ✅（`TestRemotePCSplitOverTCP`：房主 `remotePeerConnection` 经 TCP 控制面驱动边缘
   真实 pion PC 的 offer/answer，下行 track 经 hello 帧 attach；`RunRemotePCExecutor` 已改为接受
   `*webrtc.PeerConnection`，解除跨包不可见接口依赖）。
   网关会话握手 ✅（`GatewaySetup`/`NewEdgePeerConnection`/`RunEdgeGatewaySession`/`DialEdgeGatewaySession`：
   PC 配置序列化选型落地为「两端同二进制，仅 codec 列表 + 角色标志走线」；`TestEdgeGatewaySessionHandshake` 绿）。
   join 接线 ✅（`MediaRelay` 控制 listener + gateway registry（按 `SessionID`）+ `RoomManager.StartSession`
   检测 `SignalNodeID != 本地` → `nodeByID`/`DialNodeControl`/`DialGateway` → 注入
   `ParticipantParams.RemoteControlChannel` → `setupTransportManager` 建 `remotePeerConnection`；
   `TestMediaRelayControlSession` 绿。单 PC/one-shot 模式已支持，dual-PC 需双会话）。
   下行接通 ✅（`DownTrack.BindRemoteSelf` + `ParticipantImpl.addTrackLocalRemote`：dialer 建 MediaChannel
   + hello + `MediaChannelRTPWriter`；SSRC 本地随机（边缘 `TrackLocalStaticRTP` 重写，媒体面已验证）。
   注：协商 codec 首版用上游 codec 自匹配，客户端 codec 回退的正确性 + RTCP 反馈（NACK/PLI）跨节点留 3e）。
   上行控制面 ✅（`event_on_track` + 边缘 `MediaGateway.OnPublishedTrack`（SSRC→TrackRemote）+ 事件转发 +
   房主 `OnRemoteTrack` 分发，构建绿）。
   上行媒体面 ✅（3d-11：`handleRemotePublishedTrack` dial 上行 MediaChannel + `PumpRTP` 灌 buffer）。
   RTCP 反馈跨节点 ✅（3e-1/3e-2，见进度表）。dual-PC 双会话 ✅（3e-4）。
7. **3e**：订阅/选层/拥塞控制跨节点同步 —— 本架构中这些决策全在房主节点本地，无跨节点同步需求；
   跨节点 RTCP 反馈（REMB/NACK/PLI/RR/TWCC）已由 3e-1/3e-2 覆盖。剩余工作即真节点端到端验证（进度表末行 ⏳）。

## 7. 文件级改动清单

| 文件 | 改动 | 状态 |
|------|------|------|
| `pkg/rtc/transport/mediachannel.go` | `MediaChannel` 接口 | ✅ 3a |
| `pkg/rtc/transport/localmediachannel.go` | 进程内双端实现 | ✅ 3a |
| `pkg/rtc/transport/tcpmediachannel.go` | **自包含 TCP 通道**（framing + hello 帧 + listener + dial） | ✅ 3c |
| `pkg/rtc/transport/mediahello.go` | `MediaHello` + `HelloMediaChannel`（hello 帧 track 关联） | ✅ 3d |
| `pkg/rtc/transport/mediachannelrtp.go` | `MediaChannelRTPWriter`（下行）+ `PumpRTP`（上行） | ✅ 3c |
| `pkg/rtc/transport/mediabridge.go` | `pumpMediaChannelToTrackLocal` / `pumpTrackToMediaChannel`（边缘桥） | ✅ 3c |
| `pkg/rtc/transport/mediagateway.go` | `MediaGateway`（边缘组件：裸 PC + 双向桥 + `AttachHello`） | ✅ 3c |
| `pkg/rtc/transport/controlchannel.go` | `ControlChannel`（TCP 控制面通道 + listener/dial） | ✅ 3d |
| `pkg/rtc/transport.go` | `peerConnection` 接口 + `localPeerConnection` + `RemotePeerConnection` 注入 seam | ✅ 3d |
| `pkg/rtc/remote_transport.go` | `remotePeerConnection`（房主侧控制面代理） | ✅ 3d |
| `pkg/rtc/remote_executor.go` | `RunRemotePCExecutor`（边缘侧控制面执行器，接受 `*webrtc.PeerConnection`） | ✅ 3d |
| `pkg/rtc/remote_integration_test.go` | 端到端垂直切片单测（TCP 控制面 + hello 帧媒体面） | ✅ 3d |
| `pkg/rtc/gateway.go` | `GatewaySetup` + `NewEdgePeerConnection` + `DialGateway` + `RunEdgeGatewaySession`/`DialEdgeGatewaySession`（会话握手） | ✅ 3d |
| `pkg/routing/mediarelay.go` | **删除**（废弃） | ✅ 3a |
| `pkg/sfu/pacer/pacer.go` + `downtrack.go` | 写入口收窄为 `RTPWriteStream` + `DownTrack.BindRemote`/`RemoteBindContext`（远程绑定 seam） | ✅ 3b/3d |
| `pkg/config/config.go` | `advertise_ip` + `media_relay.port`（7883）/`control_port`（7884） | ✅ 1/3c |
| `pkg/service/mediarelay.go` | `MediaRelay`（媒体+控制 TCP listener + gateway registry（`SessionID`）+ accept loop + `DialNode`/`DialNodeControl`） | ✅ 3d |
| `pkg/service/server.go` | `MediaRelay` 接线（`SetRTCConfig`/`SetMediaRelay`） | ✅ 3d |
| `pkg/rtc/transportmanager.go` | `RemotePeerConnection` 贯穿到 publisher/subscriber `TransportParams` | ✅ 3d |
| `pkg/rtc/participant.go` | `ParticipantParams.SignalNodeID` + `RemoteControlChannel` + `SignalNodeID()` | ✅ 3d |
| `pkg/service/roommanager.go` | `StartSession` 提取 `SignalNodeID` + split 检测/节点发现/拨号/注入 | ✅ 3d |

## 8. 分阶段（每步可独立合入、不破坏单节点）

1. **3a** — `MediaChannel` 接口 + `localMediaChannel`，删除坏 `mediarelay.go`。（纯新增/删除，无行为变化）
2. **3b** — `DownTrack`/`WebRTCReceiver` 收窄到 `rtpWriter`/`rtpReader`，本地实现接回 pion。（纯重构，测试覆盖回归）
3. **3c** — `psrpcMediaChannel` + 边缘 transport 拆出 + 房主远端 transport 代理。（核心，最大）
4. **3d** — join 流程接线（记录客户端节点、决定 split、候选跨节点）。
5. **3e** — 订阅/选层/拥塞控制跨节点同步（复用现有 `UpdateSubscribedQuality` 等钩子）。

## 9. 风险与待定

- **SRTP 逐跳密钥**：边缘节点解密后是明文，房主↔边缘内网通道需自建鉴权/加密（至少 TLS 或内网隔离）。
- **RTCP/NACK/RTX/pli/fir**：这些反馈要随 RTCP 一起跨节点，`MediaChannel` 需承载 RTCP（已含在接口）。
- **数据通道(SCTP)**：已打通（P0-2，见 §6.8）。已知限制：`DetachDataChannels()` 全局开启下，
  依赖 detached 读循环；TURN relay-only 全路径已在集群内验证（#50，见下）。
- **拥塞控制状态**：stream allocator / TWCC 状态在房主节点，RTCP feedback 跨节点回流即可。
- **时序/时钟**：跨节点明文 RTP 需保证单调发送节奏（pacer 在房主节点，发送到边缘的调度要保序）。

## 10. 与现有 scaffold 的关系

- 保留：`advertise_ip`（Phase 1）、`room_participant_nodes`（后续可用于负载均衡/诊断）、
  TURN URL 解耦、`POD_IP` 路由。
- 废弃重写：`mediarelay.go` 的 TCP+Redis pub/sub、`MediaRouter` 接口的 `CreateRTPRelay` 签名
  （换成逐 track 的 `MediaChannel`）。

## 11. 配置示例与迁移说明（Phase 5）

### K8s NAT 多节点部署配置

每个 Pod 设置 `POD_IP` 环境变量（内部 pod IP，供 PSRPC 路由 + media_relay 拨号），
`advertise_ip` 指向对外可达的 LB/公网 IP：

```yaml
# config.yaml（所有节点同构）
rtc:
  node_ip: ${POD_IP}          # 或省略，用 POD_IP 环境变量
  advertise_ip: <external-ip> # 客户端可达的 LB/公网 IP（ICE + TURN URL）
  media_relay:
    enabled: true
    port: 7883                # 内部明文 RTP/RTCP 端口（pod 间可达）
    control_port: 7884        # 内部 pion PC 控制端口（pod 间可达）
```

### 迁移说明

- **单节点 → NAT 多节点**：只需设 `advertise_ip` + 开 `media_relay`；单节点模式下
  `SignalNodeID == 本地节点`，走原路径零变化（本地零回归已由测试覆盖）。
- **客户端无感**：ICE candidate 与 TURN URL 仍指向 `advertise_ip`，客户端无改动。
- **网络要求**：`port`/`control_port` 必须在 pod 间可达（明文，需内网隔离或 TLS，见 §9）。
- **限制**：首版 split 支持单 PC/one-shot 与 dual-PC 双会话、上行方向、RTCP 反馈跨节点（见进度表 3d/3e）；
  数据通道（SCTP）跨节点已打通（P0-2，见 §6.8）——边缘 executor 在 DC open 时 detached 读泵入
  `event_data_message`。待真节点端到端验证（进度表末行 ⏳）。
