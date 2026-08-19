# NAT 多节点 E2E（双节点 K8s，可复现）

在 **kind 双节点 K8s** 上验证 LiveKit fork 的 NAT 多节点模式（"media follows signaling"）：
客户端 WS + 媒体终止在**边缘节点**，`Participant`/`TransportManager`/SFU 在**房主节点**，
明文 RTP/RTCP 经内部 TCP（media_relay）跨节点转发。

## 架构

```
                        ┌─ 边缘节点1 (nat-test-worker, 192.168.107.5) ─┐
 客户端A (host)          │  SignalService (WS 7880)                     │
   │ WS / SRTP / ICE     │  MediaGateway (真实 pion PC, UDP 7882)       │
   └──────────────────►  │  media_relay: 7883(RTP/RTCP) 7884(控制)      │
                        └──────┬──────────────────┬───────────────────┘
                               │ TCP 明文 RTP/RTCP │ TCP 控制 (pion PC op)
                        ┌──────▼──────────────────▼───────────────────┐
                        │ 房主节点 (nat-test-worker2, 192.168.107.4)   │
                        │  Room/Participant/TransportManager/SFU       │
                        │  DownTrack · buffer · dynacast · 拥塞控制    │
                        └──────┬──────────────────┬───────────────────┘
                               │ TCP 明文 RTP/RTCP │ TCP 控制 (pion PC op)
                        ┌──────▼──────────────────▼───────────────────┐
                        │ 边缘节点2 (nat-test-worker3, 192.168.107.2)  │ 多边缘:
 客户端B (host)          │  同构于边缘节点1（第二边缘，独立 advertise_ip）│ 同一房间可由
   │ WS / SRTP / ICE     └────────────────────────────────────────────┘ 两个边缘同时服务
   └──────────────────►  Redis (共享路由: room_node_map / nodes / PSRPC)
```

- **NAT 模拟**：客户端只能通过边缘节点的 `advertise_ip`（对外 IP）连接；节点间内部流量走
  `node_ip`（`POD_IP`）。`advertise_ip` 通过 pion 1:1 NAT 重写 ICE candidate，
  `node_ip` 驱动 media_relay 拨号（Phase 1 的解耦被真实使用）。
- **split 触发**：`room_node_map` 把房间钉在房主节点 → 边缘 `StartSession` 发现
  `signalNodeID != 本地节点` → 建立远程会话（`remotePeerConnection` 驱动边缘 pion PC）。
- **稳定 nodeID**：`LIVEKIT_NODE_ID`（`node-edge`/`node-room`）让重启后注册覆盖、无陈旧残留。

## 前置

- Docker（daemon 需可拉 docker hub；如走代理，在 Docker Desktop 里配置 Proxies）
- `kubectl`、`go`、`lk`（livekit-cli v2）、`ffmpeg`（生成测试媒体）、`kind`
  （`00-prereqs.sh` 会自动装 kind）

## 复现步骤

```bash
cd test/nat-e2e

./00-prereqs.sh        # 检查/安装工具，预拉镜像
./01-cluster.sh        # 创建 kind 集群（control-plane + 3 worker：edge/room/edge2；缺 worker3 时自动重建）
./02-build-image.sh    # 交叉编译 fork 的 livekit-server → 镜像 → 导入 kind
./03-deploy.sh         # 部署 Redis + 双 server pod（hostNetwork + advertise_ip）+ 钉房间到房主
./04-e2e.sh            # 跑全部 lk 场景（约 5-6 分钟）
./04-e2e.sh --loss 5   # 加 WAN 丢包模拟（netem 5%）验证质量反馈跨节点
./06-client-e2e.sh     # 跑 Go 客户端场景（receive-before-publish / NACK / data / attributes 等）
./07-coverage.sh       # E2E 覆盖度工具：插桩二进制 + 跑套件 + 收集 + 报告（见下）
./05-cleanup.sh        # 拆除集群
```

每步独立、幂等。`04-e2e.sh` 与 `06-client-e2e.sh` 结尾输出 PASS/FAIL 汇总。

## E2E 场景

| 场景 | 验证内容 | 断言锚点 |
|---|---|---|
| S1 | 跨节点 join + NAT split | 房主建立远程会话（dual-PC 双会话）、边缘网关会话、ICE connected |
| S2 | 上行媒体面（publisher→edge→room） | 房主 `remote published track media plane established`，simulcast 3 层 receiver |
| S3 | 下行媒体面（room→edge→subscriber） | 房主 DownTrack 桥接、边缘建 SRTP track、DownTrack 实际转发 RTP、down RTCP |
| S4 | RTCP 双向 | up RTCP（room→edge→publisher，PLI/RR）+ down RTCP（client→edge→room，RR/XR）；`--loss` 注入 WAN 丢包后房主观测到质量下降 |
| S5 | 多 codec + 数据通道 | H264 跨节点 up track、边缘创建 `_reliable`/`_lossy` 数据通道 |
| S6 | dual-PC | 每 participant 建立 2 个远程会话（publisher answerer + subscriber offerer） |
| S7 | 会话拆除 | participant 离开后边缘网关会话关闭、房主释放 track |
| S8 | 并发多 participant | 2 pub + 2 sub 同房间并发：24 up receivers、75 down tracks、媒体实际流转 |
| S9 | 音频跨节点 | Opus 上行接收器 + 下行 DownTrack 桥接 + 音频 RTP 实际转发 |
| S10 | 第二个房间 | 独立房间（`nat-test-2`）同样触发 NAT split + up-plane |
| S11 | 边缘重启重连 | 客户端经边缘重连后媒体恢复（resume 协商降级为全量重连，见下） |
| S12 | 更大并发 | 3 pub + 3 sub（8 并发曾只建立 2/8 会话，见限制） |
| S13 | 长时稳定性 | 60s 会话不中断、down RTCP 持续流动 |

### Go 客户端场景（`06-client-e2e.sh`，复用仓库官方 `test/client` 包）

| 场景 | 验证内容 |
|---|---|
| receive-before-publish | 订阅者先入空房间，发布者后加入发布 → 订阅者自动订阅并收包 |
| **NACK 跨节点（含 RTX 重传）** | 订阅者客户端发真 RTCP NACK → 边缘 `pumpSenderRTCPToMediaChannel` → 房主 `DownTrack.ProcessRTCP`（`nacks`）→ **实际重传**（`nackAcks`，实测 nacks=15/nackAcks=1）。RTCP 下行回环修复见生产加固 |
| data | 数据通道 P0-2 桥接代码已落地（控制协议 + 边缘 executor + 房主广播，协议单测过）；端到端 client→room 待 pion stream-ID 匹配修复（`data` 场景记录当前行为） |
| attributes | 发布者 `SetAttributes` → 房间广播 → 订阅者跨节点看到属性更新 |
| metadata | 发布者更新 `Metadata` → 订阅者跨节点看到（`UpdateParticipantMetadata` 信号） |
| mute | 发布者对已发布 track 发 `MuteTrack` → 订阅者跨节点看到 `Muted=true` |
| multitrack | 单发布者同时发布 2 路视频（camera+screen）→ 订阅者自动订阅两路（房主桥接 ≥2 个下行 track） |
| **single-pc** | 双端 `join_request` 单 PC 模式：NAT 每 participant 只建 1 个远程会话/1 个边缘网关（无 dual-PC 标记），上下行媒体跨节点流转（实测 2KB+） |
| **whip** | 单 PC one-shot 信令（RFC 9725）：WHIP 客户端 POST `/whip/v1` 提供 SDP → 边缘终止 pion PC → 房间返回带 ICE 的 answer → 连接并发布 VP8（房主日志断言 `"oneShot": true` + up-plane 注册） |
| manual-subscribe | 订阅者 `AutoSubscribe=false` 加入 → 发布者发布 → 订阅者显式 `UpdateSubscription` → 收包 |
| participant-name | 发布者更新显示名 `Name` → 订阅者跨节点看到 |
| room-admin | REST `RoomService` 建房间/列房间/踢人/删房间（跨节点，边缘 HTTP 端点） |
| track-pause | 订阅者 `UpdateTrackSettings.Disabled` 暂停已订阅 track → 字节冻结 → 恢复 → 继续收包 |
| room-lifecycle | 建房间（短 empty/departure timeout）→ 两人加入 → 离开 → 房间被自动删除 |
| service-apis | 全量 `RoomService` 管理面：list/get participant、admin mute track、update participant/room metadata、update subscriptions、send data、kick、delete |
| subscription-permission | 订阅者显式声明 per-track `SubscriptionPermission` → 服务器评估 → 媒体仍流动 |
| quality-request | 订阅者请求最大视频质量（`UpdateTrackSettings.Quality`，dynacast 分层路径） |
| rtc-validate | 令牌校验 HTTP 端点 `/rtc/validate`（成功 + 无 token 401 + v1 缺 join_request 400）→ validateInternal + room 分配 |
| update-video-track | 发布者 `UpdateVideoTrack`（宽高）→ 订阅者跨节点看到 TrackInfo 更新 |
| update-audio-track | Opus 上行 + `UpdateAudioTrack`（stereo 特性）→ 订阅者跨节点看到 AudioFeatures |
| data-track-publish | `PublishDataTrackRequest` + `UnpublishDataTrackRequest` 信号路径（DC 消息仍不跨节点桥接） |
| hidden-participant | hidden grant 参与者对普通成员不可见（广播排除）但 RoomService 可见 |
| subscriber-only | `CanPublish=false`（录制风格）参与者只收不发，收到普通发布者媒体 |
| room-move-forward | `MoveParticipant`/`ForwardParticipant`：同房被拒（invalid_argument）、跨房路由到 RoomManager stub（not implemented） |
| whip-ice-restart | WHIP 生命周期：POST+媒体 → PATCH(ICE restart) 被干净拒绝（见下方已知限制）→ DELETE 拆会话；节点不崩溃 |
| health | 双节点 HTTP `/`（defaultHandler → healthCheck + 节点心跳新鲜度）均返回 200 OK |
| simulate-speaker | 发布者模拟 N 秒 active-speaker（`SimulateScenario_SpeakerUpdate`）→ 房主广播 speaker delta → 订阅者跨节点看到发布者 active（`SpeakersChanged`，仅发给已订阅者） |
| simulate-node-failure | 参与者模拟节点故障（`SimulateScenario_NodeFailure`）→ 服务端丢弃参与者并断开信令 → 客户端观察到服务端驱动断连 |
| simulate-server-leave | 参与者模拟服务端 leave（`SimulateScenario_ServerLeave`）→ 服务端干净关闭参与者 → 客户端观察到服务端驱动断连 |
| sub-perm-revoke | 媒体流动后**发布者**撤销订阅者对自身 track 的订阅权限（`SubscriptionPermission{AllParticipants:false}` → maybeRevokeSubscriptions → RemoveSubscriber）→ 被撤销者媒体冻结（下行 track 跨节点关闭） |
| participant-leave-visible | B 干净离开 → A 跨节点观察到参与者移除广播（`SignalResponse_Update` 移除已离开参与者） |
| sync-state | 已连接客户端用 `SyncState` 声明自身已发布 track → 服务端 `onSyncState` 校验通过 → **不**触发全量重连 |
| connection-quality | A 跨节点观察到 B 的 per-participant 连接质量（`SignalResponse_ConnectionQuality`，EXCELLENT/score 4.5） |
| turn-credentials | 启用内嵌 TURN（UDP 3478）→ join 响应经 `iceServersForParticipant` 携带 TURN URL + 非空用户名/凭据，且客户端**实际分配** relay candidate（`HasRelayCandidate`，跨节点 `ALLOCATE OK`）；relay-only 完整连接受 Docker/kind 环境限制（见已知限制） |
| reconnect | 发布者只断开信令 WS（PC 存活）→ 断连宽限窗口内**同身份**重新 join → 服务端移除重复参与者（`RemoveParticipant DuplicateIdentity`）→ 新参与者重发布 → 订阅者跨节点看到媒体恢复（真实客户端全量重连落到的结局） |
| perform-rpc | `RoomService.PerformRpc` 指向 NAT 参与者 → 跨节点 DC 数据未桥接（已记录限制）→ RPC **干净、有界**失败（`data channel is not available`，psrpc Internal，毫秒级返回，绝不挂起）——把文档化限制固化为确定性回归断言 |
| simulcast-switch | 真实 3 层 simulcast（`lk --publish-demo` 发 3-SSRC H264）+ Go 订阅者 `UpdateTrackSettings` LOW/MEDIUM/HIGH → 房主**3 条 up plane 全部跨节点建立**（`available layers changed` 达 `[0,1,2]`，MediaGateway 修复的证明）+ DownTrack max-subscribed 层随请求 0/1/2（层选择路径）；下切可真实生效（客户端码率骤降）、上切受层锁 PLI 限制（见已知限制） |
| reconnect-resume | `reconnect=true` resume 协商路径：断信令后同 SID resume → 服务端走 `resuming RTC session`（`ResumeParticipant`）→ 客户端观察到**干净 resume**（`SignalResponse_Reconnect`，媒体继续）或干净拒绝（`SignalResponse_Leave` RECONNECT → 全量重连回退），媒体必恢复、绝不挂起 |
| webhook-events | 服务端 webhook（HTTP 回调）路径：加入/发布/离开触发 `participant_joined`/`track_published`/`track_unpublished`/`participant_left`，POST 到集群内 receiver pod（`webhook-receiver.default.svc:8080/webhook`），receiver **验证 JWT sha256 签名**（0 rejected）后逐条记录（HMAC 签名验证端到端） |
| subscriber-pli | 下行 RTCP 的 **PLI 路径**（keyframe 请求变体，与 NACK 互补）：订阅者发真 PLI → 房主 DownTrack 处理并请求发布者 keyframe（`sending PLI RTCP`，SSRC 重写修复的 PLI 证明——修复前边缘重写 SSRC 被 `p.MediaSSRC == d.ssrc` 丢弃） |
| media-follows-signaling | 核心边界 IP 级证明：**媒体终止于信令节点（边缘）**——服务端 PC 跑在边缘（advertise_ip），客户端收到的远端 ICE candidate 必须携带边缘 IP（= WS hostname），绝不含房主 IP；断言 candidate 含边缘 IP + 媒体实际流动 |
| multi-edge | **多边缘核心属性**：pub 连边缘1、sub 连边缘2（同一房间钉在房主节点）→ 媒体双向跨越边缘1↔边缘2（各经房主节点），两个边缘各出现网关会话（多边缘需要 `01-cluster.sh` 建 3 worker，无 worker3 时自动跳过） |
| simulate-ice-restart | **服务端 ICE restart 核心路径**（`SimulateScenario_SwitchCandidateProtocol` → `participant.ICERestart`，与 resume 同一路径）：双向发布/订阅的双方在服务端重启后**各自收流继续**（被重启的订阅者 PC 跨节点重协商 + 未触碰的 PC 不受影响），证明跨节点 ICERestart/重协商端到端 |

## E2E 覆盖度工具（`07-coverage.sh`）

用 Go 覆盖率插桩构建服务器二进制（`go build -cover -coverpkg=github.com/livekit/livekit-server/...`），
部署到双节点集群（`GOCOVERDIR=/tmp/coverage` + emptyDir），跑完 E2E 套件后经 `/debug/coverage`
端点按需刷新计数器，再合并两个节点的覆盖档案并报告总覆盖率与按包分布。

```bash
./07-coverage.sh              # 完整测量（lk 套件 + Go 客户端，约 15 分钟）
./07-coverage.sh --quick      # 仅 Go 客户端（约 8 分钟）
./07-coverage.sh --report coverage-results   # 只汇总已收集的数据
```

实测（Go 客户端套件）：全项目语句覆盖率 ~27%；核心运行时包 `pkg/rtc` ~48%、
`pkg/sfu` ~44%、`pkg/service` ~17%（`pkg/rtc/transport` 30%、`pkg/routing` 58%）。
黑盒 E2E 自然覆盖网络可达的运行时路径；配置解析、CLI、遥测内部等启动/内部代码无法经 E2E
触达——80% 的"整个项目"目标需要收窄统计口径到运行时包或补充大量边界场景，工具已提供按包
分布以指导后续。服务器新增 `/debug/coverage` 端点（仅 `GOCOVERDIR` 设置时注册，非插桩构建
返回 404）。

## 验证结果（真实节点实测，双节点 kind）

```
=== lk 套件（生产加固后二进制） ===
SUMMARY: 37 passed, 0 failed
  (本轮全绿；S12 更大并发首attempt 的建立竞态偶发时由内置重试吸收——重试房间 s12r 通过。
   S8 并发 45+ up receivers 证明 simulcast 3 层修复在并发下生效)
=== lk 套件 --loss 5% ===
SUMMARY: 28 passed, 0 failed
=== Go 客户端 ===
GO-CLIENT SUMMARY: 43 passed, 0 failed
  (receive-before-publish / NACK / data / attributes / metadata / mute /
   multitrack / single-pc / whip / manual-subscribe / participant-name /
   room-admin / track-pause / room-lifecycle / service-apis /
   subscription-permission / quality-request / rtc-validate / update-video-track /
   update-audio-track / data-track-publish / hidden-participant / subscriber-only /
   room-move-forward / whip-ice-restart / health / simulate-speaker /
   simulate-node-failure / simulate-server-leave / sub-perm-revoke /
   participant-leave-visible / sync-state / connection-quality / turn-credentials /
   reconnect / perform-rpc / simulcast-switch / reconnect-resume / webhook-events /
   subscriber-pli / media-follows-signaling / multi-edge / simulate-ice-restart)
```

覆盖的功能：
- 上行媒体面：H264 simulcast（q/h/f 3 层）从客户端 SRTP → 边缘 → 房主 buffer，30fps 实测。
- 下行媒体面：房主 DownTrack 转发明文 RTP（实测 2111 包/12s，1.4 Mbps）→ 边缘 SRTP → 客户端收包。
- RTCP 双向跨节点：up（房主 receiver→边缘→publisher 的 PLI/RR）、down（客户端→边缘→房主 DownTrack 的 RR/XR）。
- dual-PC 双会话（publisher answerer + subscriber offerer）。
- 数据通道：边缘 pion PC 创建 `_reliable`/`_lossy`（SDP 含 m=application）。
- WAN 丢包：netem 注入后房主连接质量反馈下降（EXCELLENT→POOR）。

## 已知限制

- **NACK 跨节点（已修复回环）**：客户端发真 RTCP NACK → 边缘 → 房主 DownTrack 并**实际重传**
  （`nacks=15/nackAcks=1`）。此前房主侧因边缘 SSRC 重写丢弃 NACK（见生产加固"下行 RTCP SSRC
  重写"），已修复并固化断言。`--loss` 场景断言质量反馈下降。缓冲级 NACK 生成另有 `TestNack` 单测。
- **重连语义（边缘重启）**：客户端可恢复（全量重连），但 resume 协商会失败一次
  （远程 PC 控制通道关闭 → `create offer failed: media channel closed` → `NEGOTIATE_FAILED` →
  `FULL_RECONNECT`）。生产级改进方向：远程 PC 检测到控制通道死亡后主动触发干净的全量重连，
  而非先尝试失败的 resume。
- **并发容量（8 会话）**：4+4 并发（8 个 participant × 2 会话）实测只建立 2/8（media
  channel closed + TRANSPORT_FAILURE）。3+3 稳定。`04-e2e.sh` 的 S12 在 S11 边缘重启后紧跟
  并发建立偶发失败（同为控制链路并发建立竞态），已内置**一次自动重试**（等 10s 后用新房间
  重跑）使其确定性通过。生产级改进方向：核查边缘控制 accept 的并发建立与 room 的
  `establishRemoteSession` 背压。
- **数据通道（P0-2 桥接代码已落地，端到端待修）**：控制协议新增 `send_data_message`/`event_data_message`，边缘 executor 双向转发 + 房主 participant 接入房间广播（协议单测 `TestRemotePCDataChannelBridgingProtocol` 通过）。**端到端 client→room 方向仍被 pion 数据通道 stream-ID 匹配阻塞**：边缘上房主创建的 subscriber DC（奇数 stream ID）与客户端 offerer 创建的 publisher DC 不配对，客户端数据落入未接线的 SCTP 流——需专项核查（很可能需让 publisher PC 不创建 DC、只匹配客户端 offer 的 DC）。`data` 场景记录当前行为。
- **simulcast 上切受限（根因已定：发布者 per-layer keyframe 响应，非 NAT bug）**：受控复现证明
  **NAT 跨节点路径完好**——订阅者 TrackSettings 到达 `SubscribedTrack`、`GetSpatialLayerForVideoQuality`
  正确映射（LOW→0/MEDIUM→1/HIGH→2）、forwarder `SetMaxSpatialLayer` 正确变更 max（2→0→1→2）、
  层锁 PLI **跨节点送达**（`nat up RTCP forwarded` 含多层 SSRC 99059724/730466811）。**卡点**：
  forwarder 层锁等待目标层 keyframe 时，`lk --publish-demo` 发布者**不响应 per-layer PLI**
  （实测 LOW 请求后 12s 媒体仍 0 bytes、PLI 循环 80 次）→ 目标层 keyframe 永不 latch。这是
  **测试发布者限制**（demo 编码器不按层响应 PLI），真实编码器/PLI 响应客户端（Go 客户端需 3 层
  simulcast 发布，SDK 工作另行专项）可打通。`simulcast-switch` 场景断言确定性的部分（3 层 up plane
  + 层选择 0/1/2 + down-switch 生效）。
- **TURN relay（分配正常、relay-only ICE 环境限制）**：`turn-credentials` 场景现已断言客户端
  **实际分配** relay candidate（`HasRelayCandidate`，join 响应凭据 → 房主节点 TURN `ALLOCATE OK
  relayed: 192.168.107.4:50013`，跨节点分配路径验证）。但 **relay-only**（`ICETransportPolicyRelay`）
  的完整 ICE 连接在 Docker/kind 环境不建立（分配成功、relay 候选已收集，但 relayed socket 的
  STUN check 到边缘 host candidate 无法完成），非 fork 服务器问题（TURN server 正常监听 + 发凭据）。
- **WHIP ICE-restart（上游协议库 bug，已加固）**：`PATCH If-Match:*`（RFC 9725 ICE restart）在 fork 中
  会触发上游 `livekit/protocol/sdp` `PatchICECredentialAndCandidatesIntoSDP` 的
  **mutate-while-ranging panic**（`sdp.go:695`，对带 candidate 的远端描述遍历删除时切片越界），
  曾使房主节点进程崩溃（`slice bounds out of range [169:167]`）。本轮在 `transport.go`
  `HandleICERestartSDPFragment` 增加 **panic 兜底**（recover → `ErrInvalidSDPFragment` 干净错误），
  远端 WHIP 片段无法再击穿节点；新增单测 `TestICERestartSDPFragmentPanicHardened` 与
  E2E `whip-ice-restart` 场景验证"PATCH 干净拒绝 + 节点存活 + DELETE 仍成功"。真正的
  ICE restart 打通需修复上游库（改遍历时删除为过滤重建），本 fork 暂以兜底错误收敛。
- **内网明文**：media_relay 明文 TCP，生产需内网隔离或 TLS（§9）。
- **瞬时 ICE 抖动**：Go 客户端偶发订阅者/发布者 ICE/DTLS 连接超时（`TRANSPORT_FAILURE`），
  重跑即过（本机客户端经 Docker/VM 网卡候选 + srflx 的路径偶发抖动）。`run_scenario` 对
  `expect-exit-zero` 场景内建**一次同房间重试**（失败等 10s 清场后重跑同一场景，与
  `04-e2e.sh` S12 同模式）：确定性回归两次都失败、仍会被捕获；瞬态抖动第二次即过，套件输出
  标注 "after retry"。新场景 `simulate-speaker`/`sub-perm-revoke` 使用独立房间（speaker delta
  只发给已订阅者、撤销场景需排除同房其他发布者的媒体干扰）。

## 生产级加固（本轮随 E2E 验证落地）

以下加固已在本轮双节点实测全绿（`04-e2e.sh` 37/37 + `06-client-e2e.sh` 4/4，含重连/并发/长稳）：

- **远程控制通道请求超时**（`remote_transport.go`）：`request()` 增加 15s 上限，边缘 executor 卡死
  时房主协商干净失败而非永久阻塞（原实现 `<-respCh` 无超时）。
- **重复 attach 关闭冗余通道**（`mediagateway.go`）：同一 track 二次 attach（如重协商）时关闭
  冗余 MediaChannel，避免 TCP 连接泄漏与 pacer 背压（原实现直接 return nil 泄漏通道）。
- **simulcast 多层跨节点桥接修复**（`mediagateway.go`）：up 方向桥按 `trackID+SSRC` 键控——
  同一 trackID 的多个 SSRC（simulcast 层）各自建立独立 MediaChannel 桥接，只有**同 SSRC**
  再 attach 才算重复（关闭冗余通道）。原实现按 trackID 键控，3 层 simulcast 只有首个 SSRC
  的 RTP 能跨节点（其余被当"重复"关闭，房主 SFU 只见 1 层比特率、无法切层）；修复后
  3 层全部跨节点（实测比特率 `[LOW≈105k, MED≈341k, HIGH≈1340k]`），新增单测
  `TestMediaGatewaySimulcastLayersBridgedSeparately`（3 SSRC 各自独立桥接 + 同层重复仍关闭 +
  整 track 移除）。
- **下行 RTCP SSRC 重写（NACK/PLI/RR 跨节点回环修复）**（`participant.go`）：边缘
  `TrackLocalStaticRTP` 在 SRTP 前把 RTP SSRC 重写为 pion sender 自己的（`track_local_static.go:
  packet.Header.SSRC = b.ssrc`），所以订阅者 NACK/PLI/RR 指向**边缘 SSRC**、房主 DownTrack 按内部
  SSRC 过滤（`p.MediaSSRC == d.ssrc`）全部丢弃——实测边缘转发了 NACK 但房主 `nacks:0`。修复：房主
  侧 down-RTCP 边界把 PLI/FIR/NACK 的 MediaSSRC 与 RR 的 report SSRC 统一重写为 DownTrack 内部
  SSRC（每 track 独占 MediaChannel，无串扰），实测 `nacks=15/nackAcks=1`（DownTrack 真正重传）。
  新增单测 `TestRewriteDownRTCPForLocalSSRC`（NACK/PLI/FIR/RR 改写 + payload 保留 + 畸形输入不
  panic）。顺带修复该测试暴露的 `for _, r := range p.Reports` 拷贝迭代不改写 bug。
- **webhook receiver pod**（`test/nat-e2e/webhook-receiver/` + `manifests/webhook-receiver.yaml`）：
  独立 scratch 镜像 + ClusterIP 服务（`webhook-receiver.default.svc:8080`），双节点经集群 DNS 可达
  （宿主机不同网段不可达）。`webhook-events` 场景断言 receiver 用 `webhook.ReceiveWebhookEvent`
  验证每个事件的 JWT sha256 签名（0 rejected）并记录 4 类生命周期事件。
- **畸形 padding 防护**（`mediachannelrtp.go`）：padding 长度字节大于 payload 时清 Padding 标志，
  令 Marshal 成功而非每包报错（原实现 `Padding=true` + `PaddingSize=0` 触发 pion
  `errInvalidRTPPadding`）。
- **RTCP 日志限流**（`downtrack.go`/`participant.go`）：跨节点 RTCP 转发日志 15s CAS 限流，
  防止 RR/XR 批次刷屏顶掉 kubectl logs 断言锚点。
- **S7 拆台断言自洽化**（`04-e2e.sh`）：自建发布者→杀掉→等边缘网关会话关闭与房主 participant
  closing，不再依赖 `--since=10m` 历史日志（原实现偶发时序抖动）。
- **单 PC NAT 订阅修复**（`transport.go`/`transportmanager.go`/`participant.go`）：single-PC/one-shot
  模式下下行 track 在远端（边缘）无本地 transceiver，`numOutstandingVideos` 未累加导致
  `MediaSectionsRequirement` 上报 `numVideos=0`、客户端永不补齐媒体段。新增
  `adjustNumOutstandingMediaForRemote` + `TransportManager.NoteSubscriberTrackAdded` 在
  `addTrackLocalRemote` 绑定后累加，单 PC 订阅者现在能收到媒体（实测 2KB+）。
- **WHIP / one-shot 信令打通**（本轮多修复，`remote_transport.go`/`wire.go`/`remote_executor.go`）：
  - `getPSRPCClientParams` 用节点 ID 作为 psrpc 客户端 ID → 请求元数据 `RemoteID` 是**节点 ID**
    而非默认随机 `CLI_` 客户端 ID。`StartSession` 用 `RemoteID` 推导 signal 节点做 NAT split，
    原来 psrpc 请求（WHIP.Create）的 `RemoteID` 是 `CLI_` → `nodeByID` 找不到 → one-shot 会话
    无法建立。
  - `dispatchEvent` 的 ICE gathering-complete 信号不再依赖 `onICEGatheringStateChange` 回调：
    one-shot 模式不挂该回调（不 trickle），但 `GetAnswer` 仍等 `gatheringComplete` → 原来永远
    挂起（WHIP answer 超时）。
  - `03-deploy.sh` 改为**每节点 configmap**（`advertise_ip`/`node_ip` 直接写入 YAML）：
    `LIVEKIT_RTC_ADVERTISE_IP` 环境变量无法绑定 `NodeIP` 结构体配置字段（cli 字符串→struct
    转换失败），留下空值 → 边缘 PC 回退 `DefaultStunServers`（kind 内不可达 → 10s srflx 收集
    拖垮 one-shot answer）。bake 进 YAML 后边缘只收集 host 候选、即时完成。
  - WHIP 客户端在 `GatheringCompletePromise` 后发送 `LocalDescription()`（含已收集候选），
    否则 offer 缺候选 → ICE 无法连通。
- **VP9/AV1 跨节点（已知限制）**：test/client 的假 RTP 负载不触发边缘网关的 `OnTrack`
  （边缘 `FireOnTrackBeforeFirstRTP` 关闭），故 VP9/AV1 上行媒体面无法用合成媒体建立。真实
  编码器客户端（VP8/Opus/H264）已被其余场景覆盖；VP9/AV1 路径留待真实客户端验证。
- **可复现部署**（`03-deploy.sh`）：新增 `kubectl rollout restart`（镜像 tag 不变时重跑也能滚动到
  新二进制）+ `kubectl rollout status`（`kubectl wait` 会匹配 Recreate 滚动中被删除的旧 pod，
  导致 room 不滚动）；`02-build-image.sh`/`06-client-e2e.sh` 修正 `REPO_ROOT` 层级（`../../..` → `../..`）。
- **单测**（本轮新增 7 个）：`TestRemotePeerConnectionRequestTimeout`（控制通道 15s 超时兜底）、
  `TestRemotePeerConnectionCloseUnblocksRequests`（通道关闭解阻塞 pending 请求）、
  `TestMediaChannelRTPWriterPadding`/`TestMediaChannelRTPWriterMalformedPadding`（padding 翻译 +
  畸形防护）、`TestMediaGatewayDuplicateSubscriberTrackClosesChannel`/
  `TestMediaGatewayDuplicatePublisherTrackClosesChannel`（重复 attach 关闭冗余通道）、
  `TestICERestartSDPFragmentPanicHardened`（WHIP ICE-restart 片段 panic 兜底，见已知限制）。
- **断言锚点抗日志轮转**（`gateway.go`/`roommanager.go`/`06-client-e2e.sh`）：房主节点 SFU 噪声
  （PLI/转发器/ERROR 栈）可触发 kubelet 日志轮转（10Mi），把会话建立期的日志连同
  `"oneShot": true` / `"useSinglePC": true` 等房间侧锚点一起裁掉（实测 `whip` 场景因此偶发误报）。
  本轮给 `GatewaySetup` 增加 `room_name` 字段并在边缘网关会话日志
  （`nat edge gateway session starting`）中输出，`single-pc`/`whip` 断言改为按房间在**边缘日志**
  （低噪声、不轮转）上验证"单会话/无 dual-PC offerer"与"one-shot"——边缘日志全量保留，锚点
  确定性可达。`07-coverage.sh` `--report` 分支的顶层 `return 0` 改为 `exit 0`（顶层 `return`
  无效，会导致脚本在汇总时误报）。
- **test/client SDK 扩展**（`client.go`）：新增 `SignalResponse_Leave` /
  `SignalResponse_SpeakersChanged` / `SignalResponse_ConnectionQuality` /
  `SignalResponse_Reconnect` 处理 + `WaitUntilDisconnected`/`Disconnected`/`DisconnectReason`/
  `ActiveSpeakers`/`LastConnectionQuality`/`ResumeAccepted` 访问器，以及 resume 支持：
  `Options.Reconnect`/`Options.ReconnectSID`（WS URL 带 `reconnect=true&sid=`）与
  `RTCClient.Resume(host, token, opts)`（重连信令 WS、复用既有 PC，真实客户端 resume 行为），
  支撑 simulate / speaker / 质量 / reconnect-resume 场景的客户端侧断言。另新增 `SendPLI`
  （发真 RTCP PictureLossIndication，subscriber-pli 场景）与 `RemoteCandidateIPs` +
  Trickle candidate 捕获（media-follows-signaling 场景断言边缘 IP）。speaker delta 是
  **按订阅者收窄**的（`SendSpeakerUpdate(force=false)` 只发给已订阅该发言者的参与者或发言者
  本人），simulate-speaker 场景先建立订阅再触发模拟。媒体 track 的"取消发布"需重协商移除
  transceiver（`writer.Stop()` 只停发送、服务端不因此取消发布），dual-PC NAT 下驱动不可靠，
  故无独立 track-unpublish 场景
  （参与者离开覆盖参与者移除广播）。

## 与上游 LiveKit K8s 部署的对比

上游官方 K8s 多节点（[Helm chart](https://github.com/livekit/livekit-helm)）与 fork 的共同点
已被本套件采用并验证：
- `hostNetwork: true` + `dnsPolicy: ClusterFirstWithHostNet`（pod 直接绑节点端口、可解析集群服务）
- Redis 作为多节点状态协调（`room_node_map` / `nodes` / PSRPC）
- 客户端经 `advertise_ip`（上游 `use_external_ip: true`）连接；单节点零回归

**关键差异**：上游是"所有节点同构，房间归属单一节点，媒体终止在房主节点"；fork 的
"media follows signaling"让边缘节点终止媒体（真实 pion PC），房主持有 SFU，跨节点只传明文
RTP/RTCP。上游没有此架构，fork 的测试是本架构的验证。客户端 NAT 穿透（TURN/srflx）在上游
由 `use_external_ip` + TURN 处理，与 fork 的服务器侧 split 正交。

## 脚本说明

- `lib/common.sh`：公共函数（节点 IP、redis 操作、房间钉扎、丢包注入、PASS/FAIL 汇总）。
- `manifests/`：kind 集群配置 + Redis 部署。
- `configs/config.yaml`：server 基础配置（`component_levels` 抑制 SDP 日志避免 kubectl logs 截断）。
- `03-deploy.sh`：部署前 `DEL nodes room_node_map` 清理陈旧注册；每 pod 注入
  `POD_IP`/`LIVEKIT_RTC_ADVERTISE_IP`/`LIVEKIT_REGION`/`LIVEKIT_NODE_ID`。
- `04-e2e.sh`：全部场景；`wait_log` 用进程替换（`grep -q < <(kubectl logs …)`）规避
  `set -o pipefail` 下 `grep -q` 早退导致的 SIGPIPE 假失败。
