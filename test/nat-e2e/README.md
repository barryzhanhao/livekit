# NAT 多节点 E2E（双节点 K8s，可复现）

在 **kind 双节点 K8s** 上验证 LiveKit fork 的 NAT 多节点模式（"media follows signaling"）：
客户端 WS + 媒体终止在**边缘节点**，`Participant`/`TransportManager`/SFU 在**房主节点**，
明文 RTP/RTCP 经内部 TCP（media_relay）跨节点转发。

## 架构

```
                        ┌─ 边缘节点 (nat-test-worker, 192.168.107.3) ─┐
 客户端 (host)          │  SignalService (WS 7880)                     │
   │ WS / SRTP / ICE    │  MediaGateway (真实 pion PC, UDP 7882)       │
   └──────────────────► │  media_relay: 7883(RTP/RTCP) 7884(控制)      │
                        └──────┬──────────────────┬───────────────────┘
                               │ TCP 明文 RTP/RTCP │ TCP 控制 (pion PC op)
                        ┌──────▼──────────────────▼───────────────────┐
                        │ 房主节点 (nat-test-worker2, 192.168.107.4)   │
                        │  Room/Participant/TransportManager/SFU       │
                        │  DownTrack · buffer · dynacast · 拥塞控制    │
                        └──────────────────────────────────────────────┘
                        Redis (共享路由: room_node_map / nodes / PSRPC)
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
./01-cluster.sh        # 创建 kind 集群（control-plane + 2 worker：edge/room）
./02-build-image.sh    # 交叉编译 fork 的 livekit-server → 镜像 → 导入 kind
./03-deploy.sh         # 部署 Redis + 双 server pod（hostNetwork + advertise_ip）+ 钉房间到房主
./04-e2e.sh            # 跑全部 lk 场景（约 5-6 分钟）
./04-e2e.sh --loss 5   # 加 WAN 丢包模拟（netem 5%）验证质量反馈跨节点
./06-client-e2e.sh     # 跑 Go 客户端场景（receive-before-publish / NACK / data / attributes）
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
| **NACK 跨节点** | 订阅者客户端发真 RTCP NACK → 边缘 `pumpSenderRTCPToMediaChannel` → 房主 `DownTrack.ProcessRTCP`（实测边缘转发 6 个 nack 批） |
| data | 数据通道消息（当前未跨节点桥接，§6.8 既定暂拆，记录行为） |
| attributes | 发布者 `SetAttributes` → 房间广播 → 订阅者跨节点看到属性更新 |

## 验证结果（真实节点实测，双节点 kind）

```
=== lk 套件（生产加固后二进制） ===
SUMMARY: 37 passed, 0 failed
=== lk 套件 --loss 5% ===
SUMMARY: 28 passed, 0 failed
=== Go 客户端 ===
GO-CLIENT SUMMARY: 4 passed, 0 failed  (含 NACK 跨节点)
```

覆盖的功能：
- 上行媒体面：H264 simulcast（q/h/f 3 层）从客户端 SRTP → 边缘 → 房主 buffer，30fps 实测。
- 下行媒体面：房主 DownTrack 转发明文 RTP（实测 2111 包/12s，1.4 Mbps）→ 边缘 SRTP → 客户端收包。
- RTCP 双向跨节点：up（房主 receiver→边缘→publisher 的 PLI/RR）、down（客户端→边缘→房主 DownTrack 的 RR/XR）。
- dual-PC 双会话（publisher answerer + subscriber offerer）。
- 数据通道：边缘 pion PC 创建 `_reliable`/`_lossy`（SDP 含 m=application）。
- WAN 丢包：netem 注入后房主连接质量反馈下降（EXCELLENT→POOR）。

## 已知限制

- **NACK 跨节点**：已用 Go 客户端（`06-client-e2e.sh`）真实验证——客户端发 NACK → 边缘 →
  房主 DownTrack。`--loss` 场景断言质量反馈下降。缓冲级 NACK 生成另有 `TestNack` 单测。
- **重连语义（边缘重启）**：客户端可恢复（全量重连），但 resume 协商会失败一次
  （远程 PC 控制通道关闭 → `create offer failed: media channel closed` → `NEGOTIATE_FAILED` →
  `FULL_RECONNECT`）。生产级改进方向：远程 PC 检测到控制通道死亡后主动触发干净的全量重连，
  而非先尝试失败的 resume。
- **并发容量（8 会话）**：4+4 并发（8 个 participant × 2 会话）实测只建立 2/8（media
  channel closed + TRANSPORT_FAILURE）。3+3 稳定。生产级改进方向：核查边缘控制 accept 的并发
  建立与 room 的 establishRemoteSession 背压。
- **数据通道消息**：跨节点只转发 SDP 协商（m=application）；DC 数据消息未桥接（§6.8 既定暂拆）。
- **内网明文**：media_relay 明文 TCP，生产需内网隔离或 TLS（§9）。
- **瞬时 ICE 抖动**：Go 客户端 `nack` 场景偶发订阅者 ICE/DTLS 10s 超时（`TRANSPORT_FAILURE`），
  重跑即过（本机客户端经 Docker/VM 网卡候选 + srflx 的路径偶发抖动）。`06-client-e2e.sh` 以
  `run_scenario` 直接判定，失败不重试——CI 上如偶发失败请重跑一次确认非回归。

## 生产级加固（本轮随 E2E 验证落地）

以下加固已在本轮双节点实测全绿（`04-e2e.sh` 37/37 + `06-client-e2e.sh` 4/4，含重连/并发/长稳）：

- **远程控制通道请求超时**（`remote_transport.go`）：`request()` 增加 15s 上限，边缘 executor 卡死
  时房主协商干净失败而非永久阻塞（原实现 `<-respCh` 无超时）。
- **重复 attach 关闭冗余通道**（`mediagateway.go`）：同一 track 二次 attach（如重协商）时关闭
  冗余 MediaChannel，避免 TCP 连接泄漏与 pacer 背压（原实现直接 return nil 泄漏通道）。
- **畸形 padding 防护**（`mediachannelrtp.go`）：padding 长度字节大于 payload 时清 Padding 标志，
  令 Marshal 成功而非每包报错（原实现 `Padding=true` + `PaddingSize=0` 触发 pion
  `errInvalidRTPPadding`）。
- **RTCP 日志限流**（`downtrack.go`/`participant.go`）：跨节点 RTCP 转发日志 15s CAS 限流，
  防止 RR/XR 批次刷屏顶掉 kubectl logs 断言锚点。
- **S7 拆台断言自洽化**（`04-e2e.sh`）：自建发布者→杀掉→等边缘网关会话关闭与房主 participant
  closing，不再依赖 `--since=10m` 历史日志（原实现偶发时序抖动）。
- **可复现部署**（`03-deploy.sh`）：新增 `kubectl rollout restart`，镜像 tag 不变时重跑也能滚动到
  新二进制；`02-build-image.sh`/`06-client-e2e.sh` 修正 `REPO_ROOT` 层级（`../../..` → `../..`）。

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
