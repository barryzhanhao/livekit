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
./04-e2e.sh            # 跑全部场景（约 3-4 分钟）
./04-e2e.sh --loss 5   # 加 WAN 丢包模拟（netem 5%）验证质量反馈跨节点
./05-cleanup.sh        # 拆除集群
```

每步独立、幂等。`04-e2e.sh` 结尾输出 PASS/FAIL 汇总。

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

## 验证结果（真实节点实测）

```
=== 无丢包 ===
SUMMARY: 19 passed, 0 failed
=== --loss 5%（WAN 丢包模拟）===
SUMMARY: 20 passed, 0 failed
```

覆盖的功能：
- 上行媒体面：H264 simulcast（q/h/f 3 层）从客户端 SRTP → 边缘 → 房主 buffer，30fps 实测。
- 下行媒体面：房主 DownTrack 转发明文 RTP（实测 2111 包/12s，1.4 Mbps）→ 边缘 SRTP → 客户端收包。
- RTCP 双向跨节点：up（房主 receiver→边缘→publisher 的 PLI/RR）、down（客户端→边缘→房主 DownTrack 的 RR/XR）。
- dual-PC 双会话（publisher answerer + subscriber offerer）。
- 数据通道：边缘 pion PC 创建 `_reliable`/`_lossy`（SDP 含 m=application）。
- WAN 丢包：netem 注入后房主连接质量反馈下降（EXCELLENT→POOR）。

## 已知限制

- **NACK 跨节点**：lk（livekit-cli）的 media-sdk 客户端不发送 NACK，故 `--loss` 场景断言
  连接质量下降而非 NACK。NACK 生成/处理由单测覆盖（`DownTrack.ProcessRTCP` →
  `handleRTCP`）。如需 E2E 级 NACK 验证，需要一个启用 NACK interceptor 的真实客户端。
- **数据通道消息**：跨节点只转发 SDP 协商（m=application）；DC 数据消息未桥接（§6.8 既定暂拆）。
- **内网明文**：media_relay 明文 TCP，生产需内网隔离或 TLS（§9）。

## 脚本说明

- `lib/common.sh`：公共函数（节点 IP、redis 操作、房间钉扎、丢包注入、PASS/FAIL 汇总）。
- `manifests/`：kind 集群配置 + Redis 部署。
- `configs/config.yaml`：server 基础配置（`component_levels` 抑制 SDP 日志避免 kubectl logs 截断）。
- `03-deploy.sh`：部署前 `DEL nodes room_node_map` 清理陈旧注册；每 pod 注入
  `POD_IP`/`LIVEKIT_RTC_ADVERTISE_IP`/`LIVEKIT_REGION`/`LIVEKIT_NODE_ID`。
- `04-e2e.sh`：全部场景；`wait_log` 用进程替换（`grep -q < <(kubectl logs …)`）规避
  `set -o pipefail` 下 `grep -q` 早退导致的 SIGPIPE 假失败。
