# Production Deployment Guide — NAT Mode LiveKit

This guide covers deploying a production-grade NAT-mode LiveKit cluster on Kubernetes.

## Architecture

```
┌──────────────┐     WebSocket/Signal (7880)     ┌──────────────┐
│   Client     │ ──────────────────────────────▶  │  Edge Node   │
│  (Browser)   │     ICE/DTLS/SRTP/UDP (7882)     │  (DaemonSet) │
│              │ ◀──────────────────────────────  │  hostNetwork │
└──────────────┘     TURN TCP/UDP (7881/3478)     └──────┬───────┘
                                                          │
                                            TCP relay (7883)
                                          Control (7884)
                                                          │
                                                          ▼
                                                   ┌──────────────┐
                                                   │  Room Node   │
                                                   │ (Deployment) │
                                                   │  SFU/Session │
                                                   └──────┬───────┘
                                                          │
                                                          │ Redis (6379)
                                                          │
                                                          ▼
                                                   ┌──────────────┐
                                                   │ Redis        │
                                                   │ Sentinel HA  │
                                                   └──────────────┘
```

- **Edge nodes** (DaemonSet, hostNetwork): Terminate client-facing WebRTC. Hold real pion PeerConnections. Run on every node with `nat-role: edge`.
- **Room nodes** (Deployment): Hold Participant/TransportManager/SFU. Communicate with edge nodes via TCP relay (7883 plaintext RTP/RTCP, 7884 control).
- **Redis** (StatefulSet, Sentinel HA): Node registry, PSRPC routing, room state.

## Prerequisites

- Kubernetes 1.24+ cluster
- At least 2 edge-labeled nodes + 2 room-labeled nodes (3 each recommended for HA)
- `kubectl` configured with cluster access
- Redis Sentinel HA (included in these manifests) or existing Redis cluster

## Label Nodes

```bash
# Label nodes for edge role (client-facing WebRTC termination)
kubectl label node <node-name> nat-role=edge

# Label nodes for room role (SFU/Participant processing)
kubectl label node <node-name> nat-role=room
```

## Deploy Order

```bash
# 1. Namespace + ConfigMap
kubectl apply -f 00-namespace-config.yaml

# 2. Secrets (create before pods)
#    See "Secrets" section below
kubectl apply -f 05-secrets.yaml

# 3. Redis Sentinel HA (or skip if using existing Redis)
kubectl apply -f 60-redis-sentinel.yaml

# Wait for Redis to be ready
kubectl wait -n livekit-nat --for=condition=ready pod -l app=redis --timeout=120s

# 4. RBAC
kubectl apply -f 40-rbac.yaml

# 5. Room deployment (room nodes)
kubectl apply -f 20-room-deployment.yaml

# Wait for room pods to be ready
kubectl wait -n livekit-nat --for=condition=ready pod -l app=livekit,component=room --timeout=120s

# 6. Edge DaemonSet (edge nodes)
kubectl apply -f 10-edge-daemonset.yaml

# Wait for edge pods to be ready
kubectl wait -n livekit-nat --for=condition=ready pod -l app=livekit,component=edge --timeout=120s

# 7. Services
kubectl apply -f 30-services.yaml

# 8. HPA + PDB
kubectl apply -f 50-hpa-pdb.yaml
```

## Secrets

Create `05-secrets.yaml` for the shared media relay secret and API keys:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: livekit-relay-secret
  namespace: livekit-nat
type: Opaque
stringData:
  secret: "<generate-a-random-64-char-secret>"
---
apiVersion: v1
kind: Secret
metadata:
  name: livekit-keys
  namespace: livekit-nat
type: Opaque
stringData:
  keys: '{"devkey": "<your-api-key-secret>"}'
```

Generate the relay secret:
```bash
# Generate a secure random 64-char hex secret
openssl rand -hex 32 | tee relay-secret.txt
```

**Important**: The media relay secret must be the same on all pods (edge and room). It authenticates the cross-node TCP connections. The server refuses to start with `media_relay.enabled=true` and an empty secret.

## Scaling

### Room Nodes (HPA)

The room deployment auto-scales based on CPU utilization (target 70%). Adjust the HPA in `50-hpa-pdb.yaml`:

```bash
# Check current HPA state
kubectl -n livekit-nat get hpa livekit-room

# Manually scale (overrides HPA until next HPA cycle)
kubectl -n livekit-nat scale deployment livekit-room --replicas=5
```

### Edge Nodes (Node Pool)

Edge nodes scale by adding/removing nodes from the edge-labeled node pool. The DaemonSet ensures one edge pod per edge node. To scale:

```bash
# Add an edge node
kubectl label node <new-node> nat-role=edge

# Remove an edge node (drain first)
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data
kubectl label node <node> nat-role-
```

## Rolling Updates

### Room Deployment

The room deployment uses `maxSurge: 1, maxUnavailable: 0` for zero-downtime updates. Pods are terminated with a 60s grace period, during which ongoing sessions are migrated via `Leave(RESUME)`.

### Edge DaemonSet

The edge DaemonSet uses `maxSurge: 1, maxUnavailable: 1`. When an edge pod is terminated, the room node detects the broken control channel and triggers `FULL_RECONNECT` on the participant, forcing the client to rejoin.

## Monitoring

### Prometheus Metrics

All pods expose metrics on port 7880 at `/metrics`. The DaemonSet and Deployment annotations enable Prometheus auto-discovery:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "7880"
  prometheus.io/scheme: "http"
```

### Key Metrics to Watch

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `livekit_participants` | Active participants | Set based on capacity |
| `livekit_relay_bytes` | Media relay throughput | Monitor for saturation |
| `livekit_gateway_lost` | Edge/room disconnections | > 0 in steady state |
| `livekit_node_room_node_map_size` | Rooms per node | Watch for imbalance |

### Key Alerts

- **Edge node down**: If `livekit_gateway_lost` increments, an edge-to-room connection was lost. Clients will reconnect via `FULL_RECONNECT`.
- **Room node down**: Edge nodes detect this via `OnGatewayLost` and trigger `Leave(RESUME)` → client rejoin → room re-homed to surviving room node.
- **Redis failover**: Monitor Sentinel events. Room_node_map entries may briefly stale during failover; the stale reaper cleans them up.

## TURN Configuration

For clients behind symmetric NAT, the TURN server runs in-process on the edge nodes. The edge DaemonSet exposes:

- TURN UDP: port 3478 (host port 3478)
- TURN TCP: port 7881 (host port 7881)

In cloud VPCs, the edge node's host IP may be private. Add your VPC CIDR to `allow_restricted_peer_cidrs` in the ConfigMap:

```yaml
turn:
  allow_restricted_peer_cidrs:
    - 10.0.0.0/8
    - 172.16.0.0/12
```

## External Load Balancer

For production, place an external TCP/UDP load balancer in front of the edge nodes:

| Protocol | Edge Port | Purpose |
|----------|-----------|---------|
| TCP 7880 | 7880 | WebSocket signaling |
| TCP 7881 | 7881 | TURN TCP fallback |
| UDP 7882 | 7882 | WebRTC media |
| UDP 3478 | 3478 | TURN UDP |

**AWS NLB example**: Create a TCP NLB with target group pointing to edge node instance IDs. For UDP, enable UDP health checks (or use a TCP health check on port 7880).

**GCP GLB example**: Use a TCP/UDP load balancer with the edge node instance group as backend.

## Security Considerations

| Concern | Mitigation |
|---------|------------|
| Inter-pod relay sniffing | Media relay secret authenticates connections; plaintext RTP/RTCP within cluster trust boundary. For encryption at rest, use Cilium NetworkPolicy or WireGuard mesh. |
| Unauthorized relay access | Fail-closed: empty secret rejects all inbound connections. |
| Client→edge unencrypted WS | Use TLS termination (ingress or LB) for WebSocket signaling. |
| Edge→room plaintext media | RTP/RTCP is unencrypted on the relay. The cluster network must be trusted. |
| Redis access | Redis has no auth by default. Use the `requirepass` directive in production; update the LiveKit ConfigMap accordingly. |

## Troubleshooting

### Edge pod fails to start

```bash
kubectl -n livekit-nat logs -l app=livekit,component=edge --tail=50
```

Common causes:
- Host port 7880/7882/3478 already in use → check `ss -tlnp`
- Media relay secret missing → check `kubectl -n livekit-nat get secret livekit-relay-secret`
- Redis unreachable → `kubectl -n livekit-nat exec deploy/redis -- redis-cli PING`

### Room pod fails to connect to edge

```bash
# Check connectivity from room pod to edge pod
kubectl -n livekit-nat exec <room-pod> -- nc -zv <edge-pod-ip> 7883
kubectl -n livekit-nat exec <room-pod> -- nc -zv <edge-pod-ip> 7884
```

### Client cannot connect

```bash
# Check edge pod logs for ICE failures
kubectl -n livekit-nat logs -l app=livekit,component=edge --tail=100 | grep -i ice

# Verify advertise_ip is set correctly
kubectl -n livekit-nat exec <edge-pod> -- env | grep ADVERTISE_IP

# Verify STUN/TURN is reachable from client
# (from client machine)
nc -zv <edge-host-ip> 3478
nc -zv <edge-host-ip> 7881
```

### Redis Sentinel failover

```bash
# Check current primary
kubectl -n livekit-nat exec redis-node-0 -- redis-cli -p 26379 SENTINEL get-master-addr-by-name livekit

# Check Sentinel status
kubectl -n livekit-nat exec redis-node-0 -- redis-cli -p 26379 SENTINEL master livekit

# After failover, if the primary changed, update the LiveKit config:
#   redis.address = <new-primary>.redis-node.livekit-nat.svc:6379
# Or use the service which resolves to the current primary.
```

## Performance Tuning

### Edge Node Resources

- CPU: 2-4 cores recommended
- Memory: 1-2 GB base + ~50 MB per concurrent participant
- Network: 1 Gbps+ NIC (media relay adds ~60 bytes overhead per packet)

### Room Node Resources

- CPU: 4-8 cores recommended (SFU is CPU-intensive for simulcast + VP8/VP9 transcoding)
- Memory: 4-8 GB base + ~100 MB per participant session
- Network: 1 Gbps+ (aggregates all participant media)

### Linux Kernel Tuning (on edge nodes)

```bash
# Increase UDP buffer sizes for WebRTC media
sysctl -w net.core.rmem_max=26214400
sysctl -w net.core.wmem_max=26214400
sysctl -w net.ipv4.udp_mem="4096 87380 26214400"

# Increase connection tracking for many concurrent sessions
sysctl -w net.netfilter.nf_conntrack_max=1048576
```

## Upgrade Procedure

1. **Update configuration**: Modify ConfigMap `livekit-config` (kubectl edit or apply).
2. **Roll room pods**: `kubectl -n livekit-nat rollout restart deployment livekit-room`
   - Wait for all room pods to be Ready.
3. **Roll edge pods**: `kubectl -n livekit-nat rollout restart daemonset livekit-edge`
   - Edge pods are rolled one at a time (maxSurge: 1, maxUnavailable: 1).
   - Clients on the terminated edge pod will reconnect via `FULL_RECONNECT`.
4. **Verify**: Check metrics and logs for any errors.