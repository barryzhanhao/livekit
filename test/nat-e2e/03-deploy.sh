#!/usr/bin/env bash
# 03-deploy — deploy Redis + the two LiveKit server pods (edge on kind-worker,
# room on kind-worker2), then pin the room to the room node so the NAT split is
# deterministic (client joins via edge, room hosted on the other node).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

require kubectl
kubectl cluster-info >/dev/null 2>&1 || { echo "no cluster — run ./01-cluster.sh first"; exit 1; }

EDGE_IP="$(edge_node_ip)"
ROOM_IP="$(room_node_ip)"
log "edge node ip: $EDGE_IP   room node ip: $ROOM_IP"

# ---- Redis ----
kubectl apply -f "$DIR/manifests/redis.yaml"
wait_for "redis running" 120 kubectl get deploy/redis -o jsonpath='{.status.readyReplicas}' | grep -q 1

# Clear stale node registrations from previous runs (restarts leave random
# node IDs behind; stable IDs below prevent future duplicates).
kubectl exec deploy/redis -- redis-cli DEL nodes room_node_map >/dev/null 2>&1 || true

# ---- server config configmap ----
kubectl create configmap nat-config --from-file=config.yaml="$DIR/configs/config.yaml" --dry-run=client -o yaml | kubectl apply -f -

# ---- server deployments (hostNetwork => pod IP == node IP; advertise_ip = node IP) ----
gen_server_deploy() { # gen_server_deploy <name> <nat-role> <node-ip>
  local name="$1" role="$2" ip="$3"
  cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: livekit-$name
  namespace: $NAMESPACE
spec:
  replicas: 1
  strategy:
    type: Recreate            # hostNetwork: one pod per node; must free ports before next
  selector: {matchLabels: {app: livekit-$name}}
  template:
    metadata:
      labels:
        app: livekit-$name
        app.kubernetes.io/name: $name
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet   # resolve cluster services (redis) even with hostNetwork
      nodeSelector: {nat-role: $role}
      containers:
        - name: server
          image: $IMG
          args: ["--config", "/etc/livekit/config.yaml"]
          env:
            - {name: POD_IP, valueFrom: {fieldRef: {fieldPath: status.podIP}}}
            - {name: LIVEKIT_RTC_ADVERTISE_IP, value: "$ip"}
            - {name: LIVEKIT_REGION, value: "$role"}
            - {name: LIVEKIT_NODE_ID, value: "node-$name"}
          ports:
            - {containerPort: 7880, protocol: TCP}
            - {containerPort: 7881, protocol: TCP}
            - {containerPort: 7882, protocol: UDP}
            - {containerPort: 7883, protocol: TCP}
            - {containerPort: 7884, protocol: TCP}
          volumeMounts:
            - {name: config, mountPath: /etc/livekit}
          resources:
            limits: {cpu: "2", memory: 1Gi}
      volumes:
        - name: config
          configMap:
            name: nat-config
            items:
              - {key: config.yaml, path: config.yaml}
EOF
}

gen_server_deploy edge edge "$EDGE_IP"  | kubectl apply -f -
gen_server_deploy room room "$ROOM_IP" | kubectl apply -f -
# Image tag is stable ($IMG), so `apply` alone won't roll when the image was
# rebuilt under the same tag — force a rollout so re-running this script always
# runs the latest binary (Recreate strategy frees the hostNetwork ports).
kubectl rollout restart deploy/livekit-edge deploy/livekit-room -n "$NAMESPACE"

log "waiting for both server pods Ready..."
# rollout status waits for the NEW ReplicaSet (unlike `kubectl wait`, which can
# match the old pod still terminating after a Recreate rollout).
kubectl rollout status deploy/livekit-edge -n "$NAMESPACE" --timeout=180s
kubectl rollout status deploy/livekit-room -n "$NAMESPACE" --timeout=180s

log "waiting for both nodes registered in redis..."
wait_for "both nodes registered" 120 sh -c 'kubectl exec deploy/redis -- redis-cli --raw HLEN nodes | grep -qx 2'

log "--- registered nodes ---"
for key in $(redis_cli HKEYS nodes); do
  echo "  node-id: $key  ip: $(redis_cli HGET nodes "$key" | grep -aEo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
done

# ---- pin room -> room node (NAT split determinism) ----
seed_room_map "$ROOM"

export WS_URL="ws://$EDGE_IP:7880"
echo "WS_URL=$WS_URL"
echo "room node ip: $ROOM_IP  (clients must NOT connect here for the split to hold)"
log "deploy complete"
