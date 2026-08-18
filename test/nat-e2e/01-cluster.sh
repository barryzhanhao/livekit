#!/usr/bin/env bash
# 01-cluster — create the 3-worker kind cluster (edge + room + edge2).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  if kubectl get node "$EDGE2_NODE" >/dev/null 2>&1; then
    log "cluster $CLUSTER_NAME already exists (with edge2 node)"
  else
    log "cluster $CLUSTER_NAME exists but lacks $EDGE2_NODE (multi-edge requires a 3rd worker); recreating..."
    kind delete cluster --name "$CLUSTER_NAME"
    kind create cluster --config "$DIR/manifests/kind-cluster.yaml" --name "$CLUSTER_NAME"
  fi
else
  log "creating kind cluster '$CLUSTER_NAME' (control-plane + edge + room + edge2 workers)..."
  kind create cluster --config "$DIR/manifests/kind-cluster.yaml" --name "$CLUSTER_NAME"
fi

log "waiting for nodes Ready..."
kubectl wait --for=condition=Ready node --all --timeout=180s

echo "--- nodes ---"
kubectl get nodes -o wide
echo "EDGE_NODE_IP=$(edge_node_ip)"
echo "ROOM_NODE_IP=$(room_node_ip)"
echo "EDGE2_NODE_IP=$(edge2_node_ip)"
