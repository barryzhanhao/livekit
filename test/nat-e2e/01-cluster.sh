#!/usr/bin/env bash
# 01-cluster — create the 2-worker kind cluster.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "cluster $CLUSTER_NAME already exists"
else
  log "creating kind cluster '$CLUSTER_NAME' (control-plane + edge + room workers)..."
  kind create cluster --config "$DIR/manifests/kind-cluster.yaml" --name "$CLUSTER_NAME"
fi

log "waiting for nodes Ready..."
kubectl wait --for=condition=Ready node --all --timeout=180s

echo "--- nodes ---"
kubectl get nodes -o wide
echo "EDGE_NODE_IP=$(edge_node_ip)"
echo "ROOM_NODE_IP=$(room_node_ip)"
