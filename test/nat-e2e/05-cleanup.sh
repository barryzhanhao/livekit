#!/usr/bin/env bash
# 05-cleanup — tear down the NAT E2E cluster.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

# clear any injected loss first (ignore errors)
for n in "$EDGE_NODE" "$ROOM_NODE"; do
  clear_loss "$n" 2>/dev/null || true
done

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "deleting kind cluster '$CLUSTER_NAME'..."
  kind delete cluster --name "$CLUSTER_NAME"
else
  log "cluster '$CLUSTER_NAME' not found"
fi
log "cleanup complete"
