#!/usr/bin/env bash
# 02-build-image — cross-compile the fork's livekit-server (host, no network
# needed for deps) and build the runtime image, then load it into kind.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"   # livekit repo root (test/nat-e2e is 2 levels deep)
BUILD_DIR="$(mktemp -d)"

log "cross-compiling livekit-server (linux/amd64) from $REPO_ROOT ..."
(cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$BUILD_DIR/livekit-server" ./cmd/server)

log "building image $IMG ..."
docker build -t "$IMG" -f "$DIR/Dockerfile.nat" "$BUILD_DIR"

log "loading image into kind '$CLUSTER_NAME' ..."
kind load docker-image "$IMG" --name "$CLUSTER_NAME"

# webhook receiver (E2E): tiny static HTTP server the server nodes POST webhook
# events to. Built into its own scratch image + loaded into kind.
log "cross-compiling webhook-receiver (linux/amd64) ..."
(cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$BUILD_DIR/webhook-receiver" ./test/nat-e2e/webhook-receiver)
WEBHOOK_IMG="${WEBHOOK_IMG:-webhook-recv:dev}"
docker build -t "$WEBHOOK_IMG" -f "$DIR/Dockerfile.webhook-recv" "$BUILD_DIR"
kind load docker-image "$WEBHOOK_IMG" --name "$CLUSTER_NAME"

rm -rf "$BUILD_DIR"
log "images $IMG + $WEBHOOK_IMG ready in cluster"
