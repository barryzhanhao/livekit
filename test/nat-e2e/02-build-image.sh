#!/usr/bin/env bash
# 02-build-image — cross-compile the fork's livekit-server (host, no network
# needed for deps) and build the runtime image, then load it into kind.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../../.." && pwd)"   # livekit repo root
BUILD_DIR="$(mktemp -d)"

log "cross-compiling livekit-server (linux/amd64) from $REPO_ROOT ..."
(cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$BUILD_DIR/livekit-server" ./cmd/server)

log "building image $IMG ..."
docker build -t "$IMG" -f "$DIR/Dockerfile.nat" "$BUILD_DIR"

log "loading image into kind '$CLUSTER_NAME' ..."
kind load docker-image "$IMG" --name "$CLUSTER_NAME"

rm -rf "$BUILD_DIR"
log "image $IMG ready in cluster"
