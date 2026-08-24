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

# redis: the only image the nodes pull from the registry (everything else is
# preloaded). The kind nodes' containerd proxy points at 127.0.0.1:7897 (node
# loopback), which cannot reach the host's proxy, so a fresh cluster cannot pull
# it — preload it like the other images.
kind load docker-image redis:7-alpine --name "$CLUSTER_NAME" || true

# busybox: used by the browser E2E Job's wait-for-edge init container. Same
# proxy problem as redis — a fresh cluster cannot pull it, so preload it.
kind load docker-image busybox:1.36 --name "$CLUSTER_NAME" || true

# webhook receiver (E2E): tiny static HTTP server the server nodes POST webhook
# events to. Built into its own scratch image + loaded into kind.
log "cross-compiling webhook-receiver (linux/amd64) ..."
(cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$BUILD_DIR/webhook-receiver" ./test/nat-e2e/webhook-receiver)
WEBHOOK_IMG="${WEBHOOK_IMG:-webhook-recv:dev}"
docker build -t "$WEBHOOK_IMG" -f "$DIR/Dockerfile.webhook-recv" "$BUILD_DIR"
kind load docker-image "$WEBHOOK_IMG" --name "$CLUSTER_NAME"

rm -rf "$BUILD_DIR"
log "images $IMG + $WEBHOOK_IMG ready in cluster"

# ---- stress-test client image ----
log "cross-compiling nat stress-test client (linux/amd64) ..."
STRESS_IMG="${STRESS_IMG:-livekit-nat-dev:stress}"
STRESS_BUILD="$(mktemp -d)"
(cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$STRESS_BUILD/nat-client" ./test/nat-e2e/client)
docker build -t "$STRESS_IMG" -f "$DIR/Dockerfile.stress" "$STRESS_BUILD"
kind load docker-image "$STRESS_IMG" --name "$CLUSTER_NAME"
rm -rf "$STRESS_BUILD"
log "stress client image $STRESS_IMG ready in cluster"

# ---- browser E2E test image ----
# Skip unless explicitly requested (SKIP_BROWSER=1): the stress driver does not
# need the 2.65GB Playwright image, and rebuilding it on every stress run is slow
# and repeatedly trips OrbStack's registry-metadata corruption (node:22-alpine
# 'failed size validation'). 08-browser-e2e.sh sets SKIP_BROWSER=0.
if [ "${SKIP_BROWSER:-0}" != "1" ]; then
  log "building browser E2E test image (Playwright + Chromium + livekit-client)..."
  BROWSER_IMG="${BROWSER_IMG:-livekit-nat-dev:browser}"
  # The Dockerfile uses the test/nat-e2e directory as context; copy the Dockerfile
  # into the build context so relative COPY paths work.
  (cd "$DIR" && docker build -t "$BROWSER_IMG" -f Dockerfile.browser .)
  kind load docker-image "$BROWSER_IMG" --name "$CLUSTER_NAME"
  log "browser image $BROWSER_IMG ready in cluster"
else
  log "skipping browser E2E image build (SKIP_BROWSER=1)"
fi
