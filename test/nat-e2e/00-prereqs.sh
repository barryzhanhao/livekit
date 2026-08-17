#!/usr/bin/env bash
# 00-prereqs — verify/install host tooling for the NAT E2E.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

log "checking prerequisites..."

require docker kubectl go

# kind (multi-node k8s) — install if missing
if ! command -v kind >/dev/null 2>&1; then
  log "kind not found — installing v0.27.0 (via proxy)"
  KIND_VER=v0.27.0
  curl -sSLo /tmp/kind https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VER}/kind-darwin-amd64
  chmod +x /tmp/kind
  mv /tmp/kind /usr/local/bin/kind
fi
kind version

# lk (livekit-cli) — the E2E WebRTC client
if ! command -v lk >/dev/null 2>&1; then
  log "lk not found — install via: brew install livekit-cli  (or download from livekit-cli releases)"
  exit 1
fi
command -v lk

# ffmpeg — generate deterministic test media (vp8/opus)
if ! command -v ffmpeg >/dev/null 2>&1; then
  log "ffmpeg not found — install via: brew install ffmpeg"
  exit 1
fi

# docker up?
docker version >/dev/null 2>&1 || { echo "docker daemon not reachable"; exit 1; }

# pre-pull redis image to verify docker-hub reachability early
log "pre-pulling redis:7-alpine (docker hub must be reachable)"
docker pull redis:7-alpine >/dev/null 2>&1 || {
  echo "  ✗ docker pull redis:7-alpine failed — set proxy for the Docker daemon (Settings -> Resources -> Proxies)" >&2
  exit 1
}

log "prereqs OK"
