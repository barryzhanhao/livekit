#!/usr/bin/env bash
# 07-coverage — E2E coverage tool for the NAT multi-node cluster.
#
# Builds livekit-server with Go coverage instrumentation (GOCOVERDIR), deploys it
# to the 2-node kind cluster with /tmp/coverage mounted as an emptyDir, runs the
# full E2E suites (lk + Go client — the multi-node NAT tests), sends SIGINT to the
# server process (graceful shutdown flushes coverage into the emptyDir), waits for
# the container to restart (emptyDir persists), then copies the profiles out and
# reports total statement coverage across both nodes.
#
# Usage: ./07-coverage.sh [--quick]
#   --quick   run only the Go client suite (lk suite takes ~6 min)
#   --report <dir>   merge + report an already-collected coverage dir
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib/common.sh"

REPO_ROOT="$(cd "$DIR/../.." && pwd)"
COVER_DIR="$DIR/coverage-results"
COVER_IMG="${COVER_IMG:-livekit-nat-cover:dev}"

require go kubectl

# ---------- report an existing coverage dir ----------
if [ "${1:-}" = "--report" ]; then
  src="${2:-$COVER_DIR}"
  [ -d "$src" ] || { echo "no coverage dir: $src"; exit 1; }
  merge_dir="$src/merged"
  rm -rf "$merge_dir"; mkdir -p "$merge_dir"
  inputs=()
  for d in "$src"/edge "$src"/room; do
    [ -d "$d" ] && [ -n "$(ls -A "$d" 2>/dev/null)" ] && inputs+=("-i=$d")
  done
  if [ ${#inputs[@]} -eq 0 ]; then
    echo "no coverage data found under $src"; exit 1
  fi
  go tool covdata merge "${inputs[@]}" -o "$merge_dir"
  echo "=== per-node file counts ==="
  for d in "$src"/edge "$src"/room; do
    [ -d "$d" ] && echo "  $(basename "$d"): $(ls "$d" | wc -l | tr -d ' ') files"
  done
  echo "=== total statement coverage ==="
  go tool covdata func -i="$merge_dir" | tail -1
  echo "=== per-package coverage (bottom 15 by coverage) ==="
  go tool covdata textfmt -i="$merge_dir" -o "$merge_dir/profile.txt"
  # legacy profile lines: <pkgdir>/<file>:<s>,<e> <numStmts> <count>
  # a block is covered when count > 0
  awk '
    /^github.com\/livekit\/livekit-server\// {
      file=$1; stmts=$2; count=$3
      sub(/:.*/, "", file)         # strip :line,col
      n=split(file, parts, "/"); pkg=""; for(i=1;i<n;i++){pkg=pkg (i>1?"/":"") parts[i]}
      pt[pkg]+=stmts
      if (count>0) ph[pkg]+=stmts
    }
    END { for(p in ph) if (pt[p]>0) printf "%s\t%.1f%%\t(%d/%d)\n", p, 100*ph[p]/pt[p], ph[p], pt[p] }
  ' "$merge_dir/profile.txt" | sort -t$'\t' -k2 -n | head -15
  return 0
fi

QUICK="${1:-}"
rm -rf "$COVER_DIR"; mkdir -p "$COVER_DIR"

log "building instrumented livekit-server (GOCOVERDIR)..."
# coverpkg must include the MAIN package (cmd/server) or the binary's coverage
# mode metadata is <invalid> and on-demand WriteCountersDir fails.
(cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -cover -covermode=atomic \
  -coverpkg="github.com/livekit/livekit-server/..." \
  -o "$COVER_DIR/livekit-server" ./cmd/server)
docker build -t "$COVER_IMG" -f "$DIR/Dockerfile.nat" "$COVER_DIR"
kind load docker-image "$COVER_IMG" --name "$CLUSTER_NAME"

# deploy with GOCOVERDIR backed by an emptyDir so profiles survive container restart
export IMG="$COVER_IMG"
export NAT_COVERAGE_GOCOVERDIR='- {name: GOCOVERDIR, value: /tmp/coverage}'
export NAT_COVERAGE_VOLUME_MOUNT='- {name: coverage, mountPath: /tmp/coverage}'
export NAT_COVERAGE_VOLUME='        - name: coverage
          emptyDir: {}'
"$DIR/03-deploy.sh"

log "running E2E suites against instrumented binary..."
if [ "$QUICK" = "--quick" ]; then
  "$DIR/06-client-e2e.sh" || true
else
  "$DIR/04-e2e.sh" || true
  "$DIR/06-client-e2e.sh" || true
fi

log "flushing coverage via /debug/coverage..."
# The instrumented server exposes /debug/coverage (when GOCOVERDIR is set) which
# flushes counters on demand — no process exit required.
for role in edge room; do
  ip="$("$role"_node_ip)"
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://$ip:7880/debug/coverage" 2>/dev/null || echo 000)
  echo "  $role: /debug/coverage -> $code"
done

log "collecting coverage from emptyDir..."
for role in edge room; do
  pod=$(kubectl get pod -l app.kubernetes.io/name=$role -n "$NAMESPACE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -n "$pod" ] && kubectl exec -n "$NAMESPACE" "$pod" -- sh -c 'test -d /tmp/coverage' 2>/dev/null; then
    mkdir -p "$COVER_DIR/$role"
    # tar the coverage dir out through exec (emptyDir is node-local)
    kubectl exec -n "$NAMESPACE" "$pod" -- sh -c 'cd /tmp/coverage && tar cf - .' \
      > "$COVER_DIR/$role.tar" 2>/dev/null || true
    if [ -s "$COVER_DIR/$role.tar" ]; then
      tar xf "$COVER_DIR/$role.tar" -C "$COVER_DIR/$role" 2>/dev/null || true
      rm -f "$COVER_DIR/$role.tar"
    fi
  fi
  echo "  $role: $(find "$COVER_DIR/$role" -type f 2>/dev/null | wc -l | tr -d ' ') files"
done

"$0" --report "$COVER_DIR"
