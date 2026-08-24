#!/bin/bash
# NAT browser E2E — container entrypoint.
#
# Starts the Express server in the background, waits for it to be ready,
# runs Playwright tests, then exits with the test exit code.
set -euo pipefail

# Start the Express server
node server.js &
SERVER_PID=$!

# Wait for the server to be ready
echo "Waiting for browser-test server..."
for i in $(seq 1 30); do
  if curl -s http://localhost:8081/config > /dev/null 2>&1; then
    echo "Server ready on port 8081"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Server failed to start within 30s"
    kill $SERVER_PID 2>/dev/null
    exit 1
  fi
  sleep 1
done

# Run Playwright tests
echo "Running Playwright tests..."
npx playwright test --reporter=list "$@"
EXIT_CODE=$?

# Kill the server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true

echo "Browser E2E tests completed with exit code $EXIT_CODE"
exit $EXIT_CODE