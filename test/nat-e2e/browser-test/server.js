// Express server for NAT-mode browser E2E tests.
//
// Serves the test page, bundles, and provides a token endpoint so the
// Playwright tests can get JWTs without embedding secrets in the browser.
const express = require('express');
const path = require('path');
const { AccessToken } = require('livekit-server-sdk');

const app = express();
const PORT = process.env.PORT || 8081;
const API_KEY = process.env.API_KEY || 'devkey';
const API_SECRET = process.env.API_SECRET || 'secret';
const LIVEKIT_URL = process.env.LIVEKIT_URL || 'ws://livekit-edge:7880';

// ---- token endpoint ----
app.get('/config', (_req, res) => {
  res.json({
    wsUrl: LIVEKIT_URL,
    apiKey: API_KEY,
  });
});

app.get('/token', async (req, res) => {
  const identity = req.query.identity || 'browser-test-' + Date.now();
  const room = req.query.room || 'nat-browser-e2e';
  const canPublish = req.query.publish !== 'false';

  const at = new AccessToken(API_KEY, API_SECRET);
  at.identity = identity;
  at.name = identity;
  at.addGrant({
    roomJoin: true,
    room: room,
    canPublish: canPublish,
    canSubscribe: true,
    canPublishData: true,
  });

  // toJwt() is async in livekit-server-sdk ≥ 2.x — MUST await it or the
  // response carries a Promise, which the edge rejects with 401.
  const token = await at.toJwt();
  res.json({ token, identity, room, wsUrl: LIVEKIT_URL });
});

// ---- static files ----
app.use(express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`browser-test server listening on port ${PORT}`);
  console.log(`  wsUrl: ${LIVEKIT_URL}`);
  console.log(`  apiKey: ${API_KEY}`);
});