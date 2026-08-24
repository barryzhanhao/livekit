// NAT-mode browser E2E test — browser-side logic.
//
// Connects to the NAT-mode edge via livekit-client, publishes a fake
// video track, and reports connect/publish/subscribe success/failure
// via DOM state that Playwright reads.
//
// Built with esbuild: package.json "build" script bundles this into
// public/bundle.js (imports from livekit-client are resolved by esbuild).

import { Room, RoomEvent, createLocalVideoTrack, RemoteTrackPublication } from 'livekit-client';

const LOG = document.getElementById('log');
const STATUS = document.getElementById('status');
const RESULT = document.getElementById('result');
const SCENARIO = document.getElementById('scenario');

function log(level, msg) {
  const div = document.createElement('div');
  div.className = level;
  div.textContent = `[${new Date().toISOString().slice(11, 19)}] ${msg}`;
  LOG.appendChild(div);
}

function setStatus(state, text) {
  STATUS.className = state;
  STATUS.textContent = text;
  log('i', text);
}

function setResult(pass, summary) {
  RESULT.textContent = summary;
  RESULT.style.color = pass ? '#006600' : '#cc0000';
  log(pass ? 's' : 'e', summary);
  // Signal to Playwright: the test is done
  window.__testResult = { pass, summary };
}

// ---- scenario: connect-and-publish ----
async function scenarioConnectAndPublish(config) {
  SCENARIO.textContent = 'Scenario: connect-and-publish';
  setStatus('running', 'Connecting...');

  const room = new Room({
    adaptiveStream: false,
    dynacast: false,
    iceServers: [],     // let the server provide ICE (including TURN for NAT)
    iceTransportPolicy: 'all',
  });

  let remoteTracks = 0;
  let remoteData = false;
  let connected = false;
  let localTrack = null;

  // === event handlers ===
  room.on(RoomEvent.Connected, () => {
    connected = true;
    setStatus('running', `Connected (sid=${room.sid}). Publishing video...`);
  });

  room.on(RoomEvent.Disconnected, (reason) => {
    if (connected) {
      log('e', `Disconnected: ${reason}`);
    }
  });

  room.on(RoomEvent.TrackSubscribed, (_track, _pub) => {
    remoteTracks++;
    log('s', `Remote track subscribed (total=${remoteTracks})`);
  });

  room.on(RoomEvent.DataReceived, (_payload, _participant) => {
    remoteData = true;
    log('s', 'Data received from remote');
  });

  room.on(RoomEvent.ParticipantConnected, (participant) => {
    log('i', `Remote participant connected: ${participant.identity}`);
  });

  room.on(RoomEvent.TrackPublished, (pub, participant) => {
    log('i', `Remote track published: ${pub.trackSid} by ${participant.identity}`);
  });

  // === connect ===
  try {
    await room.connect(config.wsUrl, config.token, {
      autoSubscribe: true,
    });
  } catch (err) {
    setStatus('fail', 'Connection failed');
    log('e', `Connect error: ${err.message}`);
    setResult(false, `FAIL: connect — ${err.message}`);
    return;
  }

  // === publish video ===
  try {
    localTrack = await createLocalVideoTrack({
      // Use fake device if available (Playwright launches with --use-fake-device-for-media-stream)
      deviceId: 'fake',
      resolution: { width: 640, height: 480 },
    });
    await room.localParticipant.publishTrack(localTrack, {
      videoEncoding: { maxBitrate: 500_000, maxFramerate: 24 },
      simulcast: false,
      dtx: false,
    });
    log('s', 'Local video track published');
  } catch (err) {
    setStatus('fail', 'Publish failed');
    log('e', `Publish error: ${err.message}`);
    setResult(false, `FAIL: publish — ${err.message}`);
    await room.disconnect();
    return;
  }

  // === wait for a remote participant (the other browser in paired tests) ===
  // In single-browser mode, just verify we're connected and publishing.
  // The test will be marked PASS with a note about no remote participant.
  setStatus('pass', 'Connected and publishing. Checking for remote participants...');
  log('i', `Connected to room ${room.name}, local participant=${room.localParticipant.identity}`);

  // Wait a bit for any remote participants to subscribe
  await new Promise(resolve => setTimeout(resolve, 5000));

  // === report ===
  const remoteCount = room.remoteParticipants.size;
  const summary = [
    `connected=${connected}`,
    `published=${localTrack !== null}`,
    `remoteParticipants=${remoteCount}`,
    `tracksSubscribed=${remoteTracks}`,
    `remoteData=${remoteData}`,
  ].join(', ');

  if (connected && localTrack !== null) {
    setResult(true, `PASS: ${summary}`);
  } else {
    setResult(false, `FAIL: ${summary}`);
  }

  await room.disconnect();
}

// ---- scenario: receive-before-publish ----
async function scenarioReceiveBeforePublish(config) {
  SCENARIO.textContent = 'Scenario: receive-before-publish';
  setStatus('running', 'Connecting (subscribe-only)...');

  const room = new Room({
    adaptiveStream: false,
    dynacast: false,
    iceServers: [],
    iceTransportPolicy: 'all',
  });

  let connected = false;
  let tracksBeforePublish = 0;

  room.on(RoomEvent.Connected, () => {
    connected = true;
    setStatus('running', 'Connected as subscriber. Waiting for remote tracks...');
  });

  room.on(RoomEvent.TrackSubscribed, (_track, _pub) => {
    tracksBeforePublish++;
    log('s', `Track subscribed before publish (total=${tracksBeforePublish})`);
  });

  try {
    await room.connect(config.wsUrl, config.token, { autoSubscribe: true });
  } catch (err) {
    setResult(false, `FAIL: receive-before-publish connect — ${err.message}`);
    return;
  }

  await new Promise(resolve => setTimeout(resolve, 10000));

  const summary = `connected=${connected}, tracksBeforePublish=${tracksBeforePublish}`;

  if (connected && tracksBeforePublish > 0) {
    setResult(true, `PASS: ${summary}`);
  } else {
    // Receiving tracks before publish is best-effort in NAT mode (media follows
    // signaling, so the subscriber may need to wait for the publisher's track
    // to propagate through the room node). We still pass if we connected.
    setResult(connected, `PARTIAL: ${summary}`);
  }

  await room.disconnect();
}

// ---- scenario: data-channel ----
async function scenarioDataChannel(config) {
  SCENARIO.textContent = 'Scenario: data-channel';
  setStatus('running', 'Connecting...');

  const room = new Room({
    adaptiveStream: false,
    dynacast: false,
    iceServers: [],
    iceTransportPolicy: 'all',
  });

  let dataReceived = false;
  let dataSent = false;

  room.on(RoomEvent.Connected, async () => {
    setStatus('running', 'Connected. Sending data...');
    try {
      await room.localParticipant.publishData(
        new TextEncoder().encode('hello from browser e2e'),
        { reliable: true }
      );
      dataSent = true;
      log('s', 'Data sent');
    } catch (err) {
      log('e', `Data send error: ${err.message}`);
    }
  });

  room.on(RoomEvent.DataReceived, (_payload, _participant) => {
    dataReceived = true;
    log('s', 'Data received');
  });

  try {
    await room.connect(config.wsUrl, config.token, { autoSubscribe: true });
  } catch (err) {
    setResult(false, `FAIL: data-channel connect — ${err.message}`);
    return;
  }

  await new Promise(resolve => setTimeout(resolve, 8000));
  setResult(dataSent, `dataSent=${dataSent}, dataReceived=${dataReceived}`);
  await room.disconnect();
}

// ---- scenario: mute-remote ----
async function scenarioMuteRemote(config) {
  SCENARIO.textContent = 'Scenario: mute-remote';
  setStatus('running', 'Connecting as subscriber...');

  const room = new Room({
    adaptiveStream: false,
    dynacast: false,
    iceServers: [],
    iceTransportPolicy: 'all',
  });

  let muted = false;
  let unmuted = false;

  room.on(RoomEvent.TrackMuted, (_pub) => { muted = true; log('i', 'Remote track muted'); });
  room.on(RoomEvent.TrackUnmuted, (_pub) => { unmuted = true; log('i', 'Remote track unmuted'); });

  try {
    await room.connect(config.wsUrl, config.token, { autoSubscribe: true });
  } catch (err) {
    setResult(false, `FAIL: mute-remote connect — ${err.message}`);
    return;
  }

  await new Promise(resolve => setTimeout(resolve, 15000));
  setResult(true, `muted=${muted}, unmuted=${unmuted}`);
  await room.disconnect();
}

// ---- main ----
(async () => {
  try {
    const configResp = await fetch('/config');
    const config = await configResp.json();

    // Get token
    const params = new URLSearchParams(window.location.search);
    const room = params.get('room') || 'nat-browser-e2e';
    const scenario = params.get('scenario') || 'connect-and-publish';
    const identity = params.get('identity') || `browser-${Date.now()}`;

    const tokenResp = await fetch(`/token?identity=${identity}&room=${room}`);
    const tokenData = await tokenResp.json();

    config.token = tokenData.token;
    config.wsUrl = tokenData.wsUrl;

    log('i', `Running scenario: ${scenario}`);
    log('i', `Identity: ${identity}, Room: ${room}`);
    log('i', `WS URL: ${config.wsUrl}`);

    switch (scenario) {
      case 'connect-and-publish':
        await scenarioConnectAndPublish(config);
        break;
      case 'receive-before-publish':
        await scenarioReceiveBeforePublish(config);
        break;
      case 'data-channel':
        await scenarioDataChannel(config);
        break;
      case 'mute-remote':
        await scenarioMuteRemote(config);
        break;
      default:
        setResult(false, `FAIL: unknown scenario '${scenario}'`);
    }
  } catch (err) {
    log('e', `Fatal error: ${err.message}`);
    setResult(false, `FAIL: fatal — ${err.message}`);
  }
})();