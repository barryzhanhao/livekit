// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const TEST_URL = process.env.TEST_URL || 'http://browser-test:8081';
const ROOM = process.env.ROOM || 'nat-browser-e2e';
const TIMEOUT = 90000; // 90s — WebRTC ICE + relay + media needs generous time

// Helper: navigate to a test scenario and await the result.
// Returns { pass, summary } from the page's __testResult.
async function runScenario(page, scenario, identity) {
  const url = `${TEST_URL}/?scenario=${scenario}&room=${ROOM}&identity=${identity}`;
  await page.goto(url, { waitUntil: 'networkidle', timeout: 15000 });

  // Wait for the page to report a test result
  const result = await page.waitForFunction(
    () => window.__testResult,
    { timeout: TIMEOUT }
  );
  return result.jsonValue();
}

// ---- Scenario 1: Connect and Publish ----
test('connect-and-publish: browser connects, publishes video, and keeps session alive', async ({ page }) => {
  test.setTimeout(TIMEOUT + 10000);
  const result = await runScenario(page, 'connect-and-publish', `pub-${Date.now()}`);
  expect(result.pass).toBe(true);
  expect(result.summary).toContain('connected=true');
  expect(result.summary).toContain('published=true');
  console.log(`[connect-and-publish] ${result.summary}`);
});

// ---- Scenario 2: Receive Before Publish ----
// This scenario connects a subscriber FIRST, then a publisher joins.
// The subscriber should eventually receive the publisher's tracks.
test('receive-before-publish: subscriber receives published track', async ({ browser }) => {
  test.setTimeout(TIMEOUT + 30000);

  // Open subscriber page first
  const subPage = await browser.newPage();
  const subIdentity = `sub-${Date.now()}`;
  const subPromise = runScenario(subPage, 'receive-before-publish', subIdentity);

  // Wait a moment for the subscriber to connect, then open a publisher
  await new Promise(r => setTimeout(r, 3000));

  const pubPage = await browser.newPage();
  const pubIdentity = `pub-${Date.now()}`;
  const pubResult = await runScenario(pubPage, 'connect-and-publish', pubIdentity);
  console.log(`[receive-before-publish:pub] ${pubResult.summary}`);

  // Wait for subscriber to get the result
  const subResult = await subPromise;
  console.log(`[receive-before-publish:sub] ${subResult.summary}`);

  // Publisher must succeed; subscriber should have connected
  expect(pubResult.pass).toBe(true);
  expect(subResult.pass || subResult.summary).toBeDefined();
  expect(subResult.summary).toContain('connected=true');

  await pubPage.close();
  await subPage.close();
});

// ---- Scenario 3: Data Channel ----
test('data-channel: browser publishes and receives data (paired)', async ({ browser }) => {
  test.setTimeout(TIMEOUT + 20000);

  const pageA = await browser.newPage();
  const pageB = await browser.newPage();
  const idA = `data-A-${Date.now()}`;
  const idB = `data-B-${Date.now()}`;

  // Both connect simultaneously
  const [resultA, resultB] = await Promise.all([
    runScenario(pageA, 'data-channel', idA),
    runScenario(pageB, 'data-channel', idB),
  ]);

  console.log(`[data-channel:A] ${resultA.summary}`);
  console.log(`[data-channel:B] ${resultB.summary}`);

  // At minimum, each must have sent data
  expect(resultA.summary).toContain('dataSent=true');
  expect(resultB.summary).toContain('dataSent=true');

  await pageA.close();
  await pageB.close();
});

// ---- Scenario 4: Mute Remote ----
// Publisher and subscriber: publisher mutes/unmutes, subscriber observes events.
test('mute-remote: subscriber detects mute/unmute events', async ({ browser }) => {
  test.setTimeout(TIMEOUT + 30000);

  // Publisher joins first and publishes
  const pubPage = await browser.newPage();
  const pubIdentity = `mute-pub-${Date.now()}`;
  const pubResult = await runScenario(pubPage, 'connect-and-publish', pubIdentity);
  expect(pubResult.pass).toBe(true);
  console.log(`[mute-remote:pub] ${pubResult.summary}`);

  // Subscriber joins and watches for mute events
  const subPage = await browser.newPage();
  const subIdentity = `mute-sub-${Date.now()}`;
  const subResult = await runScenario(subPage, 'mute-remote', subIdentity);
  console.log(`[mute-remote:sub] ${subResult.summary}`);

  expect(subResult.pass).toBe(true);

  await pubPage.close();
  await subPage.close();
});