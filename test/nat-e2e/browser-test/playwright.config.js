// @ts-check
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  timeout: 120_000,               // 120s per test — WebRTC ICE + relay + media
  expect: { timeout: 30_000 },
  fullyParallel: false,
  retries: 1,                      // one retry for flaky WebRTC ICE
  workers: 1,                      // serial — single browser for NAT determinism
  reporter: [['list'], ['json', { outputFile: 'test-results.json' }]],
  use: {
    baseURL: process.env.TEST_SERVER_URL || 'http://localhost:8081',
    headless: true,
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        browserName: 'chromium',
        launchOptions: {
          args: [
            '--use-fake-ui-for-media-stream',
            '--use-fake-device-for-media-stream',
            '--no-sandbox',
            '--disable-setuid-sandbox',
          ],
        },
      },
    },
  ],
});