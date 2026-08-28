import { defineConfig, devices } from '@playwright/test';

const port = 18090;
const headed = !!process.env.HEADED;

export default defineConfig({
  testDir: './e2e',
  timeout: headed ? 0 : 60_000,
  retries: 0,
  workers: 1,
  outputDir: 'test-results',
  reporter: [
    ['list'],
    ['html', { outputFolder: 'playwright-report', open: 'never' }],
  ],

  use: {
    baseURL: `http://127.0.0.1:${port}`,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    headless: !headed,
    launchOptions: headed ? { slowMo: 500 } : {},
  },

  webServer: {
    command: [
      'java',
      '-Djava.awt.headless=true',
      `-Dprofiler.dump=${process.env.DUMP_DIR}`,
      `-jar`,
      process.env.PROFILER_WAR_PATH!,
      '--httpPort', String(port),
      '--httpListenAddress', '127.0.0.1',
    ].join(' '),
    port,
    timeout: 30_000,
    reuseExistingServer: false,
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
