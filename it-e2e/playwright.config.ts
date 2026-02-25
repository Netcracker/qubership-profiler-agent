import { defineConfig, devices } from '@playwright/test';

const port = 18090;

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  retries: 0,
  workers: 1,
  reporter: 'list',

  use: {
    baseURL: `http://127.0.0.1:${port}`,
    trace: 'on-first-retry',
  },

  webServer: {
    command: [
      'java',
      '-Djava.awt.headless=true',
      '--add-opens=java.base/java.io=ALL-UNNAMED',
      '--add-opens=java.base/java.lang=ALL-UNNAMED',
      '--add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED',
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
