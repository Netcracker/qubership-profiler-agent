import { defineConfig, devices } from '@playwright/test';

// E2E for the new embedded UI (backend/apps/ui) served by the query service
// at /ui. The stack is external: `make query-ui` brings up the dev compose
// (MinIO + collector + query), seeds it with backend/tools/ui-seed, runs
// this suite, and tears the stack down. QUERY_URL points at a custom stack.

const baseURL = process.env.QUERY_URL ?? 'http://127.0.0.1:8080';
const headed = !!process.env.HEADED;

export default defineConfig({
  testDir: './e2e-query',
  timeout: headed ? 0 : 90_000,
  retries: 0,
  workers: 1,
  outputDir: 'test-results-query',
  reporter: [['list'], ['html', { outputFolder: 'playwright-report-query', open: 'never' }]],

  use: {
    baseURL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    headless: !headed,
    launchOptions: headed ? { slowMo: 300 } : {},
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
