import { test, expect } from '@playwright/test';

test('profiler UI shows test-app method calls', async ({ page }) => {
  await page.goto('/');

  // Wait for the duration filter buttons and click "all" to remove the
  // default >=500ms filter — the test-app's calls may be shorter
  const allDurationButton = page.locator('label[for="dr_0"]');
  await expect(allDurationButton).toBeVisible({ timeout: 15_000 });
  await allDurationButton.click();

  // Wait for the calls grid to populate
  const firstRow = page.locator('.slick-row').first();
  await expect(firstRow).toBeVisible({ timeout: 15_000 });

  // Helper to get cell text by column id within the first row
  const cell = (columnId: string) =>
    firstRow.locator(`div[aria-describedby$="${columnId}"]`);

  // Title column should contain Main.main
  await expect(cell('title')).toContainText('Main.main');

  // Duration should be more than 5 seconds
  const durationText = await cell('dur').textContent();
  const durationMs = parseDuration(durationText!);
  expect(durationMs, `Expected duration > 5000ms, got ${durationText}`).toBeGreaterThan(5_000);

  // Calls should be more than 5
  const callsText = await cell('calls').textContent();
  const calls = parseInt(callsText!.trim(), 10);
  expect(calls, `Expected calls > 5, got ${callsText}`).toBeGreaterThan(5);

  // Transactions should be 0
  await expect(cell('txs')).toHaveText('0');
});

/** Parse duration text like "10010ms", "1.5s", "2min 30s" into milliseconds. */
function parseDuration(text: string): number {
  // The UI formats durations as e.g. "10010ms", "1500ms", "2s", etc.
  const ms = text.match(/(\d+)ms/);
  if (ms) return parseInt(ms[1], 10);
  const sec = text.match(/([\d.]+)s/);
  if (sec) return parseFloat(sec[1]) * 1000;
  throw new Error(`Cannot parse duration: ${text}`);
}
