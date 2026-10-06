import { test, expect } from './fixtures.js';

/**
 * Iterations the test app runs, passed as its last container argument in `it-e2e/build.gradle.kts`. Keep both in sync.
 */
const WORKLOAD_ITERATIONS = 10;

/**
 * Calls the workload always produces: `Main.doWork` plus one `Main.test` per iteration. `Main.test` is guaranteed
 * because `it-e2e/config/_config.xml` attaches a `main.test.arg` parameter to it, and logging a parameter flushes the
 * pending entry into the trace however short the call was.
 */
const WORKLOAD_CALLS = 1 + WORKLOAD_ITERATIONS;

/**
 * Headroom for JDK calls instrumented by `installer/src/main/resources/config/minimal/50main/java.xml`. The agent
 * logs a nested call only when it spans a millisecond (`ProfilerData.MINIMAL_LOGGED_DURATION`), so how many of them
 * reach the trace varies between runs. `Main.doWork` reaches two: the `java.io.FileOutputStream.write` behind each of
 * its two `System.out.println` calls. The third slot is headroom for a JDK that routes stdout differently.
 */
const INCIDENTAL_CALL_ALLOWANCE = 3;

test('profiler UI shows test-app method calls', async ({ page }) => {
  await page.goto('/');

  // Wait for the duration filter buttons and click "all" to remove the
  // default >=500ms filter — the test-app's calls may be shorter
  const allDurationButton = page.locator('label[for="dr_0"]');
  await expect(allDurationButton).toBeVisible({ timeout: 15_000 });
  await allDurationButton.click();

  // Wait for the Main.doWork row to appear in the calls grid
  const doWorkRow = page.locator('.slick-row').filter({ hasText: 'Main.doWork' });
  await expect(doWorkRow.first()).toBeVisible({ timeout: 15_000 });

  // Helper to get cell text by column id within the doWork row
  const cell = (columnId: string) =>
    doWorkRow.first().locator(`div[aria-describedby$="${columnId}"]`);

  // Duration should be more than 5 seconds
  const durationText = await cell('dur').textContent();
  const durationMs = parseDuration(durationText!);
  expect(durationMs, `Expected duration > 5000ms, got ${durationText}`).toBeGreaterThan(5_000);

  // The Calls column counts every instrumented entry inside the call tree, application code and JDK alike, so the
  // count is a range: the workload calls are guaranteed, the incidental JDK ones depend on how long they ran.
  const callsText = await cell('calls').textContent();
  const calls = parseInt(callsText!.trim(), 10);
  const callsMessage =
    `Expected ${WORKLOAD_CALLS}..${WORKLOAD_CALLS + INCIDENTAL_CALL_ALLOWANCE} calls ` +
    `(Main.doWork + ${WORKLOAD_ITERATIONS} Main.test + incidental JDK calls), got ${callsText}`;
  expect(calls, callsMessage).toBeGreaterThanOrEqual(WORKLOAD_CALLS);
  expect(calls, callsMessage).toBeLessThanOrEqual(WORKLOAD_CALLS + INCIDENTAL_CALL_ALLOWANCE);

  // JDK-level instrumentation gets its own assertion rather than riding on the call count. The byte counter runs as
  // an `execute-before` hook, so both `System.out.println` payloads are counted even when neither write is logged as
  // a call of its own.
  const fileIoText = await cell('fileio').textContent();
  const fileIoBytes = parseBytes(fileIoText!);
  expect(fileIoBytes, `Expected non-zero Disk IO for Main.doWork, got ${fileIoText}`).toBeGreaterThan(0);

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

/** Parse an IO cell like "72 B", "3 KB" or "5 MiB" into bytes. */
function parseBytes(text: string): number {
  const match = text.trim().match(/^(\d+)\s*([KMG]?)i?B$/);
  if (!match) throw new Error(`Cannot parse byte count: ${text}`);
  const scale: Record<string, number> = { '': 1, K: 1024, M: 1024 ** 2, G: 1024 ** 3 };
  return parseInt(match[1], 10) * scale[match[2]];
}
