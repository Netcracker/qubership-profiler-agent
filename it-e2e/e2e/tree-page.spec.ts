import { test, expect } from './fixtures.js';

test('tree page shows call tree for Main.doWork', async ({ page, context }) => {
  const jsErrors: string[] = [];

  await page.goto('/');

  // Wait for the duration filter buttons and click "all" to show all calls
  const allDurationButton = page.locator('label[for="dr_0"]');
  await expect(allDurationButton).toBeVisible({ timeout: 15_000 });
  await allDurationButton.click();

  // Give the page a moment to load data after clicking "all"
  await page.waitForTimeout(3000);

  // Wait for Main.doWork row to appear in the calls grid
  const mainRow = page.locator('.slick-row').filter({ hasText: 'Main.doWork' });
  await expect(mainRow.first()).toBeVisible({ timeout: 15_000 });

  // Click the duration link in Main.doWork's row to open tree page
  const durLink = mainRow.first().locator('div[aria-describedby$="dur"] a');

  const [treePage] = await Promise.all([
    context.waitForEvent('page'),
    durLink.click(),
  ]);

  // Register error listener on the new page
  treePage.on('pageerror', (error) => {
    jsErrors.push(error.message);
  });

  // Wait for tree page to load and render the call tree
  await treePage.waitForLoadState('load');
  const callTree = treePage.locator('#callTree');
  await expect(callTree).toContainText('Main.doWork', { timeout: 15_000 });

  // java.thread: main
  await expect(callTree).toContainText('java.thread: main');

  // Main.test(String) may be initially folded, so expand it before checking child params.
  const mainTestArg = callTree.getByText('main.test.arg: Hello, world!');
  if (!(await mainTestArg.isVisible())) {
    const mainTestText = callTree.getByText('Main.test(String)').first();
    await expect(mainTestText).toBeVisible({ timeout: 15_000 });

    const mainTestNode = mainTestText.locator('xpath=ancestor::div[1]');
    const mainTestFoldButton = mainTestNode.locator('span[aa="a"]').first();
    await expect(mainTestFoldButton).toBeVisible();

    const mainTestIcon = mainTestFoldButton.locator('.ui-icon').first();
    const iconClasses = (await mainTestIcon.getAttribute('class')) ?? '';
    if (!iconClasses.includes('ui-icon-minus')) {
      await mainTestFoldButton.click();
    }
  }

  await expect(callTree).toContainText('Main.test(String)');
  await expect(mainTestArg).toBeVisible({ timeout: 15_000 });

  // Verify no JavaScript errors occurred
  expect(jsErrors, `JavaScript errors on tree page: ${jsErrors.join('; ')}`).toHaveLength(0);
});
