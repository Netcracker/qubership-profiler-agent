import { expect, test } from '@playwright/test';

// The embedded UI end to end (07-ui-design.md §7): discovery over /pods,
// the calls list over /calls, and a drill into the MessagePack /tree — all
// against data the ui-seed tool pushed through the real agent protocol
// (namespace e2e, service shop, pods shop-1/shop-2, 30 calls each).

test('discovery, calls filtering, and a drill into the call tree', async ({ page, context }) => {
  await page.goto('/ui/calls');

  // Pick a quick range; the rail discovers services for the draft window
  // before Apply.
  await page.getByText('15 min', { exact: true }).click();
  await expect(page.getByText('shop ·')).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText('e2e', { exact: true })).toBeVisible();

  // Select the service (tri-state checkbox) and apply.
  await page
    .locator('.ant-tree-treenode', { hasText: 'shop ·' })
    .locator('.ant-tree-checkbox')
    .first()
    .click();
  await page.getByRole('button', { name: 'Apply' }).click();

  // The default >500ms chip hides the sub-500ms half of the seeded mix
  // (the assertions stay count-free: reseeding an already-running stack
  // adds another batch inside the same window).
  await expect(page.getByText(/com\.example\.shop\.Api\.handle/).first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(/\d+ loaded/)).toBeVisible();
  await expect(page.getByText('90ms', { exact: true })).toHaveCount(0);

  // Widening to All reveals the fast calls too. 90ms sits near the top of
  // the seeded cycle, inside the virtual table's rendered window.
  await page.getByText('All', { exact: true }).click();
  await expect(page.getByText('90ms', { exact: true }).first()).toBeVisible({ timeout: 30_000 });

  // The duration link opens the tree in a new tab, carrying the cold hints.
  const [treePage] = await Promise.all([
    context.waitForEvent('page'),
    page.locator('a[href*="/ui/tree/"]').first().click(),
  ]);
  await treePage.waitForLoadState();
  expect(treePage.url()).toContain('ts_ms=');
  expect(treePage.url()).toContain('retention_class=');

  // The merged tree renders: breadcrumb, the root method row, and the
  // aggregated params of the root.
  await expect(treePage.getByText(/e2e \/ shop \/ shop-/)).toBeVisible({ timeout: 30_000 });
  await expect(treePage.getByText(/Api\.handle/).first()).toBeVisible();
  await expect(treePage.getByText('request.id').first()).toBeVisible();

  // Hotspots groups by self time; the DB leaf dominates the seeded shape.
  // (Scoped to the active pane: AntD keeps inactive tab content in the DOM.)
  await treePage.getByRole('tab', { name: 'Hotspots' }).click();
  await expect(
    treePage.getByRole('tabpanel', { name: 'Hotspots' }).getByText(/OrderDao\.query/).first(),
  ).toBeVisible();
});
