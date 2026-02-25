import { test as base, expect, type Page, type TestInfo } from '@playwright/test';

const sanitizeFileName = (value: string): string => value.replace(/[^a-zA-Z0-9._-]+/g, '-');

async function capturePageArtifacts(page: Page, prefix: string, testInfo: TestInfo) {
  const safePrefix = sanitizeFileName(prefix);

  try {
    await page.screenshot({
      path: testInfo.outputPath(`${safePrefix}.png`),
      fullPage: true,
      timeout: 5_000,
    });
  } catch (error) {
    await testInfo.attach(`${safePrefix}-screenshot-error.txt`, {
      body: String(error),
      contentType: 'text/plain',
    });
  }

  try {
    const html = await page.content();
    await testInfo.attach(`${safePrefix}.html`, {
      body: Buffer.from(html, 'utf8'),
      contentType: 'text/html',
    });
  } catch (error) {
    await testInfo.attach(`${safePrefix}-html-error.txt`, {
      body: String(error),
      contentType: 'text/plain',
    });
  }
}

export const test = base;
export { expect };

test.afterEach(async ({ context }, testInfo) => {
  if (testInfo.status === testInfo.expectedStatus) {
    return;
  }

  const pages = context.pages();
  await Promise.all(
    pages.map((page, index) => {
      const pageLabel = page.url() === 'about:blank' ? `page-${index + 1}` : new URL(page.url()).pathname || `page-${index + 1}`;
      return capturePageArtifacts(page, `${index + 1}-${pageLabel}`, testInfo);
    }),
  );
});
