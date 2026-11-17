import { test, expect } from './fixtures.js';

for (const path of [
  '/com/netcracker/profiler/WARLauncher.class',
  '/org/apache/catalina/startup/Tomcat.class',
  '/WEB-INF/web.xml',
  '/WEB-INF/embedded-webapp/index.html',
]) {
  test(`embedded viewer keeps ${path} private`, async ({ request }) => {
    const response = await request.get(path);
    expect(response.status()).toBe(404);
  });
}
