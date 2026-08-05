/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// The base path is the single place that decides where the UI lives: '/' by
// default, or set UI_BASE_PATH=/ui/ (leading and trailing slash) to serve it
// under a sub-path. The router basename derives from it at runtime
// (import.meta.env.BASE_URL) and the query binary reads it back out of the
// built index.html, so this one switch moves the whole app. The dev server
// mirrors it; /api/v1 proxies to a running query (VITE_QUERY_URL points it at a
// remote one), or `npm run dev:mock` intercepts with MSW so no backend is needed.
const base = process.env['UI_BASE_PATH'] ?? '/';

export default defineConfig({
  base,
  plugins: [react()],
  build: {
    // antd is a deliberate single ~1 MB vendor chunk (~320 KB gzipped); the
    // default 500 KB warning would flag it on every build with advice (dynamic
    // import) that does not apply to a vendor split, so lift the threshold above
    // it. The app and react chunks stay well under.
    chunkSizeWarningLimit: 1100,
    rolldownOptions: {
      output: {
        // Split the heavy, rarely-changing vendor code out of the app bundle so
        // a UI code change re-downloads ~150 KB, not the full ~1.2 MB. antd (with
        // its rc-* internals) is by far the largest dependency, so it gets its own
        // chunk; react and the router share a second one. The self-contained HTML
        // export (src/export/build-export.ts) inlines the whole chunk graph, so
        // splitting stays transparent to it.
        codeSplitting: {
          groups: [
            { name: 'antd', test: /node_modules[/\\](antd|@ant-design|rc-[^/\\]+|@rc-component)[/\\]/ },
            { name: 'react', test: /node_modules[/\\](react|react-dom|react-router|scheduler)[/\\]/ },
          ],
        },
      },
    },
  },
  server: {
    proxy: {
      '/api/v1': {
        target: process.env['VITE_QUERY_URL'] ?? 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['src/setup-tests.ts'],
    coverage: {
      provider: 'v8',
      // lcov is what CI ships to Codecov; text-summary keeps a local run readable.
      reporter: ['text-summary', 'lcov'],
      reportsDirectory: 'coverage',
      include: ['src/**/*.{ts,tsx}'],
      // The MSW handlers and the vitest bootstrap are test scaffolding, not app
      // code — counting them would flatter the number without measuring anything.
      exclude: ['src/mocks/**', 'src/setup-tests.ts', '**/*.d.ts'],
    },
    // The tree-render integration tests decode a real wire and can take ~5s in
    // jsdom; their own findByText waits up to 5s, so the test budget needs
    // headroom above that or they flake under parallel load. V8 instrumentation
    // multiplies that again — tree-page.test.tsx runs ~12s alone under coverage
    // and ~36s when all 33 files compete for workers — and CI has fewer cores
    // than a dev machine, so the budget is sized for the instrumented run.
    testTimeout: 60000,
  },
});
