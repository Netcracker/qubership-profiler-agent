import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from './app';
import { readRestorePayload } from './export/restore';
import { RestoredApp } from './export/restored-app';
import './index.css';

// `npm run dev:mock` boots MSW before the first render so no request races
// past the worker; a plain `npm run dev` proxies /api/v1 to a real query.
async function enableMocking(): Promise<void> {
  if (!import.meta.env.DEV || import.meta.env.VITE_ENABLE_MSW !== '1') return;
  // Whether the worker already controlled the page when it loaded. On the very
  // first visit it did not: worker.start() registers the worker and claims the
  // page mid-flight, so the first /api/v1 calls can slip past it to a backend
  // dev:mock never runs — surfacing in the console as an "Uncaught (in promise)
  // TypeError: Failed to fetch" from the worker's passthrough. Reloading once,
  // after the worker is in control, closes that window: every later load starts
  // controlled from the first byte, so the reload happens at most once.
  const controlledAtLoad = navigator.serviceWorker.controller !== null;
  const { worker } = await import('./mocks/browser');
  await worker.start({
    serviceWorker: { url: `${import.meta.env.BASE_URL}mockServiceWorker.js` },
  });
  if (!controlledAtLoad) {
    location.reload();
    // Never resolve, so the render below is skipped until the reload replaces
    // this document.
    await new Promise<never>(() => {});
  }
}

const container = document.getElementById('root');
if (container === null) throw new Error('index.html must provide #root');

// A self-contained export (10b) sets window.__PROFILER_RESTORE__; boot straight
// from its embedded tree, skipping the mocks and the router.
const restore = readRestorePayload();
if (restore !== null) {
  createRoot(container).render(
    <StrictMode>
      <RestoredApp payload={restore} />
    </StrictMode>,
  );
} else {
  void enableMocking().then(() => {
    createRoot(container).render(
      <StrictMode>
        <App />
      </StrictMode>,
    );
  });
}
