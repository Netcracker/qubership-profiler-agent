import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from './app';
import './index.css';

// `npm run dev:mock` boots MSW before the first render so no request races
// past the worker; a plain `npm run dev` proxies /api/v1 to a real query.
async function enableMocking(): Promise<void> {
  if (!import.meta.env.DEV || import.meta.env.VITE_ENABLE_MSW !== '1') return;
  const { worker } = await import('./mocks/browser');
  await worker.start({
    serviceWorker: { url: `${import.meta.env.BASE_URL}mockServiceWorker.js` },
  });
}

const container = document.getElementById('root');
if (container === null) throw new Error('index.html must provide #root');

void enableMocking().then(() => {
  createRoot(container).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
});
