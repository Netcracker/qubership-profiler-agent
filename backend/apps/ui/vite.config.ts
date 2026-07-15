/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// The app is served by the query binary at /ui (07 §6), so the dev server
// mirrors that base. /api/v1 proxies to a running query; VITE_QUERY_URL
// points it at a remote one. `npm run dev:mock` intercepts the same calls
// with MSW instead, so no backend is needed.
export default defineConfig({
  base: '/ui/',
  plugins: [react()],
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
  },
});
