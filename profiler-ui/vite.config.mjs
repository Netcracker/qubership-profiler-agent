import { resolve } from 'path';
import { defineConfig } from 'vite';
import { commonBuildOptions } from './vite.config.common.mjs';

export default defineConfig({
    ...commonBuildOptions,
    server: {
      proxy: {
          // Proxy the API calls to the embedded Tomcat server, so
          // we can launch vite frontend and Tomcat-based backend for faster UI development
          '/js/': 'http://localhost:8180',
          '/api/': 'http://localhost:8180',
          '/tree/': 'http://localhost:8180',
      }
    },
    build: {
        outDir: 'build/dist/es6',
        rollupOptions: {
            input: {
                // overwrite default .html entry
                index: resolve(__dirname, 'index.html'),
                tree: resolve(__dirname, 'tree.html'),
            }
        },
    },
})
