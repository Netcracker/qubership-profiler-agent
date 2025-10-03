import { resolve } from 'path';
import { defineConfig } from 'vite';
import { commonBuildOptions } from './vite.config.common.mjs';

export default defineConfig({
    ...commonBuildOptions,
    server: {
        port: 3000,
        open: '/tests/playground/index.html'
    },
    test: {
        environment: 'jsdom',
        setupFiles: ['./tests/setup.js']
    }
});
