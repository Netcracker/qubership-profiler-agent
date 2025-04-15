import { resolve } from 'path';
import { defineConfig } from 'vite';
import { commonBuildOptions } from './vite.config.common.mjs';
import { viteSingleFile } from 'vite-plugin-singlefile';

export default defineConfig({
    ...commonBuildOptions,
    plugins: [viteSingleFile()],
    build: {
        manifest: false,
        outDir: 'build/dist/tree-singlepage',
        emptyOutDir: true,
        copyPublicDir: false,
        rollupOptions: {
            // This effectively means "embed CSS and JS into tree.html"
            // A page with all the resources combined enables "saving the results for offline browsing"
            output: {
                format: 'iife',
            },
            input: {
                tree: resolve(__dirname, 'tree.html'),
            }
        },
    },
});
