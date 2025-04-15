import { resolve } from 'path';

export const commonBuildOptions = {
    resolve: {
        alias: {
            'bootstrap-datepicker': resolve(__dirname, 'node_modules/bootstrap-datepicker'),
            'code-prettify': resolve(__dirname, 'node_modules/code-prettify'),
            'datepair.js': resolve(__dirname, 'node_modules/datepair.js'),
            'dygraphs': resolve(__dirname, 'node_modules/dygraphs'),
            'jquery': resolve(__dirname, 'node_modules/jquery'),
            'jquery-bbq': resolve(__dirname, 'node_modules/jquery-bbq'),
            'jquery-notify': resolve(__dirname, 'node_modules/jquery-notify'),
            'jquery-ui': resolve(__dirname, 'node_modules/jquery-ui/dist/jquery-ui.js'),
            'jquery-ui-themes': resolve(__dirname, 'node_modules/jquery-ui/dist/themes'),
            'jquery.cookie': resolve(__dirname, 'node_modules/jquery.cookie'),
            'jquery.event.drag': resolve(__dirname, 'node_modules/jquery.event.drag'),
            'jstz': resolve(__dirname, 'node_modules/jstz'),
            'moment': resolve(__dirname, 'node_modules/moment'),
            'moment-timezone': resolve(__dirname, 'node_modules/moment-timezone'),
            'sortablejs': resolve(__dirname, 'node_modules/sortablejs'),
            'timepicker': resolve(__dirname, 'node_modules/timepicker'),
            'url-search-params-polyfill': resolve(__dirname, 'node_modules/url-search-params-polyfill'),
            'vkbeautify': resolve(__dirname, 'node_modules/vkbeautify'),
        },
    },
    build: {
        // generate .vite/manifest.json in outDir
        manifest: true,
        emptyOutDir: true,
        sourcemap: true,
        optimizeDeps: {
            include: [
                'bootstrap-datepicker',
                'code-prettify',
                'datepair.js',
                'dygraphs',
                'jquery',
                'jquery-bbq',
                'jquery-notify',
                'jquery-ui',
                'jquery.cookie',
                'jquery.event.drag',
                'jstz',
                'moment',
                'moment-timezone',
                'sortablejs',
                'timepicker',
                'url-search-params-polyfill',
                'vkbeautify',
            ],
        },
    },
};
