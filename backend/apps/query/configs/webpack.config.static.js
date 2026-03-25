import { fileURLToPath } from 'node:url';
import path from 'node:path';
import rules from './webpack.rules.js';
import plugins from './webpack.plugins.js';
import aliases from './webpack.aliases.js';
import { EsbuildPlugin } from 'esbuild-loader';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * @type  {import('webpack').Configuration}
 */
export default {
    mode: 'production',
    target: ['browserslist'],
    entry: ['./src/index-static.tsx'],
    bail: true,
    module: {
        rules: rules({isStaticBuild: true }),
    },
    output: {
        path: path.resolve(__dirname, '../build-static'),
        pathinfo: false,
        filename: 'static/js/[name].[contenthash:8].js',
        publicPath: './',
        clean: {
            keep: /manifest|env-config/,
        },
    },
    plugins: plugins({ isStaticBuild: true }),
    resolve: {
        extensions: ['.js', '.ts', '.jsx', '.tsx', '.css'],
        alias: {
            // Custom Aliases
            ...aliases,
        },
    },
    stats: 'errors-warnings',
    optimization: {
        minimize: true,
        mergeDuplicateChunks: true,
        minimizer: [
            new EsbuildPlugin({
                minify: true,
                treeShaking: true,
                css: true,
                target: 'esnext',
            }),
        ],
    },
};
