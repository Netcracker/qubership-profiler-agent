import dotenv from 'dotenv';
import { EsbuildPlugin } from 'esbuild-loader';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import aliases from './webpack.aliases.js';
import plugins from './webpack.plugins.js';
import rules from './webpack.rules.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
/**
 * @type  {import('webpack').Configuration}
 */
export default {
    mode: 'production',
    target: ['browserslist'],
    bail: true,
    entry: ['./src/index.tsx'],
    module: {
        rules: rules(),
    },
    output: {
        path: path.resolve(__dirname, '../build'),
        pathinfo: false,
        filename: 'static/js/[name].[contenthash:8].js',
        chunkFilename: 'static/js/[name].[contenthash:8].chunk.js',
        assetModuleFilename: 'static/media/[name].[hash][ext]',
        publicPath: 'auto',
        clean: {
            keep: /manifest|env-config/,
        },
        asyncChunks: true,
    },
    plugins: plugins(),
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
        // sideEffects: true,
        // concatenateModules: true,
        // runtimeChunk: 'multiple',
        // splitChunks: {
        //     chunks: 'async',
        //     minSize: 20000,
        //     minRemainingSize: 0,
        //     minChunks: 1,
        //     maxAsyncRequests: 30,
        //     maxInitialRequests: 30,
        //     enforceSizeThreshold: 50000,
        //     cacheGroups: {
        //         vendor: {
        //             name: 'vendors',
        //             test: /[\\/]node_modules[\\/]/,
        //             reuseExistingChunk: true,
        //         },
        //     },
        // },
    },
};
