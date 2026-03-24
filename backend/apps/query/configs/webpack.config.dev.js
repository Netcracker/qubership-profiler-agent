import dotenv from 'dotenv';
import createProxy from './proxy.config.js';
import aliases from './webpack.aliases.js';
import plugins from './webpack.plugins.js';
import rules from './webpack.rules.js';

dotenv.config();

console.table({ proxy: process.env.API_URL, MSW_ENABLED: process.env.MSW_ENABLED });

/**
 * @type  {import('webpack').WebpackOptionsNormalized}
 */
export default {
    target: ['browserslist'],
    mode: 'development',
    entry: ['./src/index.tsx'],
    module: {
        rules: rules(),
    },
    output: {
        filename: '[name].js',
        pathinfo: true,
        asyncChunks: true,
        chunkFilename: '[name].chunk.js',
    },
    plugins: plugins(),
    resolve: {
        extensions: ['.js', '.ts', '.jsx', '.tsx', '.css'],
        alias: aliases,
    },
    devtool: 'cheap-module-source-map',
    /**
     * @type {import('webpack-dev-server').Configuration}
     */
    devServer: {
        open: process.env.OPEN_BROWSER ? process.env.OPEN_BROWSER === 'true' : true,
        port: parseInt(process.env.PORT) || 3030,
        compress: true,
        historyApiFallback: {
            disableDotRule: true,
            index: '/',
        },
        proxy: createProxy(process.env),
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': '*',
        },
        static: {
            directory: './public',
        },
        client: {
            logging: 'error',
            overlay: {
                errors: true,
                warnings: false,
                runtimeErrors: false,
            },
        },
    },
    infrastructureLogging: {
        level: 'none',
    },
    stats: {
        preset: 'errors-warnings',
    },
    optimization: {
        splitChunks: {
            chunks: 'all',
        },
    },
    performance: {
        hints: false,
    },
};
