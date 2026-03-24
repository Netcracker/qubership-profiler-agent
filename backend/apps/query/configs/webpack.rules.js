import { inDev } from './webpack.helpers.js';
import MiniCssExtractPlugin from 'mini-css-extract-plugin';
import getCSSModuleLocalIdent from 'react-dev-utils/getCSSModuleLocalIdent.js';

const sassRegex = /\.(scss|sass)$/;
const sassModuleRegex = /\.module\.(scss|sass)$/;

/**
 * @param {boolean} [isStaticBuild]
 * @return  {import('webpack').Configuration['module']['rules']}
 */
export default function rules({ isStaticBuild } = { isStaticBuild: false }) {
    if (isStaticBuild) {
        return [
            {
                // Ts/Js loader
                test: /\.(js|mjs|jsx|ts|tsx)$/,
                exclude: /(node_modules|\.webpack)/,
                use: {
                    loader: 'swc-loader',
                    options: {
                        jsc: {
                            transform: {
                                react: {
                                    development: inDev(),
                                    runtime: 'automatic',
                                },
                            },
                            parser: {
                                syntax: 'typescript',
                            },
                        },
                    },
                },
            },
            {
                // CSS Loader
                test: /\.css$/,
                use: [
                    {
                        loader: 'style-loader',
                        options: {
                            insert: 'head',
                            injectType: 'singletonStyleTag',
                        },
                    },
                    { loader: 'css-loader' },
                ],
            },
            {
                // SCSS (SASS) Loader
                test: sassRegex,
                exclude: sassModuleRegex,
                use: [
                    {
                        loader: 'style-loader',
                        options: {
                            insert: 'head',
                            injectType: 'singletonStyleTag',
                        },
                    },
                    { loader: 'css-loader' },
                    { loader: 'sass-loader' },
                ],
            },
            {
                test: sassModuleRegex,
                use: [
                    {
                        loader: 'style-loader',
                        options: {
                            insert: 'head',
                            injectType: 'singletonStyleTag',
                        },
                    },
                    {
                        loader: 'css-loader',
                        options: {
                            importLoaders: 1,
                            modules: {
                                mode: 'local',
                                getLocalIdent: getCSSModuleLocalIdent,
                            },
                        },
                    },
                    { loader: 'sass-loader' },
                ],
            },
            {
                // Less loader
                test: /\.less$/,
                use: [
                    {
                        loader: 'style-loader',
                        options: {
                            insert: 'head',
                            injectType: 'singletonStyleTag',
                        },
                    },
                    { loader: 'css-loader' },
                    { loader: 'less-loader' },
                ],
            },
            // Allows import svgs as resource outside of JS/TS
            {
                test: /\.svg$/i,
                type: 'asset/inline',
                issuer: { not: /\.[jt]sx?$/ },
            },
            {
                // SVGR loader
                test: /\.svg$/i,
                issuer: /\.[jt]sx?$/,
                // resourceQuery: { not: [/url/] }, // exclude react component if *.svg?url
                use: [
                    {
                        loader: '@svgr/webpack',
                        /**
                         * @type  {import('@svgr/webpack').Options}
                         */
                        options: {
                            exportType: 'named',
                            prettier: false,
                            svgo: false,
                            svgoConfig: {
                                plugins: [{ removeViewBox: false }],
                            },
                            titleProp: true,
                            ref: true,
                        },
                    },
                ],
            },
            {
                // Assets loader
                // More information here https://webpack.js.org/guides/asset-modules/
                test: /\.(gif|jpe?g|tiff|png|webp|bmp|eot|ttf|woff|woff2)$/i,
                type: 'asset/inline',
            },
        ];
    } else {
        return [
            {
                // Ts/Js loader
                test: /\.(js|mjs|jsx|ts|tsx)$/,
                exclude: /(node_modules|\.webpack)/,
                use: {
                    loader: 'swc-loader',
                    options: {
                        jsc: {
                            transform: {
                                react: {
                                    development: inDev(),
                                    runtime: 'automatic',
                                },
                            },
                            parser: {
                                syntax: 'typescript',
                            },
                        },
                    },
                },
            },
            {
                // CSS Loader
                test: /\.css$/,
                use: [{ loader: inDev() ? 'style-loader' : MiniCssExtractPlugin.loader }, { loader: 'css-loader' }],
            },
            {
                // SCSS (SASS) Loader
                test: sassRegex,
                exclude: sassModuleRegex,
                use: [
                    { loader: inDev() ? 'style-loader' : MiniCssExtractPlugin.loader },
                    { loader: 'css-loader' },
                    { loader: 'sass-loader' },
                ],
            },
            {
                test: sassModuleRegex,
                use: [
                    { loader: inDev() ? 'style-loader' : MiniCssExtractPlugin.loader },
                    {
                        loader: 'css-loader',
                        options: {
                            importLoaders: 1,
                            modules: {
                                mode: 'local',
                                getLocalIdent: getCSSModuleLocalIdent,
                            },
                        },
                    },
                    { loader: 'sass-loader' },
                ],
            },
            {
                // Less loader
                test: /\.less$/,
                use: [
                    { loader: inDev() ? 'style-loader' : MiniCssExtractPlugin.loader },
                    { loader: 'css-loader' },
                    { loader: 'less-loader' },
                ],
            },
            // Allows import svgs as resource outside of JS/TS
            {
                test: /\.svg$/i,
                type: 'asset/resource',
                issuer: { not: /\.[jt]sx?$/ },
            },
            {
                // SVGR loader
                test: /\.svg$/i,
                issuer: /\.[jt]sx?$/,
                // resourceQuery: { not: [/url/] }, // exclude react component if *.svg?url
                use: [
                    {
                        loader: '@svgr/webpack',
                        /**
                         * @type  {import('@svgr/webpack').Options}
                         */
                        options: {
                            exportType: 'named',
                            prettier: false,
                            svgo: false,
                            svgoConfig: {
                                plugins: [{ removeViewBox: false }],
                            },
                            titleProp: true,
                            ref: true,
                        },
                    },
                ],
            },
            {
                // Assets loader
                // More information here https://webpack.js.org/guides/asset-modules/
                test: /\.(gif|jpe?g|tiff|png|webp|bmp|eot|ttf|woff|woff2)$/i,
                type: 'asset',
                generator: {
                    filename: 'static/assets/[hash][ext][query]',
                },
            },
        ];
    }
}
