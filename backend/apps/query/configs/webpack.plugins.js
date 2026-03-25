import ReactRefreshWebpackPlugin from '@pmmmwh/react-refresh-webpack-plugin';
import ForkTsCheckerWebpackPlugin from 'fork-ts-checker-webpack-plugin';
import HtmlWebpackPlugin from 'html-webpack-plugin';
import MiniCssExtractPlugin from 'mini-css-extract-plugin';
import HtmlInlineScriptPlugin from 'html-inline-script-webpack-plugin';
import webpack from 'webpack';
import { WebpackManifestPlugin } from 'webpack-manifest-plugin';
import { inDev } from './webpack.helpers.js';

/**
 * @param {boolean} [isStaticBuild]
 * @return {import('webpack').Plugin[]}
 */

export default function plugins({ isStaticBuild } = { isStaticBuild: false }) {
    return [
        new webpack.DefinePlugin({
            'process.env.MSW_ENABLED': JSON.stringify(process.env.MSW_ENABLED),
        }),
        new ForkTsCheckerWebpackPlugin(),
        inDev() && new webpack.HotModuleReplacementPlugin(),
        inDev() && new ReactRefreshWebpackPlugin(),
        new HtmlWebpackPlugin({
            template: isStaticBuild ? 'index-static.html' : 'index.html',
            favicon: isStaticBuild ? false : 'src/assets/favicon.ico',
            publicPath: '/',
            inject: isStaticBuild ? 'body' : true,
            ...(!inDev()
                ? {
                      minify: {
                          removeComments: true,
                          collapseWhitespace: true,
                          removeRedundantAttributes: true,
                          useShortDoctype: true,
                          removeEmptyAttributes: true,
                          removeStyleLinkTypeAttributes: true,
                          keepClosingSlash: true,
                          minifyJS: true,
                          minifyCSS: true,
                          minifyURLs: true,
                      },
                  }
                : {}),
        }),
        isStaticBuild && new HtmlInlineScriptPlugin(),
        !isStaticBuild &&
            new MiniCssExtractPlugin({
                filename: 'static/css/[name].[contenthash:8].css',
                chunkFilename: 'static/css/[name].[contenthash:8].chunk.css',
            }),
        new WebpackManifestPlugin({
            fileName: 'asset-manifest.json',
            publicPath: '/',
            generate: (seed, files, entrypoints) => {
                const manifestFiles = files.reduce((manifest, file) => {
                    manifest[file.name] = file.path;
                    return manifest;
                }, seed);
                const entrypointFiles = entrypoints.main.filter(fileName => !fileName.endsWith('.map'));

                return {
                    files: manifestFiles,
                    entrypoints: entrypointFiles,
                };
            },
        }),
        new webpack.IgnorePlugin({
            resourceRegExp: /^\.\/locale$/,
            contextRegExp: /moment$/,
        }),
    ].filter(Boolean);
}
