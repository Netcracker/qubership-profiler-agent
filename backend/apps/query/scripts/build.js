import StatoscopeWebpackPlugin from '@statoscope/webpack-plugin';
import chalk from 'chalk';
import fs from 'fs-extra';
import glob from 'glob';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import { measureFileSizesBeforeBuild, printFileSizesAfterBuild } from 'react-dev-utils/FileSizeReporter.js';
import formatWebpackMessages from 'react-dev-utils/formatWebpackMessages.js';
import webpack from 'webpack';
import prodConfig from '../configs/webpack.config.prod.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

process.env.NODE_ENV = 'production';
process.env.BABEL_ENV = 'production';

const isReportMode = process.argv.includes('--report');

const publicFolder = path.resolve(__dirname, '../public');

function copyPublicFolder(buildFolder) {
    fs.copySync(publicFolder, buildFolder, {
        dereference: true,
        filter: file => file.includes('mockServiceWorker'),
    });
    console.log(chalk.gray('Copied content of public folder to', buildFolder));
}

const WARN_AFTER_BUNDLE_GZIP_SIZE = 512 * 1024;
const WARN_AFTER_CHUNK_GZIP_SIZE = 1024 * 1024;
console.time('build');
(async () => {
    console.log(chalk.gray('Output path is', prodConfig.output.path));
    if (isReportMode) {
        console.log(chalk.cyanBright('Report mode is on'));
        prodConfig.stats = true;
        prodConfig.plugins.push(
            new StatoscopeWebpackPlugin.default({
                saveReportTo: 'bundle-stats/report-[hash].html',
                saveStatsTo: 'bundle-stats/stats-[hash].json',
                compressor: 'gzip',
                additionalStats: glob.sync('bundle-stats/stats-*.json'),
            })
        );
    }

    const webpackCompiler = webpack(prodConfig);

    const buildPath = prodConfig.output.path;
    const prevSizes = await measureFileSizesBeforeBuild(buildPath);

    copyPublicFolder(prodConfig.output.path);
    console.log(chalk.cyan('Creating optimized production build...'));

    /**
     * @type {{ stats: import('webpack').Stats, err: unknown }}
     */
    const { stats, err } = await new Promise((resolve, reject) => {
        webpackCompiler.run((err, stats) => {
            resolve({ err, stats });
        });
    });

    let messages;
    if (err) {
        if (!err.message) throw err;
        messages = formatWebpackMessages({
            errors: [err.message],
            warnings: [],
        });
    } else {
        messages = formatWebpackMessages(stats.toJson({ all: false, warnings: true, errors: true }));
    }
    if (messages.errors.length) {
        throw new Error(messages.errors.join('\n\n'));
    }

    // Will print only in case of something changed
    printFileSizesAfterBuild(stats, prevSizes, buildPath, WARN_AFTER_BUNDLE_GZIP_SIZE, WARN_AFTER_CHUNK_GZIP_SIZE);

    console.log(chalk.green('✔ ') + 'Build complete.');
    console.timeEnd('build');
})();
