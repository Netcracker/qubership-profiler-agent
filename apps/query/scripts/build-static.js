import { fileURLToPath } from 'node:url';
import path from 'node:path';
import staticConfig from '../configs/webpack.config.static.js';
import chalk from 'chalk';
import webpack from 'webpack';
import { measureFileSizesBeforeBuild, printFileSizesAfterBuild } from 'react-dev-utils/FileSizeReporter.js';
import fs from 'fs-extra';
import formatWebpackMessages from 'react-dev-utils/formatWebpackMessages.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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

console.time('static-build');

(async () => {
    const buildPath = staticConfig.output.path;
    console.log(chalk.gray('Output path is', buildPath));
    // TODO: report?

    const webpackCompiler = webpack(staticConfig);
    const prevSizes = await measureFileSizesBeforeBuild(buildPath);

    copyPublicFolder(buildPath);
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
    console.timeEnd('static-build');
})();