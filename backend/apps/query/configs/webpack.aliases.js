import { createWebpackAliases } from './webpack.helpers.js';
/**
 * Export Webpack Aliases
 *
 * Tip: Some text editors will show the errors or invalid intellisense reports
 * based on these webpack aliases, make sure to update `tsconfig.json` file also
 * to match the `paths` we using in here for aliases in project.
 */
export default createWebpackAliases({
    '@assets': 'src/assets',
    '@mock-server': 'mock-server',
    '@api': 'src/api',
    '@app': 'src',
});
