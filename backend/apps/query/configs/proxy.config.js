/**
 *  Creates proxy configuration for webpack dev server
 * @param {Record<string, string>} env process.env
 * @returns
 */
export default function createProxy(env) {
    const { API_URL } = env;

    /**
     * @type {import('webpack-dev-server').Configuration['proxy']}
     */
    const proxy = {
        '/esc': {
            target: API_URL,
            changeOrigin: true,

            secure: false,
            logLevel: 'debug',
        },
        '/cdt': {
            target: API_URL,
            changeOrigin: true,
            pathRewrite: {
                '^/cdt': '/cdt',
            },
            secure: false,
        },
    };
    return proxy;
}
