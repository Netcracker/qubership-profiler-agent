import type { Config } from 'jest';

console.table({ node_env: process.env.NODE_ENV, ci: process.env.CI });

const config: Config = {
    roots: ['<rootDir>/src'],
    collectCoverageFrom: [
        'src/**/*{ts,tsx}',
        '!src/**/_generated_/**',
        '!**/node_modules/**',
        '!src/types/**',
        '!src/**/__fixtures__/**',
        '!src/types/**',
        '!dist/*',
        '!build/*',
    ],
    coveragePathIgnorePatterns: ['.*_generated_/.+(js)', '.*/components/icons/.+(tsx)'],
    setupFilesAfterEnv: ['<rootDir>/src/setupTests.ts'],
    setupFiles: ['react-app-polyfill/jsdom'],
    testMatch: ['<rootDir>/src/**/__tests__/**/*.{js,jsx,ts,tsx}', '<rootDir>/src/**/*.{spec,test}.{js,jsx,ts,tsx}'],
    reporters: ['default', 'jest-junit'],
    testEnvironment: 'jsdom',
    transform: {
        // .swcrc for some reason is not working
        '^.+\\.(js|jsx|mjs|cjs|ts|tsx)$': [
            '@swc/jest',
            {
                sourceMaps: true,
                jsc: {
                    parser: {
                        syntax: 'typescript',
                        tsx: true,
                    },
                    transform: {
                        react: {
                            runtime: 'automatic',
                        },
                    },
                },
            },
        ],
        '^.+\\.css$': '<rootDir>/configs/jest/css-transform.js',
        '^(?!.*\\.(js|jsx|mjs|cjs|ts|tsx|css|json)$)': '<rootDir>/configs/jest/file-transform.js',
    },
    transformIgnorePatterns: ['^.+\\.module\\.(css|sass|scss|less)$'],
    modulePaths: [],
    moduleNameMapper: {
        '@app/(.*)': '<rootDir>/src/$1',
        '@assets/(.*)': '<rootDir>/src/assets/$1',
        '@mock-server/(.*)': '<rootDir>/mock-server/$1',
        '^.+\\.module\\.(css|sass|scss|less)$': 'identity-obj-proxy',
    },
    moduleFileExtensions: ['web.js', 'js', 'web.ts', 'ts', 'web.tsx', 'tsx', 'json', 'web.jsx', 'jsx', 'node'],
    watchPlugins: ['jest-watch-typeahead/filename', 'jest-watch-typeahead/testname'],
    resetMocks: true,
};

export default config;
