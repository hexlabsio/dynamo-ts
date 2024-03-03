import * as url from 'url';
const __dirname = url.fileURLToPath(new URL('.', import.meta.url));

process.env['JEST_DYNAMODB_CONFIG'] = `${__dirname}/dynamodb-tables.cjs`


/** @type {import('jest').Config} */
const config = {
    preset: "ts-jest",
    testEnvironment: "node",
    globalSetup: "./node_modules/@shelf/jest-dynamodb/lib/setup.js",
    globalTeardown: "./node_modules/@shelf/jest-dynamodb/lib/teardown.js",
    moduleNameMapper: {
        '^(\\.{1,2}/.*)\\.js$': '$1',
    },
    transform: {
        "\.spec\.ts$": ['ts-jest', { isolatedModules: true }],
    }
}

export default config;
