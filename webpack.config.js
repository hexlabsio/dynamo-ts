const path = require('path');

module.exports = {
    entry: './src/index.ts',
    mode: 'none',
    devtool: "inline-source-map",
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
                exclude: /node_modules/,
            },
            {
                test: /\.json$/,
                use: 'json-loader',
                exclude: /node_modules/,
            },
        ],
    },
    target: 'node',
    resolve: {
        modules: ['node_modules'],
        extensions: [ '.ts', '.js'],
    },
    optimization: {
        minimize: true,
        usedExports: true,
        sideEffects: false
    },
    output: {
        filename: 'bundle.js',
        libraryTarget: "commonjs",
        path: path.resolve(__dirname, 'dist'),
    },
};
