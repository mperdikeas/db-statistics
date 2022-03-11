'use strict';

const path    = require('path');

const config = {
    mode: 'development',
    devtool: 'source-map',
    entry: './src/index.ts',
    output: {
        path: path.resolve(__dirname, 'dist'),
        filename: 'bundle.js'
    },
    resolve: {
        extensions: [ '.ts' ]
    },
    module: {
        rules: [
            {
                test: /\.ts$/,
                use: 'ts-loader',
                exclude: /node_modules/,
            },
            {
                test: /\.README$/, loader: 'null'
            }
        ]
    },
    optimization: {
        minimize: false // do not to minify the bundled code
    }
};

module.exports = config;
