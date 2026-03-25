import path from 'node:path';
import process from 'node:process';

const cwd = process.cwd();

export function inDev() {
    return process.env.NODE_ENV == 'development';
}

export function createWebpackAliases(aliases) {
    const result = {};
    for (const name in aliases) {
        result[name] = path.join(cwd, aliases[name]);
    }
    return result;
}
