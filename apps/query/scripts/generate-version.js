import path from 'path';
import { readFileSync } from 'fs';
import { execSync } from 'child_process';
import  moment from 'moment';
const cwd = process.cwd();

const packageJsonPath = path.join(cwd, 'package.json');
const packageConfigBuffer = readFileSync(packageJsonPath);
const packageConfig = JSON.parse(packageConfigBuffer)

const currentVersion = packageConfig.version;
if (!currentVersion) {
    throw 'package.version is empty';
}

//const branchName = execSync('git rev-parse --abbrev-ref HEAD', {
//    encoding: 'utf-8'
//}).trim();
const branchName = process.env.CI_COMMIT_REF_NAME

const separator = '-';
const escapedBranchName = branchName.replace(/[!@#$%^&*()_+=/]/g, separator);

const timestamp = moment().format('HH-mm-DD-MM-YYYY');
const newVersion = `${currentVersion}${separator}${escapedBranchName}${separator}${timestamp}`;

console.log(newVersion)
