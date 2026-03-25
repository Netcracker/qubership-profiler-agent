/**
 * @type {import('@rtk-query/codegen-openapi').ConfigFile}
 */
const config = {
    schemaFile: './openapi.yaml',
    apiFile: './src/store/openapi-query.ts',
    apiImport: 'openApi',

    argSuffix: 'Arg',
    responseSuffix: 'Resp',
    outputFile: './src/store/cdt-openapi.ts',
    exportName: 'openApiEndpoints',
    hooks: true,
};

module.exports = config;
