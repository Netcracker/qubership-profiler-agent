import collector from 'k6/x/cdt';

export const TestOpts = {
    data: `${__ENV.DATA_DIR || './data/'}`,

    host: `${__ENV.COLLECTOR_HOST || 'localhost'}`,
    log: `${__ENV.LOG_LEVEL || 'info'}`,
    timeout: {
        connect: '1s',
        session: '9999h'
    },
    duration: __ENV.DURATION || '5m',
    pods: __ENV.PODS || 10,

    prefix: {
        namespace: `${__ENV.EMULATOR_NAMESPACE || 'TestNamespace'}`,
        service: `${__ENV.EMULATOR_SERVICE_PREFIX || 'TestService'}`,
        podName: `${__ENV.EMULATOR_POD_PREFIX || 'k6-test-service-854987ddb8-waaa'}`
    }
};

export const suite = collector.prepareSuite(TestOpts);

