import type { CallsTreeData } from '@app/store/cdt-openapi';

export const callsTreeStub: CallsTreeData = {
    info: [
        {
            id: 'network',
            values: [1.324],
            type: 'Date',
            isList: false,
            isIndex: false,
        },
        {
            id: 'database',
            values: [418.654],
            type: 'Date',
            isList: false,
            isIndex: false,
        },
        {
            id: 'cpu',
            values: [327],
            type: 'Date',
            isList: false,
            isIndex: false,
        },
    ],
    children: [
        {
            id: '0',
            info: {
                title: 'JobRunShell.run()',
                hasStackTrace: false,
                sourceJar: 'test.jar',
                lineNumber: 123,
                calls: 75,
            },
            time: {
                self: 0,
                total: 5343,
            },
            timePercent: 79.91,
            duration: {
                self: 90,
                total: 120,
            },
            suspension: {
                self: 65,
                total: 132.1,
            },
            invocations: {
                self: 24,
                total: 26,
            },
            avg: {
                self: 2.2,
                total: 4.7,
            },
            params: [
                {
                    id: 'common.started',
                    values: ['Sat Mar 09 2024 13:40:50.013 GMT+0300 (Moscow Standard Time) (1709980850013)'],
                    type: 'String',
                    isList: false,
                    isIndex: false,
                },
            ],
            children: [
                {
                    id: '1',
                    info: {
                        title: 'com.netcracker.platform.scheduler.impl.jobs.AbstractJobImpl.execute(JobExecutionContext)',
                        hasStackTrace: true,
                        sourceJar: 'test.jar',
                        lineNumber: 3,
                        calls: 74,
                    },
                    time: {
                        self: 0,
                        total: 5343,
                    },
                    timePercent: 79.91,
                    duration: {
                        self: 65.5,
                        total: 94.8,
                    },
                    suspension: {
                        self: 62.1,
                        total: 125.7,
                    },
                    invocations: {
                        self: 4,
                        total: 12,
                    },
                    avg: {
                        self: 4.7,
                        total: 12.9,
                    },
                    params: [
                        {
                            id: 'node.name',
                            type: 'String',
                            isList: false,
                            isIndex: false,
                            values: ['clust1', 'clust2'],
                        },
                        {
                            id: 'java.thread',
                            type: 'String',
                            isList: false,
                            isIndex: false,
                            values: ['QuartzScheduler_Worker-2', 'QuartzScheduler_Worker-6'],
                        },
                    ],
                    children: [
                        {
                            id: '2',
                            info: {
                                title: 'SecurityProcessor.executeAsSystem(PrivilegedExceptionAction) : Object',
                                hasStackTrace: true,
                                sourceJar: 'test.jar',
                                lineNumber: 39,
                                calls: 73,
                            },
                            time: {
                                self: 0,
                                total: 5343,
                            },
                            timePercent: 79.91,
                            duration: {
                                self: 5.5,
                                total: 14.8,
                            },
                            suspension: {
                                self: 22.1,
                                total: 25.9,
                            },
                            invocations: {
                                self: 6,
                                total: 11,
                            },
                            avg: {
                                self: 45.7,
                                total: 125.9,
                            },
                            children: [
                                {
                                    id: '3',
                                    info: {
                                        title: 'com.netcracker.platform.scheduler.impl.jobs.AbstractJobImpl.executeAsSystem(JobExecutionContext)',
                                        hasStackTrace: true,
                                        sourceJar: 'test.jar',
                                        lineNumber: 43,
                                        calls: 66,
                                    },
                                    time: {
                                        self: 342,
                                        total: 4915,
                                    },
                                    timePercent: 73.51,
                                    duration: {
                                        self: 15.5,
                                        total: 34.7,
                                    },
                                    suspension: {
                                        self: 12.1,
                                        total: 25,
                                    },
                                    invocations: {
                                        self: 2,
                                        total: 3,
                                    },
                                    avg: {
                                        self: 5.7,
                                        total: 15.9,
                                    },
                                    children: [
                                        {
                                            id: '4',
                                            info: {
                                                title: 'SchedulerLockManager.blockJob(BigInteger) : boolean',
                                                hasStackTrace: true,
                                                sourceJar: 'test.jar',
                                                lineNumber: 43,
                                                calls: 33,
                                            },
                                            time: {
                                                self: 2,
                                                total: 4573,
                                            },
                                            timePercent: 68.4,
                                            duration: {
                                                self: 16.6,
                                                total: 34.8,
                                            },
                                            suspension: {
                                                self: 12.1,
                                                total: 25,
                                            },
                                            invocations: {
                                                self: 2,
                                                total: 3,
                                            },
                                            avg: {
                                                self: 5.7,
                                                total: 15.9,
                                            },
                                        },
                                        {
                                            id: '5',
                                            info: {
                                                title: 'SecurityProcessor.doAs(PrivilegedExceptionAction, String) : Object',
                                                hasStackTrace: true,
                                                sourceJar: 'test.jar',
                                                lineNumber: 43,
                                                calls: 33,
                                            },
                                            time: {
                                                self: 2,
                                                total: 342,
                                            },
                                            timePercent: 5.1,
                                            duration: {
                                                self: 16.6,
                                                total: 34.8,
                                            },
                                            suspension: {
                                                self: 12.1,
                                                total: 25,
                                            },
                                            invocations: {
                                                self: 2,
                                                total: 3,
                                            },
                                            avg: {
                                                self: 5.7,
                                                total: 15.9,
                                            },
                                        },
                                    ],
                                },
                                {
                                    id: '6',
                                    info: {
                                        title: 'StreamDumper.streamOpenedAndPrevStreamClosed',
                                        hasStackTrace: true,
                                        sourceJar: 'test.jar',
                                        lineNumber: 43,
                                        calls: 3,
                                    },
                                    time: {
                                        self: 0,
                                        total: 398,
                                    },
                                    timePercent: 6,
                                    duration: {
                                        self: 14.2,
                                        total: 4.7,
                                    },
                                    suspension: {
                                        self: 12.1,
                                        total: 25,
                                    },
                                    invocations: {
                                        self: 2,
                                        total: 3,
                                    },
                                    avg: {
                                        self: 5.7,
                                        total: 15.9,
                                    },
                                    children: [
                                        {
                                            id: '7',
                                            info: {
                                                title: 'ElasticSearchQueryUtils.searchAndConvert',
                                                hasStackTrace: false,
                                                sourceJar: 'test.jar',
                                                lineNumber: 43,
                                                calls: 1,
                                            },
                                            time: {
                                                self: 398,
                                                total: 398,
                                            },
                                            timePercent: 6,
                                            duration: {
                                                self: 5.5,
                                                total: 14.7,
                                            },
                                            suspension: {
                                                self: 12.1,
                                                total: 25,
                                            },
                                            invocations: {
                                                self: 2,
                                                total: 3,
                                            },
                                            avg: {
                                                self: 5.7,
                                                total: 15.9,
                                            },
                                        },
                                    ],
                                },
                                {
                                    id: '8',
                                    info: {
                                        title: 'StreamFacadeElasticsearch.getPODDetails',
                                        hasStackTrace: false,
                                        sourceJar: 'test.jar',
                                        lineNumber: 43,
                                        calls: 1,
                                    },
                                    time: {
                                        self: 0,
                                        total: 30,
                                    },
                                    timePercent: 0.44,
                                    duration: {
                                        self: 62.5,
                                        total: 21.2,
                                    },
                                    suspension: {
                                        self: 12.1,
                                        total: 25,
                                    },
                                    invocations: {
                                        self: 2,
                                        total: 3,
                                    },
                                    avg: {
                                        self: 5.7,
                                        total: 15.9,
                                    },
                                },
                            ],
                        },
                    ],
                },
                {
                    id: '9',
                    info: {
                        title: 'org.quartz.impl.jdbcjobstore.JobStoreSupport.executeInNonManagedTXLock(String, JobStoreSupport$TransactionCallback)',
                        hasStackTrace: false,
                        sourceJar: 'test.jar',
                        lineNumber: 43,
                        calls: 74,
                    },
                    time: {
                        self: 0,
                        total: 0.1,
                    },
                    timePercent: 0,
                    duration: {
                        self: 1.5,
                        total: 4.7,
                    },
                    suspension: {
                        self: 12.1,
                        total: 25,
                    },
                    invocations: {
                        self: 2,
                        total: 3,
                    },
                    avg: {
                        self: 5.7,
                        total: 15.9,
                    },
                },
            ],
        },
        {
            id: '10',
            info: {
                title: 'AbstractJobImpl.execute(JobExecutionContext)',
                hasStackTrace: false,
                sourceJar: 'test.jar',
                lineNumber: 123,
                calls: 75,
            },
            time: {
                self: 0,
                total: 1343,
            },
            timePercent: 20.1,
            duration: {
                self: 90,
                total: 120,
            },
            suspension: {
                self: 65,
                total: 132.1,
            },
            invocations: {
                self: 24,
                total: 26,
            },
            avg: {
                self: 2.2,
                total: 4.7,
            },
            params: [
                {
                    id: 'common.started',
                    values: ['Sat Mar 09 2024 13:40:50.013 GMT+0300 (Moscow Standard Time) (1709980850013)'],
                    type: 'String',
                    isList: false,
                    isIndex: false,
                },
            ],
        },
    ],
};
