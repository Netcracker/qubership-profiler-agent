import { type GetNamespacesResp } from '@app/store/cdt-openapi';

export const containersStub: GetNamespacesResp = [
    {
        namespace: 'dev-01',
        services: [
            {
                name: 'service-01',
                activePods: 1,
                lastAck: 3,
            },
            {
                name: 'service-02',
                activePods: 1,
                lastAck: 3,
            },
            {
                name: 'service-03',
                activePods: 1,
                lastAck: 3,
            },
        ],
    },
    {
        namespace: 'dev-03',
        services: [
            {
                name: 'service-01',
                activePods: 1,
                lastAck: 3,
            },
            {
                name: 'service-30',
                activePods: 1,
                lastAck: 3,
            },
            {
                name: 'service-20',
                activePods: 1,
                lastAck: 3,
            },
        ],
    },
];
