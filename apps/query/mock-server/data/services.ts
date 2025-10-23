import { type GetServicesResp } from '@app/store/cdt-openapi';
import { containersStub } from '@mock-server/data/containers';

export const servicesStub: GetServicesResp = [
    {
        namespace: containersStub.at(0)?.namespace,
        pod: 'some-pod-' + Math.random(),
        service: 'service-01',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(0)?.namespace,
        pod: 'service-02-' + Math.random(),
        service: 'service-02',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(0)?.namespace,
        pod: 'service-02-' + Math.random(),
        service: 'service-02',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(0)?.namespace,
        pod: 'service-03-' + Math.random(),
        service: 'service-03',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(0)?.namespace,
        pod: 'service-03-' + Math.random(),
        service: 'service-03',
        startTime: new Date().getTime(),
        tags: [],
    },
    // second namespace
    {
        namespace: containersStub.at(1)?.namespace,
        pod: 'some-pod-' + Math.random(),
        service: 'service-01',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(1)?.namespace,
        pod: 'service-30-' + Math.random(),
        service: 'service-30',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(1)?.namespace,
        pod: 'service-30-' + Math.random(),
        service: 'service-30',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(1)?.namespace,
        pod: 'service-30-' + Math.random(),
        service: 'service-30',
        startTime: new Date().getTime(),
        tags: [],
    },
    {
        namespace: containersStub.at(1)?.namespace,
        pod: 'service-30-' + Math.random(),
        service: 'service-30',
        startTime: new Date().getTime(),
        tags: [],
    },
];
