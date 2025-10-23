import { type GetHeapDumpsResp } from '@app/store/cdt-openapi';
import { containersStub } from '@mock-server/data/containers';
import { servicesStub } from '@mock-server/data/services';

export function createHeapDumps() {
    const dumps: GetHeapDumpsResp = [];
    containersStub.forEach(container => {
        const namespace = container.namespace;
        container.services.forEach(service => {
            const serviceName = service.name;
            const pod = servicesStub.find(it => it.service === serviceName)?.pod;
            if (pod) {
                dumps.push({
                    namespace: namespace,
                    service: serviceName,
                    pod: pod,
                    dumpId: 'quarkus-3-vertx-685456586c-2h596_1743109309237-heap-1743152230000',
                    startTime: 1656583200000,
                    creationTime: 1656583200000,
                    bytes: 59768832,
                });
            }
        });
    });
    return dumps;
}
