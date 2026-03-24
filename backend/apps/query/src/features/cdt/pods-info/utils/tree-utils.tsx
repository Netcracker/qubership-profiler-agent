import type { DumpsQueryStore } from '@app/features/cdt/pods-info/pods-table';
import type { ContainersInfoItem } from '@app/models/containers';
import type { PodModel } from '@app/models/pods';
import type { Container, GetServicesResp, ServiceDumpInfo } from '@app/store/cdt-openapi';

const getChildrenType = (container: ContainersInfoItem) => {
    switch (container.kind) {
        case 'ns':
            return 'srv';
        case 'pod':
            return undefined;
        case 'srv':
            return 'pod';
    }
};
type AccumulatedStats = Omit<PodModel, 'podName' | 'serviceName'> & {
    children?: AccumulatedStats[];
    name: string;
};

const createNode = (
    containersList: ContainersInfoItem[],
    container: ContainersInfoItem,
    podsInfo: PodModel[]
): AccumulatedStats => {
    const children = containersList.filter(child => child.pid === container.id);
    if (children.length === 0) {
        return {
            ...podsInfo.find(pod => pod.podName === container.name),
            name: container.name,
        } as AccumulatedStats;
    }
    const childrenStats = children
        .map(child => {
            const filterBy = child.kind === 'srv' ? 'serviceName' : 'podName';
            return { ...podsInfo.find(pod => pod[filterBy] === child.name), name: container.name } as PodModel;
        })
        .filter(it => it !== undefined);
    const accumulated = childrenStats.reduce(
        (acc, stat) => {
            if (acc) {
                acc.lastSampleMillis = Math.max(acc.lastSampleMillis ?? 0, stat.lastSampleMillis);
                acc.activeSinceMillis = Math.min(acc.activeSinceMillis ?? 0, stat.activeSinceMillis);
                acc.firstSampleMillis = Math.min(acc.firstSampleMillis ?? 0, stat.firstSampleMillis);
                acc.dataAtStart += stat.dataAtStart;
                acc.dataAtEnd += stat.dataAtEnd;
                acc.currentBitrate += stat.currentBitrate;
                acc.hasGC = acc.hasGC || stat.hasGC;
                acc.hasTD = acc.hasTD || stat.hasTD;
                acc.hasTops = acc.hasTops || stat.hasTops;
                acc.namespace = stat.namespace;
                acc.heapDumps ??= [];
                // acc.heapDumps.push(...(stat.heapDumps ?? []));
            }
            return acc;
        },
        {
            lastSampleMillis: 0,
            activeSinceMillis: Number.MAX_VALUE,
            firstSampleMillis: Number.MAX_VALUE,
            dataAtStart: 0,
            dataAtEnd: 0,
            currentBitrate: 0,
            hasGC: false,
            hasTops: false,
            hasTD: false,
            onlineNow: false,
            name: container.name,
        } as AccumulatedStats
    );

    return {
        ...accumulated,
        children: children.map(child => createNode(containersList, child, podsInfo)),
    };
};
export const createTableDataSource = (containersList: ContainersInfoItem[], podsInfo: PodModel[]) => {
    return containersList.reduce((acc, container) => {
        if (container.kind === 'ns') {
            acc.push(createNode(containersList, container, podsInfo));
        }
        return acc;
    }, [] as AccumulatedStats[]);
};

export const createPodsInfoTableDataSource = (
    apiResp: GetServicesResp,
    containers: Container[],
    dumps: DumpsQueryStore
) => {
    return containers.map(container => {
        const containerStats = {
            dataAvailableFrom: undefined,
            dataAvailableTo: undefined,
            startTime: undefined,
        };
        const services = container.services.map(service => {
            // const pods = apiResp
            //     .filter(it => it.service === service.name)
            //     .map(it => ({ ...it, type: 'pod', name: it.pod }));
            const response = dumps[`${container.namespace}-${service.name}`];
            const stats: Record<string, number | undefined> = {
                dataAvailableFrom: undefined,
                dataAvailableTo: undefined,
                startTime: undefined,
            };
            const dumpsResponse =
                response?.resp?.map(dump => {
                    accumulateStatsByDump(stats, dump);
                    return {
                        name: dump.pod,
                        ...dump,
                    };
                }) ?? [];
            accumulateStatsByDump(containerStats, stats);
            return {
                ...service,
                type: 'service',
                name: service.name,
                namespace: container.namespace,
                fetching: response?.fetching,
                children: dumpsResponse,
                ...stats,
            };
        });
        return {
            ...container,
            type: 'namespace',
            name: container.namespace,
            children: services,
            ...containerStats,
        };
    });
};
function accumulateStatsByDump(stats: Record<string, number | undefined>, dump: ServiceDumpInfo) {
    if (stats.dataAvailableFrom) {
        stats.dataAvailableFrom = Math.min(stats.dataAvailableFrom ?? 0, dump.dataAvailableFrom ?? 0);
    } else {
        stats.dataAvailableFrom = dump.dataAvailableFrom;
    }
    stats.dataAvailableTo = Math.max(stats.dataAvailableTo ?? 0, dump.dataAvailableTo ?? 0);
    if (stats.startTime) {
        stats.startTime = Math.min(stats.startTime ?? 0, dump.startTime ?? 0);
    } else {
        stats.startTime = dump.startTime;
    }
}
