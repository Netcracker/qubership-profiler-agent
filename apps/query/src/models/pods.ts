export type PodModel = {
    podName: string;
    activeSinceMillis: number;
    firstSampleMillis: number;
    lastSampleMillis: number;
    dataAtStart: number;
    dataAtEnd: number;
    currentBitrate: number;
    serviceName: string;
    namespace: string;
    hasGC: boolean;
    hasTops: boolean;
    hasTD: boolean;
    onlineNow: boolean;
    heapDumps?: any[];
};
