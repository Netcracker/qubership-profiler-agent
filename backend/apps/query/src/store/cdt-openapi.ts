import { openApi as api } from './openapi-query';
import type { DashboardEntity } from './slices/calls-tree-context-slices';
import { downloadFile } from '@netcracker/ux-react';

const serializeCallsLoadRequest = (request: CallsLoadRequest): string => {
    const { filters, parameters, view } = request;
    return `(${JSON.stringify({
        filters,
        parameters,
        view: { sort: view?.sortColumn, order: view?.sortOrder },
    })})`;
};

export function getDownloadDumpUrl({ dumpId, dumpType, podId }: DownloadDumpByIdArg) {
    // return `/cdt/v2/dumps/${podId}/${dumpType}/${dumpId}`;
    // return `/esc/downloadHeapDump?handle=${dumpId}`;
    return `/cdt/v2/heaps/download/${dumpId}`;
}

export const cdtOpenapi = api.injectEndpoints({
    endpoints: build => ({
        getNamespaces: build.query<GetNamespacesResp, GetNamespacesArg>({
            query: () => ({ url: `/cdt/v2/containers` }),
        }),
        getNamespacesByRange: build.query<GetNamespacesByRangeResp, GetNamespacesByRangeArg>({
            query: queryArg => ({
                url: `/cdt/v2/containers`,
                method: 'POST',
                params: {
                    timeFrom: queryArg.timeFrom,
                    timeTo: queryArg.timeTo,
                    limit: queryArg.limit,
                    page: queryArg.page,
                },
            }),
        }),
        getServices: build.query<GetServicesResp, GetServicesArg>({
            query: queryArg => ({ url: `/cdt/v2/services`, method: 'POST', body: queryArg.serviceRequest }),
        }),
        getServicesDumps: build.query<GetServicesDumpsResp, GetServicesDumpsArg>({
            query: queryArg => ({
                url: `/cdt/v2/namespaces/${queryArg.nid}/services/${queryArg.sid}/dumps`,
                method: 'POST',
                body: queryArg.servicePodRequest,
            }),
        }),
        getHeapDumps: build.query<GetHeapDumpsResp, GetHeapDumpsArg>({
            query: queryArg => ({ url: `/cdt/v2/heaps`, method: 'POST', body: queryArg.serviceRequest }),
            providesTags: ['HeapDumps'],
        }),
        downloadDumpsByRange: build.query<DownloadDumpsByRangeResp, DownloadDumpsByRangeArg>({
            query: queryArg => ({ url: `/cdt/v2/dumps/download`, method: 'POST', body: queryArg.downloadRequest }),
        }),
        downloadDumpByType: build.query<DownloadDumpByTypeResp, DownloadDumpByTypeArg>({
            query: queryArg => ({
                url: `/cdt/v2/dumps/download/${queryArg.podId}/${queryArg['type']}`,
                params: { timeFrom: queryArg.timeFrom, timeTo: queryArg.timeTo },
            }),
        }),
        downloadDumpById: build.mutation<DownloadDumpByIdResp, DownloadDumpByIdArg>({
            query: queryArg => ({
                url: `/cdt/v2/dumps/${queryArg.podId}/${queryArg.dumpType}/${queryArg.dumpId}`,
            }),
        }),
        deleteDumpById: build.mutation<DeleteDumpByIdResp, DeleteDumpByIdArg>({
            query: queryArg => ({
                url: `/cdt/v2/dumps/${queryArg.podId}/${queryArg.dumpType}/${queryArg.dumpId}`,
                method: 'DELETE',
            }),
            invalidatesTags: ['HeapDumps'],
        }),
        getCallsByCondition: build.query<GetCallsByConditionResp, GetCallsByConditionArg>({
            query: queryArg => ({ url: `/cdt/v2/calls/load`, method: 'POST', body: queryArg }),
            merge: (currentCacheData, responseData, otherArgs) => {
                // console.log("merge1")
                if (currentCacheData) {
                    // console.log("merge2")
                    // First page load - clearing cache
                    if (otherArgs.arg.view?.page === 1) {
                        return responseData;
                    }
                    // console.log("merge3")
                    const { calls = [] } = currentCacheData;
                    // console.log(calls.length)
                    const { calls: newCalls = [] } = responseData;
                    // console.log(newCalls.length)
                    return { calls: [...calls, ...newCalls], status: responseData.status };
                }
            },
            serializeQueryArgs: args => {
                return `${args.endpointName}${serializeCallsLoadRequest(args.queryArgs)}`;
            },
            // Here we can decide is it should be refetch or not.
            forceRefetch({ currentArg, previousArg }) {
                if (currentArg && !previousArg) return true;
                if (currentArg && previousArg) return currentArg.view?.page !== previousArg.view?.page;
                return false;
            },
        }),
        getCallsStatisticsByCondition: build.query<GetCallsStatByConditionResp, GetCallsStatByConditionArg>({
            query: queryArg => ({ url: `/cdt/v2/calls/stat`, method: 'POST', body: queryArg }),
            serializeQueryArgs: args => {
                return `${args.endpointName}${serializeCallsLoadRequest(args.queryArgs)}`;
            },
        }),
        getCallTreeByCondition: build.query<GetCallTreeByConditionResp, GetCallTreeByConditionArg>({
            query: () => ({ url: `/cdt/v2/tree/**` }),
        }),
        getCallTreeTemplate: build.query<GetCallTreeTemplateResp, GetCallTreeTemplateArg>({
            query: () => ({ url: `/cdt/v2/js/tree.js` }),
        }),
        exportCallsToCsv: build.query<ExportCallsToCsvResp, ExportCallsToCsvArg>({
            query: queryArg => ({
                url: `/cdt/v2/export/csv`,
                method: 'POST',
                params: { type: queryArg['type'], nodes: queryArg.nodes },
            }),
        }),
        exportCallsToXls: build.query<ExportCallsToXlsResp, ExportCallsToXlsArg>({
            query: queryArg => ({
                url: `/cdt/v2/export/excel`,
                method: 'POST',
                params: { type: queryArg['type'], nodes: queryArg.nodes },
            }),
        }),
        exportDumpsByCondition: build.query<ExportDumpsByConditionResp, ExportDumpsByConditionArg>({
            query: queryArg => ({
                url: `/cdt/v2/export/dump`,
                params: { searchConditions: queryArg.searchConditions },
            }),
        }),
        getCdtVersion: build.query<GetCdtVersionResp, GetCdtVersionArg>({
            query: () => ({ url: `/cdt/v2/version` }),
        }),
        getCommandStatus: build.query<GetCommandStatusResp, GetCommandStatusArg>({
            query: queryArg => ({
                url: `/cdt/v2/commands/status`,
                params: { podName: queryArg.podName, commandId: queryArg.commandId },
            }),
        }),
        getCallsTreeData: build.query<CallsTreeData, GetCallsTreeDataArg>({
            query: queryArg => ({
                url: `/cdt/v2/calls/tree`,
                method: 'POST',
                body: queryArg,
            }),
        }),
        downloadCallsTreeData: build.mutation<void, DownloadCallsTreeArg>({
            query: queryArg => ({
                url: `/cdt/v2/calls/tree/download`, method: 'POST', body: queryArg,
                responseHandler: async response => {
                    if (response.ok) {
                        const blob = await response.blob()
                        const url = window.URL.createObjectURL(blob);
                        downloadFile(url, "calls-tree.html")
                    }
                }
            })
        }),
    }),
    overrideExisting: false,
});
export { cdtOpenapi as openApiEndpoints };
export type GetNamespacesResp = /** status 200 OK */ Container[];
export type GetNamespacesArg = void;
export type GetNamespacesByRangeResp = /** status 200 OK */ Container[];
export type GetNamespacesByRangeArg = {
    /** Start of the time range to search */
    timeFrom: number;
    /** End of the time range to search */
    timeTo: number;
    /** The numbers of items to return */
    limit?: number;
    /** Page (starting from 1) to show from the result set */
    page?: Page;
};
export type GetServicesResp = /** status 200 OK */ ServicePodDto[];
export type GetServicesArg = {
    /** find pods by filters */
    serviceRequest: ServiceRequest;
};
export type GetServicesDumpsResp = /** status 200 OK */ ServiceDumpInfo[];
export type GetServicesDumpsArg = {
    /** Namespace id (name) */
    nid: string;
    /** Microservice id (name) */
    sid: string;
    /** find dumps for pods restarts found by filters */
    servicePodRequest: ServicePodRequest;
};
export type GetHeapDumpsResp = /** status 200 OK */ HeapDumpInfo[];
export type GetHeapDumpsArg = {
    /** find heap dumps for pods restarts found by filters */
    serviceRequest: ServiceRequest;
};
export type DownloadDumpsByRangeResp = /** status 200 OK */ Blob;
export type DownloadDumpsByRangeArg = {
    /** find pod dumps for pods restarts found by filters */
    downloadRequest: DownloadRequest;
};
export type DownloadDumpByTypeResp = /** status 200 OK */ Blob;
export type DownloadDumpByTypeArg = {
    /** Pod id */
    podId: PodId;
    /** Type of dump file */
    type: DumpType;
    /** Start of time range interval */
    timeFrom: number;
    /** End of time range interval */
    timeTo: number;
};
export type DownloadDumpByIdResp = /** status 200 OK */ Blob;
export type DownloadDumpByIdArg = {
    /** Pod id */
    podId: PodId;
    /** Type of dump file */
    dumpType: DumpType;
    /** Dump id */
    dumpId: DumpId;
};
export type DeleteDumpByIdResp = /** status 200 OK */ undefined;
export type DeleteDumpByIdArg = {
    /** Pod id */
    podId: PodId;
    /** Type of dump file */
    dumpType: DumpType;
    /** Dump id */
    dumpId: DumpId;
};
export type GetCallsByConditionResp = /** status 200 OK */ CallsListResponse;
export type GetCallsByConditionArg = CallsLoadRequest;
export type GetCallsStatByConditionResp = /** status 200 OK */ CallsStatResponse;
export type GetCallsStatByConditionArg = CallsLoadRequest;
export type GetCallTreeByConditionResp = /** status 200 OK */ undefined;
export type GetCallTreeByConditionArg = void;
export type GetCallTreeTemplateResp = /** status 200 OK */ undefined;
export type GetCallTreeTemplateArg = void;
export type ExportCallsToCsvResp = /** status 200 OK */ undefined;
export type ExportCallsToCsvArg = {
    /** Format (aggregated or as is) */
    type?: string;
    /** List of nodes calls ? */
    nodes?: string;
};
export type ExportCallsToXlsResp = /** status 200 OK */ undefined;
export type ExportCallsToXlsArg = {
    /** Format (aggregated or as is) */
    type?: string;
    /** List of nodes calls ? */
    nodes?: string;
};
export type ExportDumpsByConditionResp = /** status 200 OK */ undefined;
export type ExportDumpsByConditionArg = {
    /** Search condition in ESC Classic format */
    searchConditions: number;
};
export type GetCdtVersionResp = /** status 200 OK. Returns CDT version */ string;
export type GetCdtVersionArg = void;
export type GetCommandStatusResp = /** status 200 OK */ string;
export type GetCommandStatusArg = {
    /** pod id */
    podName: string;
    /** actual command id */
    commandId: string;
};
export type GetStatsResp = /** status 200 OK */ StatsInfo[];
export type GetStatsArg = void;

export type GetCallStatsResp = /** status 200 OK */ CallStatsInfo[];
export type GetCallStatsArg = void;

export type GetCallsTreeResp = /** status 200 OK */ CallsTreeInfo[];
export type GetCallsTreeArg = void;

export type GetCallsTreeDataResp = /** status 200 OK */ CallsTreeData;

//TODO: list of args
export type GetCallsTreeDataArg = void;
export type DownloadCallsTreeArg = {
    initialPanelState: DashboardEntity[]
};

export type LastAck = number;
export type ServiceDto = {
    name: string;
    lastAck?: LastAck;
    activePods?: number;
};

export type Container = {
    namespace: string;
    services: ServiceDto[];
};

export type ErrorDto = {
    timestamp?: string;
    errorCode?: number;
    status?: string;
    userMessage?: string;
    stackTrace?: string;
};
export type Page = any;
export type ServiceTags = 'java' | 'go';
export type ServicePodDto = {
    namespace?: string;
    service?: string;
    pod?: string;
    startTime?: number;
    tags?: ServiceTags[];
};
export type TimeRange = {
    from?: number;
    to?: number;
};
export type QueryString = string;
export type ServiceListItem = {
    namespace: string;
    service: string;
};
export type ServiceRequest = {
    timeRange?: TimeRange;
    query?: QueryString;
    services?: ServiceListItem[];
};
export type PodId = string;
export type DumpType = 'gc' | 'top' | 'td' | 'heap';
export type Link = string;
export type DownloadOptions = {
    typeName?: DumpType;
    uri?: Link;
};
export type ServiceDumpInfo = {
    namespace?: string;
    service?: string;
    pod?: PodId;
    startTime?: number;
    dataAvailableFrom?: number;
    dataAvailableTo?: number;
    downloadOptions?: DownloadOptions[];
};
export type ServicePodRequest = {
    timeRange?: TimeRange;
    query?: QueryString;
    limit?: number;
    page?: Page;
};
export type DownloadHeaplink = string;
export type DeleteHeapLink = string;
export type HeapDumpInfo = {
    namespace: string;
    service: string;
    pod: string;
    startTime?: number;
    creationTime?: number;
    dumpId: string;
    bytes?: number;
};
export type DownloadRequest = {
    type?: DumpType;
    timeRange?: TimeRange;
    pods?: PodId[];
};
export type DumpId = string;
export type CallInfo = {
    ts: number;
    duration?: number;
    cpuTime?: number;
    suspend?: number;
    queue?: number;
    calls?: number;
    transactions?: number;
    diskBytes?: number;
    netBytes?: number;
    memoryUsed?: number;
    title?: string;
    traceId: string;
    pod: {
        namespace?: string;
        service?: string;
        pod: string;
        startTime?: number;
    };
    params?: object;
};
export type CallsListResponse = {
    status?: {
        finished?: boolean;
        progress?: number;
        errorMessage?: string;
        filteredRecords?: number;
        processedRecords?: number;
    };
    calls?: CallInfo[];
};
export type CallStatInfo = {
    ts: number;
    duration?: number;
    calls?: number;
};
export type CallsStatResponse = {
    status?: {
        finished?: boolean;
        found?: number;
    };
    calls?: CallStatInfo[];
};
export type DurationRange = {
    from?: number;
    to?: number;
};
export type CallsLoadRequest = {
    parameters: {
        windowId: string;
        clientUTC: number;
    };
    filters: {
        timeRange?: TimeRange;
        duration?: DurationRange;
        query?: QueryString;
        services?: ServiceListItem[];
    };
    view: {
        limit?: number;
        page?: Page;
        sortColumn?: string;
        sortOrder: boolean;
    };
};
export type AnyValue = any;
export type PodMeta = {
    podId?: number;
    namespace?: string;
    service?: string;
    pod?: string;
    startTime?: number;
    literals?: AnyValue[][];
    paramInfos?: {
        [key: string]: AnyValue[];
    };
};
export type StatsInfo = {
    name?: string;
    totalTime?: number;
    totalTimePercent?: number;
};

export type CallsTreeInfo = {
    id: string;
    info: CallCommonInfo;    
    duration: pairStruct;
    suspension: pairStruct;
    invocations: pairStruct;
    time: pairStruct;
    timePercent?: number;
    avg: pairStruct;   
    params?: CallParameter[];
    children?: CallsTreeInfo[];
};

export type CallCommonInfo = {
    title: string;
    hasStackTrace: boolean;
    trace?: string;
    sourceJar: string;
    lineNumber: number;
    calls: number;
}

export type pairStruct = {
    self: number,
    total: number,
}

export type paramType = 'Byte' | 'Date' | 'Duration' | 'Number' | 'String'; 

export type CallParameter = {
    id: string;    
    type: paramType;    
    isList?: boolean;
    isIndex?: boolean;
    paramOrder?: number;
    values: AnyValue[];
}

export type CallParameterView = {
    id: string;
    children?: CallParameterView[];
}

export type CallStatsInfo = {
    name?: string;
    self?: AnyValue;
    total?: number;
}

export type CallsTreeData = {
    info: CallParameter[];
    children: CallsTreeInfo[];
}

export const {
    useGetNamespacesQuery,
    useGetNamespacesByRangeQuery,
    useGetServicesQuery,
    useGetServicesDumpsQuery,
    useGetHeapDumpsQuery,
    useDownloadDumpsByRangeQuery,
    useDownloadDumpByTypeQuery,
    useDownloadDumpByIdMutation,
    useDeleteDumpByIdMutation,
    useGetCallsByConditionQuery,
    useLazyGetCallsByConditionQuery,
    useGetCallsStatisticsByConditionQuery,
    useGetCallTreeByConditionQuery,
    useGetCallTreeTemplateQuery,
    useExportCallsToCsvQuery,
    useExportCallsToXlsQuery,
    useExportDumpsByConditionQuery,
    useGetCdtVersionQuery,
    useGetCommandStatusQuery,
    useGetCallsTreeDataQuery,
    useDownloadCallsTreeDataMutation,
} = cdtOpenapi;
