import type { LoadCallsResponse } from '@app/models/calls';
import type { ContainersInfoItem } from '@app/models/containers';
import type { PodModel } from '@app/models/pods';
import { baseApi } from '@app/store/base-query';

export type LoadCallsArgs = {
    windowId: string;
    clientUTC?: number;
    timerangeFrom: string | number;
    timerangeTo: string | number;
    durationFrom: number;
    durationTo: number;
    podFilter: string;
    filterString: string;
    hideSystem: boolean;
    beginIndex: number;
    pageSize: number;
    sortIndex: number;
    asc: boolean;
};
const escEndpoints = baseApi.injectEndpoints({
    endpoints: builder => ({
        listActivePods: builder.query<PodModel[], { dateFrom: number; dateTo: number; podFilter: string }>({
            query: arg => ({
                url: 'esc/listActivePODs',
                params: arg,
            }),
        }),

        containersInfo: builder.query<ContainersInfoItem[], void>({
            query: () => ({
                url: 'esc/containersInfo',
            }),
        }),
        version: builder.query<string, void>({
            query: () => ({
                url: 'esc/version',
                responseHandler: async resp => {
                    const version = await resp.text();
                    return version;
                },
            }),
        }),

        calls: builder.query<LoadCallsResponse, LoadCallsArgs>({
            query: params => ({
                url: 'esc/calls/load',
                params: {
                    ...params,
                    clientUTC: params.clientUTC || new Date().getTime(),
                },
            }),
        }),
    }),
});

export const { useContainersInfoQuery, useListActivePodsQuery, useVersionQuery, useCallsQuery } = escEndpoints;
