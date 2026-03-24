import { useRangeValues } from '@app/features/cdt/controls/use-range-values';
import { useSidebarApiArgs } from '@app/features/cdt/hooks/use-sidebar-api-args';
import { type CallsLoadRequest } from '@app/store/cdt-openapi';
import { useAppSelector } from '@app/store/hooks';
import { selectCallsTableState, useSearchParamsApplied } from '@app/store/slices/context-slices';
import { nanoid } from 'nanoid';
import { useMemo } from 'react';
import {ESC_QUERY_PARAMS} from "@app/constants/query-params";
import {useSearchParams} from "react-router-dom";

const windowId = nanoid(); // unique "session" id for current window

type CallsFetchConditions = {
    shouldSkip: boolean;
    notReady: boolean;
};

export default function useCallsFetchArg(): [CallsLoadRequest, CallsFetchConditions] {
    const tableState = useAppSelector(selectCallsTableState);
    const [urlParams] = useSearchParams();
    const query =  urlParams.get(ESC_QUERY_PARAMS.callsQuery) || '';
    const durationFrom = useMemo(() => {
        const duration = urlParams.get(ESC_QUERY_PARAMS.callsDuration);
        return duration ? +duration : 5000;
    }, [urlParams]);
    const searchParamsApplied = useSearchParamsApplied();
    const [selectedServices] = useSidebarApiArgs();
    const [from, to] = useRangeValues();

    return useMemo(() => {
        const request: CallsLoadRequest = {
            filters: {
                timeRange: {
                    from: +from,
                    to: +to,
                },
                duration: {
                    from: durationFrom,
                    to: 30879000,
                },
                services: selectedServices,
                query: query,
            },
            parameters: {
                clientUTC: new Date().getTime(),
                windowId: windowId.toString(),
            },
            view: {
                limit: 100,
                page: tableState.page,
                sortColumn: tableState.sortBy,
                sortOrder: tableState.sortOrder === 'ascend' ? true : false,
            },
        };
        const shouldSkip = !searchParamsApplied || selectedServices.length === 0;
        const fetchConditions: CallsFetchConditions = { shouldSkip, notReady: selectedServices.length === 0 };
        return [request, fetchConditions];
    }, [from, to, durationFrom, selectedServices, query, tableState.page, tableState.sortBy, tableState.sortOrder, searchParamsApplied]);
}
