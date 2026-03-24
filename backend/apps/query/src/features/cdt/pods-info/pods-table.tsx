import { defaultRange } from '@app/features/cdt/controls/fast-ranges';
import { useRangeValues } from '@app/features/cdt/controls/use-range-values';
import { useSidebarApiArgs } from '@app/features/cdt/hooks/use-sidebar-api-args';
import { createPodsInfoTableDataSource } from '@app/features/cdt/pods-info/utils/tree-utils';
import {
    type GetServicesDumpsResp,
    cdtOpenapi,
    useGetNamespacesByRangeQuery,
    useGetServicesQuery,
} from '@app/store/cdt-openapi';
import { useAppDispatch } from '@app/store/hooks';
import { ReactComponent as Warning20Icon } from '@netcracker/ux-assets/icons/warning/warning-20.svg';
import { UxIcon, UxLoader, UxTable } from '@netcracker/ux-react';
import { memo, useCallback, useMemo, useState, type FC } from 'react';
import { useSearchParams } from 'react-router-dom';
import { columns } from './columns';
import { useSearchParamsApplied } from '@app/store/slices/context-slices';
import { InfoPage } from '@netcracker/cse-ui-components';
import { isTimeoutError } from '@app/common/guards/errors';
import {ESC_QUERY_PARAMS} from "@app/constants/query-params";

export type DumpsQueryStore = Record<
    string,
    {
        fetching: boolean;
        error?: unknown;
        resp?: GetServicesDumpsResp;
    }
>;

export const warningIcon = <UxIcon style={{ fontSize: 20, color: '#FFB02E' }} component={Warning20Icon} />;
const tableIndicator = { indicator: <UxLoader /> };
const getRowKey = (row: { name: string }) => row.name;
const tableScroll = { x: 'max-content', y: 'calc(100vh - 500px)' };

function podsErrorMessage(error: unknown) {
    if (isTimeoutError(error)) {
        return 'The web server failed to respond within the specified time.';
    }
    return 'The backend server encountered an error and could not complete request.';
}
const PodsTable: FC = () => {
    const dispatch = useAppDispatch();
    const [searchParams] = useSearchParams();
    const dateFrom = searchParams.get('dateFrom') ?? defaultRange.dateFrom;
    const dateTo = searchParams.get('dateTo') ?? defaultRange.dateTo;
    const searchParamsApplied = useSearchParamsApplied();
    const [urlParams] = useSearchParams();
    const podsQuery =  urlParams.get(ESC_QUERY_PARAMS.callsQuery) || '';
    const [selectedServices] = useSidebarApiArgs();
    const [from, to] = useRangeValues();
    const { isFetching: podsIsFetching, data: services } = useGetServicesQuery(
        {
            serviceRequest: {
                services: selectedServices,
                query: podsQuery,
                timeRange: {
                    from: +dateFrom,
                    to: +dateTo,
                },
            },
        },
        { skip: !searchParamsApplied }
    );
    const {
        data: containers,
        isFetching: containersInfoLoading,
        ...containersResponse
    } = useGetNamespacesByRangeQuery(
        {
            timeFrom: +from,
            timeTo: +to,
            limit: 1000,
            page: 1,
        },
        { skip: !searchParamsApplied }
    );
    const [dumpResponses, setDumpResponses] = useState<DumpsQueryStore>({});

    const dataSource = useMemo(() => {
        if (containers && services) {
            return createPodsInfoTableDataSource(services, containers, dumpResponses);
        }
    }, [containers, dumpResponses, services]);

    // TODO: Refactor IT!
    const handleExpand = useCallback(
        async (expanded: boolean, row: any) => {
            if (row.type === 'service') {
                setDumpResponses(resps => {
                    return {
                        ...resps,
                        [`${row.namespace}-${row.name}`]: {
                            nid: row.namespace,
                            sid: row.name,
                            fetching: true,
                        },
                    };
                });
                const result = await dispatch(
                    cdtOpenapi.endpoints.getServicesDumps.initiate({
                        nid: row.namespace,
                        sid: row.name,
                        servicePodRequest: {
                            page: 1,
                            limit: 1000,
                            timeRange: {
                                from: +from,
                                to: +to,
                            },
                        },
                    })
                );
                if (result.isSuccess) {
                    setDumpResponses(resps => {
                        return {
                            ...resps,
                            [`${row.namespace}-${row.name}`]: {
                                nid: row.namespace,
                                sid: row.name,
                                fetching: false,
                                resp: result.data,
                            },
                        };
                    });
                }
                if (result.error) {
                    setDumpResponses(resps => {
                        return {
                            ...resps,
                            [`${row.namespace}-${row.name}`]: {
                                nid: row.namespace,
                                sid: row.name,
                                fetching: false,
                                error: result.error,
                            },
                        };
                    });
                }
            }
        },
        [dispatch, from, to]
    );

    const tableLoading = containersInfoLoading || podsIsFetching;
    const expandableConfig = useMemo(
        () => ({
            defaultExpandAllRows: false,
            columnWidth: 10,
            onExpand: handleExpand,
        }),
        [handleExpand]
    );
    if (containersResponse.isError) {
        const message = podsErrorMessage(containersResponse.error);
        return <InfoPage title="Pods Info is Unavailable." className="dashed-info-page" message={message} />;
    }
    return (
        <div className="table-container">
            <UxTable
                columns={columns}
                dataSource={dataSource}
                rowKey={getRowKey}
                loading={tableLoading ? tableIndicator : false}
                expandable={expandableConfig}
                scroll={tableScroll}
                className="ux-table-with-native-expand"
            />
        </div>
    );
};

export default memo(PodsTable);
