import { isTimeoutError } from '@app/common/guards/errors';
import ContentControls from '@app/features/cdt/calls/content-controls';
import { useCallsColumns } from '@app/features/cdt/calls/hooks/use-calls-columns';
import ResizableTitle, { type ResizableTitleProps } from '@app/components/table-components/resizable-title';
import useCallsFetchArg from '@app/features/cdt/calls/use-calls-fetch-arg';
import type { Call } from '@app/models/calls';
import { type CallInfo, useGetCallsByConditionQuery } from '@app/store/cdt-openapi';
import { useAppDispatch, useAppSelector } from '@app/store/hooks';
import { contextDataAction } from '@app/store/slices/context-slices';
import { ReactComponent as NoDataGraySvg } from '@assets/illustrations/no-data-gray.svg';
import { InfoPage } from '@netcracker/cse-ui-components';
import { ReactComponent as Sync20Icon } from '@netcracker/ux-assets/icons/sync/sync-20.svg';
import { UxButton, UxIcon, UxLoader, UxTable, type UxTableProps } from '@netcracker/ux-react';
import type { ColumnType, TablePaginationConfig } from 'antd/lib/table';
import type { FilterValue, SorterResult, TableCurrentDataSource, TableRowSelection } from 'antd/lib/table/interface';
import {
    type CSSProperties,
    type FC,
    type Key,
    memo,
    useCallback,
    useLayoutEffect,
    useMemo,
    useRef,
    useState,
} from 'react';
import type { ResizeCallbackData } from 'react-resizable';
import classNames from './calls-table.module.scss';
import { type ColumnWidthsMap, appDataActions, selectCallsColumnWidths } from '@app/store/slices/app-state.slice';
import { useCallsStore } from '@app/features/cdt/calls/calls-store';

function callsErrorMessage(error: unknown) {
    let message = 'Calls Info is Unavailable. The backend server encountered an error and could not complete request.';
    if (isTimeoutError(error)) {
        message = 'The web server failed to respond within the specified time';
    }
    return message;
}

const tableRowKey = (r: CallInfo) => `${r.ts}-${r.duration}-${r.pod?.pod}`;
const noDataIconStyle: CSSProperties = { fontSize: 56 };

const tableIndicator = { indicator: <UxLoader size="large" /> };
const CallsTable: FC = memo(() => {
    const [, set] = useCallsStore(s => s);
    const dispatch = useAppDispatch();
    const containerRef = useRef<HTMLDivElement>(null);
    const storedColumnWidths = useAppSelector(selectCallsColumnWidths);
    const [columnWidths, setColumnWidths] = useState<ColumnWidthsMap>(storedColumnWidths ?? {});
    const columns = useCallsColumns();
    const [callRequest, { shouldSkip, notReady }] = useCallsFetchArg();

    useLayoutEffect(() => {
        if (callRequest.view?.page === 1) {
            const tableBody = containerRef.current?.querySelector('.ux-table-body');
            if (tableBody) {
                tableBody.scrollTo({
                    top: 0,
                    behavior: 'smooth',
                });
            }
        }
    }, [callRequest.view?.page]);
    const { isFetching, data, isError, error, refetch } = useGetCallsByConditionQuery(callRequest, {
        skip: shouldSkip,
    });

    const tableScroll: UxTableProps['scroll'] = useMemo(() => {
        if (containerRef.current?.clientHeight) {
            return {
                x: 'max-content',
                y: containerRef.current?.clientHeight - 45,
            };
        }
        return {
            x: 'max-content',
            y: 500,
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [containerRef.current?.clientHeight]);

    const handleResize = useCallback((columnKey: Key) => {
        return (_: unknown, data: ResizeCallbackData) => {
            const { size } = data;
            setColumnWidths(prev => {
                return { ...prev, [columnKey]: size.width };
            });
        };
    }, []);
    const handleResizeStop = useCallback(
        (columnKey: Key) => {
            return (_: unknown, data: ResizeCallbackData) => {
                const { width } = data.size;
                setColumnWidths(prev => {
                    const widths = { ...prev, [columnKey]: width };
                    dispatch(appDataActions.setCallsColumnWidths(widths));

                    return widths;
                });
            };
        },
        [dispatch]
    );
    const tableComponents = useMemo(() => {
        return {
            header: {
                cell: (props: any) => {
                    return <ResizableTitle {...props} />;
                },
            },
        } as UxTableProps['components'];
    }, []);

    const resizableColumns = useMemo(() => {
        return columns?.map(col => {
            const width = columnWidths?.[col.key as Key] ?? (col.width as number);
            return {
                ...col,
                onHeaderCell: () => {
                    const resizableProps: ResizableTitleProps = {
                        width: width,
                        resizable: !!col.title,
                        onResize: col.key ? handleResize(col.key) : undefined,
                        onResizeStop: col.key ? handleResizeStop(col.key) : undefined,
                        columnKey: col.key,
                    };
                    return resizableProps;
                },
                width: width,
            } as ColumnType<Call>;
        });
    }, [columnWidths, columns, handleResize, handleResizeStop]);

    const handleBottomReached = useCallback(
        (event: Event) => {
            const target = event.target as HTMLElement;
            const maxScroll = target.scrollHeight - target.clientHeight;
            const currentScroll = target.scrollTop;
            if (currentScroll === maxScroll) {
                if (!isFetching) {
                    // console.log(data?.status)
                    const rows = data?.status?.filteredRecords || 0;
                    // console.log(rows);
                    const pages = Math.ceil(rows / 100);
                    // console.log(pages);
                    dispatch(contextDataAction.setMaxPage( pages ));
                    dispatch(contextDataAction.nextCallsPage());
                }
            }
        },
        [dispatch, isFetching]
    );
    useLayoutEffect(() => {
        const tableContent = document.querySelector('.dynamicTable .ux-table-body');
        if (tableContent) {
            tableContent.addEventListener('scroll', handleBottomReached);
        }

        return () => {
            tableContent?.removeEventListener('scroll', handleBottomReached);
        };
    }, [handleBottomReached]);

    const handleTableChange = useCallback(
        (
            pagination: TablePaginationConfig,
            filters: Record<string, FilterValue | null>,
            sorter: SorterResult<CallInfo> | Array<SorterResult<CallInfo>>,
            extra: TableCurrentDataSource<CallInfo>
        ) => {
            // console.log('pagination, filters, sorter, extra :>> ', { pagination, filters, sorter, extra });
            if (extra.action === 'sort' && !Array.isArray(sorter)) {
                dispatch(
                    contextDataAction.updateCallsTableState({
                        sortBy: sorter.column?.dataIndex?.toString() as string,
                        sortOrder: sorter.order ?? 'ascend',
                        page: 1,
                    })
                );
            }
        },
        [dispatch]
    );
    const rowSelection: TableRowSelection<CallInfo> = useMemo(() => {
        return {
            onChange: (selectedRowKeys: Key[], selectedRows: CallInfo[]) => {
                set({ selectedCalls: selectedRows });
            },
            type: 'checkbox',
        };
    }, [set]);
    if (notReady) {
        return (
            <InfoPage
                title={<></>}
                className="dashed-info-page"
                message={
                    <span>
                        No Data.
                        <br />
                        Select the Namespace/Service and Period
                    </span>
                }
                icon={<UxIcon component={NoDataGraySvg} style={noDataIconStyle} />}
            />
        );
    }
    if (isError) {
        const message = callsErrorMessage(error);
        return (
            <InfoPage
                title={<></>}
                className="dashed-info-page"
                message={
                    <span>
                        Calls Info is Unavailable. <br />
                        {message}
                    </span>
                }
                icon={<UxIcon component={NoDataGraySvg} style={noDataIconStyle} />}
                additionalContent={
                    <UxButton onClick={refetch} type="text" color="blue" leftIcon={<UxIcon component={Sync20Icon} />}>
                        Reload
                    </UxButton>
                }
            />
        );
    }

    return (
        <div className={classNames.container}>
            <ContentControls />
            <div ref={containerRef} className="table-container">
                <UxTable
                    rowSelection={rowSelection}
                    className="dynamicTable"
                    dataSource={data?.calls}
                    rowKey={tableRowKey}
                    onChange={handleTableChange}
                    columns={resizableColumns}
                    components={tableComponents}
                    loading={isFetching && tableIndicator}
                    scroll={tableScroll}
                />
            </div>
        </div>
    );
});

CallsTable.displayName = 'CallsTable';

export default CallsTable;
