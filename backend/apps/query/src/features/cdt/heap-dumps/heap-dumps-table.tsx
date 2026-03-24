import { useRangeValues } from '@app/features/cdt/controls/use-range-values';
import { useSidebarApiArgs } from '@app/features/cdt/hooks/use-sidebar-api-args';
import { useGetHeapDumpsQuery, type HeapDumpInfo } from '@app/store/cdt-openapi';
import { useAppSelector } from '@app/store/hooks';
import { selectSearchParamsApplied } from '@app/store/slices/context-slices';
import { UxLoader, UxTable } from '@netcracker/ux-react';
import { type ColumnType } from 'antd/lib/table';
import prettyBytes from 'pretty-bytes';
import { memo } from 'react';
import TrowActions from './trow-actions';

const columns: ColumnType<HeapDumpInfo>[] = [
    {
        key: 'time',
        title: 'Heap Dump Time',
        dataIndex: 'creationTime',
        render: time => new Date(time).toLocaleString(),
    },
    {
        key: 'pod',
        title: 'POD',
        dataIndex: 'pod',
    },
    {
        key: 'service',
        title: 'Service',
        dataIndex: 'service',
    },
    {
        key: 'namespace',
        title: 'Namespace',
        dataIndex: 'namespace',
    },
    {
        key: 'size',
        title: 'Size',
        dataIndex: 'bytes',
        render: bytes => prettyBytes(bytes),
    },
    {
        key: '_actions',
        fixed: 'right',
        width: 80,
        render: (_, row) => {
            return <TrowActions {...row} />;
        },
    },
];
const tableIndicator = { indicator: <UxLoader /> };
const tableScroll = { x: 'max-content' };

const HeapDumpsTable = memo(() => {
    const [from, to] = useRangeValues();
    const [selectedServices] = useSidebarApiArgs();
    const searchParamsApplied = useAppSelector(selectSearchParamsApplied);
    const { data, isFetching } = useGetHeapDumpsQuery(
        {
            serviceRequest: {
                query: '',
                timeRange: {
                    from: +from,
                    to: +to,
                },
                services: selectedServices,
            },
        },
        { skip: !searchParamsApplied }
    );

    return (
        <div className="table-container">
            <UxTable
                columns={columns}
                loading={isFetching && tableIndicator}
                dataSource={data}
                scroll={tableScroll}
                rowKey={(r: HeapDumpInfo) => `${r.creationTime}${r.pod}`}
            />
        </div>
    );
});

HeapDumpsTable.displayName = 'HeapDumpsTable';

export default HeapDumpsTable;
