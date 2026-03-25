import type { RequiredBy } from '@app/common/type-utils';
import HtmlEllipsis from '@app/components/html-ellipsis/html-ellipsis';
import { useCallsStoreSelector } from '@app/features/cdt/calls/calls-store';
import type { CallInfo } from '@app/store/cdt-openapi';
import { UxLink } from '@netcracker/ux-react/typography/link/link.component';
import type { ColumnType } from 'antd/lib/table/interface';
import prettyBytes from 'pretty-bytes';
import prettyMilliseconds from 'pretty-ms';
import { createCallUrl } from '../create-call-url';
import { userLocale } from '@app/common/user-locale';
import HighlightCell from '@app/components/highlight-cell/highlight-cell';

const timestampFormat: Intl.DateTimeFormatOptions = {
    second: 'numeric',
    hour: 'numeric',
    minute: 'numeric',
    hour12: false,
    fractionalSecondDigits: 3,
};

export const DEFAULT_CALLS_COLUMNS: RequiredBy<ColumnType<CallInfo>, 'key'>[] = [
    {
        title: 'Start Timestamp',
        key: 'timestamp',
        dataIndex: 'ts',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (ts: CallInfo['ts']) =>
            ts ? (
                <time title={new Date(ts).toISOString()} dateTime={new Date(ts).toISOString()}>
                    {new Date(ts).toLocaleTimeString(userLocale, timestampFormat)}
                </time>
            ) : (
                '-'
            ),
    },
    {
        title: 'Duration',
        key: 'duration',
        dataIndex: 'duration',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (duration: CallInfo['duration'], row) =>
            duration ? (
                <UxLink href={createCallUrl(row)} target="_blank">
                    <HighlightCell highlight={duration > 10_000}>{prettyMilliseconds(duration)}</HighlightCell>
                </UxLink>
            ) : (
                '0ms'
            ),
    },
    {
        title: 'Title',
        key: 'title',
        dataIndex: 'title',
        width: 150,
        render: (title: CallInfo['title']) => <HtmlEllipsis text={title ?? ''} />,
    },
    {
        title: 'Calls',
        key: 'calls',
        dataIndex: 'calls',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (calls: CallInfo['calls']) => (calls ? calls.toLocaleString() : '-'),
    },
    {
        title: 'CPU Time',
        key: 'cpu',
        dataIndex: 'cpuTime',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (cpuTime: CallInfo['cpuTime']) =>
            cpuTime ? <HighlightCell highlight={cpuTime > 10_000}>{prettyMilliseconds(cpuTime)}</HighlightCell> : '0ms',
    },
    {
        title: 'Suspension',
        ellipsis: true,
        key: 'suspend',
        dataIndex: 'suspend',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (suspend: CallInfo['suspend']) =>
            suspend ? <HighlightCell highlight={suspend > 2_000}>{prettyMilliseconds(suspend)}</HighlightCell> : '0ms',
    },
    {
        title: 'Queue Wait Time',
        ellipsis: true,
        key: 'queue-wait-time',
        dataIndex: 'queue',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (queue: CallInfo['queue']) =>
            queue ? <HighlightCell highlight={queue > 500}>{prettyMilliseconds(queue)}</HighlightCell> : '0ms',
    },

    {
        title: 'Transactions',
        ellipsis: true,
        key: 'tx',
        dataIndex: 'transactions',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (transactions: CallInfo['transactions']) =>
            transactions ? (
                <HighlightCell highlight={transactions > 10}>{transactions.toLocaleString()}</HighlightCell>
            ) : (
                '0'
            ),
    },
    {
        title: 'POD',
        width: 250,
        key: 'pod',
        dataIndex: 'pod',
        render: (pod: CallInfo['pod']) => (pod ? <HtmlEllipsis text={pod.pod} /> : '-'),
    },
    {
        title: 'Disk IO',
        key: 'disk-io',
        dataIndex: 'diskBytes',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (diskBytes: CallInfo['diskBytes']) =>
            diskBytes ? <HighlightCell highlight={diskBytes > 200_000}>{prettyBytes(diskBytes)}</HighlightCell> : '-',
    },
    {
        title: 'Network IO',
        ellipsis: true,
        key: 'network-bytes',
        dataIndex: 'netBytes',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (netBytes: CallInfo['netBytes']) =>
            netBytes ? <HighlightCell highlight={netBytes > 10_000_000}>{prettyBytes(netBytes)}</HighlightCell> : '-',
    },
    {
        title: 'Memory allocated',
        ellipsis: true,
        key: 'mem',
        dataIndex: 'memoryUsed',
        sortDirections: ['ascend', 'descend', 'ascend'],
        sorter: true,
        width: 80,
        render: (memory: CallInfo['memoryUsed']) =>
            memory ? <HighlightCell highlight={memory > 10_000_000}>{prettyBytes(memory)}</HighlightCell> : '-',
    },
];

export const CALLS_COLUMNS_KEYS = DEFAULT_CALLS_COLUMNS.map(column => column.key);

export function useSortedCallsColumns() {
    const columnsOrder = useCallsStoreSelector(s => s.columnsOrder);
    return [...DEFAULT_CALLS_COLUMNS].sort(
        (a, b) => columnsOrder.indexOf(a.key as string) - columnsOrder.indexOf(b.key as string)
    );
}
export function useCallsColumns() {
    const hiddenColumns = useCallsStoreSelector(s => s.hiddenColumns);
    const sortedColumns = useSortedCallsColumns();
    return sortedColumns.filter(column => !hiddenColumns.includes(column.key));
}
