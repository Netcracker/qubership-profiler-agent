import { userLocale } from '@app/common/user-locale';
import DumpsDownloadOpts from '@app/features/cdt/pods-info/dumps-download-opts';
import type { ServiceDumpInfo } from '@app/store/cdt-openapi';
import { UxChip, UxLoader, UxTooltip } from '@netcracker/ux-react';
import type { ColumnType } from 'antd/lib/table';
import { warningIcon } from './pods-table';

// type ServiceDumpInfo = {
//     namespace?: string;
//     service?: string;
//     pod?: string;
//     startTime?: number;
//     tags?: ServiceTags[];
//     lastAck?: LastAck;
//     dataAvailableFrom?: number;
//     dataAvailableTo?: number;
//     podId?: PodId;
//     downloadOptions?: DownloadOptions[];
//     onlineNow?: OnlineNow;
// };
export const columns: ColumnType<any>[] = [
    {
        title: 'Service',
        key: 'name',
        dataIndex: 'name',
        render: (name: string, row) => (
            // TODO: make error pretty
            <span style={{ display: 'inline-flex', gap: 8 }}>
                {name}{' '}
                <>
                    {row?.fetching && <UxLoader size="small" />}{' '}
                    {row?.error && <UxTooltip description={JSON.stringify(row.error)}>{warningIcon}</UxTooltip>}
                </>
            </span>
        ),
    },
    {
        title: 'Container Start Date',
        key: 'container start date',
        dataIndex: 'startTime',
        width: 200,
        render: (value?: number, row?) =>
            row.type != 'service' && row.type != 'namespace' &&
            value && (
                <time title={new Date(value).toISOString()} dateTime={new Date(value).toISOString()}>
                    {new Date(value).toLocaleString(userLocale)}
                </time>
            ),
    },
    {
        title: 'Available From',
        key: 'dataAvailableFrom',
        dataIndex: 'dataAvailableFrom',
        width: 200,
        render: (value?: number) =>
            !!value && (
                <time title={new Date(value).toISOString()} dateTime={new Date(value).toISOString()}>
                    {new Date(value).toLocaleString(userLocale)}
                </time>
            ),
    },
    {
        title: 'Available To',
        key: 'dataAvailableTo',
        dataIndex: 'dataAvailableTo',
        width: 200,
        render: (value?: number) =>
            !!value && (
                <time title={new Date(value).toISOString()} dateTime={new Date(value).toISOString()}>
                    {new Date(value).toLocaleString(userLocale)}
                </time>
            ),
    },
    {
        key: 'download',
        dataIndex: 'downloadOptions',
        fixed: 'right',
        width: 30,
        render: (value?: ServiceDumpInfo['downloadOptions']) => <DumpsDownloadOpts opts={value} />,
    },
];
