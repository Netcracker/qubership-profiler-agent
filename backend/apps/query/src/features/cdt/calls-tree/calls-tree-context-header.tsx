import React, { memo, type FC } from 'react';
import { ContextBar, type ContextItemModel } from '@netcracker/cse-ui-components';
import {UxIcon } from '@netcracker/ux-react';
import { ReactComponent as CybeIcon } from '@netcracker/ux-assets/icons/cube/cube-20.svg';
import { ESC_CALL_TREE_QUERY_PARAMS } from '@app/constants/query-params';
import { useSearchParams } from 'react-router-dom';
import cn from 'classnames';
import { CallsTreeAddWidgetDropdown } from './calls-tree-tools';

interface CallsTreeHeaderProps {
    leftExtra?: React.ReactNode;
    rightExtra?: React.ReactNode;
}

const CallsTreeHeader: FC<CallsTreeHeaderProps> = ({ leftExtra, rightExtra }) => {
    const [urlParams] = useSearchParams();
    // TODO what about default values?
    const podName =
        urlParams.get(ESC_CALL_TREE_QUERY_PARAMS.podName) || 'esc-collector-service-7bbdc768d8-qd7xc_1696234834423';
    const ts = urlParams.get(ESC_CALL_TREE_QUERY_PARAMS.ts) || '10:06:29.956';

    const items: ContextItemModel[] = [
        {
            id: 'podName',
            name: <div className={cn('ux-typography-13px-medium', 'magnet-label')}>{podName}</div>,
            icon: <UxIcon component={CybeIcon} />,
        },
        {
            id: 'time',
            name: <div className={cn('ux-typography-13px-medium', 'niagara-label')}>{ts}</div>,
        },
    ];

    //TODO: remove chevron icons, when it will be possible in ContextBar
    return (
        <ContextBar
            left={leftExtra}
            items={items}
            right={
                <>
                    <CallsTreeAddWidgetDropdown />
                    {rightExtra}
                </>
            }
        />
    );
};

export default memo(CallsTreeHeader);
