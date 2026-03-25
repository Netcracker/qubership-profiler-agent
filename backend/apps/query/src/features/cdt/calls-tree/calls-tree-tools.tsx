import { useDownloadCallsTreeDataMutation } from '@app/store/cdt-openapi';
import { useAppSelector } from '@app/store/hooks';
import { selectDashboardState } from '@app/store/slices/calls-tree-context-slices';
import { UxButton, UxDropdownNew, UxIcon, type UxDropdownNewItem } from '@netcracker/ux-react';
import { useCallback } from 'react';
import { ReactComponent as EntranceArrowLeftIcon } from '@netcracker/ux-assets/icons/entrance-arrow-left/entrance-arrow-left-20.svg';
import { ReactComponent as DownloadIcon } from '@netcracker/ux-assets/icons/download/download-20.svg';
import { ReactComponent as AddCircleIcon } from '@netcracker/ux-assets/icons/add-circle/add-circle-outline-20.svg';
import { ReactComponent as GraphStatisticsIcon } from '@netcracker/ux-assets/icons/graph-statistics/graph-statistics-20.svg';
import { ReactComponent as ListBulletedIcon } from '@netcracker/ux-assets/icons/list-bulleted/list-bulleted-20.svg';
import { ReactComponent as FlowCascadeIcon } from '@netcracker/ux-assets/icons/flow-cascade/flow-cascade-20.svg';
import { useEnableWidgetFunction } from './hooks/use-widgets';

export const CallsTreeBackToOverviewButton = () => {
    return (
        <UxButton
            type="light"
            leftIcon={<UxIcon component={EntranceArrowLeftIcon} />}
            onClick={() => console.log('Back to overview clicked')}
        >
            Back to Overview
        </UxButton>
    );
};

export const CallsTreeDownloadButton = () => {
    const { panels } = useAppSelector(selectDashboardState);
    const [downloadCallsTreeData] = useDownloadCallsTreeDataMutation();
    const downloadCallsTree = useCallback(() => {
        downloadCallsTreeData({ initialPanelState: panels });
    }, [panels, downloadCallsTreeData]);

    return <UxButton type="light" leftIcon={<UxIcon component={DownloadIcon} />} onClick={downloadCallsTree} />;
};

export const CallsTreeAddWidgetDropdown = () => {
    const enableWidget = useEnableWidgetFunction();

    function handleClick(item: UxDropdownNewItem) {
        switch (item.id) {
            case 'frame-graph':
                enableWidget('frame-graph', { w: 12, h: 1 });
                break;
            case 'statistics':
                enableWidget('stats', { w: 3, h: 2 });
                break;
            case 'call-tree':
                enableWidget('calls-tree', { w: 9, h: 2 });
                break;
        }
    }
    
    return (
        <UxDropdownNew
            trigger="hover"
            items={[
                {
                    id: 'frame-graph',
                    text: 'Frame Graph',
                    leftIcon: <UxIcon component={GraphStatisticsIcon} />,
                },
                {
                    id: 'statistics',
                    text: 'Statistics',
                    leftIcon: <UxIcon component={ListBulletedIcon} />,
                },
                {
                    id: 'call-tree',
                    text: 'Call Tree',
                    leftIcon: <UxIcon component={FlowCascadeIcon} />,
                },
            ]}
            onItemClick={handleClick}
        >
            <UxButton type="light" leftIcon={<UxIcon component={AddCircleIcon} />} />{' '}
        </UxDropdownNew>
    );
};
