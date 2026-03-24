import { memo, type FC, useEffect } from 'react';
import classNames from './calls-tree-page.module.scss';
import RGL, { WidthProvider } from 'react-grid-layout';
import CallsTreeDashboardEntity from './dashboard-entity/calls-tree-dashboard-entity';
import { UxIcon } from '@netcracker/ux-react';
import { ReactComponent as ResizableIcon } from '@app/assets/icons/resizable-icon.svg';
import { InfoPage } from '@netcracker/cse-ui-components';
import { ReactComponent as RedHairFail } from '@app/assets/illustrations/red-hair-fail.svg';
import LoadingPage from '@app/pages/loading.page';
import { callsTreeContextDataAction, selectDashboardState } from '@app/store/slices/calls-tree-context-slices';
import { useAppDispatch, useAppSelector } from '@app/store/hooks';
import { useCallsTreeData, useInitialPanelState } from './calls-tree-context';

const ReactGridLayout = WidthProvider(RGL);

const CallsTreeDashboard: FC = () => {
    const { isFetching, isError } = useCallsTreeData();
    const initialPanelState = useInitialPanelState();
    const { panels } = useAppSelector(selectDashboardState);
    const dispatch = useAppDispatch();

    const onLayoutChange = (layouts: RGL.Layout[]) => {
        dispatch(callsTreeContextDataAction.setLayout(layouts));
    };

    useEffect(() => {
        if (initialPanelState) {
            onLayoutChange(initialPanelState)
        }
    }, [])

    return (
        <div className={classNames.dashboard}>
            {isError && (
                <InfoPage
                    title="Something went wrong"
                    message="Please refresh the page or try again later."
                    icon={<RedHairFail />}
                />
            )}
            {isFetching && <LoadingPage style={{ height: '100%' }} />}
            {!isFetching && !isError && (
                <ReactGridLayout
                    cols={12}
                    isDraggable={true}
                    draggableHandle=".draggable-handle"
                    draggableCancel=".draggable-cancel"
                    onLayoutChange={onLayoutChange}
                    resizeHandle={
                        <div className={classNames.resizableIcon}>
                            <UxIcon component={ResizableIcon} style={{ fontSize: 8 }} />
                        </div>
                    }
                >
                    {panels.map(panel => (
                        <CallsTreeDashboardEntity key={panel.i} widget={panel} data-grid={panel} />
                    ))}
                </ReactGridLayout>
            )}
        </div>
    );
};

export default memo(CallsTreeDashboard);
