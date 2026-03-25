import { getStaticCallsTreeData, getStaticInitialLayout } from '@app/components/static-hooks';
import CallsTreeOverlay from '@app/features/cdt/calls-tree/calls-tree-overlay';
import { memo } from 'react';

const StaticProfilerPage = () => {
    const callsTreeData = getStaticCallsTreeData();
    return <CallsTreeOverlay callsTreeData={callsTreeData} initialPanelState={getStaticInitialLayout()} />;
};

export default memo(StaticProfilerPage);
