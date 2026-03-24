import { type FC } from 'react';
import classNames from './profiler.page.module.scss';
import CallsSideBar from '@app/features/cdt/sidebar/calls-sidebar';
import ControlsCard from '@app/features/cdt/controls/controls-card';
import { Navigate, Outlet, Route, Routes } from 'react-router-dom';
import CallsContainer from '@app/features/cdt/calls/calls.container';
import PodsInfoContainer from '@app/features/cdt/pods-info/pods-info';
import HeapDumpsContainer from '@app/features/cdt/heap-dumps/heap-dumps.container';
import CallsTreeOverlay from '@app/features/cdt/calls-tree/calls-tree-overlay';
import { useGetCallsTreeDataQuery } from '@app/store/cdt-openapi';
import { CallsTreeBackToOverviewButton, CallsTreeDownloadButton } from '@app/features/cdt/calls-tree/calls-tree-tools';

const CallsOverlay = () => {
    return (
        <section className={classNames.page}>
            <CallsSideBar />
            <section className={classNames.pageContent}>
                <ControlsCard />
                <Outlet />
            </section>
        </section>
    );
};

const CallsTreePage = () => {
    const callsTreeData = useGetCallsTreeDataQuery();
    return (
        <CallsTreeOverlay
            callsTreeData={callsTreeData}
            leftExtraHeader={
                <CallsTreeBackToOverviewButton />
            }
            rightExtraHeader={
               <CallsTreeDownloadButton />
            }
        />
    );
};

const ProfilerPage: FC = () => {
    return (
        <Routes>
            <Route path="calls-tree" element={<CallsTreePage />} />
            <Route path="*" element={<CallsOverlay />}>
                <Route path="calls" element={<CallsContainer />} />
                <Route path="pods-info" element={<PodsInfoContainer />} />
                <Route path="heap-dumps" element={<HeapDumpsContainer />} />
                <Route path="*" element={<Navigate to="calls" />} />
            </Route>
        </Routes>
    );
};

export default ProfilerPage;
