import CallsControls from '@app/features/cdt/calls/calls-controls';
import CallsChart from '@app/features/cdt/calls/calls-chart';
import { withCallsStore } from '@app/features/cdt/calls/calls-store';
import CallsTable from '@app/features/cdt/calls/calls-table';
import useCallsFetchArg from '@app/features/cdt/calls/use-calls-fetch-arg';
import { ContentCard } from '@netcracker/cse-ui-components';
import type { FC } from 'react';

const CallsContainer: FC = () => {
    const [, { notReady }] = useCallsFetchArg();

    return (
        <ContentCard title="Calls" titleClassName="ux-typography-18px-medium" extra={!notReady && <CallsControls />}>
            <CallsChart />
            <CallsTable />
        </ContentCard>
    );
};

export default withCallsStore(CallsContainer);
