import { memo, type FC } from 'react';
import CallsTreeContextProvider, { type CallsTreeContextModel } from './calls-tree-context';
import classNames from './calls-tree-page.module.scss';
import CallsTreeDashboard from './calls-tree-dashboard';
import CallsTreeHeader from './calls-tree-context-header';
import { UxLayout } from '@netcracker/ux-react';

type CallsTreeOverlayProps = CallsTreeContextModel & {
    leftExtraHeader?: React.ReactNode
    rightExtraHeader?: React.ReactNode
};

const CallsTreeOverlay: FC<CallsTreeOverlayProps> = ({leftExtraHeader, rightExtraHeader, ...contextProps}) => {
    return (
        <CallsTreeContextProvider {...contextProps}>
            <section className={classNames.page}>
                <CallsTreeHeader leftExtra={leftExtraHeader} rightExtra={rightExtraHeader}/>
                <UxLayout.Separator offset={0} color="#D5DCE3" />
                <CallsTreeDashboard />
            </section>
        </CallsTreeContextProvider>
    );
};

export default memo(CallsTreeOverlay);
