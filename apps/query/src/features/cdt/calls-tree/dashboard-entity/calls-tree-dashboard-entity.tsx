import {
    forwardRef,
    memo,
    type ForwardRefExoticComponent,
    type PropsWithChildren,
    type RefAttributes,
    type ReactNode,
} from 'react';
import { type Widget } from '@app/store/slices/calls-tree-context-slices';
import FrameGraphEntity from './calls-tree-dashboard-entity-frame-graph';
import CallsTreeEntity from './calls-tree-dashboard-entity-calls-tree';
import StatsEntity from './calls-tree-dashboard-entity-stats';

interface CallsTreeEntityProps {
    widget: Widget;
}

export type CallsTreeEntityModel = ForwardRefExoticComponent<
    PropsWithChildren<CallsTreeEntityProps> & RefAttributes<HTMLDivElement>
>;

const CallsTreeDashboardEntity: CallsTreeEntityModel = forwardRef(({ widget, children, ...otherProps }, ref) => {
    let component: ReactNode;
    switch (widget.i) {
        case 'frame-graph':
            component = <FrameGraphEntity />;
            break;
        case 'calls-tree':
            component = <CallsTreeEntity />;
            break;
        case 'stats':
            component = <StatsEntity />;
            break;
    }
    return (
        <div ref={ref} {...otherProps}>
            {component}
            {children}
        </div>
    );
});

CallsTreeDashboardEntity.displayName = 'CallsTreeDashboardEntity';

export default memo(CallsTreeDashboardEntity);
