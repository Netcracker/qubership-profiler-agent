import { useAppDispatch, useAppSelector } from '@app/store/hooks';
import {
    callsTreeContextDataAction,
    selectDashboardState,
    type Widget,
} from '@app/store/slices/calls-tree-context-slices';
import type { Layout } from 'react-grid-layout';

export function useDisableWidgetFunction() {
    const { panels } = useAppSelector(selectDashboardState);
    const dispatch = useAppDispatch();

    return (id: Widget['i']) => {
        dispatch(callsTreeContextDataAction.setLayout(panels.filter(panel => panel.i != id)));
    };
}

export function useEnableWidgetFunction() {
    const { panels } = useAppSelector(selectDashboardState);
    const dispatch = useAppDispatch();
    return (id: Widget['i'], layout?: Partial<Layout>) => {
        if (!panels.find(panel => panel.i == id)) {
            dispatch(
                callsTreeContextDataAction.setLayout(
                    panels.concat([
                        {
                            w: 0,
                            h: 0,
                            x: 0,
                            y: Math.max(...panels.map(panels => panels.y + panels.h)),
                            ...layout,
                            i: id,
                        },
                    ])
                )
            );
        }
    };
}
