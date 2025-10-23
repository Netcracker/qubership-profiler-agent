import type { CallInfo } from '@app/store/cdt-openapi';
import { createStoreContext } from '@netcracker/cse-ui-components';
import { type FC, type Key } from 'react';

type CallsStoreModel = {
    graphCollapsed: boolean;

    durationFrom: number;

    columnsOrder: Key[];

    hiddenColumns: Key[];

    selectedCalls?: CallInfo[];
};
export const {
    Provider: CallsStoreProvider,
    useStore: useCallsStore,
    useStoreSelector: useCallsStoreSelector,
} = createStoreContext<CallsStoreModel>({
    graphCollapsed: true,
    durationFrom: 10,

    columnsOrder: [],
    hiddenColumns: ['suspend', 'queue-wait-time', 'tx', 'mem'],
    selectedCalls: [],
});
CallsStoreProvider.displayName = 'CallsStoreProvider';
// eslint-disable-next-line react/display-name
export const withCallsStore = (Component: FC) => () => {
    return (
        <CallsStoreProvider>
            <Component />
        </CallsStoreProvider>
    );
};
