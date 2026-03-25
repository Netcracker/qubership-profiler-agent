import type { GetCallsTreeDataResp } from '@app/store/cdt-openapi';
import type { DashboardEntity } from '@app/store/slices/calls-tree-context-slices';
import { createContext, useContext, type FC, type PropsWithChildren, memo } from 'react';

export type CallsTreeData = {
    data?: GetCallsTreeDataResp;
    isFetching: boolean;
    isError: boolean;
};

export type CallsTreeContextModel = {
    callsTreeData: CallsTreeData;
    initialPanelState?: DashboardEntity[]
};

const callsTreeContext = createContext<CallsTreeContextModel>({
    callsTreeData: {
        isFetching: false,
        isError: true,
    },
});

const CallsTreeContextProvider: FC<PropsWithChildren<CallsTreeContextModel>> = ({children, ...value}) => {
    return <callsTreeContext.Provider value={value}>{children}</callsTreeContext.Provider>;
};

const useCallsTreeContext = () => useContext(callsTreeContext);

export const useCallsTreeData = () => useCallsTreeContext().callsTreeData;
export const useInitialPanelState = () => useCallsTreeContext().initialPanelState;

export default memo(CallsTreeContextProvider);
