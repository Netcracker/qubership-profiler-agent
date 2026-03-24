import { createDraftSafeSelector, createSlice, type PayloadAction } from '@reduxjs/toolkit';
import type { Layout } from 'react-grid-layout';
import type { RootState } from '../types';
import type { Key } from 'react';
import type { CallsTreeInfo } from '../cdt-openapi';

type WidgetType = 'frame-graph' | 'calls-tree' | 'stats';

export type Widget = {
    i: WidgetType;
};

export type DashboardEntity = Layout & Widget;

type Dashboard = {
    panels: DashboardEntity[];
};

type CallsTreePageState = {
    dashboard: Dashboard;
} & WidgetState;

type WidgetState = {
    callsTree: CallsTreeWidgetState;
    stats: StatsWidgetState;
};

type CallsTreeWidgetState = {
    hiddenColumns: Key[];
    columnsOrder: Key[];
};

type StatsWidgetState = {
    selectedRowId?: string;
    selectedRowTitle?: string;
};

const initialState: CallsTreePageState = {
    dashboard: {
        panels: [
            {
                i: 'frame-graph',
                x: 0,
                y: 0,
                w: 12,
                h: 1,
            },
            {
                i: 'calls-tree',
                x: 0,
                y: 0,
                w: 9,
                h: 2,
            },
            {
                i: 'stats',
                x: 9,
                y: 0,
                w: 3,
                h: 2,
            },
        ],
    },
    callsTree: {
        hiddenColumns: [],
        columnsOrder: [],
    },
    stats: {},
};

const slice = createSlice({
    name: 'callsTreeContextData',
    initialState,
    reducers: {
        setLayout: (state, { payload }: PayloadAction<Layout[]>) => {
            state.dashboard.panels = payload as DashboardEntity[];
        },
        setHiddenColumns: (state, { payload }: PayloadAction<Key[]>) => {
            state.callsTree.hiddenColumns = payload;
        },
        setColumnsOrder: (state, { payload }: PayloadAction<Key[]>) => {
            state.callsTree.columnsOrder = payload;
        },
        selectRow: (state, { payload }: PayloadAction<string[]>) => {
            state.stats.selectedRowId = payload.at(0);
            state.stats.selectedRowTitle = payload.at(1);
        },
        unselectRow: state => {
            state.stats.selectedRowTitle = undefined;
        },
    },
});

export const selectCallsTreeContextState = (state: RootState) => state.callsTreeContextData;

export const selectDashboardState = createDraftSafeSelector(selectCallsTreeContextState, state => state.dashboard);
export const selectCallsTreeState = createDraftSafeSelector(selectCallsTreeContextState, state => state.callsTree);
export const selectStatsState = createDraftSafeSelector(selectCallsTreeContextState, state => state.stats);

export const callsTreeContextDataAction = {
    ...slice.actions,
};

export const callsTreeContextDataReducer = slice.reducer;
