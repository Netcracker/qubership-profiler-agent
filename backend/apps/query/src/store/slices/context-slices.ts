import { useAppSelector } from '@app/store/hooks';
import type { RootState } from '@app/store/types';
import { type PayloadAction, createDraftSafeSelector, createSlice } from '@reduxjs/toolkit';
import type { SortOrder } from 'antd/lib/table/interface';
import type { Key } from 'react';

type TreeState = {
    expandedKeys?: Key[];
};

type SliceState = {
    searchParamsApplied: boolean;

    calls: {
        page: number;
        maxPage: number;
        sortBy: string;
        sortOrder: SortOrder;
    };

    pods: {
        query: string;
    };

    tree: TreeState;
};

const initialState: SliceState = {
    tree: {},
    calls: {
        page: 1,
        maxPage: -1,
        sortBy: 'ts',
        sortOrder: 'ascend',
    },
    pods: {
        query: '',
    },
    searchParamsApplied: true,
};

const slice = createSlice({
    name: 'contextData',
    initialState,
    reducers: {
        setExpandedKeys: (state, { payload }: PayloadAction<Key[]>) => {
            state.tree.expandedKeys = payload;
        },
        toFirstPage: state => {
            state.searchParamsApplied = false;
            state.calls.page = 1;
        },
        setSearchParamsApplied: (state, { payload }: PayloadAction<boolean>) => {
            state.searchParamsApplied = payload;
        },
        updateCallsTableState: (state, { payload }: PayloadAction<Partial<SliceState['calls']>>) => {
            state.calls.page = 1;
            state.calls = Object.assign(state.calls, payload);
        },
        setMaxPage: (state, { payload }: PayloadAction<number>) => {
            state.calls.maxPage = payload;
        },
        nextCallsPage: state => {
            if (state.calls.maxPage == -1 || state.calls.page < state.calls.maxPage) {
                state.calls.page++;
            }
        },
    },
});

export const selectContextState = (state: RootState) => state.contextData;
export const selectTreeState = createDraftSafeSelector(selectContextState, state => state.tree);
export const contextDataAction = {
    ...slice.actions,
};

export const selectCallsTableState = createDraftSafeSelector(selectContextState, state => state.calls);

// PODS
export const selectSearchParamsApplied = createDraftSafeSelector(
    selectContextState,
    state => state.searchParamsApplied
);

export const useSearchParamsApplied = () => useAppSelector(selectSearchParamsApplied);

export const contextDataReducer = slice.reducer;
