import type { RootState } from '@app/store/types';
import { createDraftSafeSelector, createSlice, type PayloadAction } from '@reduxjs/toolkit';

export type ColumnWidthsMap = {
    [key: string | number]: number;
};

export type AppStateModel = {
    language: string;
    siderCollapsed: boolean;

    callsTable?: {
        columnsWidth?: ColumnWidthsMap;
    };
};

export const appDataInitialState: AppStateModel = {
    language: 'en-US',
    siderCollapsed: true,
    callsTable: {
        columnsWidth: {},
    },
};

const slice = createSlice({
    name: 'appState',
    initialState: appDataInitialState,
    reducers: {
        changeLanguage: (state, action: PayloadAction<AppStateModel['language']>) => {
            state.language = action.payload;
        },
        toggleSiderCollapsed(state) {
            state.siderCollapsed = !state.siderCollapsed;
        },

        setCallsColumnWidths(state, { payload }: PayloadAction<ColumnWidthsMap>) {
            state.callsTable ??= {};
            state.callsTable.columnsWidth = payload;
        },
    },
});

// Actions
export const appDataActions = {
    ...slice.actions,
};

// Selectors
const selectAppDataState = (state: RootState) => state.appState;
export const selectLanguage = createDraftSafeSelector(selectAppDataState, state => state.language);
export const selectSiderCollapsed = createDraftSafeSelector(selectAppDataState, state => state.siderCollapsed);
export const selectCallsColumnWidths = createDraftSafeSelector(
    selectAppDataState,
    state => state.callsTable?.columnsWidth
);
export const appStateReducer = slice.reducer;
