import { __dev__, __prod__ } from '@app/constants/app.constants';
import { combineReducers, configureStore } from '@reduxjs/toolkit';
import { FLUSH, PAUSE, PERSIST, PURGE, REGISTER, REHYDRATE } from 'redux-persist';
import { appStateReducer } from './slices/app-state.slice';
import { contextDataReducer } from './slices/context-slices';
import { callsTreeContextDataReducer } from './slices/calls-tree-context-slices';

export const reducers = combineReducers({
    appState: appStateReducer,
    contextData: contextDataReducer,
    callsTreeContextData: callsTreeContextDataReducer,
});

const reduxDevtools =
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    __dev__ && (window as any).__REDUX_DEVTOOLS_EXTENSION__ && (window as any).__REDUX_DEVTOOLS_EXTENSION__();

export const staticStore = configureStore({
    reducer: reducers,
    middleware: getDefaultMiddleware =>
        getDefaultMiddleware({
            serializableCheck: {
                ignoredActions: [FLUSH, PAUSE, PERSIST, PURGE, REGISTER, REHYDRATE],
            },
        }),
    devTools: __prod__,
    enhancers: [reduxDevtools].filter(it => it),
});
