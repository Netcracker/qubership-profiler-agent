import { __dev__, __prod__ } from '@app/constants/app.constants';
import { baseApi } from '@app/store/base-query';
import { openApi } from '@app/store/openapi-query';
import { appStateReducer } from '@app/store/slices/app-state.slice';
import { contextDataReducer } from '@app/store/slices/context-slices';
import { combineReducers, configureStore } from '@reduxjs/toolkit';
import { createLogger } from 'redux-logger';
import { FLUSH, PAUSE, PERSIST, PURGE, REGISTER, REHYDRATE, persistReducer, persistStore } from 'redux-persist';
import storage from 'redux-persist/lib/storage';
import { callsTreeContextDataReducer } from './slices/calls-tree-context-slices';

const logger = createLogger({
    collapsed: true,

    predicate: (_, action) => !action.type.includes('/subscriptions'),
});
export const devMiddlewares = __dev__ ? [logger] : [];

export const reducers = persistReducer(
    {
        key: 'APP_NAME',
        version: 1,
        storage,
        // Data from that slice will be stored in localStorage
        whitelist: ['appState'],
    },
    combineReducers({
        appState: appStateReducer,
        contextData: contextDataReducer,
        callsTreeContextData: callsTreeContextDataReducer,
        // reducer for RTQ
        [baseApi.reducerPath]: baseApi.reducer,
        [openApi.reducerPath]: openApi.reducer,
    })
);

const reduxDevtools =
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    __dev__ && (window as any).__REDUX_DEVTOOLS_EXTENSION__ && (window as any).__REDUX_DEVTOOLS_EXTENSION__();

export const store = configureStore({
    reducer: reducers,
    middleware: getDefaultMiddleware =>
        getDefaultMiddleware({
            serializableCheck: {
                ignoredActions: [FLUSH, PAUSE, PERSIST, PURGE, REGISTER, REHYDRATE],
            },
        }).concat([...devMiddlewares, baseApi.middleware, openApi.middleware]),
    devTools: __prod__,
    enhancers: [reduxDevtools].filter(it => it),
});

export const Persistor = persistStore(store);
