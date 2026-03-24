import { store } from '@app/store/store';
import type { Action, ThunkAction } from '@reduxjs/toolkit';

export type AppDispatch = typeof store.dispatch;

export type RootState = ReturnType<typeof store.getState>;

export type AppThunk<ReturnType = void> = ThunkAction<ReturnType, RootState, unknown, Action<string>>;
