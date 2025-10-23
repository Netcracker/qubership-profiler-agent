import type { AppDispatch, RootState } from '@app/store/types';
import { type TypedUseSelectorHook, useDispatch, useSelector } from 'react-redux';

// Typed hooks.

export const useAppDispatch = () => useDispatch<AppDispatch>();
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;
