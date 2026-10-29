import { renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { useLoadable } from './use-loadable';

describe('useLoadable', () => {
  it('reports a load that rejects with something other than an Error as its text', async () => {
    const { result } = renderHook(() => useLoadable('key', () => Promise.reject('offline')));
    await waitFor(() => expect(result.current.state).toEqual({ kind: 'error', message: 'offline' }));
  });
});
