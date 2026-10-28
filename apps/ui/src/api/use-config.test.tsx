import { renderHook, waitFor } from '@testing-library/react';
import { act } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import * as endpoints from './endpoints';
import { useConfig } from './use-config';

afterEach(() => vi.restoreAllMocks());

describe('useConfig', () => {
  it('reports a failure with its message and fetches again on retry', async () => {
    const fetchConfig = vi
      .spyOn(endpoints, 'fetchConfig')
      .mockRejectedValueOnce('offline')
      .mockResolvedValueOnce({ dumps_collector_url: '' });
    const { result } = renderHook(() => useConfig());
    await waitFor(() => expect(result.current.kind).toBe('error'));
    const failed = result.current;
    if (failed.kind !== 'error') throw new Error('unreachable');
    expect(failed.message).toBe('offline');

    act(() => failed.retry());
    await waitFor(() => expect(result.current.kind).toBe('ready'));
    expect(fetchConfig).toHaveBeenCalledTimes(2);
  });
});
