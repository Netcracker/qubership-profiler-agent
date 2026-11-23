import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { ConfigResponse } from './types';
import { useConfig } from './use-config';

const fetchConfig = vi.hoisted(() => vi.fn());
vi.mock('./endpoints', () => ({ fetchConfig }));

// A deferred response that ignores the abort signal, like a response whose
// body was already read when the hook aborted.
function deferred() {
  let resolve!: (res: ConfigResponse) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<ConfigResponse>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const config: ConfigResponse = { dumps_collector_url: 'http://collector:8080' };

afterEach(() => fetchConfig.mockReset());

describe('useConfig', () => {
  it('reports the fetched config', async () => {
    const response = deferred();
    fetchConfig.mockReturnValueOnce(response.promise);

    const { result } = renderHook(() => useConfig());
    expect(result.current.kind).toBe('loading');

    await act(async () => response.resolve(config));
    expect(result.current).toEqual({ kind: 'ready', config });
  });

  it('reports an error when the fetch fails', async () => {
    const response = deferred();
    fetchConfig.mockReturnValueOnce(response.promise);

    const { result } = renderHook(() => useConfig());
    await act(async () => response.reject(new Error('503')));
    expect(result.current.kind).toBe('error');
  });

  it('aborts the fetch on unmount and ignores a late response', async () => {
    const response = deferred();
    fetchConfig.mockReturnValueOnce(response.promise);

    const { result, unmount } = renderHook(() => useConfig());
    const signal = fetchConfig.mock.calls[0]?.[0] as AbortSignal;
    unmount();
    expect(signal.aborted).toBe(true);

    await act(async () => response.resolve(config));
    expect(result.current.kind).toBe('loading');
  });

  it('ignores a failure that arrives after unmount', async () => {
    const response = deferred();
    fetchConfig.mockReturnValueOnce(response.promise);

    const { result, unmount } = renderHook(() => useConfig());
    unmount();

    await act(async () => response.reject(new DOMException('aborted', 'AbortError')));
    expect(result.current.kind).toBe('loading');
  });
});
