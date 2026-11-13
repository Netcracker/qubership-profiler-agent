import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { PodsResponse } from '../api/types';
import { usePods } from './use-pods';

const fetchPods = vi.hoisted(() => vi.fn());
vi.mock('../api/endpoints', () => ({ fetchPods }));

// A deferred response that ignores the abort signal, like a response whose
// body was already read when the hook aborted.
function deferred() {
  let resolve!: (res: PodsResponse) => void;
  const promise = new Promise<PodsResponse>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

function podsAt(restartMs: number): PodsResponse {
  return {
    pods: [
      {
        namespace: 'payments',
        service: 'billing',
        pod: `billing-${restartMs}`,
        restart_time_ms: restartMs,
        time_min_ms: restartMs,
        time_max_ms: restartMs + 1,
      },
    ],
    partial: false,
    partial_reasons: [],
  };
}

afterEach(() => fetchPods.mockReset());

describe('usePods', () => {
  it('ignores a response for a window it has already left', async () => {
    const first = deferred();
    const second = deferred();
    fetchPods.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);

    const { result, rerender } = renderHook(({ from, to }) => usePods(from, to), {
      initialProps: { from: 1_000, to: 2_000 },
    });
    rerender({ from: 3_000, to: 4_000 });

    await act(async () => second.resolve(podsAt(3_000)));
    await act(async () => first.resolve(podsAt(1_000)));

    const state = result.current.state;
    expect(state.kind).toBe('ready');
    if (state.kind !== 'ready') return;
    expect(state.namespaces[0]?.services[0]?.pods[0]?.pod).toBe('billing-3000');
  });
});
