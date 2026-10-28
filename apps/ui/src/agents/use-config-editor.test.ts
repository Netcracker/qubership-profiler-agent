import { act, renderHook, waitFor } from '@testing-library/react';
import { HttpResponse, http } from 'msw';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { resetAgentConfigMock } from '../mocks/agent-config';
import { server } from '../mocks/node';
import { useConfigEditor } from './use-config-editor';

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }));
afterEach(() => {
  server.resetHandlers();
  server.events.removeAllListeners();
  resetAgentConfigMock();
});
afterAll(() => server.close());

/**
 * Holds every write of the given method until `release` is called. The held
 * request then falls through to the mock, which answers it as usual.
 */
function holdWrites(method: 'put' | 'delete', status?: number): { release: () => void } {
  let release = (): void => {};
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  server.use(
    http[method]('/api/v1/agent-config/:scope', async () => {
      await released;
      return status === undefined ? undefined : new HttpResponse(null, { status });
    }),
  );
  return { release };
}

async function loadedEditor(scope: string) {
  const hook = renderHook(({ id }) => useConfigEditor(id), { initialProps: { id: scope } });
  await waitFor(() => expect(hook.result.current.config.kind).toBe('ready'));
  return hook;
}

describe('useConfigEditor', () => {
  it('sends nothing on apply before the config has loaded', async () => {
    let writes = 0;
    server.events.on('request:start', ({ request }) => {
      if (request.method === 'PUT') writes += 1;
    });
    const { result } = renderHook(() => useConfigEditor('prod-billing'));
    expect(result.current.config.kind).toBe('loading');

    let applied: boolean | undefined;
    await act(async () => {
      applied = await result.current.apply();
    });

    expect(applied).toBe(false);
    expect(writes).toBe(0);
    expect(result.current.saving).toBe(false);
  });

  it('keeps an edit made while the apply is on its way', async () => {
    const put = holdWrites('put');
    const { result } = await loadedEditor('prod-billing');
    const loaded = result.current.settings;
    if (loaded === null) throw new Error('the settings did not load');

    act(() => result.current.editSettings({ ...loaded, enabled: false }));
    let applying: Promise<boolean> = Promise.resolve(false);
    act(() => {
      applying = result.current.apply();
    });
    expect(result.current.saving).toBe(true);
    act(() => result.current.editSettings({ ...loaded, enabled: false, min_duration_ms: 77 }));

    put.release();
    let applied: boolean | undefined;
    await act(async () => {
      applied = await applying;
    });

    expect(applied).toBe(true);
    expect(result.current.saving).toBe(false);
    expect(result.current.config).toMatchObject({
      kind: 'ready',
      data: { revision: 15, settings: { enabled: false } },
    });
    expect(result.current.settings).toMatchObject({ enabled: false, min_duration_ms: 77 });
    expect(result.current.dirty).toBe(true);
  });

  it('leaves the open scope alone when a write of a scope left behind answers', async () => {
    const del = holdWrites('delete');
    const { result, rerender } = await loadedEditor('prod-orders/order-processing');

    let reverting: Promise<boolean> = Promise.resolve(true);
    act(() => {
      reverting = result.current.revertToInherited();
    });
    rerender({ id: 'prod-billing' });
    await waitFor(() => expect(result.current.config).toMatchObject({ data: { scope: 'prod-billing' } }));
    expect(result.current.saving).toBe(false);

    del.release();
    let reverted: boolean | undefined;
    await act(async () => {
      reverted = await reverting;
    });

    expect(reverted).toBe(false);
    expect(result.current.config).toMatchObject({ kind: 'ready', data: { scope: 'prod-billing', revision: 14 } });
    expect(result.current.error).toBeNull();
  });

  it('keeps showing the save of the open scope when a scope left behind answers', async () => {
    const del = holdWrites('delete');
    const put = holdWrites('put');
    const { result, rerender } = await loadedEditor('prod-orders/order-processing');
    let reverting: Promise<boolean> = Promise.resolve(true);
    act(() => {
      reverting = result.current.revertToInherited();
    });

    rerender({ id: 'prod-billing' });
    await waitFor(() => expect(result.current.config).toMatchObject({ data: { scope: 'prod-billing' } }));
    const loaded = result.current.settings;
    if (loaded === null) throw new Error('the settings did not load');
    act(() => result.current.editSettings({ ...loaded, enabled: false }));
    let applying: Promise<boolean> = Promise.resolve(false);
    act(() => {
      applying = result.current.apply();
    });

    del.release();
    await act(async () => {
      await reverting;
    });
    expect(result.current.saving).toBe(true);

    put.release();
    await act(async () => {
      await applying;
    });
    expect(result.current.saving).toBe(false);
  });

  it('shows no failure of a scope left behind', async () => {
    const put = holdWrites('put', 503);
    const { result, rerender } = await loadedEditor('prod-billing');
    const loaded = result.current.settings;
    if (loaded === null) throw new Error('the settings did not load');
    act(() => result.current.editSettings({ ...loaded, enabled: false }));

    let applying: Promise<boolean> = Promise.resolve(true);
    act(() => {
      applying = result.current.apply();
    });
    rerender({ id: 'staging-orders' });

    put.release();
    let applied: boolean | undefined;
    await act(async () => {
      applied = await applying;
    });

    expect(applied).toBe(false);
    expect(result.current.error).toBeNull();
    expect(result.current.conflict).toBe(false);
  });
});
