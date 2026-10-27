import { act, renderHook } from '@testing-library/react';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { resetAgentConfigMock } from '../mocks/agent-config';
import { server } from '../mocks/node';
import { useConfigEditor } from './use-config-editor';

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  server.events.removeAllListeners();
  resetAgentConfigMock();
});
afterAll(() => server.close());

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
});
