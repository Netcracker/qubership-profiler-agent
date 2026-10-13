import { HttpResponse, http } from 'msw';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { ApiError } from '../api/client';
import { fetchConfig } from '../api/endpoints';
import { server } from '../mocks/node';
import { isAgentConfigEnabled, isRevisionConflict, saveAgentConfig } from './api';
import type { AgentConfigSettings } from './types';

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const SETTINGS: AgentConfigSettings = {
  enabled: true,
  min_duration_ms: 5,
  include: [{ pattern: 'com.acme.**', min_method_size: 30 }],
  exclude: ['com.acme.generated.**'],
  plugins: { quartz: true, spring: true, jackson: false },
  params: [{ name: 'request.id', index: true, list: true }],
  max_age: '3 days',
  max_size: '1 Gb',
};

describe('features.agent_config', () => {
  const configWith = (features: unknown) =>
    server.use(http.get('/api/v1/config', () => HttpResponse.json({ dumps_collector_url: '', features })));

  it('is on when the backend reports true', async () => {
    configWith({ agent_config: true });
    expect(isAgentConfigEnabled(await fetchConfig())).toBe(true);
  });

  it('defaults to off when the backend omits it', async () => {
    configWith(undefined);
    expect(isAgentConfigEnabled(await fetchConfig())).toBe(false);

    configWith({});
    expect(isAgentConfigEnabled(await fetchConfig())).toBe(false);
  });
});

describe('saveAgentConfig', () => {
  it('sends the scope as one path segment and the expected revision in the body', async () => {
    let seenPath = '';
    let seenBody: unknown = null;
    server.use(
      http.put('/api/v1/agent-config/:scope', async ({ request }) => {
        seenPath = new URL(request.url).pathname;
        seenBody = await request.json();
        return HttpResponse.json({});
      }),
    );

    await saveAgentConfig('prod-orders/order-processing', { expected_revision: 14, settings: SETTINGS });
    expect(seenPath).toBe('/api/v1/agent-config/prod-orders%2Forder-processing');
    expect(seenBody).toEqual({ expected_revision: 14, settings: SETTINGS });
  });

  it('recognizes a 409 by its problem code, not by its status alone', async () => {
    const answer409 = (code?: string) =>
      server.use(
        http.put('/api/v1/agent-config/:scope', () =>
          HttpResponse.json(
            { type: 'about:blank', title: 'conflict', status: 409, ...(code === undefined ? {} : { code }) },
            { status: 409, headers: { 'Content-Type': 'application/problem+json' } },
          ),
        ),
      );
    const save = () =>
      saveAgentConfig('default', { expected_revision: 1, settings: SETTINGS }).catch((e: unknown) => e);

    answer409('revision_conflict');
    const conflict = await save();
    expect(conflict).toBeInstanceOf(ApiError);
    expect(isRevisionConflict(conflict)).toBe(true);

    answer409();
    expect(isRevisionConflict(await save())).toBe(false);
  });
});
