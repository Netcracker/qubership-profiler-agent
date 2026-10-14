import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  deleteAgentConfig,
  fetchAgentConfig,
  fetchAgentConfigAgents,
  fetchAgentConfigScopes,
  isAgentConfigEnabled,
  isRevisionConflict,
  renderAgentConfig,
  saveAgentConfig,
} from '../agents/api';
import type { AgentConfigSettings } from '../agents/types';
import { ApiError } from '../api/client';
import { fetchConfig } from '../api/endpoints';
import { AGENT_APPLY_DELAY_MS, AGENT_APPLY_STAGGER_MS, resetAgentConfigMock } from './agent-config';
import { server } from './node';

// The draft API has no backend, so these tests pin the behavior of the mock
// that stands in for one (plan 2026-10-13-ui-redesign §4.7).

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
beforeEach(() => resetAgentConfigMock());
afterEach(() => {
  server.resetHandlers();
  vi.useRealTimers();
});
afterAll(() => server.close());

const SETTINGS: AgentConfigSettings = {
  enabled: true,
  min_duration_ms: 5,
  include: [{ pattern: 'com.acme.**', min_method_size: 30 }],
  exclude: ['com.acme.generated.**'],
  // `no_such_plugin` is unknown to the agents and `jackson` is off; neither is rendered.
  plugins: { quartz: true, spring: true, jackson: false, no_such_plugin: true },
  params: [
    { name: 'request.id', index: true, list: true },
    { name: 'a&b', index: false, list: false },
  ],
  max_age: '3 days',
  max_size: '1 Gb',
};

const SETTINGS_XML = `<?xml version="1.0" encoding="UTF-8"?>
<profiler-configuration>
    <properties>
        <property name="profiler.enabled" value="true"/>
        <property name="profiler.minimal-logged-duration" value="5"/>
        <property name="profiler.plugins" value="spring,quartz"/>
    </properties>
    <log-retention max-age="3 days" max-size="1 Gb"/>
    <parameters>
        <parameter name="request.id" index="true" list="true" order="50"/>
        <parameter name="a&amp;b" index="false" list="false" order="100"/>
    </parameters>
    <ruleset>
        <rule>
            <class>com.acme.generated.**</class>
            <do-not-profile/>
        </rule>
        <rule>
            <class>com.acme.**</class>
            <minimum-method-size>30</minimum-method-size>
        </rule>
    </ruleset>
</profiler-configuration>`;

describe('scope inheritance', () => {
  it('resolves a scope to its own config, else its namespace, else Default', async () => {
    const own = await fetchAgentConfig('prod-orders/order-processing');
    expect(own.owner).toBe('prod-orders/order-processing');
    expect(own.settings.params.map((p) => p.name)).toContain('order.id');

    const fromNamespace = await fetchAgentConfig('staging-orders/order-processing');
    expect(fromNamespace.owner).toBe('staging-orders');
    expect(fromNamespace.settings.enabled).toBe(false);

    const fromDefault = await fetchAgentConfig('prod-orders/order-api-gateway');
    expect(fromDefault.owner).toBe('default');
    expect(fromDefault.settings).toEqual((await fetchAgentConfig('default')).settings);
  });

  it('lists scopes in tree order with own-config flags and agent counts', async () => {
    const { scopes } = await fetchAgentConfigScopes();
    expect(scopes.slice(0, 3).map((s) => [s.id, s.kind])).toEqual([
      ['default', 'default'],
      ['prod-orders', 'namespace'],
      ['prod-orders/order-processing', 'service'],
    ]);
    expect(scopes.filter((s) => s.has_own_config).map((s) => s.id)).toEqual([
      'default',
      'prod-orders/order-processing',
      'staging-orders',
    ]);
    expect(scopes[0]?.agents).toEqual({ applied: 10, pending: 1, failed: 1, unsupported: 1 });
  });

  it('answers 404 for a scope that no agent belongs to', async () => {
    const error = await fetchAgentConfig('no-such-namespace').catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(404);
    expect((error as ApiError).problem?.code).toBe('scope_not_found');
  });
});

describe('saving', () => {
  it('gives an inheriting scope its own config under a new revision', async () => {
    const before = await fetchAgentConfig('prod-billing/billing-core');
    const saved = await saveAgentConfig('prod-billing/billing-core', {
      expected_revision: before.revision,
      settings: SETTINGS,
    });
    expect(saved.owner).toBe('prod-billing/billing-core');
    expect(saved.revision).toBe(before.revision + 1);
    expect(saved.settings).toEqual(SETTINGS);
    // The sibling service still inherits from Default.
    expect((await fetchAgentConfig('prod-billing/invoice-renderer')).revision).toBe(before.revision);
  });

  it('rejects a save against a stale revision with 409 revision_conflict', async () => {
    const { revision } = await fetchAgentConfig('prod-billing');
    await saveAgentConfig('prod-billing', { expected_revision: revision, settings: SETTINGS });

    const error = await saveAgentConfig('prod-billing', {
      expected_revision: revision,
      settings: { ...SETTINGS, enabled: false },
    }).catch((e: unknown) => e);
    expect(isRevisionConflict(error)).toBe(true);
    expect((error as ApiError).status).toBe(409);
    // The losing save changed nothing.
    expect((await fetchAgentConfig('prod-billing')).settings.enabled).toBe(true);
  });

  it('returns a scope to the inherited config on delete', async () => {
    const inherited = await fetchAgentConfig('prod-billing');
    await saveAgentConfig('prod-billing', { expected_revision: inherited.revision, settings: SETTINGS });

    const afterDelete = await deleteAgentConfig('prod-billing');
    expect(afterDelete.owner).toBe('default');
    expect(afterDelete.revision).toBe(inherited.revision);
    expect(afterDelete.settings).toEqual(inherited.settings);
    const { scopes } = await fetchAgentConfigScopes();
    expect(scopes.find((s) => s.id === 'prod-billing')?.has_own_config).toBe(false);
  });

  it('refuses to delete the Default config', async () => {
    const error = await deleteAgentConfig('default').catch((e: unknown) => e);
    expect((error as ApiError).status).toBe(400);
  });
});

describe('XML', () => {
  it('renders a known settings object into _config.xml', async () => {
    expect(await renderAgentConfig(SETTINGS)).toBe(SETTINGS_XML);
  });

  it('keeps a hand-edited XML as an override until a save clears it', async () => {
    const handEdited = '<profiler-configuration><!-- edited by hand --></profiler-configuration>';
    const generated = await fetchAgentConfig('prod-billing');
    expect(generated.raw_xml).toBeNull();

    const saved = await saveAgentConfig('prod-billing', {
      expected_revision: generated.revision,
      settings: SETTINGS,
      raw_xml: handEdited,
    });
    expect(saved.raw_xml).toBe(handEdited);
    // The override belongs to the config, so an inheriting service gets it too.
    expect((await fetchAgentConfig('prod-billing/billing-core')).raw_xml).toBe(handEdited);

    const cleared = await saveAgentConfig('prod-billing', { expected_revision: saved.revision, settings: SETTINGS });
    expect(cleared.raw_xml).toBeNull();
  });
});

describe('agents', () => {
  const ROLLOUT_MS = AGENT_APPLY_DELAY_MS + 2 * AGENT_APPLY_STAGGER_MS;

  // Only the clock is faked: the mock derives agent status from Date.now(),
  // and real timers keep MSW's request handling running.
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'], now: new Date('2026-10-13T14:00:00Z') });
  });

  it('moves agents from pending to applied some time after a save', async () => {
    const { revision } = await fetchAgentConfig('prod-billing');
    const saved = await saveAgentConfig('prod-billing', { expected_revision: revision, settings: SETTINGS });

    const pending = (await fetchAgentConfigAgents('prod-billing')).agents;
    expect(pending).toHaveLength(4);
    for (const a of pending) {
      expect(a).toMatchObject({ status: 'pending', revision, target_revision: saved.revision });
    }

    vi.setSystemTime(Date.now() + AGENT_APPLY_DELAY_MS);
    const midway = (await fetchAgentConfigAgents('prod-billing')).agents.map((a) => a.status);
    expect(midway).toContain('applied');
    expect(midway).toContain('pending');

    vi.setSystemTime(Date.now() + ROLLOUT_MS);
    for (const a of (await fetchAgentConfigAgents('prod-billing')).agents) {
      expect(a).toMatchObject({ status: 'applied', revision: saved.revision, detail: '' });
    }
    const { scopes } = await fetchAgentConfigScopes();
    expect(scopes.find((s) => s.id === 'prod-billing')?.agents.applied).toBe(4);
  });

  it('keeps one failed and one unmanaged agent in the seed data', async () => {
    // The first request starts the mock, and with it the seed agent's pending period.
    const seed = (await fetchAgentConfigAgents('default')).agents;
    expect(seed.filter((a) => a.status === 'pending').map((a) => a.pod)).toEqual(['order-processing-7c9d4f6b8-b7hvn']);

    vi.setSystemTime(Date.now() + ROLLOUT_MS);
    const { agents } = await fetchAgentConfigAgents('default');
    expect(agents.filter((a) => a.status === 'failed').map((a) => a.pod)).toEqual(['order-processing-7c9d4f6b8-m4tqz']);
    const unmanaged = agents.filter((a) => a.status === 'unsupported');
    expect(unmanaged).toHaveLength(1);
    expect(unmanaged[0]).toMatchObject({ revision: null, target_revision: null, agent_version: '2.9.0' });
    expect(agents.filter((a) => a.status === 'pending')).toEqual([]);
  });

  it('fails the same agent again and leaves the unmanaged one alone after a save', async () => {
    const { revision } = await fetchAgentConfig('default');
    const saved = await saveAgentConfig('default', { expected_revision: revision, settings: SETTINGS });
    // Default owns every agent except the two scopes with their own config.
    const rollingOut = (await fetchAgentConfigAgents('default')).agents.filter(
      (a) => a.target_revision === saved.revision,
    );
    expect(rollingOut).toHaveLength(8);
    expect(rollingOut.map((a) => a.namespace)).not.toContain('staging-orders');
    expect(rollingOut.map((a) => a.service)).not.toContain('order-processing');

    const own = await fetchAgentConfig('prod-orders/order-processing');
    const resaved = await saveAgentConfig('prod-orders/order-processing', {
      expected_revision: own.revision,
      settings: own.settings,
    });
    vi.setSystemTime(Date.now() + ROLLOUT_MS);
    const failed = (await fetchAgentConfigAgents('prod-orders/order-processing')).agents.find(
      (a) => a.status === 'failed',
    );
    expect(failed).toMatchObject({ pod: 'order-processing-7c9d4f6b8-m4tqz', target_revision: resaved.revision });
    expect(failed?.revision).toBeLessThan(resaved.revision);
  });
});

describe('features.agent_config', () => {
  it('is on in the mock', async () => {
    expect(isAgentConfigEnabled(await fetchConfig())).toBe(true);
  });
});
