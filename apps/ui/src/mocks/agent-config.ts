import { HttpResponse, http } from 'msw';

import { AGENT_STATUSES, DEFAULT_SCOPE } from '../agents/types';
import type {
  AgentConfigAgent,
  AgentConfigResponse,
  AgentConfigSaveRequest,
  AgentConfigScope,
  AgentConfigScopeKind,
  AgentConfigSettings,
  AgentStartupValue,
  AgentStatus,
} from '../agents/types';
import { badRequest, pace, problem } from './respond';

// The only implementation of the agent-configuration draft API (plan
// 2026-10-13-ui-redesign §4.7). There is no backend to mirror, so the shapes
// in agents/types.ts and the behavior here are the draft itself.
//
// State lives in memory and is lost on reload. Nothing runs on a timer: an
// agent's status is derived from the clock when a request reads it, which
// keeps the data deterministic and lets tests move time with a fake Date.

/** How long after a save the first agent reports the new revision. */
export const AGENT_APPLY_DELAY_MS = 4000;
/** Agents report one step apart, in three waves, so a poll sees the rollout progress. */
export const AGENT_APPLY_STAGGER_MS = 1500;
const APPLY_WAVES = 3;

const SEED_REVISION = 14;
const PENDING_DETAIL = 'Waiting for the agent to pull the new revision.';

// Every plugin the agents know. The XML lists enabled plugins in this order.
const PLUGINS = [
  'spring',
  'springrest',
  'tomcat_http',
  'tomcat10_http',
  'jetty_http',
  'undertow_http',
  'postgresql',
  'mysql',
  'cassandra',
  'rabbitmq',
  'activemq',
  'apache_httpclient',
  'java_http_client',
  'jackson',
  'quartz',
  'opentelemetry',
  'jaeger',
  'brave',
  'elasticsearch',
  'opensearch',
  'couchbase',
  'vertx',
];

const STARTUP: AgentStartupValue[] = [
  {
    label: 'Collector address',
    value: 'profiler-collector.profiler:1715',
    source: 'REMOTE_DUMP_HOST, REMOTE_DUMP_PORT_PLAIN',
  },
  { label: 'Trace buffer size', value: '4096', source: '-Dcom.netcracker.profiler.util.LocalBuffer.SIZE' },
  { label: 'Agent log level', value: 'INFO', source: 'ESC_LOG_LEVEL' },
  { label: 'Config file on the pod', value: '/app/diag/config/_config.xml', source: '-Dprofiler.config' },
];

const clone = <T>(v: T): T => JSON.parse(JSON.stringify(v)) as T;

function seedSettings(): Record<string, AgentConfigSettings> {
  const base: AgentConfigSettings = {
    enabled: true,
    min_duration_ms: 1,
    include: [{ pattern: 'com.acme.**', min_method_size: 50 }],
    exclude: ['com.netcracker.profiler.**'],
    plugins: {
      spring: true,
      springrest: true,
      tomcat_http: true,
      postgresql: true,
      rabbitmq: true,
      apache_httpclient: true,
      jackson: false,
      quartz: true,
      opentelemetry: true,
    },
    params: [
      { name: 'request.id', index: true, list: true },
      { name: 'trace.id', index: true, list: true },
      { name: 'call.red', index: true, list: false },
      { name: 'call.idle', index: true, list: false },
      { name: 'exception.class', index: false, list: false },
    ],
    max_age: '8 days',
    max_size: '2 Gb',
  };
  const orders = clone(base);
  orders.include = [
    { pattern: 'com.acme.orders.**', min_method_size: 20 },
    { pattern: 'com.acme.common.**', min_method_size: 50 },
  ];
  orders.exclude.push('com.acme.orders.generated.**');
  orders.params.push({ name: 'order.id', index: true, list: true });
  const staging = clone(base);
  staging.enabled = false;
  return { [DEFAULT_SCOPE]: base, 'prod-orders/order-processing': orders, 'staging-orders': staging };
}

// Service scope, pod, agent version, seed state, seconds since the last report.
// One agent is failed and one runs a version that cannot pull config; the
// pending one applies shortly after the mock starts.
type SeedState = 'applied' | 'failed' | 'pending' | 'unsupported';
const SEED_AGENTS: readonly (readonly [string, string, string, SeedState, number])[] = [
  ['prod-orders/order-processing', 'order-processing-7c9d4f6b8-x2k9p', '3.2.1', 'applied', 18],
  ['prod-orders/order-processing', 'order-processing-7c9d4f6b8-m4tqz', '3.2.1', 'failed', 113],
  ['prod-orders/order-processing', 'order-processing-7c9d4f6b8-b7hvn', '3.1.4', 'pending', 5],
  ['prod-orders/order-api-gateway', 'order-api-gateway-5f8b7c6d9-q8wlk', '3.2.1', 'applied', 29],
  ['prod-orders/order-api-gateway', 'order-api-gateway-5f8b7c6d9-z5nxd', '3.2.1', 'applied', 12],
  ['prod-orders/inventory-sync', 'inventory-sync-6d7c8b9f4-k2jtr', '3.2.1', 'applied', 48],
  ['prod-orders/inventory-sync', 'inventory-sync-6d7c8b9f4-p9gfc', '3.2.1', 'applied', 10],
  ['prod-billing/billing-core', 'billing-core-8b6d5c7f9-h4wsm', '3.2.1', 'applied', 33],
  ['prod-billing/billing-core', 'billing-core-8b6d5c7f9-v6rbx', '3.2.1', 'applied', 21],
  ['prod-billing/invoice-renderer', 'invoice-renderer-9c5f7d8b6-t3lqn', '3.2.1', 'applied', 16],
  ['prod-billing/invoice-renderer', 'invoice-renderer-9c5f7d8b6-d8kzj', '3.2.1', 'applied', 42],
  ['staging-orders/order-processing', 'order-processing-5b8c7d6f9-g7mxw', '3.2.1', 'applied', 57],
  ['staging-orders/order-api-gateway', 'order-api-gateway-6c9d8b7f5-n5pvh', '2.9.0', 'unsupported', 160],
];
const FAIL_DETAIL = 'Class reload failed for 2 of 148 classes. The previous config stays active.';
const UNSUPPORTED_DETAIL = 'This agent version cannot pull config. It uses the config file on the pod.';

// --- Store ---

interface StoredConfig {
  revision: number;
  settings: AgentConfigSettings;
  rawXml: string | null;
}

interface MockAgent {
  /** `<namespace>/<service>`. */
  scope: string;
  pod: string;
  version: string;
  /** False for an agent version that cannot pull config. */
  managed: boolean;
  /** An agent with this set fails every revision it is given. */
  alwaysFails: boolean;
  revision: number | null;
  target: number | null;
  /** When the agent reports on `target`; null once it has. */
  applyAtMs: number | null;
  failed: boolean;
  lastReportMs: number;
}

interface Store {
  /** Own configs by scope id. `default` is always present. */
  configs: Map<string, StoredConfig>;
  agents: MockAgent[];
  /** Tree order: `default`, then each namespace followed by its services. */
  scopeIds: string[];
  /** The last revision handed out, across all scopes. */
  lastRevision: number;
}

function createStore(nowMs: number): Store {
  const configs = new Map<string, StoredConfig>();
  for (const [scope, settings] of Object.entries(seedSettings())) {
    configs.set(scope, { revision: SEED_REVISION, settings, rawXml: null });
  }
  const agents = SEED_AGENTS.map(([scope, pod, version, state, agoS]): MockAgent => {
    const managed = state !== 'unsupported';
    const behind = state === 'failed' || state === 'pending';
    return {
      scope,
      pod,
      version,
      managed,
      alwaysFails: state === 'failed',
      revision: managed ? SEED_REVISION - (behind ? 1 : 0) : null,
      target: managed ? SEED_REVISION : null,
      applyAtMs: state === 'pending' ? nowMs + AGENT_APPLY_DELAY_MS : null,
      failed: state === 'failed',
      lastReportMs: nowMs - agoS * 1000,
    };
  });
  const scopeIds = [DEFAULT_SCOPE];
  for (const namespace of new Set(agents.map((a) => parentOf(a.scope)))) {
    scopeIds.push(namespace, ...new Set(agents.filter((a) => parentOf(a.scope) === namespace).map((a) => a.scope)));
  }
  return { configs, agents, scopeIds, lastRevision: SEED_REVISION };
}

let store: Store | null = null;

function current(nowMs: number): Store {
  store ??= createStore(nowMs);
  settle(store, nowMs);
  return store;
}

/** Returns the mock to its seed state. For tests, which share one module instance. */
export function resetAgentConfigMock(): void {
  store = null;
}

// --- Scopes and inheritance ---

function parentOf(scope: string): string {
  const slash = scope.indexOf('/');
  return slash === -1 ? DEFAULT_SCOPE : scope.slice(0, slash);
}

function kindOf(scope: string): AgentConfigScopeKind {
  if (scope === DEFAULT_SCOPE) return 'default';
  return scope.includes('/') ? 'service' : 'namespace';
}

/** The scope's own config, else its namespace's, else Default. */
function ownerOf(s: Store, scope: string): string {
  let id = scope;
  while (!s.configs.has(id)) id = parentOf(id);
  return id;
}

function agentsIn(s: Store, scope: string): MockAgent[] {
  return s.agents.filter((a) => scope === DEFAULT_SCOPE || a.scope === scope || a.scope.startsWith(`${scope}/`));
}

// --- Agents ---

/** Lets every agent whose report time has passed report: applied, or failed for the one that always fails. */
function settle(s: Store, nowMs: number): void {
  for (const a of s.agents) {
    if (a.applyAtMs === null || nowMs < a.applyAtMs) continue;
    if (a.alwaysFails) a.failed = true;
    else a.revision = a.target;
    a.lastReportMs = a.applyAtMs;
    a.applyAtMs = null;
  }
}

/** Hands each managed agent the revision of the config now in effect for it and makes it pending. */
function rollOut(s: Store, agents: MockAgent[], nowMs: number): void {
  agents
    .filter((a) => a.managed)
    .forEach((a, i) => {
      a.target = s.configs.get(ownerOf(s, a.scope))?.revision ?? null;
      a.failed = false;
      a.applyAtMs = nowMs + AGENT_APPLY_DELAY_MS + (i % APPLY_WAVES) * AGENT_APPLY_STAGGER_MS;
    });
}

function statusOf(a: MockAgent): AgentStatus {
  if (!a.managed) return 'unsupported';
  if (a.applyAtMs !== null) return 'pending';
  return a.failed ? 'failed' : 'applied';
}

const STATUS_DETAIL: Record<AgentStatus, string> = {
  applied: '',
  pending: PENDING_DETAIL,
  failed: FAIL_DETAIL,
  unsupported: UNSUPPORTED_DETAIL,
};

function agentRow(a: MockAgent): AgentConfigAgent {
  const status = statusOf(a);
  return {
    namespace: parentOf(a.scope),
    service: a.scope.slice(a.scope.indexOf('/') + 1),
    pod: a.pod,
    agent_version: a.version,
    status,
    revision: a.revision,
    target_revision: a.target,
    detail: STATUS_DETAIL[status],
    last_report_ms: a.lastReportMs,
  };
}

function scopeRow(s: Store, id: string): AgentConfigScope {
  const agents = Object.fromEntries(AGENT_STATUSES.map((k) => [k, 0])) as Record<AgentStatus, number>;
  for (const a of agentsIn(s, id)) agents[statusOf(a)] += 1;
  return { id, kind: kindOf(id), has_own_config: s.configs.has(id), agents };
}

// --- XML ---

const escapeXml = (v: string | number | boolean): string =>
  String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

/**
 * Renders settings into the `_config.xml` agents receive. Plugins missing from
 * {@link PLUGINS} are dropped, and exclusion rules precede inclusion rules.
 */
export function renderConfigXml(settings: AgentConfigSettings): string {
  const x = escapeXml;
  const plugins = PLUGINS.filter((p) => settings.plugins[p] === true).join(',');
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<profiler-configuration>',
    '    <properties>',
    `        <property name="profiler.enabled" value="${x(settings.enabled)}"/>`,
    `        <property name="profiler.minimal-logged-duration" value="${x(settings.min_duration_ms)}"/>`,
    `        <property name="profiler.plugins" value="${x(plugins)}"/>`,
    '    </properties>',
    `    <log-retention max-age="${x(settings.max_age)}" max-size="${x(settings.max_size)}"/>`,
    '    <parameters>',
    ...settings.params.map(
      (p, i) =>
        `        <parameter name="${x(p.name)}" index="${x(p.index)}" list="${x(p.list)}" order="${(i + 1) * 50}"/>`,
    ),
    '    </parameters>',
    '    <ruleset>',
    ...settings.exclude.flatMap((pattern) => [
      '        <rule>',
      `            <class>${x(pattern)}</class>`,
      '            <do-not-profile/>',
      '        </rule>',
    ]),
    ...settings.include.flatMap((rule) => [
      '        <rule>',
      `            <class>${x(rule.pattern)}</class>`,
      `            <minimum-method-size>${x(rule.min_method_size)}</minimum-method-size>`,
      '        </rule>',
    ]),
    '    </ruleset>',
    '</profiler-configuration>',
  ].join('\n');
}

// --- Request parsing ---

function settingsError(v: unknown): string {
  if (typeof v !== 'object' || v === null) return 'settings is required';
  const s = v as Partial<AgentConfigSettings>;
  if (typeof s.enabled !== 'boolean') return 'settings.enabled must be a boolean';
  if (!Number.isInteger(s.min_duration_ms) || (s.min_duration_ms ?? -1) < 0) {
    return 'settings.min_duration_ms must be a non-negative integer';
  }
  for (const name of ['include', 'exclude', 'params'] as const) {
    if (!Array.isArray(s[name])) return `settings.${name} must be an array`;
  }
  if (typeof s.plugins !== 'object' || s.plugins === null) return 'settings.plugins must be an object';
  for (const name of ['max_age', 'max_size'] as const) {
    if (typeof s[name] !== 'string') return `settings.${name} must be a string`;
  }
  return '';
}

async function jsonBody(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const body: unknown = await request.json();
    return typeof body === 'object' && body !== null ? (body as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

// MSW hands the path parameter over already percent-decoded, so `%2F` arrives
// as the slash of `<namespace>/<service>`.
function scopeOrProblem(s: Store, raw: unknown): string | Response {
  const scope = String(raw);
  if (s.scopeIds.includes(scope)) return scope;
  return problem({
    type: 'about:blank',
    title: 'scope not found',
    status: 404,
    code: 'scope_not_found',
    detail: `no namespace or service matches the scope ${scope}`,
  });
}

function configBody(s: Store, scope: string): AgentConfigResponse {
  const owner = ownerOf(s, scope);
  const config = s.configs.get(owner);
  if (config === undefined) throw new Error(`mock store lost the config of ${owner}`);
  return {
    scope,
    owner,
    revision: config.revision,
    settings: clone(config.settings),
    raw_xml: config.rawXml,
    startup: STARTUP,
    available_plugins: PLUGINS,
  };
}

// --- Handlers ---

const BASE = '/api/v1/agent-config';

// `scopes` and `render` are registered before `:scope` so that the fixed path
// words win.
export const agentConfigHandlers = [
  http.get(`${BASE}/scopes`, async () => {
    await pace();
    const s = current(Date.now());
    return HttpResponse.json({ scopes: s.scopeIds.map((id) => scopeRow(s, id)) });
  }),

  http.post(`${BASE}/render`, async ({ request }) => {
    await pace();
    const body = await jsonBody(request);
    const invalid = settingsError(body?.['settings']);
    if (invalid !== '') return badRequest(invalid);
    return HttpResponse.json({ xml: renderConfigXml(body?.['settings'] as AgentConfigSettings) });
  }),

  http.get(`${BASE}/:scope/agents`, async ({ params }) => {
    await pace();
    const s = current(Date.now());
    const scope = scopeOrProblem(s, params['scope']);
    if (typeof scope !== 'string') return scope;
    return HttpResponse.json({ agents: agentsIn(s, scope).map(agentRow) });
  }),

  http.get(`${BASE}/:scope`, async ({ params }) => {
    await pace();
    const s = current(Date.now());
    const scope = scopeOrProblem(s, params['scope']);
    if (typeof scope !== 'string') return scope;
    return HttpResponse.json(configBody(s, scope));
  }),

  http.put(`${BASE}/:scope`, async ({ params, request }) => {
    await pace();
    const nowMs = Date.now();
    const s = current(nowMs);
    const scope = scopeOrProblem(s, params['scope']);
    if (typeof scope !== 'string') return scope;

    const body = (await jsonBody(request)) as Partial<AgentConfigSaveRequest> | null;
    if (!Number.isInteger(body?.expected_revision)) return badRequest('expected_revision must be an integer');
    const invalid = settingsError(body?.settings);
    if (invalid !== '') return badRequest(invalid);
    const rawXml = body?.raw_xml ?? null;
    if (rawXml !== null && typeof rawXml !== 'string') return badRequest('raw_xml must be a string or null');

    const inEffect = s.configs.get(ownerOf(s, scope))?.revision;
    if (body?.expected_revision !== inEffect) {
      return problem({
        type: 'about:blank',
        title: 'revision conflict',
        status: 409,
        code: 'revision_conflict',
        detail: `scope ${scope} is at revision ${inEffect}, and the request expected ${body?.expected_revision}`,
      });
    }

    s.lastRevision += 1;
    s.configs.set(scope, { revision: s.lastRevision, settings: clone(body?.settings as AgentConfigSettings), rawXml });
    rollOut(
      s,
      s.agents.filter((a) => ownerOf(s, a.scope) === scope),
      nowMs,
    );
    return HttpResponse.json(configBody(s, scope));
  }),

  http.delete(`${BASE}/:scope`, async ({ params }) => {
    await pace();
    const nowMs = Date.now();
    const s = current(nowMs);
    const scope = scopeOrProblem(s, params['scope']);
    if (typeof scope !== 'string') return scope;
    if (scope === DEFAULT_SCOPE) return badRequest('the default scope has no config to inherit');

    // Deleting a config the scope does not have changes nothing, so no agent
    // goes pending.
    if (s.configs.has(scope)) {
      const affected = s.agents.filter((a) => ownerOf(s, a.scope) === scope);
      s.configs.delete(scope);
      rollOut(s, affected, nowMs);
    }
    return HttpResponse.json(configBody(s, scope));
  }),
];
