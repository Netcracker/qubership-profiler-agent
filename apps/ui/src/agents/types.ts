// Wire models of the agent-configuration draft API (/api/v1/agent-config).
// No backend implements it: the MSW mock in mocks/agent-config.ts is the only
// implementation, and the plan that delivers config to agents (item C2 of
// profiler-plan.md) may reshape every type here.

/**
 * Scope id: `default`, `<namespace>`, or `<namespace>/<service>`.
 *
 * A scope without its own config inherits from its namespace, and a namespace
 * without one inherits from `default`. In a URL path the id is one segment,
 * percent-encoded as a whole, so the slash travels as `%2F`
 * (`/api/v1/agent-config/prod-orders%2Forder-processing/agents`). The path
 * words `scopes` and `render` are taken, so no namespace can use those names.
 */
export type AgentConfigScopeId = string;

export const DEFAULT_SCOPE: AgentConfigScopeId = 'default';

export type AgentConfigScopeKind = 'default' | 'namespace' | 'service';

/**
 * How far an agent is with the revision assigned to it. `unsupported` marks an
 * agent version that cannot pull config and keeps the file on its pod; the UI
 * labels it "Not managed".
 */
export const AGENT_STATUSES = ['applied', 'pending', 'failed', 'unsupported'] as const;

export type AgentStatus = (typeof AGENT_STATUSES)[number];

/** One node of the scope tree. */
export interface AgentConfigScope {
  id: AgentConfigScopeId;
  kind: AgentConfigScopeKind;
  /** False when the scope inherits its config. Always true for `default`. */
  has_own_config: boolean;
  /** Agents in the scope and below it, counted by status. Every status is present. */
  agents: Record<AgentStatus, number>;
}

/**
 * GET /agent-config/scopes body. The list is flat and in tree order: `default`
 * first, then each namespace followed by its services. The id carries the
 * parent, so the list needs no nesting.
 */
export interface AgentConfigScopesResponse {
  scopes: AgentConfigScope[];
}

/** A class pattern to profile, such as `com.acme.orders.**`. */
export interface AgentConfigIncludeRule {
  pattern: string;
  /** The rule's `minimum-method-size`: smaller methods are not instrumented. */
  min_method_size: number;
}

/** A call parameter the agent records. */
export interface AgentConfigParam {
  name: string;
  /** Whether the parameter can be used as a filter on the calls page. */
  index: boolean;
  /** Whether the parameter is shown in the calls list. */
  list: boolean;
}

/** The settings that the server renders into `_config.xml`. */
export interface AgentConfigSettings {
  enabled: boolean;
  /** Method calls shorter than this are not recorded. */
  min_duration_ms: number;
  include: AgentConfigIncludeRule[];
  /** Class patterns that are never profiled. They take precedence over `include`. */
  exclude: string[];
  /** Plugin name to on/off. A plugin missing from the map is off. */
  plugins: Record<string, boolean>;
  /** Call parameters in display order. The XML numbers them in this order. */
  params: AgentConfigParam[];
  /** Local dump retention by age, in the agent's own syntax, such as `8 days`. */
  max_age: string;
  /** Local dump retention by size, in the agent's own syntax, such as `2 Gb`. */
  max_size: string;
}

/** A value fixed when the JVM starts, shown read-only. */
export interface AgentStartupValue {
  label: string;
  value: string;
  /** The environment variables or system property that set the value. */
  source: string;
}

/** GET, PUT, and DELETE /agent-config/{scope} body: the config in effect for the scope. */
export interface AgentConfigResponse {
  scope: AgentConfigScopeId;
  /** The scope whose config is in effect. Equal to `scope` when the scope has its own config. */
  owner: AgentConfigScopeId;
  /**
   * Revision of the config in effect. Revisions come from one counter shared
   * by all scopes, so the number can go down when a scope returns to an older
   * inherited config.
   */
  revision: number;
  settings: AgentConfigSettings;
  /**
   * The hand-edited XML, or null while the XML is generated from `settings`.
   * When set, agents receive it as is and `settings` has no effect.
   */
  raw_xml: string | null;
  startup: AgentStartupValue[];
  /** Every plugin name the agents know, in the order the XML lists them. */
  available_plugins: string[];
}

/**
 * PUT /agent-config/{scope} body. The save gives the scope its own config.
 *
 * The server answers `409` with the problem code `revision_conflict` when
 * `expected_revision` is not the revision in effect for the scope.
 */
export interface AgentConfigSaveRequest {
  /** The `revision` of the response the edit started from. */
  expected_revision: number;
  settings: AgentConfigSettings;
  /** The hand-edited XML to store. Null or absent returns the scope to generated XML. */
  raw_xml?: string | null;
}

/** POST /agent-config/render body. */
export interface AgentConfigRenderRequest {
  settings: AgentConfigSettings;
}

/** POST /agent-config/render response. */
export interface AgentConfigRenderResponse {
  xml: string;
}

/** One agent, which is one profiled pod. */
export interface AgentConfigAgent {
  namespace: string;
  service: string;
  pod: string;
  agent_version: string;
  status: AgentStatus;
  /** The revision the agent runs. Null for an `unsupported` agent. */
  revision: number | null;
  /**
   * The revision assigned to the agent. Equal to `revision` once applied, and
   * different while the agent is `pending` or after it `failed`. Null for an
   * `unsupported` agent.
   */
  target_revision: number | null;
  /** Why the agent is not `applied`. Empty when it is. */
  detail: string;
  /** Unix ms of the agent's last status report. */
  last_report_ms: number;
}

/** GET /agent-config/{scope}/agents body: the agents in the scope and below it. */
export interface AgentConfigAgentsResponse {
  agents: AgentConfigAgent[];
}
