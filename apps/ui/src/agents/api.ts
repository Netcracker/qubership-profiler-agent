import { ApiError, getJson, sendJson } from '../api/client';
import type { ConfigResponse } from '../api/types';
import type {
  AgentConfigAgentsResponse,
  AgentConfigRenderResponse,
  AgentConfigResponse,
  AgentConfigSaveRequest,
  AgentConfigScopeId,
  AgentConfigScopesResponse,
  AgentConfigSettings,
} from './types';

// Typed calls of the agent-configuration draft API. See types.ts for the
// shapes and for why none of this is a contract yet.

const BASE = '/api/v1/agent-config';

/** Problem `code` of the `409` that a save against a stale revision gets. */
export const REVISION_CONFLICT_CODE = 'revision_conflict';

// The whole id is one path segment, so `<namespace>/<service>` sends its slash
// as %2F.
function scopeUrl(scope: AgentConfigScopeId): string {
  return `${BASE}/${encodeURIComponent(scope)}`;
}

/**
 * Whether the deployment serves the agent-configuration API. Only a literal
 * `true` counts, so a backend that predates the flag keeps the page hidden.
 */
export function isAgentConfigEnabled(config: ConfigResponse): boolean {
  return config.features?.agent_config === true;
}

/** Someone else saved the scope first; the caller reloads or keeps editing. */
export function isRevisionConflict(e: unknown): e is ApiError {
  return e instanceof ApiError && e.status === 409 && e.problem?.code === REVISION_CONFLICT_CODE;
}

export function fetchAgentConfigScopes(signal?: AbortSignal): Promise<AgentConfigScopesResponse> {
  return getJson<AgentConfigScopesResponse>(`${BASE}/scopes`, undefined, signal);
}

/** The config in effect for the scope, which is an ancestor's when the scope has none. */
export function fetchAgentConfig(scope: AgentConfigScopeId, signal?: AbortSignal): Promise<AgentConfigResponse> {
  return getJson<AgentConfigResponse>(scopeUrl(scope), undefined, signal);
}

/**
 * Saves the scope's own config as a new revision. Rejects with an error that
 * {@link isRevisionConflict} recognizes when `expected_revision` is stale.
 */
export function saveAgentConfig(
  scope: AgentConfigScopeId,
  body: AgentConfigSaveRequest,
  signal?: AbortSignal,
): Promise<AgentConfigResponse> {
  return sendJson<AgentConfigResponse>('PUT', scopeUrl(scope), body, signal);
}

/** Drops the scope's own config and resolves to the inherited one now in effect. */
export function deleteAgentConfig(scope: AgentConfigScopeId, signal?: AbortSignal): Promise<AgentConfigResponse> {
  return sendJson<AgentConfigResponse>('DELETE', scopeUrl(scope), undefined, signal);
}

/** Renders unsaved settings into the XML that agents would receive. */
export async function renderAgentConfig(settings: AgentConfigSettings, signal?: AbortSignal): Promise<string> {
  const res = await sendJson<AgentConfigRenderResponse>('POST', `${BASE}/render`, { settings }, signal);
  return res.xml;
}

export function fetchAgentConfigAgents(
  scope: AgentConfigScopeId,
  signal?: AbortSignal,
): Promise<AgentConfigAgentsResponse> {
  return getJson<AgentConfigAgentsResponse>(`${scopeUrl(scope)}/agents`, undefined, signal);
}
