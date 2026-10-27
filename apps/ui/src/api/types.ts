// Wire models of the external read API (docs/design/02-read-contract.md §2).
// Field names mirror the backend structs in libs/query/model/wire.go and
// libs/query/api.go; if the two disagree, that is a backend bug — report
// it, do not adapt the types.

/** 7-component call primary key (02 §2.2). */
export interface CallPK {
  pod_namespace: string;
  pod_service: string;
  pod_name: string;
  restart_time_ms: number;
  trace_file_index: number;
  buffer_offset: number;
  record_index: number;
}

export const RETENTION_CLASSES = [
  'short_clean',
  'normal_clean',
  'long_clean',
  'any_error',
  'corrupted',
] as const;

export type RetentionClass = (typeof RETENTION_CLASSES)[number];

export function isRetentionClass(s: string): s is RetentionClass {
  return (RETENTION_CLASSES as readonly string[]).includes(s);
}

/** One /calls row (02 §2.3), including every R1 metric column. */
export interface CallJSON {
  pk: CallPK;
  ts_ms: number;
  duration_ms: number;
  method: string;
  thread_name: string;
  cpu_time_ms: number;
  wait_time_ms: number;
  // TODO(int64-precision): memory_used / logs_* / file_* / net_* are int64 on
  // the wire; res.json() parses them as JS numbers and loses precision above
  // 2^53 (~9 PB). A bigint-aware parse would ripple through formatBytes and the
  // numeric column sort (calls/columns.tsx), so it is deferred until a real
  // value can approach that range.
  memory_used: number;
  queue_wait_ms: number;
  suspend_ms: number;
  child_calls: number;
  transactions: number;
  logs_generated: number;
  logs_written: number;
  file_read: number;
  file_written: number;
  net_read: number;
  net_written: number;
  error_flag: boolean;
  retention_class: RetentionClass;
  params: Record<string, string[]>;
  /** Blob byte length; null when the tier cannot know it, 0 when truncated (02 §2.3). */
  trace_blob_size: number | null;
  truncated_reason: string | null;
}

/** GET /calls page envelope (02 §2.3). */
export interface CallsResponse {
  calls: CallJSON[];
  next_cursor: string | null;
  partial: boolean;
  partial_reasons: string[];
}

/** One /pods row (02 §2.7): identity tuple plus the data time bounds. */
export interface PodEntry {
  namespace: string;
  service: string;
  pod: string;
  restart_time_ms: number;
  time_min_ms: number;
  time_max_ms: number;
}

/**
 * GET /pods body. 02 §2.7 words this as a bare array, but the backend wraps it
 * with the same partial markers as /calls (`podsResponse` in api.go) — the
 * fan-out can partially fail here too. Doc divergence reported upstream.
 */
export interface PodsResponse {
  pods: PodEntry[];
  partial: boolean;
  partial_reasons: string[];
}

/**
 * GET /config body: deployment-specific values the UI cannot derive on its
 * own. Empty fields mean the feature they back is unavailable in this
 * deployment, not an error.
 */
export interface ConfigResponse {
  dumps_collector_url: string;
  /**
   * Link-outs from one call to a tracing or logging system, in the operator's
   * order (02 §2.8). Absent from a server that predates the member; treat that
   * as an empty list.
   */
  links?: ConfigLink[];
  /** What the build implements of the filter model (02 §2.3.4). Absent on a build that predates it. */
  filter_capabilities?: FilterCapabilities;
  /** Optional screens. A backend that predates a flag omits it, which means off. */
  features?: {
    /** The agent-configuration page and its draft API (agents/types.ts). */
    agent_config?: boolean;
  };
}

/** One `links` entry of GET /config: a button label and a URL template the client expands (02 §2.8). */
export interface ConfigLink {
  label: string;
  kind: 'trace' | 'logs';
  /** An absolute http or https URL with `{placeholder}` parts; see tree/links.ts. */
  url_template: string;
}

/** The `filter_capabilities` member of /config (02 §2.3.4). */
export interface FilterCapabilities {
  /** Each implemented field and its implemented operators. A missing field is not implemented. */
  fields: Record<string, string[]>;
  /** The operators that work on call params. */
  param_operators: string[];
  /** `None` and `Any`, once they work. */
  special_values: string[];
  /** The implemented `/calls` orders (02 §2.3.5). */
  orders: string[];
  /** The implemented named `/stats` groupings (02 §2.9). */
  group_by: string[];
  /** True when `group_by` also takes the name of any call param. */
  group_by_params?: boolean;
}

/** One /stats row (02 §2.9). `key` is absent on the `other` row. */
export interface StatsRow {
  key?: string;
  count: number;
  errors: number;
  duration_ms_sum: number;
  /** Null for a row with no calls. */
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
  cpu_time_ms_sum: number;
  wait_time_ms_sum: number;
  queue_wait_ms_sum: number;
  suspend_ms_sum: number;
}

/** GET /stats body (02 §2.9). */
export interface StatsResponse {
  group_by: string;
  /** `group_by=time` only: the width of each bucket; the last one may be shorter. */
  bucket_ms?: number;
  rows: StatsRow[];
  other: StatsRow | null;
  distinct: number;
  totals: { count: number; errors: number };
  approximate: boolean;
  partial: boolean;
  partial_reasons: string[];
}

/** RFC 7807 body with the §2.3.2 wide-query guard extensions (02 §8). */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail?: string;
  /**
   * Machine-readable condition (02 §8), e.g. `cursor_rejected`. Branch on this
   * rather than on `title` or `detail`, which are prose. Optional because a
   * response from an older backend carries no code.
   */
  code?: string;
  suggested_filters?: string[];
  estimated_files?: number;
  estimated_bytes?: number;
  by_class?: Record<string, number>;
}
