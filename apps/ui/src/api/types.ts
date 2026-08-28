// Wire models of the external read API (backend/docs/design/02-read-contract.md §2).
// Field names mirror the backend structs in backend/libs/query/model/wire.go and
// backend/libs/query/api.go; if the two disagree, that is a backend bug — report
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
}

/** RFC 7807 body with the §2.3.2 wide-query guard extensions (02 §8). */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail?: string;
  suggested_filters?: string[];
  estimated_files?: number;
  estimated_bytes?: number;
  by_class?: Record<string, number>;
}
