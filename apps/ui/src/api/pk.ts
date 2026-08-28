import type { CallPK } from './types';

// The PK travels in a URL path segment as
// <ns>:<svc>:<pod>:<restartMs>:<file>:<off>:<rec> (02 §2.2). Kubernetes names
// cannot contain ':', so the segments split unambiguously; percent-encoding of
// the whole segment happens where the URL is assembled, mirroring
// PK.PathString / ParsePKPath in backend/libs/query/model/wire.go.

export function pkToPath(pk: CallPK): string {
  return [
    pk.pod_namespace,
    pk.pod_service,
    pk.pod_name,
    pk.restart_time_ms,
    pk.trace_file_index,
    pk.buffer_offset,
    pk.record_index,
  ].join(':');
}

export function parsePkPath(s: string): CallPK {
  const parts = s.split(':');
  if (parts.length !== 7) {
    throw new Error(`pk "${s}": expected 7 colon-separated components (02 §2.2)`);
  }
  const nums = parts.slice(3).map((part, i) => {
    const v = Number(part);
    if (!Number.isSafeInteger(v)) {
      throw new Error(`pk "${s}": component ${i + 4} is not an integer`);
    }
    return v;
  });
  return {
    pod_namespace: parts[0]!,
    pod_service: parts[1]!,
    pod_name: parts[2]!,
    restart_time_ms: nums[0]!,
    trace_file_index: nums[1]!,
    buffer_offset: nums[2]!,
    record_index: nums[3]!,
  };
}

/**
 * Component-wise PK order (02 §2.3.1): pod_* byte-wise, then the numeric
 * components. Both tiers sort by (ts_ms DESC, pk ASC); the mock and any
 * client-side merge must apply the same comparator.
 */
export function comparePk(a: CallPK, b: CallPK): number {
  return (
    compareBytewise(a.pod_namespace, b.pod_namespace) ||
    compareBytewise(a.pod_service, b.pod_service) ||
    compareBytewise(a.pod_name, b.pod_name) ||
    a.restart_time_ms - b.restart_time_ms ||
    a.trace_file_index - b.trace_file_index ||
    a.buffer_offset - b.buffer_offset ||
    a.record_index - b.record_index
  );
}

// Byte-wise (not locale-aware) string order, as 02 §2.3.1 pins. JS `<` compares
// UTF-16 code units, which matches UTF-8 byte order for any pair where one
// side is ASCII — true for Kubernetes names, which this comparator receives.
function compareBytewise(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}
