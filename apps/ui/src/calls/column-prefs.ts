import { DEFAULT_COLUMN_ORDER, DEFAULT_HIDDEN_COLUMNS } from './columns';

// Column show/hide/order/width, persisted in localStorage (09 §2.3).

export interface ColumnPrefs {
  /** Visible + hidden columns in display order. */
  order: string[];
  hidden: string[];
  widths: Record<string, number>;
}

const STORAGE_KEY = 'profiler-ui.calls.columns.v2';

/** Version 1 stored camel-case keys for the IO columns and predates `trace_id`. */
const LEGACY_STORAGE_KEY = 'profiler-ui.calls.columns.v1';

/** Version 1 keys that version 2 renamed. A key absent here kept its name. */
const LEGACY_KEY_RENAMES: Readonly<Record<string, string>> = {
  netIo: 'net_io',
  diskIo: 'disk_io',
};

export function defaultColumnPrefs(
  defaultOrder: readonly string[] = DEFAULT_COLUMN_ORDER,
  defaultHidden: readonly string[] = DEFAULT_HIDDEN_COLUMNS,
): ColumnPrefs {
  return { order: [...defaultOrder], hidden: defaultHidden.filter((k) => defaultOrder.includes(k)), widths: {} };
}

/**
 * Reads the stored preferences. Without a version 2 entry, a version 1 entry is migrated: renamed keys take their
 * new names. Either way, a key the table no longer has is dropped, and a column the stored entry does not know is
 * placed after its default predecessor with its default visibility.
 */
export function loadColumnPrefs(
  defaultOrder: readonly string[] = DEFAULT_COLUMN_ORDER,
  defaultHidden: readonly string[] = DEFAULT_HIDDEN_COLUMNS,
): ColumnPrefs {
  const fallback = defaultColumnPrefs(defaultOrder, defaultHidden);
  let raw: string | null = null;
  let renames: Readonly<Record<string, string>> = {};
  try {
    raw = localStorage.getItem(STORAGE_KEY);
    if (raw === null) {
      raw = localStorage.getItem(LEGACY_STORAGE_KEY);
      renames = LEGACY_KEY_RENAMES;
    }
  } catch {
    return fallback;
  }
  if (raw === null) return fallback;
  try {
    return normalizePrefs(JSON.parse(raw) as Partial<ColumnPrefs>, renames, defaultOrder, defaultHidden);
  } catch {
    return fallback;
  }
}

function normalizePrefs(
  parsed: Partial<ColumnPrefs>,
  renames: Readonly<Record<string, string>>,
  defaultOrder: readonly string[],
  defaultHidden: readonly string[],
): ColumnPrefs {
  const known = new Set(defaultOrder);
  const rename = (k: string): string => renames[k] ?? k;
  const keys = (list: unknown): string[] =>
    Array.isArray(list) ? [...new Set(list.filter((k) => typeof k === 'string').map(rename))].filter((k) => known.has(k)) : [];

  const order = keys(parsed.order);
  const hidden = keys(parsed.hidden);
  defaultOrder.forEach((k, i) => {
    if (order.includes(k)) return;
    // After the nearest default predecessor the stored order has; first if none.
    let at = 0;
    for (let j = i - 1; j >= 0; j--) {
      const idx = order.indexOf(defaultOrder[j]!);
      if (idx >= 0) {
        at = idx + 1;
        break;
      }
    }
    order.splice(at, 0, k);
    if (defaultHidden.includes(k) && !hidden.includes(k)) hidden.push(k);
  });

  const widths: Record<string, number> = {};
  if (typeof parsed.widths === 'object' && parsed.widths !== null) {
    for (const [k, v] of Object.entries(parsed.widths)) {
      const key = rename(k);
      if (known.has(key) && typeof v === 'number' && Number.isFinite(v) && v >= 40) widths[key] = v;
    }
  }
  return { order, hidden, widths };
}

export function saveColumnPrefs(prefs: ColumnPrefs): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
    localStorage.removeItem(LEGACY_STORAGE_KEY);
  } catch {
    // Storage full or unavailable: prefs simply do not persist.
  }
}
