// Column show/hide/order/width, persisted in localStorage (09 §2.3).

export interface ColumnPrefs {
  /** Visible + hidden columns in display order. */
  order: string[];
  hidden: string[];
  widths: Record<string, number>;
}

const STORAGE_KEY = 'profiler-ui.calls.columns.v1';

export function defaultColumnPrefs(defaultOrder: readonly string[]): ColumnPrefs {
  return { order: [...defaultOrder], hidden: [], widths: {} };
}

export function loadColumnPrefs(defaultOrder: readonly string[]): ColumnPrefs {
  const fallback = defaultColumnPrefs(defaultOrder);
  let raw: string | null = null;
  try {
    raw = localStorage.getItem(STORAGE_KEY);
  } catch {
    return fallback;
  }
  if (raw === null) return fallback;
  try {
    const parsed = JSON.parse(raw) as Partial<ColumnPrefs>;
    const known = new Set(defaultOrder);
    // Stored order wins; columns added since (or mangled storage) fall back
    // to their default position at the end.
    const order = (parsed.order ?? []).filter((k) => known.has(k));
    for (const k of defaultOrder) if (!order.includes(k)) order.push(k);
    return {
      order,
      hidden: (parsed.hidden ?? []).filter((k) => known.has(k)),
      widths: Object.fromEntries(
        Object.entries(parsed.widths ?? {}).filter(([k, v]) => known.has(k) && Number.isFinite(v) && v >= 40),
      ),
    };
  } catch {
    return fallback;
  }
}

export function saveColumnPrefs(prefs: ColumnPrefs): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    // Storage full or unavailable: prefs simply do not persist.
  }
}
