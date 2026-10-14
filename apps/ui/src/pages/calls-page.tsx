import { Alert, Button, ConfigProvider, Empty, Layout, Space, Typography, message } from 'antd';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';

import {
  AllFailedBanner,
  CursorExpiredBanner,
  EmptyPausedBanner,
  LoadMoreErrorBanner,
  PartialBanner,
  PodsPartialBanner,
  SelectionTooWideBanner,
  TooWideBanner,
} from '../calls/calls-banners';
import { CallsTable } from '../calls/calls-table';
import { loadColumnPrefs, saveColumnPrefs } from '../calls/column-prefs';
import type { ColumnPrefs } from '../calls/column-prefs';
import { DEFAULT_COLUMN_ORDER } from '../calls/columns';
import { CONTROL_HEIGHT_STYLE, CONTROL_THEME } from '../calls/control-height';
import { FilterBar } from '../calls/filter-bar/filter-bar';
import type { PresetToken } from '../calls/filter-bar/filter-bar';
import { HIDE_SYSTEM_TOKEN, isIdleMethod } from '../calls/idle-tags';
import { QueryControls, SortBy } from '../calls/query-controls';
import { DEFAULT_ORDER, orderLabel, serverCanOrder, sortCalls } from '../calls/sort-order';
import type { CallsOrder } from '../calls/sort-order';
import { useCallsQuery } from '../calls/use-calls-query';
import { PeriodControls } from '../controls/period-controls';
import { podValueSource } from '../discovery/pod-values';
import { usePods } from '../discovery/use-pods';
import type { Condition } from '../filter/condition';
import { formatCondition } from '../filter/grammar';
import { translateConditions, unsupportedReason } from '../filter/translate';
import { HeaderSlot } from '../shell/header-slot';
import { CALLS_URL_LENGTH_LIMIT, callsQueryUrlLength } from '../api/endpoints';
import type { CallsFilter } from '../api/endpoints';
import { callsSearchToParams, freezeWindow, hasRelativeWindow, parseCallsSearch } from '../url/search-params';
import type { CallsSearchState } from '../url/search-params';
import { usePermalinkHotkey } from '../url/use-permalink-hotkey';
import styles from './calls-page.module.css';

// Calls (09 §2). The filter bar, the duration chips, the switches, and "Sort by"
// edit a draft; "Run query" writes it to the URL, which keys the /calls fetch. A time
// range change and a row shortcut apply at once (redesign plan §4.2).

// jsdom reports zero heights, which starves the virtual scroller of rows.
const VIRTUAL = import.meta.env.MODE !== 'test';

/** The part of the query the controls edit before it runs. */
interface QueryDraft {
  conditions: Condition[];
  hideSystem: boolean;
  order: CallsOrder;
}

const draftKey = (d: QueryDraft): string => JSON.stringify([d.conditions.map(formatCondition), d.hideSystem, d.order]);

const draftOf = (s: CallsSearchState): QueryDraft => ({ conditions: s.conditions, hideSystem: s.hideSystem, order: s.order });

export function CallsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  // Resolve relative window tokens against one `now` per URL change; the memo
  // freezes it, so `now-3h` does not drift (and refetch) on every render.
  const search = useMemo(() => parseCallsSearch(searchParams, Date.now(), { defaults: true }), [searchParams]);

  // A visit with no query string gets the defaults; write them, so the URL is complete from then on.
  const bare = searchParams.size === 0;
  useEffect(() => {
    if (bare) setSearchParams(callsSearchToParams(search), { replace: true });
  }, [bare, search, setSearchParams]);

  const appliedKey = draftKey(search);
  const [draft, setDraft] = useState<QueryDraft>(() => draftOf(search));
  // Resync the draft when the applied state changes under it (back/forward, a shared link).
  useEffect(() => {
    setDraft(draftOf(search));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appliedKey]);
  const dirty = draftKey(draft) !== appliedKey;

  const [prefs, setPrefs] = useState<ColumnPrefs>(() => loadColumnPrefs(DEFAULT_COLUMN_ORDER));

  // /pods for the window feeds the namespace, service, and pod translation and the bar's value suggestions.
  const { state: podsState, refetch: refetchPods } = usePods(search.fromMs, search.toMs);
  const namespaces = podsState.kind === 'ready' ? podsState.namespaces : null;
  const values = useMemo(() => podValueSource(namespaces), [namespaces]);

  // /calls takes no `filter` parameter yet: the applied conditions travel as today's parameters (plan §4.2.1).
  const translation = useMemo(() => translateConditions(search.conditions, namespaces), [search.conditions, namespaces]);
  const needsPods = search.conditions.some((c) => ['namespace', 'service', 'pod'].includes(c.field));
  const waitingForPods = translation.params === null && podsState.kind !== 'error';

  // A draft token the translation skips renders disabled with the reason, and is not sent.
  const draftReasons = useMemo(
    () => new Map(translateConditions(draft.conditions, namespaces).skipped.map((s) => [s.condition, s.reason])),
    [draft.conditions, namespaces],
  );
  const tokenReason = useCallback((c: Condition) => draftReasons.get(c) ?? null, [draftReasons]);

  // "Hide system/proxy" shows in the bar as a token of the draft: the switch adds and removes it, and so do its ×
  // and Backspace. The URL keeps the hide_system flag (redesign plan §4.2).
  const presets: PresetToken[] = draft.hideSystem
    ? [{ key: 'hide_system', ...HIDE_SYSTEM_TOKEN, onRemove: () => setDraft((d) => ({ ...d, hideSystem: false })) }]
    : [];

  const serverOrdered = serverCanOrder(search.order);
  const rawFilter: CallsFilter | null = useMemo(() => {
    if (search.fromMs === null || search.toMs === null) return null;
    if (translation.params === null || translation.noPods || translation.contradiction) return null;
    // An order the server cannot sort by stays out of the request; the loaded rows are sorted below instead.
    const order = serverOrdered && search.order !== DEFAULT_ORDER ? { order: search.order } : {};
    return { fromMs: search.fromMs, toMs: search.toMs, ...translation.params, ...order };
  }, [search.fromMs, search.toMs, translation, serverOrdered, search.order]);

  // A wide namespace or service filter expands to repeatable `pod` params (no
  // `service` param exists, 02 §2.3) and can build a request line a proxy
  // or browser rejects outright; catch that before sending it rather than
  // surfacing an opaque network failure (PR 708 review #8).
  const selectionTooWide = rawFilter !== null && callsQueryUrlLength(rawFilter) > CALLS_URL_LENGTH_LIMIT;
  const filter = selectionTooWide ? null : rawFilter;

  const { state, loadMore, refetch } = useCallsQuery(filter);

  const commitSearch = (next: CallsSearchState): void => setSearchParams(callsSearchToParams(next));

  // Runs a query state: the URL takes it, and the fetch restarts from page one even when nothing changed.
  const runQuery = (next: QueryDraft): void => {
    setDraft(next);
    commitSearch({ ...search, ...next });
    refetch();
  };
  const run = (): void => runQuery(draft);

  // A row shortcut replaces that field's token and runs the query (plan §4.2).
  const pickFilter = (field: 'pod' | 'trace_id', value: string): void =>
    runQuery({ ...draft, conditions: [...draft.conditions.filter((c) => c.field !== field), { field, op: '=', values: [value] }] });

  // A range change stores its raw tokens (a relative range stays live) and applies at once; the draft stays a draft.
  const commitRange = (tokens: { from: string; to: string }): void =>
    commitSearch({ ...search, from: tokens.from || null, to: tokens.to || null });

  // `y` freezes a live relative range into an absolute permalink (GitHub-style).
  usePermalinkHotkey(hasRelativeWindow(search), () => {
    commitSearch(freezeWindow(search));
    message.success('Time range pinned to a permalink');
  });

  const changePrefs = (next: ColumnPrefs): void => {
    setPrefs(next);
    saveColumnPrefs(next);
  };

  const rows = state.kind === 'ready' ? state.rows : [];
  const shownRows = useMemo(
    () => (search.hideSystem ? rows.filter((c) => !isIdleMethod(c.method)) : rows),
    [rows, search.hideSystem],
  );
  const hiddenCount = rows.length - shownRows.length;
  const visibleRows = useMemo(
    () => (serverOrdered ? shownRows : sortCalls(shownRows, search.order)),
    [shownRows, serverOrdered, search.order],
  );
  const sortNote = serverOrdered ? undefined : `Sort by ${orderLabel(search.order)} applies to the loaded rows only`;

  const footer =
    state.kind === 'ready' ? (
      <Space>
        <Button size="small" onClick={loadMore} loading={state.loadingMore} disabled={state.nextCursor === null || state.cursorExpired}>
          Load more
        </Button>
        <Typography.Text type="secondary">
          {state.rows.length} loaded
          {hiddenCount > 0 ? ` · ${hiddenCount} hidden as system/proxy` : ''}
          {state.nextCursor === null ? ' · end of range' : ''}
        </Typography.Text>
      </Space>
    ) : undefined;

  const applyConditions = (conditions: Condition[]): void => runQuery({ ...draftOf(search), conditions });
  const noMatch = translation.contradiction
    ? 'The filters exclude each other, so no call can match.'
    : translation.noPods
      ? 'No pod in this window matches the namespace, service, and pod filters.'
      : null;

  return (
    <Layout className={styles.page}>
      <Layout.Content className={styles.content}>
        <HeaderSlot>
          <PeriodControls
            value={{ from: search.from, to: search.to, fromMs: search.fromMs, toMs: search.toMs }}
            onCommitRange={commitRange}
          />
        </HeaderSlot>
        {/* One control height for the bar, "Sort by", the chips, the switches, and "Run query". */}
        <ConfigProvider theme={CONTROL_THEME}>
          <div className={styles.controls} style={CONTROL_HEIGHT_STYLE}>
            <div className={styles.barRow}>
              <div className={styles.bar}>
                <FilterBar
                  conditions={draft.conditions}
                  onChange={(conditions) => setDraft((d) => ({ ...d, conditions }))}
                  presets={presets}
                  supported={unsupportedReason}
                  tokenReason={tokenReason}
                  values={values}
                  onRun={run}
                />
              </div>
              <SortBy value={draft.order} onChange={(order) => setDraft((d) => ({ ...d, order }))} />
            </div>
            <QueryControls
              conditions={draft.conditions}
              onConditionsChange={(conditions) => setDraft((d) => ({ ...d, conditions }))}
              hideSystem={draft.hideSystem}
              onHideSystemChange={(hideSystem) => setDraft((d) => ({ ...d, hideSystem }))}
              dirty={dirty}
              running={state.kind === 'loading'}
              disabled={search.fromMs === null || search.toMs === null}
              onRun={run}
            />
          </div>
        </ConfigProvider>
        <Space orientation="vertical" className={styles.fullWidth} size={8}>
          {selectionTooWide ? <SelectionTooWideBanner conditions={search.conditions} onApply={applyConditions} /> : null}
          {state.kind === 'too-wide' ? (
            <TooWideBanner problem={state.problem} conditions={search.conditions} onApply={applyConditions} />
          ) : null}
          {state.kind === 'all-failed' ? <AllFailedBanner detail={state.detail} onRetry={refetch} /> : null}
          {state.kind === 'error' ? (
            <Alert
              type="error"
              showIcon
              title="Loading calls failed"
              description={state.message}
              action={
                <Button size="small" onClick={refetch}>
                  Retry
                </Button>
              }
            />
          ) : null}
          {podsState.kind === 'error' && needsPods ? (
            <Alert
              type="error"
              showIcon
              title="Cannot resolve the namespace, service, and pod filters without /pods"
              description={podsState.message}
              action={
                <Button size="small" onClick={refetchPods}>
                  Retry
                </Button>
              }
            />
          ) : null}
          {podsState.kind === 'ready' && podsState.partial && needsPods ? (
            <PodsPartialBanner reasons={podsState.partialReasons} onRetry={refetchPods} />
          ) : null}
          {state.kind === 'ready' && state.partial ? (
            <PartialBanner reasons={state.partialReasons} onRetry={refetch} />
          ) : null}
          {state.kind === 'ready' && state.cursorExpired ? <CursorExpiredBanner onReload={refetch} /> : null}
          {state.kind === 'ready' && state.emptyPaused ? <EmptyPausedBanner onContinue={loadMore} /> : null}
          {state.kind === 'ready' && state.loadMoreError !== null ? (
            <LoadMoreErrorBanner message={state.loadMoreError} onRetry={loadMore} />
          ) : null}
        </Space>
        {/* jsdom cannot size the virtual scroller; tests render plain rows. */}
        {/* selectionTooWide already has its own banner above; no Empty needed. */}
        {state.kind === 'idle' && !waitingForPods && !selectionTooWide ? (
          <Empty className={styles.emptyTop} description={noMatch ?? 'Pick a time range to see calls.'} />
        ) : state.kind === 'loading' || (state.kind === 'idle' && waitingForPods) ? (
          <CallsTable rows={[]} loading prefs={prefs} onPrefsChange={changePrefs} virtual={VIRTUAL} />
        ) : state.kind === 'ready' ? (
          visibleRows.length === 0 && state.nextCursor === null && !state.emptyPaused ? (
            <Empty className={styles.emptyTop} description="No calls match the filters in this window." />
          ) : (
            <CallsTable
              rows={visibleRows}
              loading={false}
              prefs={prefs}
              onPrefsChange={changePrefs}
              onPickFilter={pickFilter}
              virtual={VIRTUAL}
              footer={footer}
              sortNote={sortNote}
            />
          )
        ) : null}
      </Layout.Content>
    </Layout>
  );
}
