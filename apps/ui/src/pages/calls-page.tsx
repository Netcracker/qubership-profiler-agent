import { Alert, App, Button, ConfigProvider, Empty, Layout, Space, Typography } from 'antd';
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
import { CallsHistogram } from '../calls/calls-histogram';
import { CallsTable } from '../calls/calls-table';
import { loadColumnPrefs, saveColumnPrefs } from '../calls/column-prefs';
import type { ColumnPrefs } from '../calls/column-prefs';
import { DEFAULT_COLUMN_ORDER } from '../calls/columns';
import { CONTROL_HEIGHT_STYLE, CONTROL_THEME } from '../calls/control-height';
import { FilterBar } from '../calls/filter-bar/filter-bar';
import type { PresetToken } from '../calls/filter-bar/filter-bar';
import { ALL_FIELDS } from '../calls/filter-bar/suggestions';
import type { ValueSuggestion } from '../calls/filter-bar/suggestions';
import { suggestionNote, useParamNames, useValueSuggestions } from '../calls/filter-bar/use-value-suggestions';
import { HIDE_SYSTEM_TOKEN, isIdleMethod } from '../calls/idle-tags';
import { QueryControls, SortBy } from '../calls/query-controls';
import { planQuery } from '../calls/query-plan';
import { orderLabel, sortCalls } from '../calls/sort-order';
import type { CallsOrder } from '../calls/sort-order';
import { useCallsQuery } from '../calls/use-calls-query';
import { useStats } from '../calls/use-stats';
import { PeriodControls } from '../controls/period-controls';
import { podValueSource } from '../discovery/pod-values';
import { usePods } from '../discovery/use-pods';
import { browserNote, browserSupports, filterInBrowser } from '../filter/browser';
import { capabilitiesOf } from '../filter/capabilities';
import { fieldDef } from '../filter/catalog';
import type { Capabilities } from '../filter/capabilities';
import type { Condition, Operator } from '../filter/condition';
import { formatCondition } from '../filter/grammar';
import { unsupportedReason } from '../filter/translate';
import { HeaderSlot } from '../shell/header-slot';
import { CALLS_URL_LENGTH_LIMIT, callsQueryUrlLength } from '../api/endpoints';
import { useConfig } from '../api/use-config';
import type { CallsFilter, StatsQuery } from '../api/endpoints';
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

/**
 * The fields and operators the bar offers on a backend without the filter model: those the translation sends, and
 * those the browser applies to the loaded rows.
 */
const legacySupported = (field: string, op?: Operator): string | null =>
  browserSupports(field, op) ? null : unsupportedReason(field, op);

/** The bars of the histogram (redesign plan §4.3). */
const HISTOGRAM_BUCKETS = 60;

const draftOf = (s: CallsSearchState): QueryDraft => ({ conditions: s.conditions, hideSystem: s.hideSystem, order: s.order });

export function CallsPage() {
  // The App's toast renders in the app's own React tree and unmounts with it.
  // The static `message` mounts a separate root that nothing unmounts.
  const { message } = App.useApp();
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

  // What the backend implements of the filter model: null for a backend without it, and undefined while that is
  // unknown. A failed /config leaves it unknown and holds the query: a legacy query would drop the conditions a
  // backend with the filter model takes.
  const config = useConfig();
  const fc = config.kind === 'ready' ? config.config.filter_capabilities : undefined;
  const caps: Capabilities | null | undefined = useMemo(
    () => (config.kind !== 'ready' ? undefined : fc === undefined ? null : capabilitiesOf(fc)),
    [config.kind, fc],
  );

  // /pods for the window feeds the bar's value suggestions and, without the filter model, the translation of the
  // namespace, service, and pod conditions to pod tuples.
  const { state: podsState, refetch: refetchPods } = usePods(search.fromMs, search.toMs);
  const namespaces = podsState.kind === 'ready' ? podsState.namespaces : null;
  const podValues = useMemo(() => podValueSource(namespaces), [namespaces]);

  // Value suggestions come from /stats when the backend groups by the field, with the other draft conditions, and
  // from /pods otherwise (redesign plan §4.3). The bar says which field it lists values for.
  const [valueField, setValueField] = useState<string | null>(null);
  const suggestions = useValueSuggestions({
    field: valueField,
    fromMs: search.fromMs,
    toMs: search.toMs,
    conditions: draft.conditions,
    caps,
  });
  const values = useCallback(
    (field: string): readonly ValueSuggestion[] =>
      suggestions !== null && suggestions.field === field
        ? suggestions.kind === 'ready'
          ? suggestions.values
          : []
        : podValues(field),
    [suggestions, podValues],
  );
  const valueNote = useCallback((field: string) => suggestionNote(suggestions, field), [suggestions]);

  // The call param names of the window join the field list, so a tag such as `user.tenant` need not be typed.
  const paramNames = useParamNames(search.fromMs, search.toMs, caps);
  const fields = useMemo(
    () => [...ALL_FIELDS, ...paramNames.filter((name) => fieldDef(name)?.kind === 'param')],
    [paramNames],
  );

  const plan = useMemo(
    () => planQuery(search.conditions, search.order, caps, namespaces),
    [search.conditions, search.order, caps, namespaces],
  );
  const needsPods = plan.expandsPods && search.conditions.some((c) => ['namespace', 'service', 'pod'].includes(c.field));
  const waitingForPods = plan.waitingForPods && podsState.kind !== 'error';

  // A draft token the backend cannot take renders disabled with the reason, and is not sent. Without the filter
  // model, a trace_id token is not sent either, but it filters the loaded rows in the browser, so it renders applied,
  // with that note.
  const draftReasons = useMemo(
    () => new Map(planQuery(draft.conditions, draft.order, caps, namespaces).skipped.map((s) => [s.condition, s.reason])),
    [draft.conditions, draft.order, caps, namespaces],
  );
  const tokenReason = useCallback((c: Condition) => draftReasons.get(c) ?? null, [draftReasons]);
  const supported = caps ? caps.supported : legacySupported;
  const tokenNote = caps ? undefined : browserNote;

  // "Hide system/proxy" shows in the bar as a token of the draft: the switch adds and removes it, and so do its ×
  // and Backspace. The URL keeps the hide_system flag (redesign plan §4.2).
  const presets: PresetToken[] = draft.hideSystem
    ? [{ key: 'hide_system', ...HIDE_SYSTEM_TOKEN, onRemove: () => setDraft((d) => ({ ...d, hideSystem: false })) }]
    : [];

  const rawFilter: CallsFilter | null = useMemo(() => {
    if (search.fromMs === null || search.toMs === null || plan.params === null) return null;
    return { fromMs: search.fromMs, toMs: search.toMs, ...plan.params };
  }, [search.fromMs, search.toMs, plan]);

  // A long `in` list, or without the filter model a wide namespace or service filter expanded to repeatable `pod`
  // params, can build a request line a proxy or browser rejects outright; catch that before sending it rather than
  // surfacing an opaque network failure (PR 708 review #8).
  const selectionTooWide = rawFilter !== null && callsQueryUrlLength(rawFilter) > CALLS_URL_LENGTH_LIMIT;
  const filter = selectionTooWide ? null : rawFilter;

  const { state, loadMore, refetch } = useCallsQuery(filter);

  // The histogram counts the same query by time (redesign plan §4.3), on a backend that groups by time.
  const statsQuery: StatsQuery | null = useMemo(() => {
    if (filter === null || !caps || !caps.canGroupBy('time')) return null;
    return { fromMs: filter.fromMs, toMs: filter.toMs, filters: filter.filters, groupBy: 'time', buckets: HISTOGRAM_BUCKETS };
  }, [filter, caps]);
  const { state: statsState, refetch: refetchStats } = useStats(statsQuery);

  const commitSearch = (next: CallsSearchState): void => setSearchParams(callsSearchToParams(next));

  // Runs a query state: the URL takes it, and the fetch restarts from page one even when nothing changed.
  const runQuery = (next: QueryDraft): void => {
    setDraft(next);
    commitSearch({ ...search, ...next });
    refetch();
    refetchStats();
    if (config.kind === 'error') config.retry();
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
  const unhiddenRows = useMemo(
    () => (search.hideSystem ? rows.filter((c) => !isIdleMethod(c.method)) : rows),
    [rows, search.hideSystem],
  );
  const hiddenCount = rows.length - unhiddenRows.length;
  const shownRows = useMemo(() => filterInBrowser(unhiddenRows, plan.browser), [unhiddenRows, plan.browser]);
  const traceHiddenCount = unhiddenRows.length - shownRows.length;
  const visibleRows = useMemo(
    () => (plan.serverOrdered ? shownRows : sortCalls(shownRows, search.order)),
    [shownRows, plan.serverOrdered, search.order],
  );
  const sortNote = plan.serverOrdered ? undefined : `Sort by ${orderLabel(search.order)} applies to the loaded rows only`;

  const footer =
    state.kind === 'ready' ? (
      <Space>
        <Button size="small" onClick={loadMore} loading={state.loadingMore} disabled={state.nextCursor === null || state.cursorExpired}>
          Load more
        </Button>
        <Typography.Text type="secondary">
          {state.rows.length} loaded
          {hiddenCount > 0 ? ` · ${hiddenCount} hidden as system/proxy` : ''}
          {traceHiddenCount > 0 ? ` · ${traceHiddenCount} hidden by the trace_id filter` : ''}
          {state.nextCursor === null ? ' · end of range' : ''}
        </Typography.Text>
      </Space>
    ) : undefined;

  const applyConditions = (conditions: Condition[]): void => runQuery({ ...draftOf(search), conditions });
  const noMatch = plan.noMatch;

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
                  supported={supported}
                  conditionReason={caps?.conditionReason}
                  tokenNote={tokenNote}
                  tokenReason={tokenReason}
                  values={values}
                  valueNote={valueNote}
                  onValueField={setValueField}
                  fields={fields}
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
        {search.fromMs !== null && search.toMs !== null && statsState.kind !== 'idle' && statsState.kind !== 'hidden' ? (
          <CallsHistogram
            state={statsState}
            fromMs={search.fromMs}
            toMs={search.toMs}
            onZoom={(from, to) => commitRange({ from: String(from), to: String(to) })}
            onRetry={refetchStats}
            hideSystem={search.hideSystem}
          />
        ) : null}
        <Space orientation="vertical" className={styles.fullWidth} size={8}>
          {config.kind === 'error' ? (
            <Alert
              type="error"
              showIcon
              title="Cannot run the query: loading /config failed"
              description={config.message}
              action={
                <Button size="small" onClick={config.retry}>
                  Retry
                </Button>
              }
            />
          ) : null}
          {selectionTooWide ? (
            <SelectionTooWideBanner conditions={search.conditions} expandsPods={plan.expandsPods} onApply={applyConditions} />
          ) : null}
          {state.kind === 'too-wide' ? (
            <TooWideBanner problem={state.problem} conditions={search.conditions} onApply={applyConditions} />
          ) : null}
          {state.kind === 'invalid' ? (
            <Alert type="warning" showIcon title="The backend rejected the query" description={state.detail} />
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
        {state.kind === 'idle' && !waitingForPods && caps !== undefined && !selectionTooWide ? (
          <Empty className={styles.emptyTop} description={noMatch ?? 'Pick a time range to see calls.'} />
        ) : state.kind === 'loading' || (state.kind === 'idle' && (waitingForPods || config.kind === 'loading')) ? (
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
