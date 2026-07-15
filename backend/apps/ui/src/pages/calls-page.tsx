import { Alert, Button, Empty, Layout, Space, Typography } from 'antd';
import { useEffect, useMemo, useState } from 'react';
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
import { CallsToolbar } from '../calls/calls-toolbar';
import { loadColumnPrefs, saveColumnPrefs } from '../calls/column-prefs';
import type { ColumnPrefs } from '../calls/column-prefs';
import { DEFAULT_COLUMN_ORDER } from '../calls/columns';
import { isIdleMethod } from '../calls/idle-tags';
import { useCallsQuery } from '../calls/use-calls-query';
import { PeriodControls } from '../controls/period-controls';
import type { DraftWindow } from '../controls/period-controls';
import { DiscoveryRail } from '../discovery/discovery-rail';
import type { RailSelection } from '../discovery/discovery-rail';
import { expandSelection } from '../discovery/group-pods';
import { usePods } from '../discovery/use-pods';
import { CALLS_URL_LENGTH_LIMIT, callsQueryUrlLength } from '../api/endpoints';
import type { CallsFilter } from '../api/endpoints';
import { callsSearchToParams, parseCallsSearch } from '../url/search-params';
import type { CallsSearchState } from '../url/search-params';

// Discovery + Calls (09 §2). The URL is the source of truth; the rail and the
// period picker edit a draft that Apply commits to the URL, which keys the
// /calls fetch. Toolbar filters narrow an applied query and commit directly.

// jsdom reports zero heights, which starves the virtual scroller of rows.
const VIRTUAL = import.meta.env.MODE !== 'test';

export function CallsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const search = useMemo(() => parseCallsSearch(searchParams), [searchParams]);

  const [draftWindow, setDraftWindow] = useState<DraftWindow>({ fromMs: search.fromMs, toMs: search.toMs });
  const [draftSelection, setDraftSelection] = useState<RailSelection>({
    services: search.services,
    pods: search.pods,
  });
  const [prefs, setPrefs] = useState<ColumnPrefs>(() => loadColumnPrefs(DEFAULT_COLUMN_ORDER));

  // Resync drafts when the committed state changes under us (back/forward,
  // a shared link, a pinned pod).
  const selectionKey = [...search.services, '|', ...search.pods].join(',');
  useEffect(() => {
    setDraftWindow({ fromMs: search.fromMs, toMs: search.toMs });
    setDraftSelection({ services: search.services, pods: search.pods });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search.fromMs, search.toMs, selectionKey]);

  // The rail follows the draft window, so services are selectable before the
  // first Apply.
  const { state: railPodsState } = usePods(draftWindow.fromMs, draftWindow.toMs);

  // The calls fan-out stays keyed on the committed URL: expanding the
  // committed selection must use /pods for the committed window, not the
  // draft one, or moving the draft period re-expands the still-committed
  // services into a different pod set before Apply.
  const { state: committedPodsState, refetch: refetchCommittedPods } = usePods(search.fromMs, search.toMs);
  const namespaces = committedPodsState.kind === 'ready' ? committedPodsState.namespaces : null;

  // /calls has no service param (02 §2.3): expand selected services into pod
  // tuples once /pods data is there.
  const podFilter = useMemo(
    () => expandSelection(namespaces, search.services, search.pods),
    [namespaces, search.services, search.pods],
  );
  const hasSelection = search.services.length > 0 || search.pods.length > 0;
  const selectionEmpty = hasSelection && podFilter !== null && podFilter.length === 0;
  const waitingForExpansion = search.fromMs !== null && podFilter === null && committedPodsState.kind !== 'error';

  const rawFilter: CallsFilter | null = useMemo(() => {
    if (search.fromMs === null || search.toMs === null) return null;
    if (podFilter === null || selectionEmpty) return null;
    return {
      fromMs: search.fromMs,
      toMs: search.toMs,
      pods: podFilter,
      method: search.query === '' ? undefined : search.query,
      durationMinMs: search.durationMinMs === 0 ? undefined : search.durationMinMs,
      errorOnly: search.errorOnly ? true : undefined,
      retentionClasses: search.retentionClasses.length === 0 ? undefined : search.retentionClasses,
    };
  }, [search, podFilter, selectionEmpty]);

  // A wide service selection expands to repeatable `pod` params (no
  // `service` param exists, 02 §2.3) and can build a request line a proxy
  // or browser rejects outright; catch that before sending it rather than
  // surfacing an opaque network failure (PR 708 review #8).
  const selectionTooWide = rawFilter !== null && callsQueryUrlLength(rawFilter) > CALLS_URL_LENGTH_LIMIT;
  const filter = selectionTooWide ? null : rawFilter;

  const { state, loadMore, refetch } = useCallsQuery(filter);

  const commitSearch = (next: CallsSearchState): void => setSearchParams(callsSearchToParams(next));

  const apply = (): void =>
    commitSearch({
      ...search,
      fromMs: draftWindow.fromMs,
      toMs: draftWindow.toMs,
      services: draftSelection.services,
      pods: draftSelection.pods,
    });

  const pinPod = (tuple: string): void => {
    if (search.pods.includes(tuple)) return;
    commitSearch({ ...search, pods: [...search.pods, tuple] });
  };

  const changePrefs = (next: ColumnPrefs): void => {
    setPrefs(next);
    saveColumnPrefs(next);
  };

  const rows = state.kind === 'ready' ? state.rows : [];
  const visibleRows = useMemo(
    () => (search.hideSystem ? rows.filter((c) => !isIdleMethod(c.method)) : rows),
    [rows, search.hideSystem],
  );
  const hiddenCount = rows.length - visibleRows.length;

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

  return (
    <Layout style={{ flex: 1, minHeight: 0 }}>
      <Layout.Sider
        width={320}
        theme="light"
        breakpoint="lg"
        collapsedWidth={0}
        // The default zero-width trigger has no header row to sit in here
        // (CallsPage has none) and ends up painted under the toolbar; pin it
        // to the top-left corner, above everything, so it stays visible and
        // clickable once the sider auto-collapses on narrow viewports.
        zeroWidthTriggerStyle={{ top: 0, left: 0, zIndex: 20 }}
        // `overflow: auto` here (rather than on DiscoveryRail's own Tree,
        // which already scrolls itself) clips the zero-width trigger to
        // invisible once the sider collapses to ~1px — the trigger is an
        // absolutely-positioned sibling inside this same clipped box.
        style={{ borderRight: '1px solid #f0f0f0' }}
      >
        <DiscoveryRail pods={railPodsState} selection={draftSelection} onSelectionChange={setDraftSelection} />
      </Layout.Sider>
      <Layout.Content style={{ padding: '12px 16px', display: 'flex', flexDirection: 'column', minWidth: 0 }}>
        <PeriodControls
          window={draftWindow}
          onWindowChange={setDraftWindow}
          onApply={apply}
          applying={state.kind === 'loading'}
        />
        <CallsToolbar
          search={search}
          onSearchChange={commitSearch}
          prefs={prefs}
          onPrefsChange={changePrefs}
          disabled={search.fromMs === null}
        />
        <Space orientation="vertical" style={{ width: '100%' }} size={8}>
          {selectionTooWide ? <SelectionTooWideBanner search={search} onSearchChange={commitSearch} /> : null}
          {state.kind === 'too-wide' ? (
            <TooWideBanner problem={state.problem} search={search} onSearchChange={commitSearch} />
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
          {committedPodsState.kind === 'error' && hasSelection ? (
            <Alert
              type="error"
              showIcon
              title="Cannot resolve the service selection without /pods"
              description={committedPodsState.message}
              action={
                <Button size="small" onClick={refetchCommittedPods}>
                  Retry
                </Button>
              }
            />
          ) : null}
          {committedPodsState.kind === 'ready' && committedPodsState.partial && hasSelection ? (
            <PodsPartialBanner reasons={committedPodsState.partialReasons} onRetry={refetchCommittedPods} />
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
        {state.kind === 'idle' && !waitingForExpansion && !selectionTooWide ? (
          <Empty
            style={{ marginTop: 48 }}
            description={
              selectionEmpty
                ? 'The selected services have no pods in this window.'
                : 'Select a namespace or service and a period, then Apply.'
            }
          />
        ) : state.kind === 'loading' || waitingForExpansion ? (
          <CallsTable rows={[]} loading prefs={prefs} onPrefsChange={changePrefs} virtual={VIRTUAL} />
        ) : state.kind === 'ready' ? (
          visibleRows.length === 0 && state.nextCursor === null && !state.emptyPaused ? (
            <Empty style={{ marginTop: 48 }} description="No calls match the filters in this window." />
          ) : (
            <CallsTable
              rows={visibleRows}
              loading={false}
              prefs={prefs}
              onPrefsChange={changePrefs}
              handlers={{ onPinPod: pinPod }}
              virtual={VIRTUAL}
              footer={footer}
            />
          )
        ) : null}
      </Layout.Content>
    </Layout>
  );
}
