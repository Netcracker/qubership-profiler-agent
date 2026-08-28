import { Alert, Button, Empty, Layout, Space, Typography, message } from 'antd';
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
import { resolveUrlTime } from '../controls/time-range';
import { DiscoveryRail, selectionEquals } from '../discovery/discovery-rail';
import type { RailSelection } from '../discovery/discovery-rail';
import { expandSelection } from '../discovery/group-pods';
import { usePods } from '../discovery/use-pods';
import { CALLS_URL_LENGTH_LIMIT, callsQueryUrlLength } from '../api/endpoints';
import type { CallsFilter } from '../api/endpoints';
import { callsSearchToParams, freezeWindow, hasRelativeWindow, parseCallsSearch } from '../url/search-params';
import type { CallsSearchState } from '../url/search-params';
import { usePermalinkHotkey } from '../url/use-permalink-hotkey';
import styles from './calls-page.module.css';

// Discovery + Calls (09 §2). The URL is the source of truth; the rail and the
// period picker edit a draft that Apply commits to the URL, which keys the
// /calls fetch. Toolbar filters narrow an applied query and commit directly.

// jsdom reports zero heights, which starves the virtual scroller of rows.
const VIRTUAL = import.meta.env.MODE !== 'test';

export function CallsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  // Resolve relative window tokens against one `now` per URL change; the memo
  // freezes it, so `now-3h` does not drift (and refetch) on every render.
  const search = useMemo(() => parseCallsSearch(searchParams, Date.now(), { defaultWindow: true }), [searchParams]);

  const [draftWindow, setDraftWindow] = useState<DraftWindow>({ fromMs: search.fromMs, toMs: search.toMs });
  const [draftSelection, setDraftSelection] = useState<RailSelection>({
    services: search.services,
    pods: search.pods,
  });
  const [prefs, setPrefs] = useState<ColumnPrefs>(() => loadColumnPrefs(DEFAULT_COLUMN_ORDER));
  // Tracked so the content can reserve a gutter for the collapsed rail's
  // trigger, which is pinned to the top-left corner (PR 708 review #8).
  const [railCollapsed, setRailCollapsed] = useState(false);

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
      durationMaxMs: search.durationMaxMs === 0 ? undefined : search.durationMaxMs,
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

  // A range change stores its raw tokens (a relative range stays live) and
  // applies at once, keeping the current rail selection; the rail follows via
  // the draft window without waiting for the URL round-trip.
  const commitRange = (tokens: { from: string; to: string }): void => {
    const now = Date.now();
    setDraftWindow({ fromMs: resolveUrlTime(tokens.from, now), toMs: resolveUrlTime(tokens.to, now) });
    commitSearch({
      ...search,
      from: tokens.from || null,
      to: tokens.to || null,
      services: draftSelection.services,
      pods: draftSelection.pods,
    });
  };

  // The sibling Apply commits rail-selection edits against the committed window.
  const apply = (): void =>
    commitSearch({ ...search, services: draftSelection.services, pods: draftSelection.pods });
  // Apply has work only when the draft selection diverges from the committed one.
  const selectionDirty = !selectionEquals(draftSelection, search);

  // `y` freezes a live relative range into an absolute permalink (GitHub-style).
  usePermalinkHotkey(hasRelativeWindow(search), () => {
    commitSearch(freezeWindow(search));
    message.success('Time range pinned to a permalink');
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
    <Layout className={styles.page}>
      <Layout.Sider
        width={320}
        theme="light"
        breakpoint="lg"
        collapsedWidth={0}
        onCollapse={setRailCollapsed}
        // The default zero-width trigger has no header row to sit in here
        // (CallsPage has none) and ends up painted under the toolbar; pin it
        // to the top-left corner, above everything, so it stays visible and
        // clickable once the sider auto-collapses on narrow viewports. The
        // content reserves a left gutter while collapsed so it does not sit on
        // top of the period control (PR 708 review #8).
        zeroWidthTriggerStyle={{ top: 0, left: 0, zIndex: 20 }}
        // `overflow: auto` here (rather than on DiscoveryRail's own Tree,
        // which already scrolls itself) clips the zero-width trigger to
        // invisible once the sider collapses to ~1px — the trigger is an
        // absolutely-positioned sibling inside this same clipped box.
        className={styles.sider}
      >
        <DiscoveryRail pods={railPodsState} selection={draftSelection} onSelectionChange={setDraftSelection} />
      </Layout.Sider>
      <Layout.Content className={`${styles.content}${railCollapsed ? ` ${styles.railGutter}` : ''}`}>
        <PeriodControls
          value={{ from: search.from, to: search.to, fromMs: search.fromMs, toMs: search.toMs }}
          onCommitRange={commitRange}
          onApply={apply}
          canApply={selectionDirty}
          applying={state.kind === 'loading'}
        />
        <CallsToolbar
          search={search}
          onSearchChange={commitSearch}
          prefs={prefs}
          onPrefsChange={changePrefs}
          disabled={search.fromMs === null}
        />
        <Space orientation="vertical" className={styles.fullWidth} size={8}>
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
            className={styles.emptyTop}
            description={
              selectionEmpty
                ? 'The selected services have no pods in this window.'
                : 'Select a namespace or service, then Apply.'
            }
          />
        ) : state.kind === 'loading' || waitingForExpansion ? (
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
