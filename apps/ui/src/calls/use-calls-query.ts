import { useCallback, useEffect, useRef, useState } from 'react';

import { ApiError, isCursorRejection, isWideQueryRejection } from '../api/client';
import { fetchCallsFirstPage, fetchCallsNextPage } from '../api/endpoints';
import type { CallsFilter } from '../api/endpoints';
import { pkToPath } from '../api/pk';
import type { CallJSON, ProblemDetails } from '../api/types';

// Keyset-paginated /calls loading with the 09 §5 states first-class. Pages
// accumulate; the opaque cursor (frozen query, 02 §2.3.1) is transient state
// here — never in the URL.

export const PAGE_LIMIT = 100;

/** Auto-follow at most this many consecutive empty pages, then hand over (09 §5). */
export const MAX_EMPTY_PAGES = 3;

export interface CallsReady {
  kind: 'ready';
  rows: CallJSON[];
  nextCursor: string | null;
  partial: boolean;
  partialReasons: string[];
  loadingMore: boolean;
  /** The scroll cursor expired or broke; offer a restart from page one. */
  cursorExpired: boolean;
  /** Auto-paging stopped after MAX_EMPTY_PAGES empty rounds; manual continue. */
  emptyPaused: boolean;
  loadMoreError: string | null;
}

export type CallsQueryState =
  | { kind: 'idle' }
  | { kind: 'loading' }
  | { kind: 'too-wide'; problem: ProblemDetails }
  | { kind: 'all-failed'; detail: string }
  | { kind: 'error'; message: string }
  | CallsReady;

export interface UseCallsQueryResult {
  state: CallsQueryState;
  /** Fetch the next page (or resume after an empty-page pause). */
  loadMore: () => void;
  /** Restart from page one with the same filter. */
  refetch: () => void;
}

/** Deduplicate by PK across pages: the §4.3 overlap window can hand a page
 * boundary the same call twice; the server dedups within a page, the client
 * finishes the job across pages. */
function appendRows(prev: CallJSON[], next: CallJSON[]): CallJSON[] {
  const seen = new Set(prev.map((c) => pkToPath(c.pk)));
  return prev.concat(next.filter((c) => !seen.has(pkToPath(c.pk))));
}

export function useCallsQuery(filter: CallsFilter | null): UseCallsQueryResult {
  const [state, setState] = useState<CallsQueryState>({ kind: 'idle' });
  const [epoch, setEpoch] = useState(0);
  const abortRef = useRef<AbortController | null>(null);
  // The latest ready snapshot, so loadMore never races setState.
  const readyRef = useRef<CallsReady | null>(null);

  const commit = useCallback((next: CallsQueryState) => {
    readyRef.current = next.kind === 'ready' ? next : null;
    setState(next);
  }, []);

  const filterKey =
    filter === null
      ? null
      : JSON.stringify([
          filter.fromMs,
          filter.toMs,
          [...(filter.pods ?? [])].sort(),
          filter.method ?? '',
          filter.durationMinMs ?? 0,
          filter.durationMaxMs ?? 0,
          filter.errorOnly ?? false,
          [...(filter.retentionClasses ?? [])].sort(),
        ]);

  useEffect(() => {
    abortRef.current?.abort();
    if (filter === null || filterKey === null) {
      commit({ kind: 'idle' });
      return;
    }
    const controller = new AbortController();
    abortRef.current = controller;
    commit({ kind: 'loading' });

    void (async () => {
      try {
        let page = await fetchCallsFirstPage({ ...filter, limit: PAGE_LIMIT }, controller.signal);
        let rows: CallJSON[] = page.calls;
        let partial = page.partial;
        let reasons = [...page.partial_reasons];
        let emptyRounds = 0;
        // An empty page with a non-null cursor is not end-of-stream
        // (02 §2.3.1); follow it a bounded number of times.
        while (rows.length === 0 && page.next_cursor !== null && emptyRounds < MAX_EMPTY_PAGES) {
          emptyRounds += 1;
          page = await fetchCallsNextPage(page.next_cursor, controller.signal);
          rows = appendRows(rows, page.calls);
          partial = partial || page.partial;
          reasons = [...new Set([...reasons, ...page.partial_reasons])];
        }
        commit({
          kind: 'ready',
          rows,
          nextCursor: page.next_cursor,
          partial,
          partialReasons: reasons,
          loadingMore: false,
          cursorExpired: false,
          emptyPaused: rows.length === 0 && page.next_cursor !== null,
          loadMoreError: null,
        });
      } catch (e: unknown) {
        if (controller.signal.aborted) return;
        if (isWideQueryRejection(e) && e.problem !== null) {
          commit({ kind: 'too-wide', problem: e.problem });
        } else if (e instanceof ApiError && e.status === 504) {
          commit({ kind: 'all-failed', detail: e.problem?.detail ?? e.message });
        } else {
          commit({ kind: 'error', message: e instanceof Error ? e.message : String(e) });
        }
      }
    })();
    return () => controller.abort();
    // filterKey serialises every field the fetch reads; `filter` itself is a
    // fresh object each render, so keying on it would refetch in a loop.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterKey, epoch, commit]);

  const loadMore = useCallback(() => {
    const ready = readyRef.current;
    if (ready === null || ready.loadingMore || ready.nextCursor === null || ready.cursorExpired) return;
    const cursor = ready.nextCursor;
    const controller = abortRef.current ?? new AbortController();
    commit({ ...ready, loadingMore: true, emptyPaused: false, loadMoreError: null });

    void (async () => {
      const base = readyRef.current;
      if (base === null) return;
      try {
        let page = await fetchCallsNextPage(cursor, controller.signal);
        let rows = appendRows(base.rows, page.calls);
        let added = page.calls.length;
        let partial = base.partial || page.partial;
        let reasons = [...new Set([...base.partialReasons, ...page.partial_reasons])];
        let emptyRounds = added === 0 ? 1 : 0;
        while (added === 0 && page.next_cursor !== null && emptyRounds < MAX_EMPTY_PAGES) {
          page = await fetchCallsNextPage(page.next_cursor, controller.signal);
          rows = appendRows(rows, page.calls);
          added = page.calls.length;
          partial = partial || page.partial;
          reasons = [...new Set([...reasons, ...page.partial_reasons])];
          if (added === 0) emptyRounds += 1;
        }
        commit({
          kind: 'ready',
          rows,
          nextCursor: page.next_cursor,
          partial,
          partialReasons: reasons,
          loadingMore: false,
          cursorExpired: false,
          emptyPaused: added === 0 && page.next_cursor !== null,
          loadMoreError: null,
        });
      } catch (e: unknown) {
        if (controller.signal.aborted) return;
        const current = readyRef.current;
        if (current === null) return;
        if (isCursorRejection(e)) {
          commit({ ...current, loadingMore: false, cursorExpired: true });
        } else {
          commit({
            ...current,
            loadingMore: false,
            loadMoreError: e instanceof Error ? e.message : String(e),
          });
        }
      }
    })();
  }, [commit]);

  const refetch = useCallback(() => setEpoch((n) => n + 1), []);

  return { state, loadMore, refetch };
}
