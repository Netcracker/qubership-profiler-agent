import { useEffect } from 'react';
import { useBlocker } from 'react-router';
import type { Blocker } from 'react-router';

import { parseAgentsSearch } from '../url/search-params';

const scopeOf = (search: string): string => parseAgentsSearch(new URLSearchParams(search)).scope;

/**
 * Holds a navigation that would lose unsaved changes: to another screen or to
 * another scope, by a link or by the browser's back and forward buttons. A tab
 * switch within the scope passes. The caller shows the question while the
 * returned blocker is `blocked` and answers with `proceed` or `reset`.
 *
 * Closing or reloading the browser tab gets the browser's own prompt, which is
 * the only one a page may show there.
 */
export function useUnsavedGuard(dirty: boolean): Blocker {
  const blocker = useBlocker(
    ({ currentLocation, nextLocation }) =>
      dirty &&
      (currentLocation.pathname !== nextLocation.pathname ||
        scopeOf(currentLocation.search) !== scopeOf(nextLocation.search)),
  );

  useEffect(() => {
    if (!dirty) return;
    const ask = (e: BeforeUnloadEvent): void => e.preventDefault();
    window.addEventListener('beforeunload', ask);
    return () => window.removeEventListener('beforeunload', ask);
  }, [dirty]);

  return blocker;
}
