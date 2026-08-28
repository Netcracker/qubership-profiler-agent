import { useEffect, useRef } from 'react';

// GitHub-style `y`: freeze a live relative time range (`now-3h`) into an absolute
// permalink, the way `y` on a branch page pins the URL to a commit SHA.

function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || target.isContentEditable;
}

/**
 * Bind `y` to `onFreeze` while `enabled`. Ignores the key when a field is focused
 * or a modifier is held, so it never steals a keystroke meant for typing.
 */
export function usePermalinkHotkey(enabled: boolean, onFreeze: () => void): void {
  // Keep the latest callback without re-binding the listener each render.
  const freeze = useRef(onFreeze);
  freeze.current = onFreeze;

  useEffect(() => {
    if (!enabled) return undefined;
    const onKey = (e: KeyboardEvent): void => {
      if (e.key !== 'y' || e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return;
      if (isTypingTarget(e.target)) return;
      e.preventDefault();
      freeze.current();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [enabled]);
}
