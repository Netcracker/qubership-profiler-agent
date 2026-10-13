import { useLayoutEffect, useRef, useState } from 'react';
import type { RefObject } from 'react';

/**
 * Tracks an element's pixel width. A hidden element measures 0 (an inactive
 * tab pane, or any element under jsdom); the fallback then stands in, so a
 * caller that divides by the width always gets a usable number.
 */
export function useElementWidth<T extends HTMLElement>(fallback = 1200): [RefObject<T | null>, number] {
  const ref = useRef<T | null>(null);
  const [width, setWidth] = useState(fallback);
  useLayoutEffect(() => {
    const el = ref.current;
    if (el === null || typeof ResizeObserver === 'undefined') return;
    const measure = (): void => setWidth(el.clientWidth > 0 ? el.clientWidth : fallback);
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    measure();
    return () => observer.disconnect();
  }, [fallback]);
  return [ref, width];
}
