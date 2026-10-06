import { useLayoutEffect, useRef, useState } from 'react';
import type { RefObject } from 'react';

/** Tracks an element's pixel height for virtualised bodies. */
export function useElementHeight<T extends HTMLElement>(fallback = 400): [RefObject<T | null>, number] {
  const ref = useRef<T | null>(null);
  const [height, setHeight] = useState(fallback);
  useLayoutEffect(() => {
    const el = ref.current;
    if (el === null || typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(() => setHeight(el.clientHeight));
    observer.observe(el);
    setHeight(el.clientHeight);
    return () => observer.disconnect();
  }, []);
  return [ref, height];
}
