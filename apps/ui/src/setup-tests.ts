import '@testing-library/jest-dom/vitest';

// jsdom lacks a few browser APIs AntD components read; stub the minimum.

if (typeof window !== 'undefined') {
  if (window.matchMedia === undefined) {
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: (query: string): MediaQueryList =>
        ({
          matches: false,
          media: query,
          onchange: null,
          addListener: () => undefined,
          removeListener: () => undefined,
          addEventListener: () => undefined,
          removeEventListener: () => undefined,
          dispatchEvent: () => false,
        }) as MediaQueryList,
    });
  }

  // d3-transition animates an SVG `transform` by reading `transform.baseVal`,
  // which jsdom leaves out. The flame graph only translates its frames, so a
  // matrix read from `translate(x,y)` is enough.
  if (!('transform' in SVGElement.prototype)) {
    Object.defineProperty(SVGElement.prototype, 'transform', {
      get(this: SVGElement) {
        const match = /translate\(\s*([-\d.e]+)[\s,]+([-\d.e]+)\s*\)/.exec(this.getAttribute('transform') ?? '');
        const e = match === null ? 0 : Number(match[1]);
        const f = match === null ? 0 : Number(match[2]);
        return { baseVal: { consolidate: () => ({ matrix: { a: 1, b: 0, c: 0, d: 1, e, f } }) } };
      },
    });
  }

  if (window.ResizeObserver === undefined) {
    class ResizeObserverStub implements ResizeObserver {
      observe(): void {}
      unobserve(): void {}
      disconnect(): void {}
    }
    Object.defineProperty(window, 'ResizeObserver', { writable: true, value: ResizeObserverStub });
  }
}
