import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useElementWidth } from './use-element-width';

function Probe() {
  const [ref, width] = useElementWidth<HTMLDivElement>(800);
  return (
    <div ref={ref} data-testid="probe">
      {width}
    </div>
  );
}

describe('useElementWidth', () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it('reports the width the element measures', () => {
    vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(640);
    render(<Probe />);
    expect(screen.getByTestId('probe').textContent).toBe('640');
  });

  it('falls back when the element measures 0, as a hidden tab pane does', () => {
    render(<Probe />);
    expect(screen.getByTestId('probe').textContent).toBe('800');
  });

  it('falls back when the browser has no ResizeObserver', () => {
    vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(640);
    const observer = window.ResizeObserver;
    // setup-tests.ts defines it writable but not configurable, so vi.stubGlobal cannot replace it.
    (window as { ResizeObserver?: typeof ResizeObserver }).ResizeObserver = undefined;
    try {
      render(<Probe />);
      expect(screen.getByTestId('probe').textContent).toBe('800');
    } finally {
      window.ResizeObserver = observer;
    }
  });
});
