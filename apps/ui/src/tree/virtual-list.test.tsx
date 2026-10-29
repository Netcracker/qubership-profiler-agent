import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { VirtualList } from './virtual-list';

// 100 rows of 10 px in a 50 px viewport: five rows fit, plus the overscan.
const ITEMS = Array.from({ length: 100 }, (_, i) => i);

function List({ ensureVisible }: { ensureVisible?: { key: string | number; index: number } }) {
  return (
    <VirtualList
      items={ITEMS}
      rowHeight={10}
      height={50}
      overscan={2}
      itemKey={(i) => i}
      renderRow={(i) => <span>row {i}</span>}
      ensureVisible={ensureVisible}
    />
  );
}

const viewport = (container: HTMLElement): HTMLElement => container.firstElementChild as HTMLElement;

describe('VirtualList', () => {
  afterEach(cleanup);

  it('renders only the rows in view and the overscan', () => {
    render(<List />);
    expect(screen.getByText('row 0')).toBeInTheDocument();
    expect(screen.getByText('row 6')).toBeInTheDocument();
    expect(screen.queryByText('row 7')).toBeNull();
  });

  it('scrolls a row below the view to the middle of the viewport', () => {
    const { container } = render(<List ensureVisible={{ key: 'a', index: 80 }} />);
    // Row 80 starts at 800 px; centered in 50 px, the viewport starts at 780 px.
    expect(viewport(container).scrollTop).toBe(780);
    expect(screen.getByText('row 80')).toBeInTheDocument();
    expect(screen.queryByText('row 0')).toBeNull();
  });

  it('leaves the scroll alone when the row is in view already', () => {
    const { container } = render(<List ensureVisible={{ key: 'a', index: 3 }} />);
    expect(viewport(container).scrollTop).toBe(0);
  });

  it('scrolls once per key, not again when the row moves', () => {
    const view = render(<List ensureVisible={{ key: 'a', index: 80 }} />);
    view.rerender(<List ensureVisible={{ key: 'a', index: 90 }} />);
    expect(viewport(view.container).scrollTop).toBe(780);

    // A new key is a new request.
    view.rerender(<List ensureVisible={{ key: 'b', index: 20 }} />);
    expect(viewport(view.container).scrollTop).toBe(180);
  });
});
