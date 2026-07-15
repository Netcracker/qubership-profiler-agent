import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import type { TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { buildTreeModel } from './model';
import { TreeView } from './tree-view';

// Row layout of the redesigned tree row (old treeNode2html order):
// [toggle] [direction/operations menu] [bar][number] … Class.method(args),
// with the package hidden yet copyable in front of the visible label.

const METHODS = [
  'void org.apache.catalina.connector.CoyoteAdapter.service(Request,Response) (CoyoteAdapter.java:340) [catalina.jar]',
  'Order com.acme.orders.CheckoutFlow.placeOrder(Cart) (CheckoutFlow.java:120) [orders.jar]',
];

function wire(): TreeWire {
  return {
    v: 1,
    methods: [...METHODS],
    params: [],
    root: {
      methodIdx: 0,
      durationMs: 1000,
      selfDurationMs: 400,
      suspensionMs: 0,
      selfSuspensionMs: 0,
      executions: 2,
      selfExecutions: 1,
      children: [
        {
          methodIdx: 1,
          durationMs: 600,
          selfDurationMs: 600,
          suspensionMs: 0,
          selfSuspensionMs: 0,
          executions: 1,
          selfExecutions: 1,
        },
      ],
    },
  };
}

describe('TreeView row layout', () => {
  afterEach(cleanup);

  it('shows the bare Class.method label and keeps the package copyable', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const label = screen.getByTitle(METHODS[1]!);
    // Selecting the row copies the qualified name…
    expect(label.textContent).toBe('com.acme.orders.CheckoutFlow.placeOrder(Cart)');
    // …but the package span is invisible (font-size 0, never display:none).
    const pkg = label.querySelector('span');
    expect(pkg?.textContent).toBe('com.acme.orders.');
    expect(pkg?.style.fontSize).toBe('0px');
  });

  it('puts an accessible operations menu left of the label', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const label = screen.getByTitle(METHODS[1]!);
    const row = label.closest('div')!;
    const buttons = row.querySelectorAll('button');
    expect(buttons[1]!.title).toBe('Operations');
    expect(buttons[1]!.getAttribute('aria-label')).toBe('Open node operations');
    expect(buttons[1]!.compareDocumentPosition(label) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    // The fixed-width bar slot is gone: the bar hugs the duration number.
    for (const span of row.querySelectorAll('span')) {
      expect(span.style.width).not.toBe('62px');
    }
  });

  it('notes the bottom-up view in the operations menu label on caller trees', () => {
    render(<TreeView model={buildTreeModel(wire())} direction="bottom-up" />);
    const label = screen.getByTitle(METHODS[1]!);
    const buttons = label.closest('div')!.querySelectorAll('button');
    expect(buttons[1]!.getAttribute('aria-label')).toBe('Open node operations (bottom-up view)');
  });

  it('shows the Ctrl stats in an overlay without remounting the hovered row', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const row = screen.getByTitle(METHODS[1]!).closest('div')!;
    fireEvent.mouseEnter(row);
    expect(screen.queryByText('Self time')).toBeNull();

    fireEvent.keyDown(window, { key: 'Control' });
    expect(screen.getByText('Self time')).toBeInTheDocument();
    // A Popover wrap would reparent the row and clear the text selection;
    // the overlay must leave the row element itself untouched.
    expect(screen.getByTitle(METHODS[1]!).closest('div')).toBe(row);

    fireEvent.keyUp(window, { key: 'Control' });
    expect(screen.queryByText('Self time')).toBeNull();
  });
});

describe('TreeView pass-through chains', () => {
  afterEach(cleanup);

  const CHAIN_METHODS = [
    'void com.acme.Root.serve() (Root.java:1) [app.jar]',
    'void com.acme.FilterA.pass() (FilterA.java:1) [app.jar]',
    'void com.acme.FilterB.pass() (FilterB.java:1) [app.jar]',
    'void com.acme.FilterC.pass() (FilterC.java:1) [app.jar]',
    'void com.acme.Business.work() (Business.java:1) [app.jar]',
    'void com.acme.LeafX.run() (LeafX.java:1) [app.jar]',
    'void com.acme.LeafY.run() (LeafY.java:1) [app.jar]',
  ];

  const leaf = (methodIdx: number, durationMs: number): TreeNodeWire => ({
    methodIdx,
    durationMs,
    selfDurationMs: durationMs,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 1,
    selfExecutions: 1,
  });

  const passThrough = (methodIdx: number, child: TreeNodeWire): TreeNodeWire => ({
    methodIdx,
    durationMs: child.durationMs,
    selfDurationMs: 0,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 1 + child.executions,
    selfExecutions: 1,
    children: [child],
  });

  function chainWire(): TreeWire {
    const business: TreeNodeWire = {
      methodIdx: 4,
      durationMs: 900,
      selfDurationMs: 100,
      suspensionMs: 0,
      selfSuspensionMs: 0,
      executions: 3,
      selfExecutions: 1,
      children: [leaf(5, 500), leaf(6, 300)],
    };
    const chain = passThrough(1, passThrough(2, passThrough(3, business)));
    return {
      v: 1,
      methods: [...CHAIN_METHODS],
      params: [],
      root: {
        methodIdx: 0,
        durationMs: 1000,
        selfDurationMs: 100,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 1 + chain.executions,
        selfExecutions: 1,
        children: [chain],
      },
    };
  }

  const visibleLabels = (): string[] =>
    [...document.querySelectorAll('[title$=".jar]"]')].map((el) => el.textContent ?? '');

  it('reveal → fold restores the original visible-row set', () => {
    render(<TreeView model={buildTreeModel(chainWire())} />);
    // FilterA skips through FilterB and FilterC to Business, the chain end.
    const reveal = screen.getByText(/⤵/);
    expect(reveal.textContent).toBe('⤵3');
    const before = visibleLabels();
    expect(before.some((l) => l.includes('FilterB.pass'))).toBe(false);
    expect(before.some((l) => l.includes('LeafX.run'))).toBe(true);

    fireEvent.click(reveal);
    const revealed = visibleLabels();
    expect(revealed.some((l) => l.includes('FilterB.pass'))).toBe(true);
    expect(revealed.some((l) => l.includes('FilterC.pass'))).toBe(true);
    expect(revealed.some((l) => l.includes('Business.work'))).toBe(true);
    expect(screen.queryByText(/⤵/)).toBeNull();

    // Every revealed chain node offers a fold; the head's restores it all.
    fireEvent.click(screen.getAllByText(/⤴/).find((el) => el.textContent === '⤴3')!);
    expect(visibleLabels()).toEqual(before);
    expect(screen.getByText(/⤵/).textContent).toBe('⤵3');
  });
});
