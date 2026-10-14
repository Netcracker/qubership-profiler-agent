import { cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { syncTreePrefs } from '../ui/tree-prefs';
import { buildTreeModel } from './model';
import { TreeView } from './tree-view';
import styles from './tree-view.module.css';

// Row layout of the redesigned tree row (09 §3.3):
// [caret] [operations menu] total (self) ×calls package.Class.method(args)
// [chain badge] [category badge] … [share bar] share,
// with the package dimmed in front of the class and part of the copied text.

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

  it('dims the package in front of the Class.method label and keeps it copyable', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const label = screen.getByTitle(METHODS[1]!);
    // Selecting the row copies the qualified name.
    expect(label.textContent).toBe('com.acme.orders.CheckoutFlow.placeOrder(Cart)');
    const pkg = label.querySelector('span');
    expect(pkg?.textContent).toBe('com.acme.orders.');
    expect(pkg?.className).toContain(styles.pkg);
  });

  it('puts an accessible operations menu left of the label', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const label = screen.getByTitle(METHODS[1]!);
    const menu = within(label.closest('div')!).getByTitle('Operations');
    expect(menu.getAttribute('aria-label')).toBe('Open node operations');
    expect(menu.compareDocumentPosition(label) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it('keeps the three core operations on top of the row menu and offers no AI entry', async () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    fireEvent.click(screen.getAllByTitle('Operations')[0]!);
    const items = (await screen.findAllByRole('menuitem')).map((el) => el.textContent);
    expect(items).toEqual([
      'Incoming calls',
      'Outgoing calls',
      'Get stacktrace',
      'Find usages',
      'Local hotspots',
      'Adjust duration',
      'Add category',
      'Mark red',
    ]);
  });

  it('prints total (self) ×calls in front of the label, with the count always shown', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const row = screen.getByTitle(METHODS[1]!).closest('div')!;
    expect(row.textContent).toContain('600ms (600ms) ×1');
  });

  it('highlights the numbers that stand out, with no switch to turn it off', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    // The child takes 60% of the call, all of it self time: a bold total and a red self time.
    const row = screen.getByTitle(METHODS[1]!).closest('div')!;
    expect(within(row).getByText('600ms').className).toContain(styles.totalBold);
    expect(within(row).getByText('(600ms)').className).toContain(styles.heatCrit);
    expect(within(row).getByText('×1').className).not.toContain(styles.heatWarn);
    expect(screen.queryByRole('switch', { name: /highlight/i })).toBeNull();
  });

  it('draws the share bar in two parts and prints the share', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    // The root: 100% of the call, 40% of it in the method itself.
    const rootRow = screen.getByTitle(METHODS[0]!).closest('div')!;
    const bar = within(rootRow).getByTitle('100% of the call, 40% in the method itself');
    const [self, children] = bar.querySelectorAll('span');
    expect(self!.style.width).toBe('40px');
    expect(children!.style.width).toBe('60px');
    expect(rootRow.textContent).toMatch(/100%$/);
  });

  it('selects a row from its label and clears the selection on a second click', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const label = screen.getByTitle(METHODS[1]!);
    expect(label).toHaveAttribute('aria-pressed', 'false');
    fireEvent.click(label);
    expect(screen.getByTitle(METHODS[1]!)).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByTitle(METHODS[1]!).closest('div')!.className).toContain(styles.rowSelected);
    // Selecting a row adds no strip of operations beside the search field.
    expect(screen.queryByRole('button', { name: 'Incoming calls' })).toBeNull();
    fireEvent.keyDown(screen.getByTitle(METHODS[1]!), { key: 'Enter' });
    expect(screen.getByTitle(METHODS[1]!)).toHaveAttribute('aria-pressed', 'false');
  });

  it('reports the selection to the page when the page owns it', () => {
    const onSelect = vi.fn();
    const model = buildTreeModel(wire());
    render(<TreeView model={model} selectedId={model.root.id} onSelect={onSelect} />);
    expect(screen.getByTitle(METHODS[0]!)).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(screen.getByTitle(METHODS[1]!));
    expect(onSelect).toHaveBeenCalledWith(model.root.children[0]!.id);
    fireEvent.click(screen.getByTitle(METHODS[0]!));
    expect(onSelect).toHaveBeenLastCalledWith(null);
  });

  it('notes the bottom-up view in the operations menu label on caller trees', () => {
    render(<TreeView model={buildTreeModel(wire())} direction="bottom-up" />);
    const label = screen.getByTitle(METHODS[1]!);
    const menu = within(label.closest('div')!).getByTitle('Operations');
    expect(menu.getAttribute('aria-label')).toBe('Open node operations (bottom-up view)');
  });

  it('shows the Ctrl stats in an overlay without remounting the hovered row', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    const row = screen.getByTitle(METHODS[1]!).closest('div')!;
    fireEvent.mouseEnter(row);
    expect(screen.queryByText('Total suspension')).toBeNull();

    fireEvent.keyDown(window, { key: 'Control' });
    expect(screen.getByText('Total suspension')).toBeInTheDocument();
    // A Popover wrap would reparent the row and clear the text selection;
    // the overlay must leave the row element itself untouched.
    expect(screen.getByTitle(METHODS[1]!).closest('div')).toBe(row);

    fireEvent.keyUp(window, { key: 'Control' });
    expect(screen.queryByText('Total suspension')).toBeNull();
  });
});

describe('TreeView display switches and help', () => {
  afterEach(() => {
    cleanup();
    localStorage.clear();
    syncTreePrefs();
  });

  const childRow = (): HTMLElement => screen.getByTitle(METHODS[1]!).closest('div')!;

  it('draws the connector of a child and drops it with "Tree lines" off', () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    // The only child is the last one: an elbow that ends at the row.
    expect(childRow().querySelector(`.${styles.guideEnd}`)).not.toBeNull();

    fireEvent.click(screen.getByRole('switch', { name: /Tree lines/ }));
    expect(childRow().querySelector(`.${styles.guideEnd}`)).toBeNull();
    // The cell stays, so the row keeps its indent.
    expect(childRow().querySelectorAll(`.${styles.guide}`)).toHaveLength(1);
  });

  it('keeps both switches across a remount', () => {
    const first = render(<TreeView model={buildTreeModel(wire())} />);
    expect(screen.getByRole('switch', { name: /Tree lines/ })).toBeChecked();
    expect(screen.getByRole('switch', { name: /Row lines/ })).toBeChecked();
    fireEvent.click(screen.getByRole('switch', { name: /Row lines/ }));
    expect(childRow().closest(`.${styles.noRowLines}`)).not.toBeNull();
    first.unmount();

    // A new page load reads the stored value.
    syncTreePrefs();
    render(<TreeView model={buildTreeModel(wire())} />);
    expect(screen.getByRole('switch', { name: /Row lines/ })).not.toBeChecked();
    expect(screen.getByRole('switch', { name: /Tree lines/ })).toBeChecked();
    expect(childRow().closest(`.${styles.noRowLines}`)).not.toBeNull();
  });

  it('explains the row, the colors, and the badges in the Help popover', async () => {
    render(<TreeView model={buildTreeModel(wire())} />);
    expect(screen.queryByText('How to read the call tree')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: /Help/ }));
    expect(await screen.findByText('How to read the call tree')).toBeInTheDocument();
    // The thresholds come from the same constants as the highlighting.
    expect(screen.getByText(/Self time is orange from 10% of the call and red from 25%/)).toBeInTheDocument();
    expect(screen.getByText(/A call count is orange from 10 and red from 100/)).toBeInTheDocument();
    expect(screen.getByText('Ctrl + hover')).toBeInTheDocument();
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

  it('opens the way to a selection made in another view, through a skipped chain', () => {
    const model = buildTreeModel(chainWire());
    // FilterB is inside the chain that the root skips, so it has no row yet.
    const filterB = model.root.children[0]!.children[0]!;
    expect(model.methods[filterB.methodIdx]).toContain('FilterB');
    const view = render(<TreeView model={model} selectedId={null} onSelect={() => undefined} />);
    expect(screen.queryByTitle(CHAIN_METHODS[2]!)).toBeNull();

    view.rerender(<TreeView model={model} selectedId={filterB.id} onSelect={() => undefined} />);
    expect(screen.getByTitle(CHAIN_METHODS[2]!)).toHaveAttribute('aria-pressed', 'true');
  });

  it('opens a collapsed branch for a selection made in another view', () => {
    const model = buildTreeModel(chainWire());
    const business = model.root.children[0]!.children[0]!.children[0]!.children[0]!;
    const leafY = business.children.find((c) => model.methods[c.methodIdx]!.includes('LeafY'))!;
    const view = render(<TreeView model={model} selectedId={null} onSelect={() => undefined} />);
    // Fold the root, so nothing under it has a row.
    fireEvent.click(within(screen.getByTitle(CHAIN_METHODS[0]!).closest('div')!).getByLabelText('Collapse node'));
    expect(screen.queryByTitle(CHAIN_METHODS[6]!)).toBeNull();

    view.rerender(<TreeView model={model} selectedId={leafY.id} onSelect={() => undefined} />);
    expect(screen.getByTitle(CHAIN_METHODS[6]!)).toHaveAttribute('aria-pressed', 'true');
  });

  const visibleLabels = (): string[] =>
    [...document.querySelectorAll('[title$=".jar]"]')].map((el) => el.textContent ?? '');

  it('reveal → fold restores the original visible-row set', () => {
    render(<TreeView model={buildTreeModel(chainWire())} />);
    // FilterA skips through FilterB and FilterC to Business, the chain end.
    const reveal = screen.getByText('+3');
    const before = visibleLabels();
    expect(before.some((l) => l.includes('FilterB.pass'))).toBe(false);
    expect(before.some((l) => l.includes('LeafX.run'))).toBe(true);

    fireEvent.click(reveal);
    const revealed = visibleLabels();
    expect(revealed.some((l) => l.includes('FilterB.pass'))).toBe(true);
    expect(revealed.some((l) => l.includes('FilterC.pass'))).toBe(true);
    expect(revealed.some((l) => l.includes('Business.work'))).toBe(true);
    expect(screen.queryByText('+3')).toBeNull();

    // Every revealed chain node offers a fold; the head's restores it all.
    fireEvent.click(screen.getByText('−3'));
    expect(visibleLabels()).toEqual(before);
    expect(screen.getByText('+3')).toBeInTheDocument();
  });
});

describe('TreeView parameter value viewer', () => {
  // Short enough to stay under the reformat threshold (param-value-viewer.ts
  // beautifySql, ported from the old UI's printReformatted) — these two
  // tests cover the viewer/copy basics without the reformat toggle.
  const SQL_TEXT = "SELECT id FROM orders WHERE id = 1";
  // Long, single-line, and reformats to something different — exercises the
  // view-original/view-reformatted toggle.
  const LONG_SQL_TEXT = "SELECT id, total FROM orders WHERE status = 'open' AND total > 100";

  function wireWithSqlParam(value: string): TreeWire {
    return {
      v: 1,
      methods: [...METHODS],
      params: ['sql'],
      root: {
        methodIdx: 0,
        durationMs: 1000,
        selfDurationMs: 1000,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 1,
        selfExecutions: 1,
        params: [{ paramIdx: 0, groups: [{ value, durationMs: 500, executions: 1 }] }],
      },
    };
  }

  afterEach(cleanup);

  beforeEach(() => {
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: vi.fn().mockResolvedValue(undefined) },
      configurable: true,
    });
  });

  it('opens the full-value viewer with SQL highlighting from an inline param row', () => {
    render(
      <AntdApp>
        <TreeView model={buildTreeModel(wireWithSqlParam(SQL_TEXT))} />
      </AntdApp>,
    );
    // The row itself still truncates via CSS ellipsis; the button is the
    // only way to reach the full, untruncated text.
    expect(screen.getByTitle(SQL_TEXT)).toBeInTheDocument();

    fireEvent.click(screen.getByTitle('View full value'));
    expect(screen.getByText('Full value — sql')).toBeInTheDocument();
    // Rendered inside the modal, not just the ellipsised row label, via
    // highlight.js (hljs-keyword class, not just an inline style).
    const select = screen.getAllByText('SELECT').find((el) => el.className === 'hljs-keyword');
    expect(select).toBeDefined();
    // No reformat toggle for a value under the threshold.
    expect(screen.queryByText('view original')).toBeNull();
  });

  it('copies the full param value to the clipboard', async () => {
    render(
      <AntdApp>
        <TreeView model={buildTreeModel(wireWithSqlParam(SQL_TEXT))} />
      </AntdApp>,
    );
    fireEvent.click(screen.getByTitle('View full value'));
    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(SQL_TEXT);
  });

  it('reformats a long single-line SQL value, with a toggle back to the original', () => {
    render(
      <AntdApp>
        <TreeView model={buildTreeModel(wireWithSqlParam(LONG_SQL_TEXT))} />
      </AntdApp>,
    );
    fireEvent.click(screen.getByTitle('View full value'));

    // Reformatted (multi-line) is shown by default, matching the old UI.
    const toggle = screen.getByText('view original');
    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));
    expect(navigator.clipboard.writeText).toHaveBeenLastCalledWith(expect.stringContaining('\n'));

    fireEvent.click(toggle);
    expect(screen.getByText('view reformatted')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));
    expect(navigator.clipboard.writeText).toHaveBeenLastCalledWith(LONG_SQL_TEXT);
  });
});

describe('TreeView Expand all and Collapse all', () => {
  afterEach(cleanup);

  const leaf = (methodIdx: number, durationMs: number): TreeNodeWire => ({
    methodIdx,
    durationMs,
    selfDurationMs: durationMs,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 1,
    selfExecutions: 1,
  });
  const branch = (methodIdx: number, durationMs: number, children: TreeNodeWire[]): TreeNodeWire => ({
    ...leaf(methodIdx, durationMs),
    selfDurationMs: durationMs - children.reduce((sum, c) => sum + c.durationMs, 0),
    executions: 1 + children.reduce((sum, c) => sum + c.executions, 0),
    children,
  });

  // Root(1000) ── Big(900) ── BigChild(400)
  //            └─ Small(50) ── SmallChild(20)   ← ≤10% of the call: starts collapsed
  const NAMES = ['Root.run', 'Big.work', 'BigChild.bit', 'Small.work', 'SmallChild.bit'];
  const methods = NAMES.map((name) => `void a.${name}() (${name.split('.')[0]}.java:1) [a.jar]`);
  const nested = (): TreeWire => ({
    v: 1,
    methods,
    params: [],
    root: branch(0, 1000, [branch(1, 900, [leaf(2, 400)]), branch(3, 50, [leaf(4, 20)])]),
  });
  const shown = (i: number): boolean => screen.queryByTitle(methods[i]!) !== null;

  /** Rows the list holds, on screen or not: its spacer is one row height per row. */
  const rowCount = (container: HTMLElement): number => {
    const spacer = container.querySelector<HTMLElement>('[style*="height"] > [style*="height"]')!;
    return parseInt(spacer.style.height, 10) / 28;
  };

  it('expands every node and collapses everything under the root', () => {
    render(
      <AntdApp>
        <TreeView model={buildTreeModel(nested())} />
      </AntdApp>,
    );
    expect(shown(4)).toBe(false);

    fireEvent.click(screen.getByRole('button', { name: /Expand all/ }));
    expect([0, 1, 2, 3, 4].every(shown)).toBe(true);

    fireEvent.click(screen.getByRole('button', { name: /Collapse all/ }));
    expect(shown(1)).toBe(true);
    expect(shown(3)).toBe(true);
    expect(shown(2)).toBe(false);
    expect(shown(4)).toBe(false);
  });

  it('stops at the 20,000-row budget and says so', async () => {
    // 200 children of 200 leaves each: 40,201 rows when fully open. Breadth
    // first, the root and 98 children fit: 1 + 200 + 98 × 200 = 19,801 rows,
    // and the 99th would make 20,001.
    const wide: TreeWire = {
      v: 1,
      methods: ['void a.Root.run() (Root.java:1) [a.jar]', 'void a.Mid.run() (Mid.java:1) [a.jar]', 'void a.Leaf.run() (Leaf.java:1) [a.jar]'],
      params: [],
      root: branch(
        0,
        40_000,
        Array.from({ length: 200 }, () => branch(1, 200, Array.from({ length: 200 }, () => leaf(2, 1)))),
      ),
    };
    const { container } = render(
      <AntdApp>
        <TreeView model={buildTreeModel(wide)} />
      </AntdApp>,
    );
    fireEvent.click(screen.getByRole('button', { name: /Expand all/ }));
    expect(rowCount(container)).toBe(19_801);
    expect(await screen.findByText(/The tree has more than 20,000 rows/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /Collapse all/ }));
    expect(rowCount(container)).toBe(201);
  });
});
