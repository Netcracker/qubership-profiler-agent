import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { CallJSON } from '../api/types';
import { treeForCall } from '../mocks/synthetic';
import type { TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { setThemePreference, syncThemeMode } from '../ui/theme-mode';
import { FlameView } from './flame-view';
import { buildTreeModel } from './model';
import type { TreeModel, TreeNode } from './model';
import { applyCategories, parseCategoryConfig } from './transforms/categories';
import { toFlameData } from './transforms/flame-data';
import type { FlameDatum } from './transforms/flame-data';

// The view draws with d3-flame-graph, which runs under jsdom: these tests
// drive the real library. The frame geometry is read from the attributes the
// library sets at once (a frame's `width` and an entering frame's
// `transform`), not from the ones it animates (a rect's width).

// Synthetic call (the model sorts children by duration, descending):
//   Entry(1000, self 100)
//   ├─ ServiceA(500, self 200)
//   │   └─ Query.run(300, self 300)
//   ├─ ServiceB(400, self 400)
//   └─ Tiny.blip(0 ms)              ← no width: not drawn
const METHODS = [
  'void e.Entry.handle() (Entry.java:1) [app.jar]',
  'void s.ServiceA.doWork() (ServiceA.java:2) [app.jar]',
  'ResultSet db.Query.run() (Query.java:3) [db.jar]',
  'void s.ServiceB.other() (ServiceB.java:4) [app.jar]',
  'void t.Tiny.blip() (Tiny.java:5) [app.jar]',
];

/** The width the view falls back to under jsdom, where elements measure 0. */
const WIDTH = 1200;

const node = (methodIdx: number, durationMs: number, selfDurationMs: number, children?: TreeNodeWire[]): TreeNodeWire => ({
  methodIdx,
  durationMs,
  selfDurationMs,
  suspensionMs: 0,
  selfSuspensionMs: 0,
  executions: 1,
  selfExecutions: 1,
  ...(children === undefined ? {} : { children }),
});

function fixture(): TreeModel {
  const wire: TreeWire = {
    v: 1,
    methods: [...METHODS],
    params: [],
    root: node(0, 1000, 100, [node(1, 500, 200, [node(2, 300, 300)]), node(3, 400, 400), node(4, 0, 0)]),
  };
  return buildTreeModel(wire);
}

const byMethod = (model: TreeModel, methodIdx: number): TreeNode => {
  const found: TreeNode[] = [];
  const visit = (n: TreeNode): void => {
    if (n.methodIdx === methodIdx) found.push(n);
    n.children.forEach(visit);
  };
  visit(model.root);
  return found[0]!;
};

/** The page's role: it owns the selection that the two tabs share. */
function Harness({ model, onSelect }: { model: TreeModel; onSelect?: (id: number | null) => void }) {
  const [selectedId, setSelectedId] = useState<number | null>(null);
  return (
    <FlameView
      model={model}
      selectedId={selectedId}
      onSelect={(id) => {
        setSelectedId(id);
        onSelect?.(id);
      }}
    />
  );
}

const frame = (name: RegExp): SVGGElement => screen.getByRole('button', { name }) as unknown as SVGGElement;
const widthOf = (name: RegExp): number => Number(frame(name).getAttribute('width'));
const fillOf = (name: RegExp): string | null => frame(name).querySelector('rect')!.getAttribute('fill');

describe('FlameView', () => {
  afterEach(() => {
    cleanup();
    document.documentElement.style.removeProperty('--pf-color-flame-1');
    localStorage.clear();
    syncThemeMode();
  });

  it('draws one frame per node, the whole call on top, sized by its share of the call', () => {
    render(<Harness model={fixture()} />);
    expect(widthOf(/^Entry\.handle · /)).toBe(WIDTH);
    expect(frame(/^Entry\.handle · /).getAttribute('transform')).toBe('translate(0,0)');

    expect(widthOf(/^ServiceA\.doWork · /)).toBe(WIDTH * 0.5);
    expect(frame(/^ServiceA\.doWork · /).getAttribute('transform')).toBe('translate(0,28)');
    // The next sibling starts where the first one ends.
    expect(widthOf(/^ServiceB\.other · /)).toBe(WIDTH * 0.4);
    expect(frame(/^ServiceB\.other · /).getAttribute('transform')).toBe(`translate(${WIDTH * 0.5},28)`);
    expect(frame(/^Query\.run · /).getAttribute('transform')).toBe('translate(0,56)');
  });

  it('does not draw a frame without width', () => {
    render(<Harness model={fixture()} />);
    expect(screen.queryByRole('button', { name: /^Tiny\.blip/ })).toBeNull();
    expect(document.querySelectorAll('g.frame')).toHaveLength(4);
  });

  it('describes a frame in its accessible name, its tooltip, and the readout on hover', () => {
    render(<Harness model={fixture()} />);
    expect(screen.getByText('Hover a frame for its numbers. Click one to zoom in on it and select it.')).toBeInTheDocument();
    const text = 'ServiceA.doWork · 500ms · 50% of the call · self 200ms · ×1';
    const serviceA = frame(/^ServiceA\.doWork · /);
    expect(serviceA).toHaveAccessibleName(text);
    expect(serviceA.querySelector('title')?.textContent).toBe(text);

    fireEvent.mouseOver(serviceA);
    expect(screen.getByText(text, { selector: 'span' })).toBeInTheDocument();
    fireEvent.mouseOut(serviceA);
    expect(screen.queryByText(text, { selector: 'span' })).toBeNull();
  });

  it('zooms on a click and selects the node; a second click clears the selection', () => {
    const onSelect = vi.fn();
    const model = fixture();
    render(<Harness model={model} onSelect={onSelect} />);
    const serviceA = byMethod(model, 1);
    expect(screen.queryByRole('button', { name: 'Reset zoom' })).toBeNull();

    fireEvent.click(frame(/^ServiceA\.doWork · /));
    expect(onSelect).toHaveBeenLastCalledWith(serviceA.id);
    expect(frame(/^ServiceA\.doWork · /)).toHaveAttribute('aria-pressed', 'true');
    // The frame now spans the graph, its child scales with it, and its sibling is gone.
    expect(widthOf(/^ServiceA\.doWork · /)).toBe(WIDTH);
    expect(widthOf(/^Query\.run · /)).toBe(WIDTH * 0.6);
    expect(screen.queryByRole('button', { name: /^ServiceB\.other/ })).toBeNull();
    // The numbers still describe the whole call.
    expect(frame(/^ServiceA\.doWork · /)).toHaveAccessibleName(/50% of the call/);
    // The readout falls back to the selection.
    expect(screen.getByText(/^ServiceA\.doWork · 500ms/, { selector: 'span' })).toBeInTheDocument();
    // Selecting adds no strip of operations.
    expect(screen.queryByRole('button', { name: 'Incoming calls' })).toBeNull();

    fireEvent.click(frame(/^ServiceA\.doWork · /));
    expect(onSelect).toHaveBeenLastCalledWith(null);
  });

  it('returns to the whole call from the reset link and keeps the selection', () => {
    const onSelect = vi.fn();
    render(<Harness model={fixture()} onSelect={onSelect} />);
    fireEvent.click(frame(/^ServiceA\.doWork · /));
    expect(screen.getByText(/^Zoomed in on/)).toHaveTextContent('Zoomed in on ServiceA.doWork.');

    fireEvent.click(screen.getByRole('button', { name: /Reset zoom/ }));
    expect(widthOf(/^ServiceA\.doWork · /)).toBe(WIDTH * 0.5);
    expect(frame(/^ServiceB\.other · /)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Reset zoom/ })).toBeNull();
    expect(onSelect).toHaveBeenCalledTimes(1);
    expect(frame(/^ServiceA\.doWork · /)).toHaveAttribute('aria-pressed', 'true');
  });

  it('reaches a frame with the keyboard and zooms on it with Enter', async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    const model = fixture();
    render(<Harness model={model} onSelect={onSelect} />);

    await user.tab();
    expect(frame(/^Entry\.handle · /)).toHaveFocus();
    await user.tab();
    expect(frame(/^ServiceA\.doWork · /)).toHaveFocus();
    // Focus feeds the readout, the way hover does.
    expect(screen.getByText(/^ServiceA\.doWork · 500ms/, { selector: 'span' })).toBeInTheDocument();

    await user.keyboard('{Enter}');
    expect(onSelect).toHaveBeenLastCalledWith(byMethod(model, 1).id);
    expect(widthOf(/^ServiceA\.doWork · /)).toBe(WIDTH);
  });

  it('zooms on a node selected in the Call Tree tab, without selecting again', () => {
    const onSelect = vi.fn();
    const model = fixture();
    const view = render(<FlameView model={model} selectedId={null} onSelect={onSelect} />);
    view.rerender(<FlameView model={model} selectedId={byMethod(model, 2).id} onSelect={onSelect} />);
    expect(widthOf(/^Query\.run · /)).toBe(WIDTH);
    expect(frame(/^Query\.run · /)).toHaveAttribute('aria-pressed', 'true');
    expect(screen.queryByRole('button', { name: /^ServiceB\.other/ })).toBeNull();
    expect(onSelect).not.toHaveBeenCalled();

    // From one zoom straight to a frame the first one hid.
    view.rerender(<FlameView model={model} selectedId={byMethod(model, 3).id} onSelect={onSelect} />);
    expect(widthOf(/^ServiceB\.other · /)).toBe(WIDTH);
    expect(onSelect).not.toHaveBeenCalled();
  });

  it('does not zoom on a node selected in the Call Tree tab that takes no time', () => {
    const model = fixture();
    const view = render(<FlameView model={model} selectedId={null} onSelect={() => undefined} />);
    // Tiny.blip takes no time, so the graph draws no frame for it.
    view.rerender(<FlameView model={model} selectedId={byMethod(model, 4).id} onSelect={() => undefined} />);
    expect(screen.getByText(/^Tiny\.blip · /, { selector: 'span' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Reset zoom/ })).toBeNull();
    expect(widthOf(/^ServiceA\.doWork · /)).toBe(WIDTH * 0.5);
  });

  it('keeps the zoom when the model is rebuilt', () => {
    const first = fixture();
    const view = render(<FlameView model={first} selectedId={null} onSelect={() => undefined} />);
    fireEvent.click(frame(/^ServiceA\.doWork · /));
    // Node ids survive a rebuild (Adjust, Setup), so the zoom finds its node again.
    view.rerender(<FlameView model={fixture()} selectedId={null} onSelect={() => undefined} />);
    expect(widthOf(/^ServiceA\.doWork · /)).toBe(WIDTH);
    expect(screen.getByRole('button', { name: /Reset zoom/ })).toBeInTheDocument();
  });

  it('colors frames by category and lists the categories in the legend', () => {
    const model = fixture();
    applyCategories(model, parseCategoryConfig('db db.Query.run'));
    render(<Harness model={model} />);
    // jsdom applies no stylesheet, so the variable stays unresolved and names the choice.
    expect(fillOf(/^Query\.run · /)).toBe('var(--pf-color-flame-1)');
    expect(fillOf(/^ServiceB\.other · /)).toBe('var(--pf-color-bar-soft)');
    expect(screen.getByText('db')).toBeInTheDocument();
    expect(screen.getByText('No category')).toBeInTheDocument();
  });

  it('resolves the category colors of the current theme and redraws on a theme change', () => {
    const root = document.documentElement;
    root.style.setProperty('--pf-color-flame-1', 'rgb(1, 2, 3)');
    const model = fixture();
    applyCategories(model, parseCategoryConfig('db db.Query.run'));
    render(<Harness model={model} />);
    expect(fillOf(/^Query\.run · /)).toBe('rgb(1, 2, 3)');

    // theme.css would switch the variable with data-theme; the test switches it by hand.
    root.style.setProperty('--pf-color-flame-1', 'rgb(4, 5, 6)');
    act(() => setThemePreference('dark'));
    expect(fillOf(/^Query\.run · /)).toBe('rgb(4, 5, 6)');
  });
});

// Not asserted beyond a sanity bound (timing in CI is noisy): logged for the
// task report, like the WP-F scale check in tree-scale.test.ts.
describe('flame graph scale', () => {
  afterEach(cleanup);

  function longCall(bufferOffset: number): CallJSON {
    return {
      pk: {
        pod_namespace: 'orders',
        pod_service: 'checkout',
        pod_name: 'checkout-0001',
        restart_time_ms: 1_700_000_000_000,
        trace_file_index: 1,
        buffer_offset: bufferOffset,
        record_index: 0,
      },
      ts_ms: 1_700_000_432_198,
      duration_ms: 28_262,
      method: 'com.acme.orders.CheckoutFlow.placeOrder',
      thread_name: 'http-nio-8080-exec-1',
      cpu_time_ms: 20_000,
      wait_time_ms: 1000,
      memory_used: 1 << 20,
      queue_wait_ms: 5,
      suspend_ms: 20,
      child_calls: 4000,
      transactions: 2,
      logs_generated: 0,
      logs_written: 0,
      file_read: 0,
      file_written: 0,
      net_read: 0,
      net_written: 0,
      error_flag: false,
      retention_class: 'long_clean',
      params: { 'request.id': ['deadbeef'] },
      trace_blob_size: null,
      truncated_reason: null,
    };
  }

  /** A tree at the row budget: `fanout` children per node, each taking a fixed cut of its parent. */
  function budgetTree(nodes: number, fanout: number): TreeModel {
    let left = nodes - 1;
    const build = (durationMs: number, depth: number): TreeNodeWire => {
      const children: TreeNodeWire[] = [];
      // Breadth first within a level keeps the tree balanced until the budget runs out.
      const count = Math.min(fanout, left);
      left -= count;
      const cuts = [0.5, 0.25, 0.12, 0.06];
      for (let i = 0; i < count; i++) children.push({ ...node(depth + 1, durationMs * cuts[i % cuts.length]!, 0) });
      return { ...node(depth, durationMs, 0), children };
    };
    const root = build(1_000_000, 0);
    let level = root.children ?? [];
    let depth = 1;
    while (left > 0 && level.length > 0) {
      const next: TreeNodeWire[] = [];
      for (const parent of level) {
        if (left <= 0) break;
        const built = build(parent.durationMs, depth);
        parent.children = built.children;
        next.push(...(built.children ?? []));
      }
      level = next;
      depth += 1;
    }
    const methods = Array.from({ length: depth + 2 }, (_, i) => `void a.L${i}.run() (L${i}.java:1) [a.jar]`);
    return buildTreeModel({ v: 1, methods, params: [], root });
  }

  /** Frames wider than one pixel at `width`, the library's culling rule; a culled frame takes its subtree along. */
  function expectedFrames(datum: FlameDatum, width: number): number {
    const scale = width / datum.value;
    const count = (d: FlameDatum): number =>
      d.value * scale > 1 ? 1 + d.children.reduce((sum, c) => sum + count(c), 0) : 0;
    return count(datum);
  }

  it('reports the DOM size and the render time of the largest trees', () => {
    let largest = buildTreeModel(treeForCall(longCall(432_198)));
    for (let offset = 0; offset < 40; offset++) {
      const candidate = buildTreeModel(treeForCall(longCall(400_000 + offset)));
      if (candidate.nodeCount > largest.nodeCount) largest = candidate;
    }
    const budget = budgetTree(20_000, 4);

    for (const [name, model] of [
      ['largest mock tree', largest],
      ['20,000-node tree', budget],
    ] as const) {
      const t0 = performance.now();
      const data = toFlameData(model.root, (n) => String(n.methodIdx));
      const transformMs = performance.now() - t0;

      const t1 = performance.now();
      const view = render(<Harness model={model} />);
      const renderMs = performance.now() - t1;
      const frames = view.container.querySelectorAll('g.frame').length;
      const elements = view.container.querySelectorAll('*').length;
      view.unmount();

      // The view draws exactly the frames the culling keeps, never more than the nodes.
      expect(frames).toBe(expectedFrames(data, WIDTH));
      expect(frames).toBeLessThanOrEqual(model.nodeCount);
      console.info(
        `flame scale, ${name}: ${model.nodeCount} nodes, ${frames} frames at ${WIDTH} px, ${elements} DOM elements, ` +
          `transform ${transformMs.toFixed(1)}ms, first render ${renderMs.toFixed(0)}ms (jsdom)`,
      );
    }
  });
});
