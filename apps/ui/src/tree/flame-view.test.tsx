import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { CallJSON } from '../api/types';
import { treeForCall } from '../mocks/synthetic';
import type { TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { FlameView } from './flame-view';
import { buildTreeModel } from './model';
import type { TreeModel } from './model';
import { applyCategories, parseCategoryConfig } from './transforms/categories';
import { flameLayout } from './transforms/flame';

// Synthetic call (the model sorts children by duration, descending):
//   Entry(1000, self 100)
//   ├─ ServiceA(500, self 200)
//   │   └─ Query.run(300, self 300)
//   ├─ ServiceB(400, self 400)
//   └─ Tiny.blip(0 ms)              ← under one pixel: not drawn
const METHODS = [
  'void e.Entry.handle() (Entry.java:1) [app.jar]',
  'void s.ServiceA.doWork() (ServiceA.java:2) [app.jar]',
  'ResultSet db.Query.run() (Query.java:3) [db.jar]',
  'void s.ServiceB.other() (ServiceB.java:4) [app.jar]',
  'void t.Tiny.blip() (Tiny.java:5) [app.jar]',
];

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

const block = (name: RegExp): HTMLElement => screen.getByRole('button', { name });

describe('FlameView', () => {
  afterEach(cleanup);

  it('draws one block per node, sized and placed by its share of the call', () => {
    render(<Harness model={fixture()} />);
    const entry = block(/^Entry\.handle · /);
    expect(entry.style.left).toBe('0%');
    expect(entry.style.width).toBe('100%');
    expect(entry.style.top).toBe('0px');

    const serviceA = block(/^ServiceA\.doWork · /);
    expect(serviceA.style.width).toBe('50%');
    expect(serviceA.style.top).toBe('28px');
    // The next sibling starts where the first one ends.
    const serviceB = block(/^ServiceB\.other · /);
    expect(serviceB.style.left).toBe('50%');
    expect(serviceB.style.width).toBe('40%');
    expect(block(/^Query\.run · /).style.top).toBe('56px');
  });

  it('does not draw a block under one pixel wide', () => {
    render(<Harness model={fixture()} />);
    expect(screen.queryByRole('button', { name: /^Tiny\.blip/ })).toBeNull();
  });

  it('describes a block in its accessible name and in the readout on hover', () => {
    render(<Harness model={fixture()} />);
    expect(screen.getByText('Hover a block for its numbers. Click one to select it.')).toBeInTheDocument();
    const serviceA = block(/^ServiceA\.doWork · /);
    expect(serviceA).toHaveAccessibleName('ServiceA.doWork · 500ms · 50% of the call · self 200ms · ×1');
    fireEvent.mouseEnter(serviceA);
    expect(screen.getByText('ServiceA.doWork · 500ms · 50% of the call · self 200ms · ×1')).toBeInTheDocument();
  });

  it('selects a block on click and clears the selection on a second click', () => {
    const onSelect = vi.fn();
    const model = fixture();
    render(<Harness model={model} onSelect={onSelect} />);
    const serviceA = model.root.children.find((c) => c.methodIdx === 1)!;

    fireEvent.click(block(/^ServiceA\.doWork · /));
    expect(onSelect).toHaveBeenLastCalledWith(serviceA.id);
    expect(block(/^ServiceA\.doWork · /)).toHaveAttribute('aria-pressed', 'true');
    expect(block(/^ServiceB\.other · /)).toHaveAttribute('aria-pressed', 'false');
    // Selecting adds no strip of operations.
    expect(screen.queryByRole('button', { name: 'Incoming calls' })).toBeNull();

    fireEvent.click(block(/^ServiceA\.doWork · /));
    expect(onSelect).toHaveBeenLastCalledWith(null);
  });

  it('reaches a block with the keyboard and selects it with Enter', async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    const model = fixture();
    render(<Harness model={model} onSelect={onSelect} />);

    await user.tab();
    expect(block(/^Entry\.handle · /)).toHaveFocus();
    await user.tab();
    expect(block(/^ServiceA\.doWork · /)).toHaveFocus();
    // Focus feeds the readout, the way hover does.
    expect(screen.getByText('ServiceA.doWork · 500ms · 50% of the call · self 200ms · ×1')).toBeInTheDocument();

    await user.keyboard('{Enter}');
    expect(onSelect).toHaveBeenLastCalledWith(model.root.children.find((c) => c.methodIdx === 1)!.id);
  });

  it('re-roots on a double click and returns to the whole call from the reset link', () => {
    render(<Harness model={fixture()} />);
    expect(screen.queryByRole('button', { name: 'Reset to the whole call' })).toBeNull();

    fireEvent.doubleClick(block(/^ServiceA\.doWork · /));
    // The block now spans the graph on the top row, and its siblings are gone.
    const serviceA = block(/^ServiceA\.doWork · /);
    expect(serviceA.style.width).toBe('100%');
    expect(serviceA.style.top).toBe('0px');
    expect(serviceA).toHaveAttribute('aria-pressed', 'true');
    expect(block(/^Query\.run · /).style.width).toBe('60%');
    expect(screen.queryByRole('button', { name: /^ServiceB\.other/ })).toBeNull();
    // The numbers still describe the whole call.
    expect(serviceA).toHaveAccessibleName(/50% of the call/);

    fireEvent.click(screen.getByRole('button', { name: 'Reset to the whole call' }));
    expect(block(/^ServiceA\.doWork · /).style.width).toBe('50%');
    expect(block(/^ServiceB\.other · /)).toBeInTheDocument();
  });

  it('re-roots from the keyboard with Shift+Enter', () => {
    render(<Harness model={fixture()} />);
    fireEvent.keyDown(block(/^ServiceA\.doWork · /), { key: 'Enter', shiftKey: true });
    expect(block(/^ServiceA\.doWork · /).style.width).toBe('100%');
  });

  it('colors blocks by category and lists the categories in the legend', () => {
    const model = fixture();
    applyCategories(model, parseCategoryConfig('db db.Query.run'));
    render(<Harness model={model} />);
    expect(block(/^Query\.run · /).style.background).toBe('var(--pf-color-flame-1)');
    // No inline fill: the stylesheet gives an uncategorized block the plain bar color.
    expect(block(/^ServiceB\.other · /).style.background).toBe('');
    expect(screen.getByText('db')).toBeInTheDocument();
    expect(screen.getByText('No category')).toBeInTheDocument();
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

  it('reports the DOM block count and the render time of the largest trees', () => {
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
      const all = flameLayout(model.root).blocks.length;
      const t0 = performance.now();
      const layout = flameLayout(model.root, { minWidth: 1 / 1200 });
      const layoutMs = performance.now() - t0;

      const t1 = performance.now();
      const view = render(<Harness model={model} />);
      const renderMs = performance.now() - t1;
      const dom = view.container.querySelectorAll('button[aria-pressed]').length;
      view.unmount();

      // The view draws exactly what the layout keeps, and culling never adds blocks.
      expect(dom).toBe(layout.blocks.length);
      expect(dom).toBeLessThanOrEqual(model.nodeCount);
      console.info(
        `flame scale, ${name}: ${model.nodeCount} nodes, ${all} blocks unculled, ${dom} DOM blocks at 1200 px, ` +
          `${layout.rows} rows, layout ${layoutMs.toFixed(1)}ms, first render ${renderMs.toFixed(0)}ms (jsdom)`,
      );
    }
  });
});
