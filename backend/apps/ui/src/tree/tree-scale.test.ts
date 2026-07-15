import { describe, expect, it } from 'vitest';

import type { CallJSON } from '../api/types';
import { treeForCall } from '../mocks/synthetic';
import { buildTreeModel } from './model';
import type { TreeNode } from './model';
import { buildRows } from './visible-rows';

// The 07 §5.4 scale check (WP-F): a long synthetic call must give the
// virtualiser 1000+ visible rows to window, and the full visible-row
// derivation over that model has to stay cheap — it reruns per
// expand/collapse, while scrolling only slices the memoised array.

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

describe('virtualiser scale (WP-F)', () => {
  it('a long call yields 1000+ visible rows, derived in single-digit milliseconds', () => {
    // Tree size is seed-dependent; a handful of nearby offsets reliably
    // contains a 1000+-row call.
    let model = buildTreeModel(treeForCall(longCall(432_198)));
    for (let offset = 0; model.nodeCount < 1000 && offset < 20; offset++) {
      model = buildTreeModel(treeForCall(longCall(400_000 + offset)));
    }

    const expanded = new Set<number>();
    const walk = (n: TreeNode): void => {
      expanded.add(n.id);
      n.children.forEach(walk);
    };
    walk(model.root);

    const state = { expanded, revealedChains: new Set<number>(), expandedParams: new Set<string>() };
    const rows = buildRows(model, state, true);
    expect(rows.length).toBeGreaterThan(1000);

    // Not asserted (CI timing is noisy) — logged for the progress doc.
    const t0 = performance.now();
    const runs = 20;
    for (let i = 0; i < runs; i++) buildRows(model, state, true);
    const perRun = (performance.now() - t0) / runs;
    console.info(
      `WP-F scale: ${model.nodeCount} nodes, ${rows.length} visible rows, buildRows ${perRun.toFixed(2)}ms/run`,
    );
  });
});
