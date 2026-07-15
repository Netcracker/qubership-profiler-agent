import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { server } from '../mocks/node';
import { ApiError } from './client';
import { fetchCallsFirstPage, fetchTree } from './endpoints';

// Cold-call behaviour of /tree (02 §2.2, 09 §5): a bare PK outside the hot
// window is a 404 that names the missing hint; with the hints the same call
// decodes into a merged tree.

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe('fetchTree against the mock', () => {
  async function coldCall() {
    // Anything older than the 15-minute hot window is cold in the mock.
    const to = Date.now() - 60 * 60 * 1000;
    const page = await fetchCallsFirstPage({ fromMs: to - 15 * 60 * 1000, toMs: to, durationMinMs: 0 });
    const call = page.calls.find((c) => c.truncated_reason === null);
    if (call === undefined) throw new Error('mock produced no intact cold call');
    return call;
  }

  it('rejects a cold call without hints, naming ts_ms in the detail', async () => {
    const call = await coldCall();
    const error = await fetchTree(call.pk, {}).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    const apiError = error as ApiError;
    expect(apiError.status).toBe(404);
    expect(apiError.problem?.title).toBe('call not found');
    expect(apiError.problem?.detail).toContain('ts_ms');
  });

  it('decodes the tree when the hints travel with the request', async () => {
    const call = await coldCall();
    const tree = await fetchTree(call.pk, { tsMs: call.ts_ms, retentionClass: call.retention_class });
    expect(tree.v).toBe(1);
    expect(tree.methods.length).toBeGreaterThan(0);
    expect(tree.root.durationMs).toBe(call.duration_ms);
    // Merge invariants the backend guarantees (08 R5/R6) hold in the mock.
    const walk = (node: typeof tree.root): void => {
      const children = node.children ?? [];
      const childDuration = children.reduce((sum, c) => sum + c.durationMs, 0);
      expect(node.selfDurationMs).toBe(node.durationMs - childDuration);
      expect(node.executions).toBe(node.selfExecutions + children.reduce((sum, c) => sum + c.executions, 0));
      children.forEach(walk);
    };
    walk(tree.root);
  });
});
