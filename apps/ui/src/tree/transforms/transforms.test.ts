import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../../msgpack/tree-wire';
import { buildTreeModel, findNodeById } from '../model';
import { applyAdjustments, factorByMethod, invalidAdjustLines, parseAdjustConfig } from './adjust';
import { applyCategories, invalidCategoryLines, parseCategoryConfig } from './categories';
import { computeFlatProfile } from './flat-profile';
import { findUsages, incomingCalls, localHotspots, outgoingCalls } from './merge';

// Synthetic model (durations consistent; wire executions = self + children):
//   Entry(1000, self 100)
//   ├─ ServiceA(500, self 100, ×5)
//   │   ├─ Query.run(300, self 300, ×30, sql)
//   │   └─ Util.recurse(100, self 50)
//   │       └─ Util.recurse(50, self 50)        ← self-recursion
//   └─ ServiceB(400, self 100, ×2)
//       └─ Query.run(300, self 300, ×10, sql)
const METHODS = [
  'void e.Entry.handle() (Entry.java:1) [app.jar]',
  'void s.ServiceA.doWork() (ServiceA.java:2) [app.jar]',
  'ResultSet db.Query.run() (Query.java:3) [db.jar]',
  'void s.ServiceB.other() (ServiceB.java:4) [app.jar]',
  'void u.Util.recurse() (Util.java:5) [app.jar]',
];

const SQL = 'select * from t where id = ?';

function fixture(): TreeWire {
  const node = (
    methodIdx: number,
    durationMs: number,
    selfDurationMs: number,
    selfExecutions: number,
    executions: number,
    children?: TreeWire['root'][],
    sqlStats?: { durationMs: number; executions: number },
  ): TreeWire['root'] => ({
    methodIdx,
    durationMs,
    selfDurationMs,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions,
    selfExecutions,
    ...(children === undefined ? {} : { children }),
    ...(sqlStats === undefined
      ? {}
      : { params: [{ paramIdx: 0, groups: [{ value: SQL, ...sqlStats }] }] }),
  });

  return {
    v: 1,
    methods: [...METHODS],
    params: ['sql'],
    root: node(0, 1000, 100, 1, 50, [
      node(1, 500, 100, 5, 37, [
        node(2, 300, 300, 30, 30, undefined, { durationMs: 300, executions: 30 }),
        node(4, 100, 50, 1, 2, [node(4, 50, 50, 1, 1)]),
      ]),
      node(3, 400, 100, 2, 12, [node(2, 300, 300, 10, 10, undefined, { durationMs: 300, executions: 10 })]),
    ]),
  };
}

describe('computeFlatProfile (hotspots)', () => {
  it('ranks methods by self time and counts recursion once', () => {
    const profiles = computeFlatProfile(buildTreeModel(fixture()));
    expect(profiles).toHaveLength(1);
    const methods = profiles[0]!.methods;
    expect(methods.map((m) => m.methodIdx)).toEqual([2, 0, 1, 3, 4]);
    const query = methods[0]!;
    expect(query.selfDurationMs).toBe(600);
    expect(query.selfExecutions).toBe(40);
    expect(query.params[0]!.groups[0]).toMatchObject({ value: SQL, durationMs: 600, executions: 40 });
    // Util.recurse: the nested occurrence adds self time but not total time.
    const util = methods.find((m) => m.methodIdx === 4)!;
    expect(util.selfDurationMs).toBe(100);
    expect(util.durationMs).toBe(100);
  });

  it('groups by category first when categories are set', () => {
    const model = buildTreeModel(fixture());
    applyCategories(model, parseCategoryConfig('orders s.ServiceA.doWork*\nbilling s.ServiceB.other*'));
    const profiles = computeFlatProfile(model);
    const byName = new Map(profiles.map((p) => [p.category?.name ?? null, p]));
    expect(new Set(byName.keys())).toEqual(new Set([null, 'billing', 'orders']));
    // Query.run's 600ms of self time splits by the category context.
    expect(byName.get('orders')!.methods.find((m) => m.methodIdx === 2)!.selfDurationMs).toBe(300);
    expect(byName.get('billing')!.methods.find((m) => m.methodIdx === 2)!.selfDurationMs).toBe(300);
    expect(byName.get(null)!.methods.map((m) => m.methodIdx)).toEqual([0]);
    // Uncategorised time sorts last.
    expect(profiles[profiles.length - 1]!.category).toBeNull();
  });
});

describe('findUsages', () => {
  it('collects every call path to the method, rooted at the callers', () => {
    const model = buildTreeModel(fixture());
    const usages = findUsages(model, 2);
    const root = usages.root;
    expect(root.methodIdx).toBe(0); // single entry path unwraps to Entry
    expect(root.durationMs).toBe(600);
    expect(root.children.map((c) => c.methodIdx).sort()).toEqual([1, 3]);
    const viaA = root.children.find((c) => c.methodIdx === 1)!;
    expect(viaA.durationMs).toBe(300);
    expect(viaA.children[0]!.methodIdx).toBe(2);
    expect(viaA.children[0]!.selfExecutions).toBe(30);
  });

  it('attributes a recursive method once per outermost occurrence', () => {
    const model = buildTreeModel(fixture());
    const usages = findUsages(model, 4);
    // 100ms total, not 150: the inner occurrence's 50ms is subtracted from
    // the outer one (the time[] mechanics).
    expect(usages.root.durationMs).toBe(100);
  });
});

describe('incomingCalls', () => {
  it('roots at the method and grows towards the callers', () => {
    const model = buildTreeModel(fixture());
    const incoming = incomingCalls(model, 2);
    const root = incoming.root;
    expect(root.methodIdx).toBe(2);
    expect(root.durationMs).toBe(600);
    expect(root.selfExecutions).toBe(40);
    const callers = root.children.map((c) => c.methodIdx).sort();
    expect(callers).toEqual([1, 3]);
    const viaB = root.children.find((c) => c.methodIdx === 3)!;
    expect(viaB.durationMs).toBe(300);
    expect(viaB.children[0]!.methodIdx).toBe(0);
  });
});

describe('outgoingCalls', () => {
  it('merges every occurrence and their params into one subtree', () => {
    const model = buildTreeModel(fixture());
    const outgoing = outgoingCalls(model, 2);
    expect(outgoing.root.methodIdx).toBe(2);
    expect(outgoing.root.selfDurationMs).toBe(600);
    expect(outgoing.root.selfExecutions).toBe(40);
    expect(outgoing.root.children).toHaveLength(0);
    expect(outgoing.root.params[0]!.groups[0]).toMatchObject({ value: SQL, durationMs: 600, executions: 40 });
  });

  it('folds a nested self-recursive occurrence into the root', () => {
    const model = buildTreeModel(fixture());
    const outgoing = outgoingCalls(model, 4);
    expect(outgoing.root.selfDurationMs).toBe(100);
    expect(outgoing.root.selfExecutions).toBe(2);
    expect(outgoing.root.durationMs).toBe(100);
    expect(outgoing.root.children).toHaveLength(0);
  });
});

describe('localHotspots (PR 708 review #7)', () => {
  it('scopes to the selected node subtree, not every occurrence of the method', () => {
    const model = buildTreeModel(fixture());
    // Query.run (methodIdx 2) appears under both ServiceA (×30) and ServiceB
    // (×10). The node under ServiceA has the stable pre-order id 2.
    const underServiceA = findNodeById(model.root, 2)!;
    expect(underServiceA.methodIdx).toBe(2);
    expect(underServiceA.selfExecutions).toBe(30);

    const local = localHotspots(model, underServiceA);
    expect(local.root.methodIdx).toBe(2);
    expect(local.root.selfDurationMs).toBe(300);
    expect(local.root.selfExecutions).toBe(30);
    expect(local.root.children).toHaveLength(0);

    // The whole-method merge would double it — the bug this fix closes.
    expect(outgoingCalls(model, 2).root.selfExecutions).toBe(40);
  });

  it('folds self-recursion within the selected subtree, and one occurrence stays one', () => {
    const model = buildTreeModel(fixture());
    // The outer Util.recurse (id 3) has a nested self-recursive child (id 4).
    const outer = findNodeById(model.root, 3)!;
    expect(outer.methodIdx).toBe(4);
    const folded = localHotspots(model, outer);
    expect(folded.root.selfDurationMs).toBe(100); // 50 own + 50 nested, folded
    expect(folded.root.selfExecutions).toBe(2);
    expect(folded.root.children).toHaveLength(0);

    // Selecting the inner occurrence alone keeps only its own 50ms.
    const inner = localHotspots(model, findNodeById(model.root, 4)!);
    expect(inner.root.selfDurationMs).toBe(50);
    expect(inner.root.selfExecutions).toBe(1);
  });
});

describe('adjust duration', () => {
  it('parses factors, fractions, comments, and wildcards', () => {
    const rules = parseAdjustConfig('# speed up the db\n1/10 *Query.run*\n2 s.ServiceB.other');
    expect(rules).toHaveLength(2);
    const model = buildTreeModel(fixture());
    const factors = factorByMethod(model, rules);
    expect(factors.get(2)).toBeCloseTo(0.1);
    expect(factors.get(3)).toBe(2);
  });

  it('scales matched subtrees and recomputes ancestor totals', () => {
    const model = buildTreeModel(fixture());
    applyAdjustments(model, factorByMethod(model, parseAdjustConfig('1/10 *Query.run*')));
    const root = model.root;
    // Query self 300 → 30 on both paths; ancestors re-add: ServiceA
    // 100+30+100 = 230, ServiceB 100+30 = 130, Entry 100+230+130 = 460.
    expect(root.durationMs).toBe(460);
    const serviceA = root.children.find((c) => c.methodIdx === 1)!;
    expect(serviceA.durationMs).toBe(230);
    const query = serviceA.children.find((c) => c.methodIdx === 2)!;
    expect(query.selfDurationMs).toBe(30);
    expect(query.selfExecutions).toBe(3); // executions scale with the factor
    expect(query.params[0]!.groups[0]!.durationMs).toBe(30); // kTags scaling
    // ServiceA's child-call count recomputes from the scaled children.
    expect(serviceA.childExecutions).toBe(3 + 2);
  });

  it('cascades the factor down the matched subtree', () => {
    const model = buildTreeModel(fixture());
    applyAdjustments(model, factorByMethod(model, parseAdjustConfig('10 s.ServiceB.other')));
    const serviceB = model.root.children.find((c) => c.methodIdx === 3)!;
    // Self 100 → 1000 and the Query below scales too: 300 → 3000.
    expect(serviceB.durationMs).toBe(4000);
    expect(model.root.durationMs).toBe(100 + 500 + 4000);
  });

  it('flags a line with no pattern, or a factor that is neither a number nor a fraction', () => {
    expect(invalidAdjustLines('not-a-valid-rule')).toEqual([1]);
    expect(invalidAdjustLines('abc *Query.run*')).toEqual([1]);
    expect(invalidAdjustLines('1/10 *Query.run*')).toEqual([]);
  });

  it('reports 1-based line numbers, skipping blanks and comments', () => {
    expect(invalidAdjustLines('# ok\n1/10 *Query.run*\n\nbroken\n2 s.ServiceB.other\nalso broken')).toEqual([4, 6]);
  });
});

describe('setup categories', () => {
  it('propagates down the subtree with child overrides', () => {
    const model = buildTreeModel(fixture());
    applyCategories(model, parseCategoryConfig('web e.Entry.handle*\nbilling s.ServiceB.other*'));
    const root = model.root;
    expect(root.category?.name).toBe('web');
    const serviceA = root.children.find((c) => c.methodIdx === 1)!;
    expect(serviceA.category?.name).toBe('web'); // inherited
    const serviceB = root.children.find((c) => c.methodIdx === 3)!;
    expect(serviceB.category?.name).toBe('billing'); // override
    expect(serviceB.children[0]!.category?.name).toBe('billing');
  });

  it("a '>' pattern assigns the children, not the node", () => {
    const model = buildTreeModel(fixture());
    applyCategories(model, parseCategoryConfig('db >s.ServiceA.doWork*'));
    const serviceA = model.root.children.find((c) => c.methodIdx === 1)!;
    expect(serviceA.category).toBeUndefined();
    expect(serviceA.children.every((c) => c.category?.name === 'db')).toBe(true);
  });

  it('the longest pattern wins and colours stay stable per category', () => {
    const config = parseCategoryConfig('broad s.Service*\nnarrow s.ServiceA.doWork*');
    const model = buildTreeModel(fixture());
    applyCategories(model, config);
    const serviceA = model.root.children.find((c) => c.methodIdx === 1)!;
    const serviceB = model.root.children.find((c) => c.methodIdx === 3)!;
    expect(serviceA.category?.name).toBe('narrow');
    expect(serviceB.category?.name).toBe('broad');
    expect(serviceA.category?.color).toMatch(/^hsl\(/);
  });

  it('flags a line with no pattern', () => {
    expect(invalidCategoryLines('invalidlinewithoutpattern')).toEqual([1]);
    expect(invalidCategoryLines('db >s.ServiceA.doWork*')).toEqual([]);
  });

  it('reports 1-based line numbers, skipping blanks and comments', () => {
    expect(invalidCategoryLines('# ok\nweb e.Entry.handle*\n\nbroken\nbilling s.ServiceB.other*')).toEqual([4]);
  });
});
