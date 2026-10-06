import { comparePk, pkToPath } from '../api/pk';
import type { CallJSON, CallPK, PodEntry, RetentionClass } from '../api/types';
import type { ParamGroupWire, TreeNodeWire, TreeWire } from '../msgpack/tree-wire';

// Deterministic synthetic dataset behind the MSW mock. Every value derives
// from a hash of (pod, time bucket), so any window renders the same rows on
// every request — which is what keyset pagination needs — and no fixture is
// ever committed (WORKFLOW.md §6). Rows stream in the shared total order
// (ts_ms DESC, pk ASC; 02 §2.3.1) by walking one-second buckets downward.

export const HOT_WINDOW_MS = 15 * 60 * 1000;
const RESTART_PERIOD_MS = 6 * 60 * 60 * 1000;
const BUCKET_MS = 1000;

// --- Deterministic randomness ---

function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/** mulberry32: tiny deterministic PRNG, plenty for mock data. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// --- Topology ---

interface ServiceSpec {
  namespace: string;
  service: string;
  pods: number;
  methods: string[];
}

// A few methods come from the old UI's idleTags list
// (profiler-ui/src/dataFormat.mjs) so the "Hide system/proxy" toggle has
// something to hide.
export const IDLE_METHODS = [
  'org.quartz.core.QuartzSchedulerThread.run',
  'org.quartz.impl.jdbcjobstore.JobStoreSupport$ClusterManager.run',
  'weblogic.jms.bridge.internal.MessagingBridge.run',
];

const TOPOLOGY: ServiceSpec[] = [
  {
    namespace: 'payments',
    service: 'billing',
    pods: 3,
    methods: [
      'com.acme.billing.InvoiceService.createInvoice',
      'com.acme.billing.InvoiceService.listInvoices',
      'com.acme.billing.TaxCalculator.calculate',
    ],
  },
  {
    namespace: 'payments',
    service: 'gateway',
    pods: 2,
    methods: ['com.acme.gateway.PaymentGateway.authorize', 'com.acme.gateway.PaymentGateway.capture'],
  },
  {
    namespace: 'orders',
    service: 'checkout',
    pods: 3,
    methods: ['com.acme.orders.CheckoutFlow.placeOrder', 'com.acme.orders.CartService.validate'],
  },
  {
    namespace: 'orders',
    service: 'inventory',
    pods: 2,
    methods: ['com.acme.orders.InventoryService.reserve', 'com.acme.orders.InventoryService.release'],
  },
  {
    namespace: 'infra',
    service: 'proxy',
    pods: 2,
    methods: ['org.apache.catalina.connector.CoyoteAdapter.service'],
  },
];

// REST-ish routes per service, so the HTTP title (Calls Title column, Call
// Tree header) has a plausible path to show — real content is not the point.
const HTTP_ROUTES: Record<string, string[]> = {
  billing: ['/invoices', '/invoices/1', '/tax'],
  gateway: ['/payments/authorize', '/payments/capture'],
  checkout: ['/checkout', '/cart'],
  inventory: ['/inventory/sku-42'],
  proxy: ['/health'],
};

const SQL_TEXTS = [
  'select * from invoices where customer_id = ? and status = ?',
  'update inventory set reserved = reserved + ? where sku = ?',
  'insert into audit_log (actor, action, at) values (?, ?, ?)',
  'select o.id, o.total from orders o join order_lines l on l.order_id = o.id where o.id = ?',
];

interface PodRestart {
  namespace: string;
  service: string;
  pod: string;
  restartTimeMs: number;
  /** Exclusive end of this restart's data (next restart or "now"). */
  endMs: number;
  spec: ServiceSpec;
}

function podRestarts(fromMs: number, toMs: number, nowMs: number): PodRestart[] {
  const out: PodRestart[] = [];
  for (const spec of TOPOLOGY) {
    for (let i = 0; i < spec.pods; i++) {
      const pod = `${spec.service}-${(fnv1a(`${spec.namespace}/${spec.service}/${i}`) % 0xffff).toString(16).padStart(4, '0')}`;
      // Restart grid: every RESTART_PERIOD_MS, offset per pod so restarts
      // do not all line up.
      const offset = fnv1a(`${pod}-offset`) % RESTART_PERIOD_MS;
      const firstIdx = Math.floor((fromMs - offset) / RESTART_PERIOD_MS) - 1;
      const lastIdx = Math.floor((Math.min(toMs, nowMs) - offset) / RESTART_PERIOD_MS);
      for (let idx = firstIdx; idx <= lastIdx; idx++) {
        const restartTimeMs = idx * RESTART_PERIOD_MS + offset;
        const endMs = Math.min(restartTimeMs + RESTART_PERIOD_MS, nowMs);
        if (endMs <= restartTimeMs || endMs <= fromMs || restartTimeMs >= toMs) continue;
        out.push({ namespace: spec.namespace, service: spec.service, pod, restartTimeMs, endMs, spec });
      }
    }
  }
  return out;
}

export function podsInRange(fromMs: number, toMs: number, nowMs: number): PodEntry[] {
  return podRestarts(fromMs, toMs, nowMs)
    .map((pr) => ({
      namespace: pr.namespace,
      service: pr.service,
      pod: pr.pod,
      restart_time_ms: pr.restartTimeMs,
      time_min_ms: Math.max(pr.restartTimeMs, fromMs),
      time_max_ms: Math.min(pr.endMs, toMs),
    }))
    .sort(
      (a, b) =>
        a.namespace.localeCompare(b.namespace) ||
        a.service.localeCompare(b.service) ||
        a.pod.localeCompare(b.pod) ||
        a.restart_time_ms - b.restart_time_ms,
    );
}

// --- Calls ---

function retentionClassOf(durationMs: number, errorFlag: boolean): RetentionClass {
  if (errorFlag) return 'any_error';
  if (durationMs < 100) return 'short_clean';
  if (durationMs < 1000) return 'normal_clean';
  return 'long_clean';
}

/** All calls of one pod-restart starting inside one one-second bucket. */
function callsInBucket(pr: PodRestart, bucketStartMs: number): CallJSON[] {
  const key = `${pr.namespace}/${pr.service}/${pr.pod}@${pr.restartTimeMs}`;
  const r = rng(fnv1a(`${key}#${bucketStartMs}`));
  // ~0.7 calls per pod-second keeps a 15-minute window in the hundreds of rows.
  const count = r() < 0.5 ? (r() < 0.85 ? 1 : 2) : 0;
  const out: CallJSON[] = [];
  for (let i = 0; i < count; i++) {
    const tsMs = bucketStartMs + Math.floor(r() * BUCKET_MS);
    const idle = r() < 0.12;
    const methods = idle ? IDLE_METHODS : pr.spec.methods;
    const method = methods[Math.floor(r() * methods.length)]!;
    // Log-uniform duration, 1 ms .. ~30 s; idle housekeeping stays short.
    const durationMs = idle ? 1 + Math.floor(r() * 40) : Math.floor(10 ** (r() * 4.5));
    const errorFlag = !idle && r() < 0.04;
    const truncated = r() < 0.01;
    const cpu = Math.floor(durationMs * (0.2 + r() * 0.6));
    const hasSql = !idle && r() < 0.45;
    const params: Record<string, string[]> = {
      'request.id': [(fnv1a(`${key}#${tsMs}#${i}`) >>> 0).toString(16)],
    };
    if (hasSql) params['sql'] = ['1'];
    if (errorFlag) params['error'] = ['java.lang.IllegalStateException'];
    // Business calls arrive over HTTP (the mock's tree root is always the
    // Tomcat entry point, treeForCall below) — carry web.method/web.url the
    // same way the real agent does, so the Title column and tree header have
    // something to derive a human-readable endpoint from.
    if (!idle) {
      const routes = HTTP_ROUTES[pr.service] ?? ['/'];
      params['web.method'] = [r() < 0.85 ? 'GET' : 'POST'];
      params['web.url'] = [`http://${pr.service}:8080${routes[Math.floor(r() * routes.length)]}`];
    }
    const call: CallJSON = {
      pk: {
        pod_namespace: pr.namespace,
        pod_service: pr.service,
        pod_name: pr.pod,
        restart_time_ms: pr.restartTimeMs,
        trace_file_index: Math.floor((tsMs - pr.restartTimeMs) / 300000),
        buffer_offset: tsMs - pr.restartTimeMs,
        record_index: i,
      },
      ts_ms: tsMs,
      duration_ms: durationMs,
      method,
      thread_name: idle ? `scheduler-${1 + Math.floor(r() * 4)}` : `http-nio-8080-exec-${1 + Math.floor(r() * 16)}`,
      cpu_time_ms: cpu,
      wait_time_ms: Math.floor((durationMs - cpu) * r() * 0.5),
      memory_used: Math.floor(durationMs * 1024 * (0.5 + r())),
      queue_wait_ms: Math.floor(r() * 40),
      suspend_ms: r() < 0.2 ? Math.floor(r() * 30) : 0,
      child_calls: Math.floor(durationMs / 8) + Math.floor(r() * 10),
      transactions: hasSql ? 1 + Math.floor(r() * 3) : 0,
      logs_generated: Math.floor(r() * 4096),
      logs_written: Math.floor(r() * 1024),
      file_read: Math.floor(r() * 65536),
      file_written: Math.floor(r() * 16384),
      net_read: Math.floor(r() * 262144),
      net_written: Math.floor(r() * 131072),
      error_flag: errorFlag,
      retention_class: retentionClassOf(durationMs, errorFlag),
      params,
      trace_blob_size: truncated ? 0 : null,
      truncated_reason: truncated ? 'blob dropped under buffer pressure' : null,
    };
    out.push(call);
  }
  return out;
}

export interface CallsQueryShape {
  fromMs: number;
  toMs: number;
  pods: string[];
  method: string;
  durationMinMs: number;
  durationMaxMs: number;
  errorOnly: boolean;
  retentionClasses: string[];
}

export interface Position {
  tsMs: number;
  pkPath: string;
}

function matches(call: CallJSON, q: CallsQueryShape): boolean {
  if (q.pods.length > 0) {
    const tuple = `${call.pk.pod_namespace}/${call.pk.pod_service}/${call.pk.pod_name}`;
    if (!q.pods.includes(tuple)) return false;
  }
  if (q.method !== '' && !call.method.includes(q.method)) return false;
  if (q.durationMinMs > 0 && call.duration_ms < q.durationMinMs) return false;
  if (q.durationMaxMs > 0 && call.duration_ms > q.durationMaxMs) return false;
  if (q.errorOnly && !call.error_flag) return false;
  if (q.retentionClasses.length > 0 && !q.retentionClasses.includes(call.retention_class)) return false;
  return true;
}

/**
 * One keyset page: rows strictly after `after` in (ts_ms DESC, pk ASC) order.
 * Walks second buckets downward and stops at limit + 1, so deep windows do
 * not generate rows the page will not show.
 */
export function callsPage(
  q: CallsQueryShape,
  after: Position | null,
  limit: number,
  nowMs: number,
): { calls: CallJSON[]; nextPos: Position | null } {
  const restarts = podRestarts(q.fromMs, q.toMs, nowMs);
  const topBucket = Math.floor((Math.min(q.toMs, nowMs) - 1) / BUCKET_MS);
  const bottomBucket = Math.floor(q.fromMs / BUCKET_MS);
  const startBucket = after !== null ? Math.floor(after.tsMs / BUCKET_MS) : topBucket;

  const out: CallJSON[] = [];
  for (let bucket = startBucket; bucket >= bottomBucket && out.length <= limit; bucket--) {
    const bucketStart = bucket * BUCKET_MS;
    let rows: CallJSON[] = [];
    for (const pr of restarts) {
      if (bucketStart < pr.restartTimeMs || bucketStart >= pr.endMs) continue;
      rows = rows.concat(callsInBucket(pr, bucketStart));
    }
    rows.sort((a, b) => b.ts_ms - a.ts_ms || comparePk(a.pk, b.pk));
    for (const call of rows) {
      if (call.ts_ms < q.fromMs || call.ts_ms >= q.toMs) continue;
      if (after !== null) {
        // Seek: emit only rows strictly after the cursor position.
        if (call.ts_ms > after.tsMs) continue;
        if (call.ts_ms === after.tsMs && pkToPath(call.pk) <= after.pkPath) continue;
      }
      if (!matches(call, q)) continue;
      out.push(call);
      if (out.length > limit) break;
    }
  }

  const page = out.slice(0, limit);
  const more = out.length > limit;
  const last = page[page.length - 1];
  return {
    calls: page,
    nextPos: more && last !== undefined ? { tsMs: last.ts_ms, pkPath: pkToPath(last.pk) } : null,
  };
}

/** Reconstructs one call from its PK (buffer_offset encodes ts − restart). */
export function findCall(pk: CallPK, nowMs: number): CallJSON | null {
  const tsMs = pk.restart_time_ms + pk.buffer_offset;
  const bucketStart = Math.floor(tsMs / BUCKET_MS) * BUCKET_MS;
  for (const pr of podRestarts(bucketStart, bucketStart + BUCKET_MS, nowMs)) {
    if (
      pr.namespace !== pk.pod_namespace ||
      pr.service !== pk.pod_service ||
      pr.pod !== pk.pod_name ||
      pr.restartTimeMs !== pk.restart_time_ms
    ) {
      continue;
    }
    for (const call of callsInBucket(pr, bucketStart)) {
      if (call.pk.record_index === pk.record_index && call.pk.buffer_offset === pk.buffer_offset) {
        return call;
      }
    }
  }
  return null;
}

// --- Trees ---

// Arg lists carry no space after the comma — the agent's dictionary words
// never do (line_parser.go), and the mock is the contract.
const TREE_METHODS = [
  'void org.apache.catalina.connector.CoyoteAdapter.service(Request,Response) (CoyoteAdapter.java:340) [catalina.jar]',
  'void com.acme.web.ApiFilter.doFilter(ServletRequest,ServletResponse,FilterChain) (ApiFilter.java:52) [app.jar]',
  'Invoice com.acme.billing.InvoiceService.createInvoice(CreateInvoiceRequest) (InvoiceService.java:88) [billing.jar]',
  'TaxBreakdown com.acme.billing.TaxCalculator.calculate(Invoice) (TaxCalculator.java:31) [billing.jar]',
  'Order com.acme.orders.CheckoutFlow.placeOrder(Cart) (CheckoutFlow.java:120) [orders.jar]',
  'boolean com.acme.orders.InventoryService.reserve(Sku,int) (InventoryService.java:64) [orders.jar]',
  'ResultSet org.postgresql.jdbc.PgPreparedStatement.executeQuery() (PgPreparedStatement.java:107) [postgresql.jar]',
  'int org.postgresql.jdbc.PgPreparedStatement.executeUpdate() (PgPreparedStatement.java:132) [postgresql.jar]',
  'Response com.acme.gateway.PaymentGateway.authorize(PaymentRequest) (PaymentGateway.java:75) [gateway.jar]',
  'void com.acme.audit.AuditWriter.append(AuditEvent) (AuditWriter.java:19) [app.jar]',
  'void org.springframework.web.servlet.DispatcherServlet.doDispatch(HttpServletRequest,HttpServletResponse) (DispatcherServlet.java:1089) [spring-webmvc.jar]',
  'Object com.acme.web.OrderController.handle(OrderRequest) (OrderController.java:44) [app.jar]',
];

const TREE_PARAM_KEYS = ['sql', 'binds', 'request.id', 'node.name', 'java.thread', 'web.method', 'web.url'];

/**
 * Deterministic merged tree for one call. Invariants the backend guarantees
 * hold here too: Σ children.durationMs ≤ durationMs, selfDurationMs is the
 * difference, executions = selfExecutions + Σ children.executions, and a
 * degenerate pass-through chain exists on larger calls so the collapse
 * heuristic (08 §5) has something to skip.
 */
export function treeForCall(call: CallJSON): TreeWire {
  const seed = fnv1a(pkToPath(call.pk));
  const r = rng(seed);

  const sqlGroups = (durationMs: number, executions: number): ParamGroupWire[] => {
    const groups: ParamGroupWire[] = [];
    const kinds = 1 + Math.floor(r() * 3);
    let left = durationMs;
    for (let i = 0; i < kinds && left > 0; i++) {
      const share = Math.floor(left * (0.4 + r() * 0.4));
      left -= share;
      groups.push({
        value: SQL_TEXTS[Math.floor(r() * SQL_TEXTS.length)]!,
        durationMs: share,
        executions: Math.max(1, Math.floor(executions / kinds)),
        params: [
          {
            paramIdx: 1, // binds
            groups: [{ value: `[${Math.floor(r() * 1000)}, 'ACTIVE']`, durationMs: share, executions: Math.max(1, Math.floor(executions / kinds)) }],
          },
        ],
      });
    }
    if (left > 0 && groups.length > 1) {
      groups.push({ value: '::other', durationMs: left, executions: Math.max(1, executions - groups.length) });
    }
    if (r() < 0.15) {
      groups.push({ value: `sql:${Math.floor(r() * 100)}:${Math.floor(r() * 65536)}`, durationMs: Math.floor(durationMs * 0.05), executions: 1, unresolved: true });
    }
    return groups.sort((a, b) => b.durationMs - a.durationMs);
  };

  // Long calls grow deep and wide, so the virtualiser has thousands of
  // visible rows to prove itself on (07 §5.4 scale check).
  const large = call.duration_ms > 10_000;
  const maxDepth = large ? 11 : 6;
  const minFanout = large ? 2 : 1;
  const maxFanout = large ? 4 : 3;

  const build = (methodIdx: number, durationMs: number, executions: number, depth: number): TreeNodeWire => {
    const suspension = call.suspend_ms > 0 ? Math.floor(durationMs * 0.02) : 0;
    const node: TreeNodeWire = {
      methodIdx,
      durationMs,
      selfDurationMs: durationMs,
      suspensionMs: suspension,
      selfSuspensionMs: suspension,
      executions,
      selfExecutions: executions,
    };
    const isDb = methodIdx === 6 || methodIdx === 7;
    if (isDb) {
      node.params = [{ paramIdx: 0, groups: sqlGroups(durationMs, executions) }];
      return node;
    }
    if (depth >= maxDepth || durationMs < 4) return node;

    const fanout = minFanout + Math.floor(r() * maxFanout);
    const children: TreeNodeWire[] = [];
    let left = Math.floor(durationMs * (large ? 0.75 + r() * 0.2 : 0.55 + r() * 0.4));
    let childExecutions = 0;
    let childSuspension = 0;
    for (let i = 0; i < fanout && left >= 2; i++) {
      const share = i === fanout - 1 ? left : Math.floor(left * (0.3 + r() * 0.5));
      left -= share;
      const childIdx = 2 + Math.floor(r() * (TREE_METHODS.length - 2));
      const child = build(childIdx, share, executions * (1 + Math.floor(r() * 3)), depth + 1);
      children.push(child);
      childExecutions += child.executions;
      childSuspension += child.suspensionMs;
    }
    if (children.length > 0) {
      node.children = children;
      const childDuration = children.reduce((sum, c) => sum + c.durationMs, 0);
      node.selfDurationMs = durationMs - childDuration;
      node.executions = executions + childExecutions;
      node.suspensionMs = node.selfSuspensionMs + childSuspension;
    }
    return node;
  };

  // Entry chain: CoyoteAdapter → ApiFilter → DispatcherServlet →
  // OrderController each pass ~everything to the business node — the
  // degenerate chain the one-click expand must skip (07 §5.4).
  const passThrough = (methodIdx: number, child: TreeNodeWire): TreeNodeWire => ({
    methodIdx,
    durationMs: child.durationMs,
    selfDurationMs: 0,
    suspensionMs: child.suspensionMs,
    selfSuspensionMs: 0,
    executions: 1 + child.executions,
    selfExecutions: 1,
    children: [child],
  });
  const business = build(2 + Math.floor(r() * 4), Math.max(1, Math.floor(call.duration_ms * 0.97)), 1, 2);
  const filter = passThrough(1, passThrough(10, passThrough(11, business)));
  const root: TreeNodeWire = {
    methodIdx: 0,
    durationMs: call.duration_ms,
    selfDurationMs: call.duration_ms - filter.durationMs,
    suspensionMs: filter.suspensionMs,
    selfSuspensionMs: 0,
    executions: 1 + filter.executions,
    selfExecutions: 1,
    params: [
      { paramIdx: 2, groups: [{ value: call.params['request.id']?.[0] ?? 'n/a', durationMs: call.duration_ms, executions: 1 }] },
      { paramIdx: 4, groups: [{ value: call.thread_name, durationMs: call.duration_ms, executions: 1 }] },
      ...(call.params['web.method'] !== undefined
        ? [{ paramIdx: 5, groups: [{ value: call.params['web.method']![0]!, durationMs: call.duration_ms, executions: 1 }] }]
        : []),
      ...(call.params['web.url'] !== undefined
        ? [{ paramIdx: 6, groups: [{ value: call.params['web.url']![0]!, durationMs: call.duration_ms, executions: 1 }] }]
        : []),
    ],
    children: [filter],
  };
  return { v: 1, methods: [...TREE_METHODS], params: [...TREE_PARAM_KEYS], root };
}
