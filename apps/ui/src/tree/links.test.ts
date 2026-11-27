import { describe, expect, it } from 'vitest';

import type { TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { buildTreeModel } from './model';
import { callLinks, expandLink, findTraceId, linkWindow, missingReason } from './links';
import type { LinkValues } from './links';

const VALUES: LinkValues = {
  trace_id: '4bf92f3577b34da6a3ce929d0e0e4736',
  from_ms: '1714060812345',
  to_ms: '1714060817165',
  namespace: 'prod-orders',
  service: 'order-processing',
  pod: 'order-processing-7c9d4f6b8-x2k9p',
};

describe('expandLink', () => {
  it('fills every placeholder of a template', () => {
    expect(
      expandLink('https://grafana.example/logs?ns={namespace}&svc={service}&pod={pod}&from={from_ms}&to={to_ms}', VALUES),
    ).toEqual({
      kind: 'ready',
      url: 'https://grafana.example/logs?ns=prod-orders&svc=order-processing&pod=order-processing-7c9d4f6b8-x2k9p&from=1714060812345&to=1714060817165',
    });
  });

  it('fills a placeholder each time it occurs', () => {
    expect(expandLink('https://t.example/{trace_id}/spans?trace={trace_id}', { ...VALUES, trace_id: 'abc' })).toEqual({
      kind: 'ready',
      url: 'https://t.example/abc/spans?trace=abc',
    });
  });

  it('encodes a value the way encodeURIComponent does', () => {
    // A value must not be able to add a query parameter, a fragment, or a path segment.
    const hostile = 'a b&admin=1#x/../y?z=ü';
    const link = expandLink('https://logs.example/q?pod={pod}', { ...VALUES, pod: hostile });
    expect(link).toEqual({ kind: 'ready', url: `https://logs.example/q?pod=${encodeURIComponent(hostile)}` });
    expect(link.kind === 'ready' && link.url).toBe('https://logs.example/q?pod=a%20b%26admin%3D1%23x%2F..%2Fy%3Fz%3D%C3%BC');
  });

  it('reports the placeholders the call has no value for', () => {
    const untraced = { ...VALUES, trace_id: null };
    expect(expandLink('https://t.example/explore?trace={trace_id}&pod={pod}', untraced)).toEqual({
      kind: 'missing',
      placeholders: ['trace_id'],
    });
    // A template that does not use the missing value is unaffected.
    expect(expandLink('https://logs.example/q?pod={pod}', untraced).kind).toBe('ready');
  });

  it('keeps the braces of a LogQL selector and of a regexp escape', () => {
    expect(expandLink('https://l.example/?q={kubernetes_pod_name="{pod}"} |~ "\\x{a0}"', VALUES)).toEqual({
      kind: 'ready',
      url: 'https://l.example/?q={kubernetes_pod_name="order-processing-7c9d4f6b8-x2k9p"} |~ "\\x{a0}"',
    });
  });

  it('leaves braces that name no known placeholder alone', () => {
    expect(expandLink('https://t.example/{span_id}?q={"a":1}&pod={pod}', VALUES)).toEqual({
      kind: 'ready',
      url: 'https://t.example/{span_id}?q={"a":1}&pod=order-processing-7c9d4f6b8-x2k9p',
    });
  });
});

describe('callLinks', () => {
  const LINKS = [
    { label: 'Open trace · Tempo', kind: 'trace', url_template: 'https://grafana.example/explore?trace={trace_id}' },
    { label: 'Logs · Grafana', kind: 'logs', url_template: 'https://grafana.example/logs?pod={pod}&from={from_ms}&to={to_ms}' },
  ] as const;

  it('keeps the operator order and resolves each link', () => {
    expect(callLinks(LINKS, VALUES)).toEqual([
      {
        label: 'Open trace · Tempo',
        kind: 'trace',
        url: 'https://grafana.example/explore?trace=4bf92f3577b34da6a3ce929d0e0e4736',
        reason: null,
      },
      {
        label: 'Logs · Grafana',
        kind: 'logs',
        url: 'https://grafana.example/logs?pod=order-processing-7c9d4f6b8-x2k9p&from=1714060812345&to=1714060817165',
        reason: null,
      },
    ]);
  });

  it('disables a link whose template needs a value the call lacks, and says why', () => {
    const [trace, logs] = callLinks(LINKS, { ...VALUES, trace_id: null });
    expect(trace).toMatchObject({ url: null, reason: 'Unavailable: this call carries no trace ID' });
    expect(logs!.url).not.toBeNull();
  });

  it.each([
    ['a quote', '4bf9"} OR 1=1'],
    ['a space', '4bf9 2f35'],
    ['a colon', 'trace:4bf9'],
    ['a dot', '4bf9.2f35'],
    ['a non-ASCII letter', 'réq1'],
  ])('disables a trace link whose trace ID has %s, and keeps the others', (_, traceId) => {
    const [trace, logs] = callLinks(LINKS, { ...VALUES, trace_id: traceId });
    expect(trace).toMatchObject({
      url: null,
      reason: 'Unavailable: the trace ID of this call has characters other than A-Z, a-z, 0-9, _ and -',
    });
    expect(logs!.url).not.toBeNull();
  });

  it('links a trace ID made of hex digits, a UUID, or a name with _ and -', () => {
    for (const traceId of ['4bf92f3577b34da6a3ce929d0e0e4736', '0b6e2c4a-7f1d-4e8b-9a3c-2d5f6e7a8b9c', 'req_A-1']) {
      expect(callLinks(LINKS, { ...VALUES, trace_id: traceId })[0]!.url, traceId).toBe(
        `https://grafana.example/explore?trace=${traceId}`,
      );
    }
  });

  it('names the reason for a missing time window and for any other placeholder', () => {
    expect(missingReason(['from_ms', 'to_ms'])).toBe('Unavailable: the start time of this call is unknown');
    expect(missingReason(['pod'])).toBe('Unavailable: this call has no value for {pod}');
  });

  it('refuses a template that is not an http or https URL', () => {
    const [link] = callLinks([{ label: 'Bad', kind: 'trace', url_template: 'javascript:alert({trace_id})' }], VALUES);
    expect(link).toMatchObject({ url: null, reason: 'Unavailable: the link is not an http or https URL' });
  });

  it('returns nothing without configured links', () => {
    expect(callLinks([], VALUES)).toEqual([]);
  });
});

describe('linkWindow', () => {
  it('widens the call by 1000 ms on each side', () => {
    expect(linkWindow(1714060812345, 4820)).toEqual({ from_ms: '1714060811345', to_ms: '1714060818165' });
  });

  it('has no window without a start, and no end without a duration', () => {
    expect(linkWindow(null, 4820)).toEqual({ from_ms: null, to_ms: null });
    expect(linkWindow(1714060812345, null)).toEqual({ from_ms: '1714060811345', to_ms: null });
  });
});

describe('findTraceId', () => {
  const node = (methodIdx: number, params?: TreeNodeWire['params'], children?: TreeNodeWire[]): TreeNodeWire => ({
    methodIdx,
    durationMs: 10,
    selfDurationMs: 10,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 1,
    selfExecutions: 1,
    ...(params === undefined ? {} : { params }),
    ...(children === undefined ? {} : { children }),
  });
  const one = (paramIdx: number, value: string): NonNullable<TreeNodeWire['params']>[number] => ({
    paramIdx,
    groups: [{ value, durationMs: 10, executions: 1 }],
  });
  const wire = (params: string[], root: TreeNodeWire): TreeWire => ({
    v: 1,
    methods: ['void a.Root.run() (Root.java:1) [a.jar]', 'void a.Span.start() (Span.java:2) [a.jar]'],
    params,
    root,
  });

  it('reads the root call params', () => {
    const model = buildTreeModel(wire(['request.id', 'trace.id'], node(0, [one(0, 'r-1'), one(1, 'aaaa1111')])));
    expect(findTraceId(model)).toBe('aaaa1111');
  });

  it.each(['trace.id', 'brave.trace_id', 'X-B3-TraceId'])('knows the %s param', (name) => {
    expect(findTraceId(buildTreeModel(wire([name], node(0, [one(0, 'bbbb2222')]))))).toBe('bbbb2222');
  });

  it('prefers trace.id over the other names on one node', () => {
    const model = buildTreeModel(wire(['X-B3-TraceId', 'trace.id'], node(0, [one(0, 'from-header'), one(1, 'from-tracer')])));
    expect(findTraceId(model)).toBe('from-tracer');
  });

  it('falls back to the first frame below the root that carries one', () => {
    const model = buildTreeModel(wire(['brave.trace_id'], node(0, undefined, [node(1, [one(0, 'cccc3333')])])));
    expect(findTraceId(model)).toBe('cccc3333');
  });

  it('takes the first frame in the order of the /tree response, not in display order', () => {
    // The second child runs longer, so the model shows it first.
    const short = { ...node(1, [one(0, 'first-in-response')]), durationMs: 2, selfDurationMs: 2 };
    const long = { ...node(1, [one(0, 'longer-sibling')]), durationMs: 8, selfDurationMs: 8 };
    const model = buildTreeModel(wire(['trace.id'], { ...node(0, undefined, [short, long]), selfDurationMs: 0 }));
    expect(model.root.children.map((c) => c.durationMs)).toEqual([8, 2]);
    expect(findTraceId(model)).toBe('first-in-response');
  });

  it('skips the ::other bucket and takes the next param of the node', () => {
    const evicted = { paramIdx: 0, groups: [{ value: '::other', durationMs: 10, executions: 1 }] };
    const model = buildTreeModel(wire(['trace.id', 'X-B3-TraceId'], node(0, [evicted, one(1, 'dddd4444')])));
    expect(findTraceId(model)).toBe('dddd4444');
  });

  it('returns null for a call that was not traced', () => {
    expect(findTraceId(buildTreeModel(wire(['request.id'], node(0, [one(0, 'r-1')]))))).toBeNull();
    // The key is in the dictionary, and no node of this call uses it.
    expect(findTraceId(buildTreeModel(wire(['trace.id'], node(0))))).toBeNull();
  });
});
