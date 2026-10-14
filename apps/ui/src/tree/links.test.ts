import { describe, expect, it } from 'vitest';

import type { TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { buildTreeModel } from './model';
import { callLinks, expandLink, findTraceId, missingReason } from './links';
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

  it('returns null for a call that was not traced', () => {
    expect(findTraceId(buildTreeModel(wire(['request.id'], node(0, [one(0, 'r-1')]))))).toBeNull();
    // The key is in the dictionary, and no node of this call uses it.
    expect(findTraceId(buildTreeModel(wire(['trace.id'], node(0))))).toBeNull();
  });
});
