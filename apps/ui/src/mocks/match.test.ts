import { describe, expect, it } from 'vitest';

import type { CallJSON } from '../api/types';
import type { Condition, Operator } from '../filter/condition';
import { callClass, callEndpoint, fieldValues, matchCondition } from './match';

// The mock's reading of 02 §2.3.3: what each field reads from a call and how each operator compares.

function call(over: Partial<CallJSON> = {}, params: Record<string, string[]> = {}): CallJSON {
  return {
    pk: {
      pod_namespace: 'payments',
      pod_service: 'billing',
      pod_name: 'billing-7f9c',
      restart_time_ms: 1_700_000_000_000,
      trace_file_index: 0,
      buffer_offset: 0,
      record_index: 0,
    },
    ts_ms: 1_700_000_100_000,
    duration_ms: 250,
    method: 'void com.example.OrderService.place(long) (OrderService.java) [orders.jar]',
    thread_name: 'http-nio-8080-exec-1',
    cpu_time_ms: 40,
    wait_time_ms: 0,
    memory_used: 0,
    queue_wait_ms: 0,
    suspend_ms: 0,
    child_calls: 0,
    transactions: 0,
    logs_generated: 0,
    logs_written: 0,
    file_read: 0,
    file_written: 0,
    net_read: 0,
    net_written: 0,
    error_flag: false,
    retention_class: 'normal_clean',
    params,
    trace_blob_size: 0,
    truncated_reason: null,
    ...over,
  };
}

const cond = (field: string, op: Operator, ...values: string[]): Condition => ({ field, op, values });

describe('matchCondition', () => {
  it('compares durations with every duration operator', () => {
    const c = call({ duration_ms: 250 });
    expect(matchCondition(c, cond('duration', '>', '250ms'))).toBe(false);
    expect(matchCondition(c, cond('duration', '>=', '250ms'))).toBe(true);
    expect(matchCondition(c, cond('duration', '<', '1s'))).toBe(true);
    expect(matchCondition(c, cond('duration', '<=', '249ms'))).toBe(false);
    expect(matchCondition(c, cond('duration', 'between', '100ms..250ms'))).toBe(true);
    expect(matchCondition(c, cond('duration', 'between', '251ms..2s'))).toBe(false);
  });

  it('matches = and in on the whole value, and negates them', () => {
    const c = call();
    expect(matchCondition(c, cond('service', '=', 'billing'))).toBe(true);
    expect(matchCondition(c, cond('service', '=', 'bill'))).toBe(false);
    expect(matchCondition(c, cond('service', '!=', 'billing'))).toBe(false);
    expect(matchCondition(c, cond('namespace', 'in', 'orders', 'payments'))).toBe(true);
    expect(matchCondition(c, cond('namespace', 'not in', 'orders', 'payments'))).toBe(false);
  });

  it('matches =~ anywhere in the value, unanchored', () => {
    const c = call();
    expect(matchCondition(c, cond('method', '=~', 'Order.*place'))).toBe(true);
    expect(matchCondition(c, cond('method', '!~', 'Order.*place'))).toBe(false);
    expect(matchCondition(c, cond('pod', '=~', '^billing-'))).toBe(true);
  });

  it('lets a negative condition match a call that lacks the field', () => {
    const c = call();
    expect(matchCondition(c, cond('trace_id', '!=', 'abc'))).toBe(true);
    expect(matchCondition(c, cond('user.tenant', 'not in', 'acme'))).toBe(true);
    expect(matchCondition(c, cond('user.tenant', '=', 'acme'))).toBe(false);
  });

  it('reads None and Any as absence and presence with = and != only', () => {
    const traced = call({}, { 'trace.id': ['abc'] });
    const untraced = call();
    expect(matchCondition(untraced, cond('trace_id', '=', 'None'))).toBe(true);
    expect(matchCondition(traced, cond('trace_id', '=', 'None'))).toBe(false);
    expect(matchCondition(traced, cond('trace_id', '=', 'Any'))).toBe(true);
    expect(matchCondition(traced, cond('trace_id', '!=', 'Any'))).toBe(false);
    expect(matchCondition(untraced, cond('trace_id', '!=', 'None'))).toBe(false);
    // With =~, Any is plain text.
    expect(matchCondition(call({ method: 'AnyService.run()' }), cond('method', '=~', 'Any'))).toBe(true);
  });

  it('matches a multi-valued param when any value matches, and negates when none does', () => {
    const c = call({}, { 'user.role': ['admin', 'viewer'] });
    expect(matchCondition(c, cond('user.role', '=', 'viewer'))).toBe(true);
    expect(matchCondition(c, cond('user.role', '!=', 'viewer'))).toBe(false);
    expect(matchCondition(c, cond('user.role', '!=', 'guest'))).toBe(true);
  });

  it('treats a param recorded with no value as absent', () => {
    const c = call({}, { 'x-request-id': [''] });
    expect(matchCondition(c, cond('request_id', '=', 'None'))).toBe(true);
  });

  it('reads error as true or false', () => {
    expect(matchCondition(call({ error_flag: true }), cond('error', '=', 'true'))).toBe(true);
    expect(matchCondition(call(), cond('error', '=', 'true'))).toBe(false);
    expect(matchCondition(call(), cond('error', '=', 'false'))).toBe(true);
  });
});

describe('fieldValues', () => {
  it('reads a trace ID from all three param names, once per value', () => {
    const c = call({}, { 'trace.id': ['a1'], 'brave.trace_id': ['b2'], 'X-B3-TraceId': ['a1'] });
    expect(fieldValues(c, 'trace_id')).toEqual(['a1', 'b2']);
  });

  it('reads the request ID from x-request-id and the retention class from the call', () => {
    const c = call({ retention_class: 'any_error' }, { 'x-request-id': ['r-1'] });
    expect(fieldValues(c, 'request_id')).toEqual(['r-1']);
    expect(fieldValues(c, 'retention_class')).toEqual(['any_error']);
  });
});

describe('callClass', () => {
  it('takes the qualified name before the first parenthesis, up to its last dot', () => {
    expect(callClass('void com.example.OrderService.place(long) (OrderService.java) [orders.jar]')).toBe(
      'com.example.OrderService',
    );
  });

  it('names no class for a method without a dot', () => {
    expect(callClass('#42')).toBeNull();
  });
});

describe('callEndpoint', () => {
  it('joins the HTTP method and the path, with ID segments replaced', () => {
    const c = call({}, {
      'web.method': ['GET'],
      'web.url': ['https://shop.example:8443/orders/123/items/550e8400-e29b-41d4-a716-446655440000?x=1#top'],
    });
    expect(callEndpoint(c)).toBe('GET /orders/{id}/items/{id}');
  });

  it('replaces a long hex segment and keeps a short one', () => {
    const c = call({}, { 'web.method': ['POST'], 'web.url': ['/blobs/0123456789abcdef/cafe'] });
    expect(callEndpoint(c)).toBe('POST /blobs/{id}/cafe');
  });

  it('gives the path alone without web.method, and / for a URL without a path', () => {
    expect(callEndpoint(call({}, { 'web.url': ['http://host'] }))).toBe('/');
  });

  it('gives nothing without web.url', () => {
    expect(callEndpoint(call({}, { 'web.method': ['GET'] }))).toBeNull();
    expect(matchCondition(call(), cond('endpoint', '!=', 'GET /'))).toBe(true);
  });
});
