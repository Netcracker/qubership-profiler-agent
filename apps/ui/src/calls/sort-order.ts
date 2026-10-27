import type { CallJSON } from '../api/types';

// "Sort by" (09 §2.2): `order=<field>_<asc|desc>` on /calls (02 §2.3.5). The backend lists the orders it implements in
// `filter_capabilities.orders`; any other order sorts the loaded rows in the browser.

export const ORDER_FIELDS = [
  { field: 'start', label: 'Start time', key: (c: CallJSON) => c.ts_ms },
  { field: 'duration', label: 'Duration', key: (c: CallJSON) => c.duration_ms },
  { field: 'cpu', label: 'CPU time', key: (c: CallJSON) => c.cpu_time_ms },
  { field: 'queue', label: 'Queue wait', key: (c: CallJSON) => c.queue_wait_ms },
  { field: 'calls', label: 'Child calls', key: (c: CallJSON) => c.child_calls },
  { field: 'net', label: 'Net IO', key: (c: CallJSON) => c.net_read + c.net_written },
] as const;

export type OrderField = (typeof ORDER_FIELDS)[number]['field'];
export type OrderDirection = 'asc' | 'desc';
export type CallsOrder = `${OrderField}_${OrderDirection}`;

/** The order of /calls without an `order` parameter. */
export const DEFAULT_ORDER: CallsOrder = 'start_desc';

/** Every order of the contract, in menu order, descending first. */
export const CALLS_ORDERS: readonly CallsOrder[] = ORDER_FIELDS.flatMap(({ field }) => [
  `${field}_desc` as const,
  `${field}_asc` as const,
]);

export function isCallsOrder(s: string): s is CallsOrder {
  return (CALLS_ORDERS as readonly string[]).includes(s);
}

export function orderField(order: CallsOrder): OrderField {
  return order.slice(0, order.lastIndexOf('_')) as OrderField;
}

export function orderDirection(order: CallsOrder): OrderDirection {
  return order.endsWith('_asc') ? 'asc' : 'desc';
}

export function makeOrder(field: OrderField, direction: OrderDirection): CallsOrder {
  return `${field}_${direction}`;
}

export function orderLabel(order: CallsOrder): string {
  const field = orderField(order);
  return ORDER_FIELDS.find((f) => f.field === field)!.label;
}

/** The orders of a backend that predates `order`: start time, newest first. */
export const LEGACY_ORDERS: ReadonlySet<CallsOrder> = new Set([DEFAULT_ORDER]);

/** True when /calls sorts by this order; otherwise the page sorts the loaded rows in the browser. */
export function serverCanOrder(order: CallsOrder, served: ReadonlySet<string>): boolean {
  return served.has(order);
}

/** The loaded rows in this order. The sort is stable, so ties keep the order the server sent. */
export function sortCalls(rows: readonly CallJSON[], order: CallsOrder): CallJSON[] {
  const field = orderField(order);
  const key = ORDER_FIELDS.find((f) => f.field === field)!.key;
  const sign = orderDirection(order) === 'asc' ? 1 : -1;
  return [...rows].sort((a, b) => sign * (key(a) - key(b)));
}
