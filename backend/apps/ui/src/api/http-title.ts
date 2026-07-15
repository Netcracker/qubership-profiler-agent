import type { ParamWire } from '../msgpack/tree-wire';

// Ported from the old UI's `http` decoder (profiler-ui/src/decoders.mjs): a
// call that carries `web.url` reads better as its HTTP endpoint than as the
// Tomcat/Reactor internals the agent captured as the method name.

function urlPath(rawUrl: string): string {
  const schemeEnd = rawUrl.indexOf('://');
  if (schemeEnd < 0) return rawUrl;
  const pathStart = rawUrl.indexOf('/', schemeEnd + 3);
  return pathStart < 0 ? '/' : rawUrl.slice(pathStart);
}

/** `GET /owners/1` from `web.method`/`web.url`/`web.query` params, or `null` without a URL. */
export function httpTitle(params: Record<string, string[]>): string | null {
  // TODO: support other methods (e.g. kafka)
  const rawUrl = params['web.url']?.[0];
  if (rawUrl === undefined) return null;
  const method = params['web.method']?.[0];
  const query = params['web.query']?.[0];
  const path = query !== undefined ? `${urlPath(rawUrl)}?${query}` : urlPath(rawUrl);
  return method !== undefined ? `${method} ${path}` : path;
}

/** Same lookup, over a tree node's own param wire (08 R11 groups) rather than the flat /calls shape. */
export function httpTitleFromNodeParams(params: readonly ParamWire[], paramKeys: readonly string[]): string | null {
  const record: Record<string, string[]> = {};
  for (const param of params) {
    const key = paramKeys[param.paramIdx];
    if (key === undefined) continue;
    const value = param.groups.find((g) => g.value !== '::other')?.value;
    if (value !== undefined) record[key] = [value];
  }
  return httpTitle(record);
}
