import { describe, expect, it } from 'vitest';

import { httpTitle, httpTitleFromNodeParams } from './http-title';

describe('httpTitle', () => {
  it('combines method and URL path', () => {
    expect(httpTitle({ 'web.method': ['GET'], 'web.url': ['http://api-gateway:8080/owners/1'] })).toBe(
      'GET /owners/1',
    );
  });

  it('appends the query string when present', () => {
    expect(
      httpTitle({
        'web.method': ['GET'],
        'web.url': ['http://api-gateway:8080/owners'],
        'web.query': ['lastName=Franklin'],
      }),
    ).toBe('GET /owners?lastName=Franklin');
  });

  it('omits the method when web.method is absent', () => {
    expect(httpTitle({ 'web.url': ['http://api-gateway:8080/owners/1'] })).toBe('/owners/1');
  });

  it('returns null without a web.url param', () => {
    expect(httpTitle({})).toBeNull();
    expect(httpTitle({ 'web.method': ['GET'] })).toBeNull();
  });

  it('keeps a URL without a path as the root path', () => {
    expect(httpTitle({ 'web.method': ['GET'], 'web.url': ['http://api-gateway:8080'] })).toBe('GET /');
  });
});

describe('httpTitleFromNodeParams', () => {
  const paramKeys = ['web.method', 'web.url', 'sql'];

  it('builds the title from a tree node param wire', () => {
    const params = [
      { paramIdx: 0, groups: [{ value: 'GET', durationMs: 0, executions: 1 }] },
      { paramIdx: 1, groups: [{ value: 'http://api-gateway:8080/owners/1', durationMs: 0, executions: 1 }] },
    ];
    expect(httpTitleFromNodeParams(params, paramKeys)).toBe('GET /owners/1');
  });

  it('skips the ::other bucket when picking a representative value', () => {
    const params = [
      {
        paramIdx: 1,
        groups: [
          { value: '::other', durationMs: 0, executions: 1 },
          { value: 'http://api-gateway:8080/owners/1', durationMs: 0, executions: 1 },
        ],
      },
    ];
    expect(httpTitleFromNodeParams(params, paramKeys)).toBe('/owners/1');
  });

  it('returns null when the node carries no web.url param', () => {
    const params = [{ paramIdx: 2, groups: [{ value: 'select 1', durationMs: 0, executions: 1 }] }];
    expect(httpTitleFromNodeParams(params, paramKeys)).toBeNull();
  });
});
