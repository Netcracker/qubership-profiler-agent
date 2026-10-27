import { describe, expect, it } from 'vitest';

import { ApiError, isCursorRejection, isInvalidRequest, isWideQueryRejection } from './client';
import type { ProblemDetails } from './types';

// The helpers branch on the problem `code` (02 §8) and fall back on the prose of a build that sends no code.

const problem = (over: Partial<ProblemDetails>): ApiError =>
  new ApiError(400, { type: 'about:blank', title: 'invalid request', status: 400, ...over }, '400');

describe('error classification', () => {
  it('reads the code first', () => {
    expect(isWideQueryRejection(problem({ code: 'query_too_wide', title: 'reworded' }))).toBe(true);
    expect(isCursorRejection(problem({ code: 'cursor_rejected', detail: 'the token expired' }))).toBe(true);
    expect(isInvalidRequest(problem({ code: 'invalid_request', title: 'bad input' }))).toBe(true);
  });

  it('trusts a code over prose that would match another helper', () => {
    const e = problem({ code: 'invalid_request', detail: 'cursor and filter given together' });
    expect(isCursorRejection(e)).toBe(false);
    expect(isInvalidRequest(e)).toBe(true);
  });

  it('falls back on the prose of a build without codes', () => {
    expect(isWideQueryRejection(problem({ title: 'query too wide' }))).toBe(true);
    expect(isCursorRejection(problem({ detail: 'Cursor expired' }))).toBe(true);
    expect(isInvalidRequest(problem({ title: 'invalid request', detail: 'limit must be positive' }))).toBe(true);
  });

  it('needs the status and a problem body', () => {
    expect(isInvalidRequest(new ApiError(500, { type: 'about:blank', title: 'x', status: 500, code: 'invalid_request' }, ''))).toBe(false);
    expect(isInvalidRequest(new ApiError(400, null, '400 Bad Request'))).toBe(false);
    expect(isInvalidRequest(new Error('network'))).toBe(false);
  });
});
