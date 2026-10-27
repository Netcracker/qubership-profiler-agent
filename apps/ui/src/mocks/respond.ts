import { HttpResponse, delay } from 'msw';

import type { ProblemDetails } from '../api/types';

// Response helpers shared by the mock's handler files.

export function problem(p: ProblemDetails): Response {
  return HttpResponse.json(p, {
    status: p.status,
    headers: { 'Content-Type': 'application/problem+json' },
  });
}

export const badRequest = (detail: string): Response =>
  problem({ type: 'about:blank', title: 'invalid request', status: 400, detail });

// Human pacing in the browser; instant under vitest.
export async function pace(): Promise<void> {
  if (import.meta.env.MODE !== 'test') await delay(120 + Math.random() * 200);
}
