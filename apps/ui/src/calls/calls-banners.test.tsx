import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { ProblemDetails } from '../api/types';
import type { Condition } from '../filter/condition';
import { SelectionTooWideBanner, TooWideBanner } from './calls-banners';

afterEach(cleanup);

const POD: Condition = { field: 'pod', op: '=', values: ['checkout-a'] };
const SERVICE: Condition = { field: 'service', op: '=', values: ['checkout'] };
const SLOW: Condition = { field: 'duration', op: '>=', values: ['100ms'] };
const NOT_ERRORS: Condition = { field: 'error', op: '=', values: ['false'] };
const CLEAN: Condition = { field: 'retention_class', op: '=', values: ['normal_clean'] };

const PROBLEM: ProblemDetails = {
  type: 'about:blank',
  title: 'Query too wide',
  status: 400,
  detail: 'The query would scan 12 GiB.',
  suggested_filters: ['duration_min_ms', 'error_only', 'retention_class', 'pod'],
  by_class: { normal_clean: 2048, long_clean: 4096 },
};

function renderTooWide(problem: ProblemDetails = PROBLEM) {
  const onApply = vi.fn<(next: Condition[]) => void>();
  render(<TooWideBanner problem={problem} conditions={[POD, SLOW, NOT_ERRORS, CLEAN]} onApply={onApply} />);
  return { last: () => onApply.mock.lastCall?.[0] };
}

describe('TooWideBanner', () => {
  it('replaces the duration condition with the picked threshold and keeps the rest', async () => {
    const { last } = renderTooWide();
    await userEvent.click(screen.getByRole('button', { name: '>3s' }));
    expect(last()).toEqual([POD, NOT_ERRORS, CLEAN, { field: 'duration', op: '>', values: ['3s'] }]);
    await userEvent.click(screen.getByRole('button', { name: '>500ms' }));
    expect(last()).toEqual([POD, NOT_ERRORS, CLEAN, { field: 'duration', op: '>', values: ['500ms'] }]);
  });

  it('replaces the error condition with error=true', async () => {
    const { last } = renderTooWide();
    await userEvent.click(screen.getByRole('button', { name: 'Errors only' }));
    expect(last()).toEqual([POD, SLOW, CLEAN, { field: 'error', op: '=', values: ['true'] }]);
  });

  it('replaces the retention_class condition with the long and errored classes', async () => {
    const { last } = renderTooWide();
    await userEvent.click(screen.getByRole('button', { name: 'Long or errored calls' }));
    expect(last()).toEqual([POD, SLOW, NOT_ERRORS, { field: 'retention_class', op: 'in', values: ['long_clean', 'any_error'] }]);
  });

  it('lists the estimated scan by class, largest first, and suggests the pod filters', () => {
    renderTooWide();
    expect(screen.getByText('The query would scan 12 GiB.')).toBeInTheDocument();
    const tags = [...document.querySelectorAll('.ant-tag')].map((t) => t.textContent);
    expect(tags).toEqual(['long_clean: 4.0 KiB', 'normal_clean: 2.0 KiB']);
    expect(screen.getByText('…or filter by namespace, service, or pod.')).toBeInTheDocument();
  });

  it('offers only the suggested chips', () => {
    renderTooWide({ ...PROBLEM, suggested_filters: ['error_only'], by_class: undefined });
    expect(screen.getAllByRole('button').map((b) => b.textContent)).toEqual(['Errors only']);
    expect(screen.queryByText(/Estimated scan by class/)).toBeNull();
    expect(screen.queryByText(/or filter by namespace/)).toBeNull();
  });
});

describe('SelectionTooWideBanner', () => {
  it('clears the namespace, service, and pod conditions and keeps the others', async () => {
    const onApply = vi.fn<(next: Condition[]) => void>();
    render(<SelectionTooWideBanner conditions={[SERVICE, SLOW, POD]} onApply={onApply} />);
    await userEvent.click(screen.getByRole('button', { name: 'Clear selection' }));
    expect(onApply).toHaveBeenCalledWith([SLOW]);
  });
});
