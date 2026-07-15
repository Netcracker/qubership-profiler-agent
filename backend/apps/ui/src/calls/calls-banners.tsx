import { Alert, Button, Space, Tag, Typography } from 'antd';

import type { ProblemDetails } from '../api/types';
import type { CallsSearchState } from '../url/search-params';
import { formatBytes } from './format';

// The 09 §5 states, each naming what happened and the one action that
// resolves it. Amber = handled and usable; red = blocked and needs a choice.

interface TooWideBannerProps {
  problem: ProblemDetails;
  search: CallsSearchState;
  onSearchChange: (next: CallsSearchState) => void;
}

/** Wide-query guard rejection → one-click narrowing chips (09 §5). */
export function TooWideBanner({ problem, search, onSearchChange }: TooWideBannerProps) {
  const suggested = problem.suggested_filters ?? [];
  const byClass = Object.entries(problem.by_class ?? {}).sort((a, b) => b[1] - a[1]);
  const dominant = byClass[0]?.[0];

  const chips: React.ReactNode[] = [];
  if (suggested.includes('duration_min_ms')) {
    chips.push(
      <Button key="d500" size="small" onClick={() => onSearchChange({ ...search, durationMinMs: 500 })}>
        &gt;500ms
      </Button>,
      <Button key="d3s" size="small" onClick={() => onSearchChange({ ...search, durationMinMs: 3000 })}>
        &gt;3s
      </Button>,
    );
  }
  if (suggested.includes('error_only')) {
    chips.push(
      <Button key="errors" size="small" onClick={() => onSearchChange({ ...search, errorOnly: true })}>
        Errors only
      </Button>,
    );
  }
  if (suggested.includes('retention_class')) {
    chips.push(
      <Button
        key="long"
        size="small"
        onClick={() => onSearchChange({ ...search, retentionClasses: ['long_clean', 'any_error'] })}
      >
        Long + errors only
      </Button>,
    );
  }

  return (
    <Alert
      type="error"
      showIcon
      title="Query too wide"
      description={
        <Space orientation="vertical" size={4}>
          <Typography.Text>{problem.detail}</Typography.Text>
          {byClass.length > 0 ? (
            <Typography.Text type="secondary">
              Estimated scan by class:{' '}
              {byClass.map(([cls, bytes]) => (
                <Tag key={cls} color={cls === dominant ? 'orange' : undefined}>
                  {cls}: {formatBytes(bytes)}
                </Tag>
              ))}
            </Typography.Text>
          ) : null}
          <Space wrap>
            {chips}
            {suggested.includes('pod') ? (
              <Typography.Text type="secondary">…or select services in the left rail.</Typography.Text>
            ) : null}
          </Space>
        </Space>
      }
    />
  );
}

export function PartialBanner({ reasons, onRetry }: { reasons: string[]; onRetry: () => void }) {
  return (
    <Alert
      type="warning"
      showIcon
      title="Results may be incomplete — some sources did not answer"
      description={reasons.length > 0 ? reasons.join('; ') : undefined}
      action={
        <Button size="small" onClick={onRetry}>
          Retry
        </Button>
      }
    />
  );
}

/**
 * A partial /pods result drops pods from the rail, which silently narrows the
 * /calls pod filter (expandSelection). The rail shows its own notice, but the
 * results table gives no hint they may be incomplete — so surface it here too
 * (09 §5 Partial). Amber: the shown calls are real, just possibly not all.
 */
export function PodsPartialBanner({ reasons, onRetry }: { reasons: string[]; onRetry: () => void }) {
  return (
    <Alert
      type="warning"
      showIcon
      title="These results may be narrowed — the pod list is incomplete"
      description={reasons.length > 0 ? reasons.join('; ') : undefined}
      action={
        <Button size="small" onClick={onRetry}>
          Retry pods
        </Button>
      }
    />
  );
}

export function CursorExpiredBanner({ onReload }: { onReload: () => void }) {
  return (
    <Alert
      type="warning"
      showIcon
      title="The scroll position expired. Reload from the first page."
      action={
        <Button size="small" type="primary" onClick={onReload}>
          Reload
        </Button>
      }
    />
  );
}

export function EmptyPausedBanner({ onContinue }: { onContinue: () => void }) {
  return (
    <Alert
      type="info"
      showIcon
      title="Several pages in a row came back empty — data in this stretch may have aged out."
      action={
        <Button size="small" onClick={onContinue}>
          Keep searching
        </Button>
      }
    />
  );
}

export function LoadMoreErrorBanner({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <Alert
      type="error"
      showIcon
      title="Loading the next page failed"
      description={message}
      action={
        <Button size="small" onClick={onRetry}>
          Retry
        </Button>
      }
    />
  );
}

export function AllFailedBanner({ detail, onRetry }: { detail: string; onRetry: () => void }) {
  return (
    <Alert
      type="error"
      showIcon
      title="No source answered in time. Narrow the range or retry."
      description={detail}
      action={
        <Button size="small" type="primary" onClick={onRetry}>
          Retry
        </Button>
      }
    />
  );
}
