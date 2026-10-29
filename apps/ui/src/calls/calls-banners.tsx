import { Alert, Button, Space, Tag, Typography } from 'antd';

import type { ProblemDetails } from '../api/types';
import type { Condition } from '../filter/condition';
import { formatBytes } from './format';

// The 09 §5 states, each naming what happened and the one action that
// resolves it. Amber = handled and usable; red = blocked and needs a choice.

interface TooWideBannerProps {
  problem: ProblemDetails;
  /** The applied conditions. */
  conditions: readonly Condition[];
  /** Applies the narrowed conditions and runs the query. */
  onApply: (next: Condition[]) => void;
}

const withDuration = (conditions: readonly Condition[], value: string): Condition[] => [
  ...conditions.filter((c) => c.field !== 'duration'),
  { field: 'duration', op: '>', values: [value] },
];

const withoutField = (conditions: readonly Condition[], field: string): Condition[] =>
  conditions.filter((c) => c.field !== field);

/** Wide-query guard rejection → one-click narrowing chips (09 §5). */
export function TooWideBanner({ problem, conditions, onApply }: TooWideBannerProps) {
  const suggested = problem.suggested_filters ?? [];
  const byClass = Object.entries(problem.by_class ?? {}).sort((a, b) => b[1] - a[1]);
  const dominant = byClass[0]?.[0];

  const chips: React.ReactNode[] = [];
  if (suggested.includes('duration_min_ms')) {
    chips.push(
      <Button key="d500" size="small" onClick={() => onApply(withDuration(conditions, '500ms'))}>
        &gt;500ms
      </Button>,
      <Button key="d3s" size="small" onClick={() => onApply(withDuration(conditions, '3s'))}>
        &gt;3s
      </Button>,
    );
  }
  if (suggested.includes('error_only')) {
    chips.push(
      <Button
        key="errors"
        size="small"
        onClick={() => onApply([...withoutField(conditions, 'error'), { field: 'error', op: '=', values: ['true'] }])}
      >
        Errors only
      </Button>,
    );
  }
  if (suggested.includes('retention_class')) {
    chips.push(
      // Narrows the scan to the long-duration and error retention classes
      // (02 §2.3.2: error_only already prunes to any_error/corrupted, so this
      // stays a retention_class filter rather than also setting error_only —
      // that would drop the long_clean matches this chip is meant to keep).
      <Button
        key="long"
        size="small"
        onClick={() =>
          onApply([
            ...withoutField(conditions, 'retention_class'),
            { field: 'retention_class', op: 'in', values: ['long_clean', 'any_error'] },
          ])
        }
      >
        Long or errored calls
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
                <Tag key={cls} color={cls === dominant ? 'warning' : undefined}>
                  {cls}: {formatBytes(bytes)}
                </Tag>
              ))}
            </Typography.Text>
          ) : null}
          <Space wrap>
            {chips}
            {suggested.includes('pod') ? (
              <Typography.Text type="secondary">…or filter by namespace, service, or pod.</Typography.Text>
            ) : null}
          </Space>
        </Space>
      }
    />
  );
}

/**
 * The namespace, service, and pod filters expand to a pod list too long to
 * fit in one GET /calls request line. /calls has no `service` param to fall
 * back to (02 §2.3), so the only way out is fewer pods (PR 708 review #8).
 */
export function SelectionTooWideBanner({
  conditions,
  expandsPods = true,
  onApply,
}: {
  conditions: readonly Condition[];
  /** True when the namespace, service, and pod filters expand to pods, so they make the request long. */
  expandsPods?: boolean;
  onApply: (next: Condition[]) => void;
}) {
  if (!expandsPods) {
    return (
      <Alert
        type="error"
        showIcon
        title="Query too long"
        description="The filters make the request too long to send. Use fewer values in an in list, or a regex instead."
      />
    );
  }
  return (
    <Alert
      type="error"
      showIcon
      title="Selection too wide"
      description={
        <Space orientation="vertical" size={4}>
          <Typography.Text>
            The namespace, service, and pod filters match too many pods for one request. Narrow them, or filter by
            individual pods.
          </Typography.Text>
          <Button
            size="small"
            onClick={() => onApply(conditions.filter((c) => !['namespace', 'service', 'pod'].includes(c.field)))}
          >
            Clear selection
          </Button>
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
 * A partial /pods result drops pods from the namespace, service, and pod
 * expansion, which silently narrows the /calls pod filter. Surface it next to
 * the results (09 §5 Partial). Amber: the shown calls are real, just possibly
 * not all.
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
