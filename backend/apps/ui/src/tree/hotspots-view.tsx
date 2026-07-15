import { Button, Table, Tag, Typography } from 'antd';
import { useMemo } from 'react';

import { formatCount, formatDurationMs } from '../calls/format';
import { parseMethod } from './method-info';
import type { TreeModel, TreeNode } from './model';
import type { CategoryProfile, MethodAggregate } from './transforms/flat-profile';

// Hotspots (09 §3.5): a flat self-time profile; with categories set it
// groups by category first, so a business operation's share of time reads
// directly. Each method row can pivot into its incoming calls.

interface HotspotsViewProps {
  model: TreeModel;
  profiles: CategoryProfile[];
  onIncoming?: (methodIdx: number, category: string | undefined) => void;
}

export function HotspotsView({ model, profiles, onIncoming }: HotspotsViewProps) {
  const signatures = useMemo(() => model.methods.map((m) => parseMethod(m).signature), [model]);
  const totalSelf = profiles.reduce((sum, p) => sum + p.totalSelfMs, 0);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, overflow: 'auto' }}>
      {profiles.map((profile) => {
        const name = profile.category?.name ?? (profiles.length > 1 ? 'unsorted' : null);
        return (
          <div key={profile.category?.name ?? '∅'}>
            {name !== null ? (
              <Typography.Title level={5} style={{ background: profile.category?.color, padding: '2px 8px' }}>
                {name}{' '}
                <Typography.Text type="secondary">
                  {formatDurationMs(profile.totalSelfMs)}
                  {totalSelf > 0 ? ` · ${((100 * profile.totalSelfMs) / totalSelf).toFixed(1)}%` : ''}
                </Typography.Text>
              </Typography.Title>
            ) : null}
            <Table<MethodAggregate>
              size="small"
              rowKey={(m) => m.methodIdx}
              dataSource={profile.methods}
              pagination={profile.methods.length > 100 ? { pageSize: 100, showSizeChanger: false } : false}
              columns={[
                {
                  title: 'Self',
                  width: 110,
                  align: 'right',
                  render: (_, m) => (
                    <Typography.Text strong style={{ fontVariantNumeric: 'tabular-nums' }}>
                      {formatDurationMs(m.selfDurationMs)}
                    </Typography.Text>
                  ),
                },
                {
                  title: '%',
                  width: 120,
                  render: (_, m) => {
                    const share = profile.totalSelfMs > 0 ? m.selfDurationMs / profile.totalSelfMs : 0;
                    return (
                      <span title={`${(share * 100).toFixed(1)}% of the category's self time`}>
                        <span
                          style={{
                            display: 'inline-block',
                            width: Math.max(1, share * 90),
                            height: 8,
                            background: '#91caff',
                            borderRadius: 2,
                          }}
                        />
                      </span>
                    );
                  },
                },
                { title: 'Total', width: 110, align: 'right', render: (_, m) => formatDurationMs(m.durationMs) },
                {
                  title: 'Susp',
                  width: 90,
                  align: 'right',
                  render: (_, m) => (m.suspensionMs > 0 ? formatDurationMs(m.suspensionMs) : '—'),
                },
                { title: 'Inv', width: 90, align: 'right', render: (_, m) => formatCount(m.selfExecutions) },
                {
                  title: 'Method',
                  render: (_, m) => (
                    <span>
                      <Typography.Text title={model.methods[m.methodIdx]}>{signatures[m.methodIdx]}</Typography.Text>
                      {m.params.some((p) => (model.paramKeys[p.paramIdx] ?? '') === 'sql') ? (
                        <Tag color="blue" style={{ marginLeft: 6 }}>
                          sql
                        </Tag>
                      ) : null}
                    </span>
                  ),
                },
                ...(onIncoming === undefined
                  ? []
                  : [
                      {
                        title: '',
                        width: 110,
                        render: (_: unknown, m: MethodAggregate) => (
                          <Button size="small" type="link" onClick={() => onIncoming(m.methodIdx, profile.category?.name)}>
                            Incoming
                          </Button>
                        ),
                      },
                    ]),
              ]}
              footer={
                profile.zeroSelfCount > 0
                  ? () => (
                      <Typography.Text type="secondary">
                        {profile.zeroSelfCount} method{profile.zeroSelfCount === 1 ? ' with' : 's with'} 0ms self time{' '}
                        {profile.zeroSelfCount === 1 ? 'is' : 'are'} not displayed here.
                      </Typography.Text>
                    )
                  : undefined
              }
            />
          </div>
        );
      })}
    </div>
  );
}

/** Names a node for op titles: the short signature. */
export function nodeTitle(model: TreeModel, node: TreeNode): string {
  return parseMethod(model.methods[node.methodIdx] ?? '').signature;
}
