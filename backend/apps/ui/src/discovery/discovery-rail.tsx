import { Alert, Badge, Empty, Input, Spin, Tree, Typography } from 'antd';
import { useMemo, useState } from 'react';
import type { Key } from 'react';

import type { ServiceNode } from './group-pods';
import type { PodsState } from './use-pods';

// Left rail (09 §2.1): namespace → service → pod tree with a tri-state
// checkbox per service. The service is the selection unit — pods are
// ephemeral; selecting individual pods drives the parent into the partial
// state, which AntD's check conduction gives us for free.

export interface RailSelection {
  /** Fully selected services, `namespace/service`. */
  services: string[];
  /** Individually selected pods, `namespace/service/pod`. */
  pods: string[];
}

interface DiscoveryRailProps {
  pods: PodsState;
  selection: RailSelection;
  onSelectionChange: (selection: RailSelection) => void;
}

const nsKey = (ns: string): string => `ns:${ns}`;
const svcKey = (svc: ServiceNode): string => `svc:${svc.key}`;
const podKey = (tuple: string): string => `pod:${tuple}`;

function matchesSearch(svc: ServiceNode, needle: string): boolean {
  if (needle === '') return true;
  const q = needle.toLowerCase();
  return (
    svc.service.toLowerCase().includes(q) ||
    svc.namespace.toLowerCase().includes(q) ||
    svc.pods.some((p) => p.pod.toLowerCase().includes(q))
  );
}

// "live"/"closed" described profiler data freshness in the selected window,
// not Kubernetes pod status — a healthy, Running pod reads as "closed" here
// if it simply has no recent profiled calls (PR 708 review #17).
function liveDot(live: boolean): React.ReactNode {
  return (
    <Badge status={live ? 'success' : 'default'} title={live ? 'recent data in this window' : 'no recent data in this window'} />
  );
}

export function DiscoveryRail({ pods, selection, onSelectionChange }: DiscoveryRailProps) {
  const [search, setSearch] = useState('');
  const [expandedKeys, setExpandedKeys] = useState<Key[] | null>(null);

  const namespaces = pods.kind === 'ready' ? pods.namespaces : [];

  const visible = useMemo(
    () =>
      namespaces
        .map((ns) => ({ ...ns, services: ns.services.filter((svc) => matchesSearch(svc, search)) }))
        .filter((ns) => ns.services.length > 0),
    [namespaces, search],
  );

  const treeData = useMemo(
    () =>
      visible.map((ns) => ({
        key: nsKey(ns.namespace),
        selectable: false,
        title: <Typography.Text strong>{ns.namespace}</Typography.Text>,
        children: ns.services.map((svc) => ({
          key: svcKey(svc),
          selectable: false,
          title: (
            <span>
              {liveDot(svc.live)} {svc.service}{' '}
              <Typography.Text type="secondary">
                · {svc.restartCount} profiler session{svc.restartCount === 1 ? '' : 's'}
              </Typography.Text>
            </span>
          ),
          children: svc.pods.map((pod) => ({
            key: podKey(pod.tuple),
            selectable: false,
            title: (
              <span>
                {liveDot(pod.live)} {pod.pod}
                {pod.restarts.length > 1 ? (
                  <Typography.Text type="secondary"> ×{pod.restarts.length}</Typography.Text>
                ) : null}
              </span>
            ),
          })),
        })),
      })),
    [visible],
  );

  // Controlled checkedKeys: a fully selected service contributes its own key
  // plus every pod key (conduction shows children of a checked parent only
  // when the keys are actually present).
  const checkedKeys = useMemo(() => {
    const keys: Key[] = [];
    for (const ns of namespaces) {
      for (const svc of ns.services) {
        if (selection.services.includes(svc.key)) {
          keys.push(svcKey(svc), ...svc.pods.map((p) => podKey(p.tuple)));
          if (svc.pods.length > 0 && ns.services.every((s) => selection.services.includes(s.key))) {
            // Let a fully covered namespace render checked too.
            keys.push(nsKey(ns.namespace));
          }
        } else {
          keys.push(...svc.pods.filter((p) => selection.pods.includes(p.tuple)).map((p) => podKey(p.tuple)));
        }
      }
    }
    return [...new Set(keys)];
  }, [namespaces, selection]);

  const handleCheck = (checked: Key[] | { checked: Key[]; halfChecked: Key[] }): void => {
    const keys = new Set((Array.isArray(checked) ? checked : checked.checked).map(String));
    const services: string[] = [];
    const podTuples: string[] = [];
    for (const ns of namespaces) {
      for (const svc of ns.services) {
        if (keys.has(svcKey(svc))) {
          services.push(svc.key);
        } else {
          podTuples.push(...svc.pods.filter((p) => keys.has(podKey(p.tuple))).map((p) => p.tuple));
        }
      }
    }
    onSelectionChange({ services, pods: podTuples });
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, height: '100%', padding: 12 }}>
      <Input.Search
        placeholder="Filter services"
        allowClear
        value={search}
        onChange={(e) => setSearch(e.target.value)}
      />
      {pods.kind === 'loading' ? (
        <Spin style={{ marginTop: 24 }} />
      ) : pods.kind === 'error' ? (
        <Alert type="error" title="Cannot load pods" description={pods.message} showIcon />
      ) : pods.kind === 'idle' ? (
        <Typography.Text type="secondary">Pick a period to discover services.</Typography.Text>
      ) : treeData.length === 0 ? (
        <Empty description={search === '' ? 'No pods in this window.' : 'No service matches.'} />
      ) : (
        <>
          {pods.partial ? (
            <Alert type="warning" showIcon title="Pod list may be incomplete" description={pods.partialReasons.join('; ')} />
          ) : null}
          <Tree
            checkable
            blockNode
            treeData={treeData}
            checkedKeys={checkedKeys}
            onCheck={handleCheck}
            expandedKeys={expandedKeys ?? visible.map((ns) => nsKey(ns.namespace))}
            onExpand={setExpandedKeys}
            style={{ overflow: 'auto', flex: 1 }}
          />
        </>
      )}
    </div>
  );
}
