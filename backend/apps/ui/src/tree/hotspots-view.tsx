import { Typography } from 'antd';
import { useMemo } from 'react';

import { parseMethod } from './method-info';
import type { TreeModel, TreeNode } from './model';
import { buildHotspotTree, graftIncoming } from './transforms/hotspot-tree';
import type { CategoryProfile } from './transforms/flat-profile';
import { TreeView } from './tree-view';

// Hotspots (09 §3.5) as the old UI's bottom-up tree: dotted category names
// group hierarchically, and a method row expands in place into its incoming
// callers — several hotspots' callers compare side by side without leaving
// the tab.

interface HotspotsViewProps {
  model: TreeModel;
  profiles: CategoryProfile[];
}

export function HotspotsView({ model, profiles }: HotspotsViewProps) {
  const hotspot = useMemo(() => buildHotspotTree(model, profiles), [model, profiles]);
  const zeroSelf = profiles.reduce((sum, p) => sum + p.zeroSelfCount, 0);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, height: '100%' }}>
      <div style={{ flex: 1, minHeight: 0 }}>
        <TreeView
          model={hotspot.model}
          direction="bottom-up"
          initialExpanded={hotspot.initialExpanded}
          computeChildren={(node: TreeNode) => graftIncoming(model, node)}
        />
      </div>
      {zeroSelf > 0 ? (
        <Typography.Text type="secondary">
          {zeroSelf} method{zeroSelf === 1 ? ' with' : 's with'} 0ms self time {zeroSelf === 1 ? 'is' : 'are'} not
          displayed here.
        </Typography.Text>
      ) : null}
    </div>
  );
}

/** Names a node for op titles: the short signature. */
export function nodeTitle(model: TreeModel, node: TreeNode): string {
  return parseMethod(model.methods[node.methodIdx] ?? '').signature;
}
