import { App as AntdApp } from 'antd';
import { useState } from 'react';
import { MemoryRouter } from 'react-router';

import { parsePkPath } from '../api/pk';
import { isRetentionClass } from '../api/types';
import { decodeTree } from '../msgpack/decode';
import { TreePage } from '../pages/tree-page';
import { ThemeProvider } from '../theme/theme-provider';
import { isThemeMode, pinThemeMode } from '../ui/theme-mode';
import { base64ToBytes } from './restore';
import type { RestorePayload } from './restore';

// Boot for a self-contained export (10b): decode the embedded wire and render
// the tree straight into a read-only page — no router URL, no fetch. A
// MemoryRouter satisfies TreePage's useParams/useSearchParams without a URL,
// and the restore prop feeds it the wire and the saved view state.
export function RestoredApp({ payload }: { payload: RestorePayload }) {
  // Pin the saved theme once, before the first paint, so the copy opens the
  // way it was saved rather than in the reader's stored or system theme.
  useState(() => {
    if (isThemeMode(payload.theme)) pinThemeMode(payload.theme);
  });
  const wire = decodeTree(base64ToBytes(payload.treeB64));
  const pk = parsePkPath(payload.pkPath);
  const retentionClass =
    payload.retentionClass !== null && isRetentionClass(payload.retentionClass) ? payload.retentionClass : null;

  return (
    <ThemeProvider>
      <AntdApp>
        <MemoryRouter>
          <TreePage
            restore={{
              wire,
              pk,
              tsMs: payload.tsMs,
              retentionClass,
              adjustText: payload.adjustText,
              categoryText: payload.categoryText,
              tabs: payload.tabs,
              activeTab: payload.activeTab,
            }}
          />
        </MemoryRouter>
      </AntdApp>
    </ThemeProvider>
  );
}
