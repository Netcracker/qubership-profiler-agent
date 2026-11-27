import { App as AntdApp } from 'antd';
import { useState } from 'react';
import { MemoryRouter } from 'react-router';

import { parsePkPath } from '../api/pk';
import { isRetentionClass } from '../api/types';
import { isTreeTab } from '../url/search-params';
import { decodeTree } from '../msgpack/decode';
import { TreePage } from '../pages/tree-page';
import { ThemeProvider } from '../theme/theme-provider';
import { pinThemeMode } from '../ui/theme-mode';
import { base64ToBytes } from './restore';
import type { RestorePayload } from './restore';

// Boot for a self-contained export (10b): decode the embedded wire and render
// the tree straight into a read-only page — no router URL, no fetch. A
// MemoryRouter satisfies TreePage's useParams/useSearchParams without a URL,
// and the restore prop feeds it the wire and the saved view state.
export function RestoredApp({ payload }: { payload: RestorePayload }) {
  // Before the first render, so the copy never flashes the other theme. In a
  // state initializer because it must run once and ahead of ThemeProvider's
  // first read; nothing subscribes to the theme store yet.
  useState(() => {
    if (payload.theme === 'light' || payload.theme === 'dark') pinThemeMode(payload.theme);
  });
  const wire = decodeTree(base64ToBytes(payload.treeB64));
  const pk = parsePkPath(payload.pkPath);
  const retentionClass =
    payload.retentionClass !== null && isRetentionClass(payload.retentionClass) ? payload.retentionClass : null;

  return (
    <ThemeProvider>
      <AntdApp>
        {/* The page reads its fixed tab from the URL, so the saved one is handed over as one. */}
        <MemoryRouter initialEntries={[isTreeTab(payload.activeTab) ? `/?tab=${payload.activeTab}` : '/']}>
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
