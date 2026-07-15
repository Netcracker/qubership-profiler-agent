import { App as AntdApp, ConfigProvider } from 'antd';
import { MemoryRouter } from 'react-router';

import { parsePkPath } from '../api/pk';
import { isRetentionClass } from '../api/types';
import { decodeTree } from '../msgpack/decode';
import { TreePage } from '../pages/tree-page';
import { profilerTheme } from '../theme/profiler-theme';
import { base64ToBytes } from './restore';
import type { RestorePayload } from './restore';

// Boot for a self-contained export (10b): decode the embedded wire and render
// the tree straight into a read-only page — no router URL, no fetch. A
// MemoryRouter satisfies TreePage's useParams/useSearchParams without a URL,
// and the restore prop feeds it the wire and the saved view state.
export function RestoredApp({ payload }: { payload: RestorePayload }) {
  const wire = decodeTree(base64ToBytes(payload.treeB64));
  const pk = parsePkPath(payload.pkPath);
  const retentionClass =
    payload.retentionClass !== null && isRetentionClass(payload.retentionClass) ? payload.retentionClass : null;

  return (
    <ConfigProvider theme={profilerTheme}>
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
    </ConfigProvider>
  );
}
