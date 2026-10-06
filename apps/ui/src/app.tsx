import { App as AntdApp, ConfigProvider } from 'antd';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router';

import { CallsPage } from './pages/calls-page';
import { PodsPage } from './pages/pods-page';
import { TreePage } from './pages/tree-page';
import { AppShell } from './shell/app-shell';
import { profilerTheme } from './theme/profiler-theme';

// The router base follows the build-time base (vite base → BASE_URL, 07 §6),
// so '/' and '/ui' are one config switch: '/' yields no basename.
const basename = import.meta.env.BASE_URL.replace(/\/+$/, '') || undefined;

/**
 * One SPA served under the base: {base}calls, {base}pods under the shell, and
 * {base}tree/:pk standalone — it opens in a new tab and carries its own header.
 */
export function App() {
  return (
    <ConfigProvider theme={profilerTheme}>
      <AntdApp>
        <BrowserRouter basename={basename}>
          <Routes>
            <Route element={<AppShell />}>
              <Route path="/calls" element={<CallsPage />} />
              <Route path="/pods" element={<PodsPage />} />
              <Route path="/" element={<Navigate to="/calls" replace />} />
            </Route>
            <Route path="/tree/:pk" element={<TreePage />} />
          </Routes>
        </BrowserRouter>
      </AntdApp>
    </ConfigProvider>
  );
}
