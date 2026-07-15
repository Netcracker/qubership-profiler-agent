import { App as AntdApp, ConfigProvider } from 'antd';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router';

import { CallsPage } from './pages/calls-page';
import { PodsPage } from './pages/pods-page';
import { TreePage } from './pages/tree-page';
import { AppShell } from './shell/app-shell';

/**
 * One SPA served at /ui (07 §6): /ui/calls, /ui/pods under the shell, and
 * /ui/tree/:pk standalone — it opens in a new tab and carries its own header.
 */
export function App() {
  return (
    <ConfigProvider>
      <AntdApp>
        <BrowserRouter basename="/ui">
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
