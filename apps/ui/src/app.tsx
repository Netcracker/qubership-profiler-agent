import { App as AntdApp } from 'antd';
import { Navigate, Route, RouterProvider, Routes, createBrowserRouter } from 'react-router';

import { isAgentConfigEnabled } from './agents/api';
import { ConfigProvider, useConfig } from './api/use-config';
import { AgentsPage } from './pages/agents-page';
import { CallsPage } from './pages/calls-page';
import { PodsPage } from './pages/pods-page';
import { TreePage } from './pages/tree-page';
import { AppShell } from './shell/app-shell';
import { AGENTS_PATH } from './shell/nav-rail';
import { ThemeProvider } from './theme/theme-provider';

// The router base follows the build-time base (vite base → BASE_URL, 07 §6),
// so '/' and '/ui' are one config switch: '/' yields no basename.
const basename = import.meta.env.BASE_URL.replace(/\/+$/, '') || undefined;

/**
 * One SPA served under the base: {base}calls, {base}pods under the shell, and
 * {base}tree/:pk standalone — it opens in a new tab and carries its own header.
 * {base}agents exists only when /config reports the agent-configuration
 * feature. Without it the path matches nothing, like any unknown path.
 */
export function AppRoutes() {
  const config = useConfig();
  const agents = config.kind === 'ready' && isAgentConfigEnabled(config.config);
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route path="/calls" element={<CallsPage />} />
        <Route path="/pods" element={<PodsPage />} />
        {agents ? <Route path={AGENTS_PATH} element={<AgentsPage />} /> : null}
        <Route path="/" element={<Navigate to="/calls" replace />} />
      </Route>
      <Route path="/tree/:pk" element={<TreePage />} />
    </Routes>
  );
}

/**
 * The route table of a data router that hands every path to {@link AppRoutes}.
 * The routes stay declarative; the data router is there for `useBlocker`, which
 * the agent-configuration page needs to hold a navigation while it has unsaved
 * changes.
 */
export const APP_ROUTES = [{ path: '*', element: <AppRoutes /> }];

// Created on the first render, not on import: main.tsx imports this module for
// a restored export too, which has no router.
let router: ReturnType<typeof createBrowserRouter> | undefined;

function appRouter(): ReturnType<typeof createBrowserRouter> {
  router ??= createBrowserRouter(APP_ROUTES, basename === undefined ? {} : { basename });
  return router;
}

export function App() {
  return (
    <ThemeProvider>
      <AntdApp>
        <ConfigProvider>
          <RouterProvider router={appRouter()} />
        </ConfigProvider>
      </AntdApp>
    </ThemeProvider>
  );
}
