import { App as AntdApp } from 'antd';
import { Navigate, Route, RouterProvider, Routes, createBrowserRouter } from 'react-router';

import { ConfigProvider } from './api/use-config';
import { CallsPage } from './pages/calls-page';
import { PodsPage } from './pages/pods-page';
import { TreePage } from './pages/tree-page';
import { AppShell } from './shell/app-shell';
import { ThemeProvider } from './theme/theme-provider';

// The router base follows the build-time base (vite base → BASE_URL, 07 §6),
// so '/' and '/ui' are one config switch: '/' yields no basename.
const basename = import.meta.env.BASE_URL.replace(/\/+$/, '') || undefined;

/**
 * One SPA served under the base: {base}calls, {base}pods under the shell, and
 * {base}tree/:pk standalone — it opens in a new tab and carries its own header.
 */
export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route path="/calls" element={<CallsPage />} />
        <Route path="/pods" element={<PodsPage />} />
        <Route path="/" element={<Navigate to="/calls" replace />} />
      </Route>
      <Route path="/tree/:pk" element={<TreePage />} />
    </Routes>
  );
}

/**
 * The route table of a data router that hands every path to {@link AppRoutes}.
 * The routes stay declarative; the data router is there so a page can use the
 * hooks that need one, such as `useBlocker`.
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
