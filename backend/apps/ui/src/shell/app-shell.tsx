import { Layout, Menu, Typography } from 'antd';
import { Outlet, useLocation, useNavigate } from 'react-router';

const NAV_ITEMS = [
  { key: '/calls', label: 'Calls' },
  { key: '/pods', label: 'Pods Info' },
];

/**
 * App shell for the data screens (09 §1): top bar with the tab switch, content
 * below. The left rail mounts inside the Calls/Pods screens (5.1). The tree
 * route renders outside this shell — it replaces the bar with a context header.
 */
export function AppShell() {
  const location = useLocation();
  const navigate = useNavigate();
  const active = NAV_ITEMS.find((item) => location.pathname.startsWith(item.key))?.key;

  return (
    // height (not minHeight): the data screens clamp to the viewport so the
    // virtualised table body is the only scroller.
    <Layout style={{ height: '100vh' }}>
      <Layout.Header style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
        <Typography.Title level={4} style={{ margin: 0, color: '#fff', whiteSpace: 'nowrap' }}>
          Profiler
        </Typography.Title>
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={active === undefined ? [] : [active]}
          items={NAV_ITEMS}
          // The search string carries the window and selection (09 §6); keep
          // it when switching tabs so the view survives.
          onClick={({ key }) => void navigate({ pathname: key, search: location.search })}
          style={{ flex: 1, minWidth: 0 }}
        />
      </Layout.Header>
      <Outlet />
    </Layout>
  );
}
