import { Layout } from 'antd';
import { useState } from 'react';
import { Outlet, useLocation } from 'react-router';

import styles from './app-shell.module.css';
import { HeaderSlotContext } from './header-slot';
import { NavRail, useScreens } from './nav-rail';
import { ThemeSwitch } from './theme-switch';

/**
 * App shell for the data screens (09 §1): the header with the screen's
 * controls, the navigation rail, and the screen. A screen puts its controls in
 * the header through HeaderSlot. The tree route renders outside this shell; it
 * replaces the header with a context header.
 */
export function AppShell() {
  const location = useLocation();
  // State, not a ref: the screen must render again once the slot element exists.
  const [slot, setSlot] = useState<HTMLElement | null>(null);
  const screen = useScreens().find((item) => location.pathname.startsWith(item.path));

  return (
    // height (not minHeight): the data screens clamp to the viewport so the
    // virtualised table body is the only scroller.
    <Layout className={styles.shell}>
      <header className={styles.header}>
        <svg className={styles.logo} width="22" height="22" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M4 19V9M9 19V5M14 19v-7M19 19v-11" />
        </svg>
        <div className={styles.product}>Profiler</div>
        {screen === undefined ? null : <div className={styles.screen}>{screen.title ?? screen.label}</div>}
        <div className={styles.controls}>
          <div ref={setSlot} className={styles.slot} />
          <span className={styles.divider} />
          <ThemeSwitch />
        </div>
      </header>
      <div className={styles.body}>
        <NavRail />
        {/* A div: the screen brings its own <main> (antd Layout.Content). */}
        <div className={styles.main}>
          <HeaderSlotContext.Provider value={slot}>
            <Outlet />
          </HeaderSlotContext.Provider>
        </div>
      </div>
    </Layout>
  );
}
