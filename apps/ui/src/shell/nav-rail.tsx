import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { NavLink, useLocation } from 'react-router';

import { isAgentConfigEnabled } from '../agents/api';
import { useConfig } from '../api/use-config';
import styles from './nav-rail.module.css';

export const AGENTS_PATH = '/agents';

export interface Screen {
  path: string;
  label: string;
  /** The screen name in the header, when it differs from the rail label. */
  title?: string;
  /**
   * Screens with the same value read the same query-string scheme, so a switch
   * between them keeps the search string.
   */
  urlScheme: 'calls' | 'agents';
  icon: ReactNode;
}

/** The screens the rail links to, in rail order. The header reads the labels. */
export const SCREENS: readonly Screen[] = [
  { path: '/calls', label: 'Calls', urlScheme: 'calls', icon: <path d="M2 4h12M2 8h12M2 12h8" /> },
  {
    path: '/pods',
    label: 'Pods Info',
    urlScheme: 'calls',
    icon: (
      <>
        <path d="M8 1.8 13.5 5v6L8 14.2 2.5 11V5z" />
        <path d="M2.5 5 8 8.2 13.5 5M8 8.2v6" />
      </>
    ),
  },
  {
    path: AGENTS_PATH,
    label: 'Agents',
    title: 'Agent configuration',
    urlScheme: 'agents',
    icon: (
      <>
        <path d="M2 4.5h7M12 4.5h2M2 11.5h2M7 11.5h7" />
        <circle cx="10.5" cy="4.5" r="1.5" />
        <circle cx="5.5" cy="11.5" r="1.5" />
      </>
    ),
  },
];

/**
 * The screens this deployment has. Agents is listed only when /config reports
 * the feature, so the entry is absent while /config loads and when it fails.
 */
export function useScreens(): readonly Screen[] {
  const config = useConfig();
  const agents = config.kind === 'ready' && isAgentConfigEnabled(config.config);
  return agents ? SCREENS : SCREENS.filter((screen) => screen.path !== AGENTS_PATH);
}

const STORAGE_KEY = 'profiler.navRail.collapsed';

function loadCollapsed(): boolean {
  try {
    return localStorage.getItem(STORAGE_KEY) === '1';
  } catch {
    return false;
  }
}

function saveCollapsed(collapsed: boolean): void {
  try {
    localStorage.setItem(STORAGE_KEY, collapsed ? '1' : '0');
  } catch {
    // Storage may be unavailable (private mode, quota); the rail still toggles for this visit.
  }
}

/**
 * The navigation rail of the data screens: 196 px open, 52 px collapsed to
 * icons. The collapsed state is stored, so it survives a reload.
 */
export function NavRail() {
  const location = useLocation();
  const screens = useScreens();
  const [collapsed, setCollapsed] = useState(loadCollapsed);

  // The search string each URL scheme last had. A link to a screen of another
  // scheme restores that scheme's own string, so a visit to Agents does not
  // cost Calls its window and filters.
  const current = screens.find((screen) => location.pathname.startsWith(screen.path))?.urlScheme;
  const lastSearch = useRef(new Map<Screen['urlScheme'], string>());
  useEffect(() => {
    if (current !== undefined) lastSearch.current.set(current, location.search);
  }, [current, location.search]);
  const searchFor = (screen: Screen): string =>
    screen.urlScheme === current ? location.search : (lastSearch.current.get(screen.urlScheme) ?? '');

  const toggle = (): void => {
    const next = !collapsed;
    setCollapsed(next);
    saveCollapsed(next);
  };
  const toggleLabel = collapsed ? 'Expand navigation' : 'Collapse navigation';

  return (
    <nav aria-label="Screens" className={`${styles.rail}${collapsed ? ` ${styles.collapsed}` : ''}`}>
      <div className={styles.head}>
        {collapsed ? null : <span className={styles.caption}>Workspace</span>}
        <button
          type="button"
          className={styles.toggle}
          title={toggleLabel}
          aria-label={toggleLabel}
          aria-expanded={!collapsed}
          onClick={toggle}
        >
          <svg className={styles.icon} width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
            <rect x="2" y="3" width="12" height="10" rx="1.5" />
            <path d="M6 3v10M11 6.5 9.5 8 11 9.5" />
          </svg>
        </button>
      </div>
      {screens.map((screen) => (
        <NavLink
          key={screen.path}
          // The search string carries the window and selection (09 §6); keep
          // it when switching screens so the view survives.
          to={{ pathname: screen.path, search: searchFor(screen) }}
          title={screen.label}
          aria-label={screen.label}
          className={({ isActive }) => `${styles.link}${isActive ? ` ${styles.active}` : ''}`}
        >
          <svg className={styles.icon} width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
            {screen.icon}
          </svg>
          {collapsed ? null : <span>{screen.label}</span>}
        </NavLink>
      ))}
    </nav>
  );
}
