import { ConfigProvider } from 'antd';
import type { ReactNode } from 'react';

import { useThemeMode } from '../ui/theme-mode';
import { profilerThemes } from './profiler-theme';

/** Hands antd the token set of the current color theme and follows the switch. */
export function ThemeProvider({ children }: { children: ReactNode }) {
  const mode = useThemeMode();
  return <ConfigProvider theme={profilerThemes[mode]}>{children}</ConfigProvider>;
}
