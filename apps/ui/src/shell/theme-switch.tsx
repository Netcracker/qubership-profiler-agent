import { DesktopOutlined, MoonOutlined, SunOutlined } from '@ant-design/icons';
import { Segmented } from 'antd';

import { setThemePreference, useThemePreference } from '../ui/theme-mode';
import type { ThemePreference } from '../ui/theme-mode';
import styles from './theme-switch.module.css';

// Icons only, to fit the header. Each option keeps a visually hidden label, so
// its radio button has a name, and a tooltip names it for the pointer.
const OPTIONS = [
  { value: 'light', name: 'Light', title: 'Light theme', Icon: SunOutlined },
  { value: 'dark', name: 'Dark', title: 'Dark theme', Icon: MoonOutlined },
  { value: 'auto', name: 'Auto', title: 'Auto: follow the system theme', Icon: DesktopOutlined },
] as const;

/** The theme switch: Light, Dark, or Auto. It sits in the shell header and in the tree page's own header. */
export function ThemeSwitch() {
  const preference = useThemePreference();
  return (
    <Segmented<ThemePreference>
      size="small"
      className={styles.switch}
      aria-label="Color theme"
      value={preference}
      onChange={setThemePreference}
      options={OPTIONS.map(({ value, name, title, Icon }) => ({
        value,
        title,
        icon: <Icon aria-hidden="true" />,
        label: <span className={styles.hidden}>{name}</span>,
      }))}
    />
  );
}
