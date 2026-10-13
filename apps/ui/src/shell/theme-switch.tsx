import { toggleThemeMode, useThemeMode } from '../ui/theme-mode';
import styles from './theme-switch.module.css';

/** The header's theme switch. It shows the current mode and names the one a click selects. */
export function ThemeSwitch() {
  const mode = useThemeMode();
  const label = mode === 'dark' ? 'Switch to light theme' : 'Switch to dark theme';
  return (
    <button type="button" className={styles.button} title={label} aria-label={label} onClick={toggleThemeMode}>
      <svg className={styles.icon} width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
        {mode === 'dark' ? (
          <path d="M13.5 9.6A5.8 5.8 0 0 1 6.4 2.5a5.8 5.8 0 1 0 7.1 7.1z" />
        ) : (
          <>
            <circle cx="8" cy="8" r="2.8" />
            <path d="M8 1.5v1.7M8 12.8v1.7M1.5 8h1.7M12.8 8h1.7M3.4 3.4l1.2 1.2M11.4 11.4l1.2 1.2M3.4 12.6l1.2-1.2M11.4 4.6l1.2-1.2" />
          </>
        )}
      </svg>
    </button>
  );
}
