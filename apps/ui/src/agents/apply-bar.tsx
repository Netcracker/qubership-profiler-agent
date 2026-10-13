import { Button } from 'antd';

import styles from './apply-bar.module.css';

/** "Apply to 3 agents"; plain "Apply" until the agents of the scope are known. */
export function applyLabel(managedAgents: number | null): string {
  if (managedAgents === null) return 'Apply';
  return `Apply to ${managedAgents} ${managedAgents === 1 ? 'agent' : 'agents'}`;
}

interface ApplyBarProps {
  /** Whether applying gives an inheriting scope its own config. */
  createsOwnConfig: boolean;
  /** Agents that will pull the config; null while the list loads. */
  managedAgents: number | null;
  saving: boolean;
  onDiscard: () => void;
  onApply: () => void;
}

/** The bar that sticks to the bottom of the page while it has unsaved changes. */
export function ApplyBar({ createsOwnConfig, managedAgents, saving, onDiscard, onApply }: ApplyBarProps) {
  return (
    <div className={styles.bar} role="region" aria-label="Unsaved changes">
      <span className={styles.text}>
        You have unsaved changes.{' '}
        {createsOwnConfig
          ? 'Applying gives this scope its own config, as a new revision.'
          : 'Applying creates a new revision.'}
      </span>
      <Button onClick={onDiscard} disabled={saving}>
        Discard
      </Button>
      <Button type="primary" onClick={onApply} loading={saving}>
        {applyLabel(managedAgents)}
      </Button>
    </div>
  );
}
