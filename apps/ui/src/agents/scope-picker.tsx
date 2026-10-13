import { Button, Popover } from 'antd';
import { useState } from 'react';

import { scopeLabel, scopeShortLabel, worstStatus } from './scope';
import styles from './scope-picker.module.css';
import { StatusDot } from './status-dot';
import type { AgentConfigScope, AgentConfigScopeId, AgentStatus } from './types';

const INDENT: Record<AgentConfigScope['kind'], number> = { default: 0, namespace: 1, service: 2 };

function agentCount(scope: AgentConfigScope): number {
  return Object.values(scope.agents).reduce((sum, n) => sum + n, 0);
}

interface ScopePickerProps {
  /** The scope tree in tree order; empty until it loads. */
  scopes: readonly AgentConfigScope[];
  value: AgentConfigScopeId;
  /** The worst status among the agents of `value`, for the dot on the button. */
  status: AgentStatus;
  onChange: (scope: AgentConfigScopeId) => void;
  /** Called when the list opens, so the caller can refresh the tree. */
  onOpen?: () => void;
}

/**
 * The scope button and its list: Default, then each namespace with its
 * services. A row shows the worst status of the scope's agents, whether the
 * scope has its own config, and how many agents it has.
 */
export function ScopePicker({ scopes, value, status, onChange, onOpen }: ScopePickerProps) {
  const [open, setOpen] = useState(false);

  const list = (
    <div className={styles.menu}>
      <div role="listbox" aria-label="Scope">
        {scopes.map((scope) => (
          <button
            key={scope.id}
            type="button"
            role="option"
            aria-selected={scope.id === value}
            className={`${styles.option}${scope.id === value ? ` ${styles.selected}` : ''}`}
            style={{ paddingLeft: 12 + INDENT[scope.kind] * 16 }}
            onClick={() => {
              setOpen(false);
              if (scope.id !== value) onChange(scope.id);
            }}
          >
            <StatusDot status={worstStatus(scope.agents)} labelled />
            <span className={styles.name}>{scopeShortLabel(scope.id)}</span>
            {scope.has_own_config && scope.kind !== 'default' ? <span className={styles.badge}>own config</span> : null}
            <span className={`pf-mono ${styles.count}`}>{agentCount(scope)}</span>
          </button>
        ))}
      </div>
      <div className={styles.help}>
        A service uses its own config if it has one, then its namespace&apos;s, then Default. The number is the count
        of connected agents.
      </div>
    </div>
  );

  return (
    <Popover
      trigger="click"
      placement="bottomLeft"
      arrow={false}
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (next) onOpen?.();
      }}
      content={list}
      styles={{ container: { padding: 0 } }}
    >
      <Button className={styles.trigger} aria-haspopup="listbox" aria-expanded={open}>
        <span className={styles.caption}>Scope</span>
        <StatusDot status={status} />
        <span className={styles.current}>{scopeLabel(value)}</span>
        <svg className={styles.chevron} width="11" height="11" viewBox="0 0 12 12" aria-hidden="true">
          <path d="M2.5 4.5 6 8l3.5-3.5" />
        </svg>
      </Button>
    </Popover>
  );
}
