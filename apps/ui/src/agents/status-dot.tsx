import { STATUS_LABELS } from './scope';
import styles from './status-dot.module.css';
import type { AgentStatus } from './types';

/**
 * The colored dot of an agent status. `data-status` carries the status for the
 * stylesheet, and the title names it for a pointer. Text beside the dot names
 * it for everyone else, so the dot itself is hidden from assistive technology
 * unless `labelled` is set.
 */
export function StatusDot({ status, labelled = false }: { status: AgentStatus; labelled?: boolean }) {
  return (
    <span
      className={styles.dot}
      data-status={status}
      title={STATUS_LABELS[status]}
      {...(labelled ? { role: 'img', 'aria-label': STATUS_LABELS[status] } : { 'aria-hidden': true })}
    />
  );
}
