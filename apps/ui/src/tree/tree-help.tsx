import type { ReactNode } from 'react';

import { CALLS_CRIT, CALLS_WARN, SELF_CRIT_SHARE, SELF_WARN_SHARE, TOTAL_BOLD_SHARE, formatShare } from './row-metrics';
import styles from './tree-help.module.css';

// The Help popover of the Call Tree tab (09 §3.3): one example row taken
// apart, then the rest of the page's vocabulary. The thresholds are printed
// from row-metrics.ts, so the text cannot drift from the highlighting.

function Item({ term, children }: { term: ReactNode; children: ReactNode }) {
  return (
    <div className={styles.item}>
      <span className={`pf-mono ${styles.term}`}>{term}</span>
      <span className={styles.text}>{children}</span>
    </div>
  );
}

export function TreeHelp() {
  return (
    <div className={styles.help} role="dialog" aria-label="How to read the call tree">
      <div className={styles.title}>How to read the call tree</div>
      <div className={styles.example}>
        <span className={styles.caption}>Example row</span>
        <div className={`pf-mono ${styles.exampleRow}`}>
          <span className={styles.strong}>4.8s</span> <span className={styles.dim}>(3ms) ×1</span>{' '}
          <span className={styles.dim}>com.acme.orders.</span>OrderController.create
        </div>
        <Item term="4.8s">Total time: the method and everything it calls.</Item>
        <Item term="(3ms)">
          Self time: the method&apos;s own code only. Here almost all of the 4.8 s is spent in the methods it calls.
        </Item>
        <Item term="×1">How many times the method ran at this place in the tree.</Item>
        <Item term="com.acme.orders.">The package, dimmed so the class stands out.</Item>
        <Item term="OrderController.create">The class and the method. Click it to select the row.</Item>
      </div>
      <Item term="Lines on the left">
        A rounded line runs from a method down to each method it calls. Parameters have no line of their own, because
        they are not calls.
      </Item>
      <Item term="Orange, red, bold">
        Numbers that stand out in this call. Self time is orange from {formatShare(SELF_WARN_SHARE)} of the call and
        red from {formatShare(SELF_CRIT_SHARE)}. A call count is orange from {CALLS_WARN} and red from {CALLS_CRIT}. A
        total is bold from {formatShare(TOTAL_BOLD_SHARE)}.
      </Item>
      <Item term="Bar on the right">
        The share of the whole call. The solid part is the method&apos;s own time, and the light part is time in the
        methods it calls.
      </Item>
      <Item term="+6">
        Six pass-through levels under this method are hidden, because they only forward the call. Click the badge to
        show them.
      </Item>
      <Item term="Category badge">
        A category from Setup categories. It marks a method and everything under it, so you can see what kind of work
        the time went to.
      </Item>
      <Item term="sql, web.url">
        A parameter recorded with the method, with its time and count. Expand a method to see its parameters.
      </Item>
      <Item term="Three dots">
        Operations for one method: incoming and outgoing calls, stacktrace, usages, local hotspots, and more.
      </Item>
      <Item term="Ctrl + hover">
        Shows the method&apos;s full stats: durations, suspension, invocations, averages, source file, and jar.
      </Item>
      <Item term="Flame graph tab">
        The same tree as frames. Width is the share of the call, and a method&apos;s callees sit under it. Click a
        frame to zoom in on it. A row you select here is selected there too.
      </Item>
    </div>
  );
}
