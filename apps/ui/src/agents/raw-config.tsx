import { Alert, Button, Card, Input } from 'antd';
import { useEffect, useRef, useState } from 'react';

import { renderAgentConfig } from './api';
import styles from './raw-config.module.css';
import type { AgentConfigSettings } from './types';
import type { Loadable } from './use-loadable';

interface RawConfigProps {
  /** The settings with the unsaved edits, which the server renders into XML. */
  settings: AgentConfigSettings;
  /** The hand-edited XML, or null while the XML is generated from `settings`. */
  rawXml: string | null;
  /** Receives the text of a hand edit. The first one starts the override. */
  onEdit: (xml: string) => void;
  onDiscardRaw: () => void;
}

/**
 * The Raw config tab: the XML that the agents of the scope receive. Until
 * someone edits it, the server renders it from the settings, so that one
 * component knows how settings become `_config.xml`. A hand edit replaces the
 * generated text until it is discarded.
 */
export function RawConfig({ settings, rawXml, onEdit, onDiscardRaw }: RawConfigProps) {
  const edited = rawXml !== null;
  const [generated, setGenerated] = useState<Loadable<string>>({ kind: 'loading' });
  const [attempt, setAttempt] = useState(0);
  const settingsRef = useRef(settings);
  settingsRef.current = settings;
  const settingsKey = JSON.stringify(settings);

  // Hand-edited XML needs no render. Otherwise render again whenever the
  // settings change, and drop the answer to a request that is out of date.
  useEffect(() => {
    if (edited) return;
    const controller = new AbortController();
    setGenerated({ kind: 'loading' });
    renderAgentConfig(settingsRef.current, controller.signal)
      .then((xml) => setGenerated({ kind: 'ready', data: xml }))
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setGenerated({ kind: 'error', message: e instanceof Error ? e.message : String(e) });
      });
    return () => controller.abort();
  }, [edited, settingsKey, attempt]);

  const ready = edited || generated.kind === 'ready';
  const text = edited ? rawXml : generated.kind === 'ready' ? generated.data : '';

  return (
    <Card size="small" classNames={{ body: styles.card }}>
      <div className={styles.head}>
        <h2 className={styles.title}>Raw config</h2>
        <span className={styles.state}>
          <span className={styles.dot} data-edited={edited} aria-hidden="true" />
          {edited ? 'Edited by hand' : 'Generated from Settings'}
        </span>
        {edited ? <Button onClick={onDiscardRaw}>Discard raw edits</Button> : null}
      </div>
      <div className={styles.help}>
        The XML the agents in this scope receive, in the same format as <span className="pf-mono">_config.xml</span>.
        It is generated from Settings until you edit it. After that it is sent as written, and Settings stops applying.
      </div>
      {!edited && generated.kind === 'error' ? (
        <Alert
          type="error"
          showIcon
          title="Could not generate the XML"
          description={generated.message}
          action={<Button onClick={() => setAttempt((n) => n + 1)}>Retry</Button>}
        />
      ) : null}
      {/* One element for both states: swapping it on the first keystroke would drop the focus. */}
      <Input.TextArea
        aria-label="Raw config XML"
        className={`pf-mono ${styles.xml}`}
        spellCheck={false}
        wrap="off"
        // Typing into text that has not arrived would save the blank as the override.
        readOnly={!ready}
        placeholder={generated.kind === 'loading' ? 'Generating the XML…' : ''}
        value={text}
        onChange={(e) => onEdit(e.target.value)}
      />
    </Card>
  );
}
