import hljs from 'highlight.js/lib/core';
import jsonLang from 'highlight.js/lib/languages/json';
import sqlLang from 'highlight.js/lib/languages/sql';
import xmlLang from 'highlight.js/lib/languages/xml';
import 'highlight.js/styles/github.css';
import { App, Button, Modal, Typography } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import vkbeautify from 'vkbeautify';

import styles from './param-value-viewer.module.css';

// Full-value viewer for parameter rows (PR 708 review #21): truncated
// inline/table text had no way to read or copy the full value. Mirrors the
// stacktrace modal's copy-to-clipboard UX (tree-view.tsx) and the old UI's
// SQL/XML/JSON rendering pipeline (profiler.mjs printReformatted): a long
// single-line value reformats through vkbeautify with a
// view-original/view-reformatted toggle, then renders through a syntax
// highlighter. The old UI used code-prettify + color-themes-for-google-code
// -prettify — both dormant since ~2022 and loaded as global-attaching UMD
// scripts; highlight.js is their actively maintained, ESM-native equivalent
// (own TS types, tree-shakeable per-language imports), so this ports the
// pipeline onto it instead.
//
// Free-text auto-detection (hljs.highlightAuto) was tried and dropped: its
// relevance score doesn't separate real SQL from ordinary prose that merely
// contains common words like "select"/"from"/"and" — a log line scored
// *higher* than genuine SQL in testing. Language is decided the same
// deterministic way the old UI did it instead: by param key, then by the
// value's first non-whitespace character for XML/JSON — both are safe
// signals (a JVM thread name or a UUID never starts with '<', '{', or '[').

hljs.registerLanguage('sql', sqlLang);
hljs.registerLanguage('xml', xmlLang);
hljs.registerLanguage('json', jsonLang);

export type ParamLanguage = 'sql' | 'xml' | 'json';

/** The tree's SQL params carry the fixed key 'sql' (see mocks/synthetic.ts, calls/columns.tsx). */
export function isSqlParamKey(key: string): boolean {
  return key.toLowerCase() === 'sql';
}

const KEY_LANGUAGE: Record<string, ParamLanguage> = {
  sql: 'sql',
  'sql.monitor': 'sql',
  mdx: 'sql',
  'cassandra.query': 'sql',
};

/**
 * Picks a highlighter language for a parameter, the way the old UI's
 * printReformatted call sites did (profiler.mjs ~4059-4091): an explicit key
 * match first, then — since many params carry a generic key like
 * `response.body` — a content sniff on the value's first character, which is
 * a reliable enough signal for XML/JSON without the false positives a
 * statistical language guess produces on plain text (see file header).
 */
export function detectLanguage(key: string, value: string): ParamLanguage | null {
  const known = KEY_LANGUAGE[key.toLowerCase()];
  if (known !== undefined) return known;
  const k = key.toLowerCase();
  if (k.includes('xml')) return 'xml';
  if (k.includes('json')) return 'json';
  const trimmed = value.trimStart();
  if (trimmed.startsWith('<')) return 'xml';
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) return 'json';
  return null;
}

/**
 * Reformats a long single-line value the way the old UI did — only worth it
 * past a length threshold, and only when the value isn't already multi-line.
 * Returns null when reformatting doesn't apply or doesn't change anything
 * (vkbeautify no-ops on malformed input by returning it unchanged).
 */
export function beautifyValue(value: string, language: ParamLanguage): string | null {
  if (value.length <= 60 || value.includes('\n')) return null;
  try {
    const beautified = vkbeautify[language](value, 2);
    return beautified !== value ? beautified : null;
  } catch {
    return null;
  }
}

/**
 * Inline single-line syntax highlighting for a parameter value, shown in the
 * call tree and the Parameters table where the full-value modal is one click
 * away. The caller decides the language with detectLanguage and keeps its own
 * plain text + ellipsis when the value has none, so only SQL/XML/JSON rows pay
 * for a highlight pass. Styling inherits the row's font and clips with an
 * ellipsis; the github.css `.hljs` background is overridden to blend in.
 */
export function InlineHighlight({ language, value }: { language: ParamLanguage; value: string }) {
  const html = hljs.highlight(value, { language }).value;
  return (
    <code
      className={`hljs ${styles.inlineCode}`}
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}

export interface ParamValueTarget {
  key: string;
  value: string;
}

interface ParamValueModalProps {
  target: ParamValueTarget | null;
  onClose: () => void;
}

export function ParamValueModal({ target, onClose }: ParamValueModalProps) {
  const { message } = App.useApp();
  const language = target !== null ? detectLanguage(target.key, target.value) : null;
  const reformatted = useMemo(
    () => (language !== null && target !== null ? beautifyValue(target.value, language) : null),
    [language, target],
  );
  const [showOriginal, setShowOriginal] = useState(false);
  useEffect(() => setShowOriginal(false), [target]);

  const displayValue = target === null ? '' : reformatted !== null && !showOriginal ? reformatted : target.value;
  const highlighted = language !== null ? hljs.highlight(displayValue, { language }).value : null;

  return (
    <Modal
      open={target !== null}
      title={target === null ? '' : `Full value — ${target.key}`}
      onCancel={onClose}
      width={800}
      footer={
        <Button
          type="primary"
          onClick={() => void navigator.clipboard.writeText(displayValue).then(() => message.success('Copied'))}
        >
          Copy
        </Button>
      }
    >
      {reformatted !== null ? (
        <Typography.Link onClick={() => setShowOriginal((v) => !v)} className={styles.toggleLink}>
          {showOriginal ? 'view reformatted' : 'view original'}
        </Typography.Link>
      ) : null}
      <pre className={styles.valuePre}>
        {highlighted !== null ? <code className="hljs" dangerouslySetInnerHTML={{ __html: highlighted }} /> : displayValue}
      </pre>
    </Modal>
  );
}
