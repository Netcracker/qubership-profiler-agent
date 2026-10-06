import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ParamValueModal, beautifyValue, detectLanguage, isSqlParamKey } from './param-value-viewer';

describe('isSqlParamKey', () => {
  it('matches the sql key case-insensitively and rejects other keys', () => {
    expect(isSqlParamKey('sql')).toBe(true);
    expect(isSqlParamKey('SQL')).toBe(true);
    expect(isSqlParamKey('binds')).toBe(false);
    expect(isSqlParamKey('node.name')).toBe(false);
  });
});

describe('detectLanguage', () => {
  it('matches known sql-shaped keys regardless of the value', () => {
    expect(detectLanguage('sql', 'anything')).toBe('sql');
    expect(detectLanguage('sql.monitor', 'anything')).toBe('sql');
    expect(detectLanguage('mdx', 'anything')).toBe('sql');
    expect(detectLanguage('cassandra.query', 'anything')).toBe('sql');
  });

  it('matches xml/json by key substring', () => {
    expect(detectLanguage('response.xml', 'anything')).toBe('xml');
    expect(detectLanguage('payload.json', 'anything')).toBe('json');
  });

  it('falls back to sniffing the value for a generic key', () => {
    expect(detectLanguage('response.body', '<root><a/></root>')).toBe('xml');
    expect(detectLanguage('response.body', '{"a": 1}')).toBe('json');
    expect(detectLanguage('response.body', '[1, 2, 3]')).toBe('json');
    // Leading whitespace shouldn't defeat the sniff.
    expect(detectLanguage('response.body', '  \n<root/>')).toBe('xml');
  });

  it('returns null for a generic key and a value that looks like neither', () => {
    expect(detectLanguage('node.name', 'http-nio-8080-exec-5')).toBeNull();
    expect(detectLanguage('request.id', '40de9e47')).toBeNull();
  });

  it('sniffs a leading "[" as JSON even for an unrelated key (a bind array, say)', () => {
    // A deliberate, narrow exception: the first-character sniff is a
    // structural signal, not a language guess, so it still fires here — the
    // old UI special-cased 'binds' to skip formatting entirely instead.
    expect(detectLanguage('binds', "[583, 'ACTIVE']")).toBe('json');
  });
});

describe('beautifyValue', () => {
  it('does not reformat short values', () => {
    expect(beautifyValue('select * from t where id = 1', 'sql')).toBeNull();
  });

  it('does not reformat values that already span multiple lines', () => {
    const alreadyMultiline = "select id, total\nfrom orders\nwhere status = 'open' and total > 100";
    expect(beautifyValue(alreadyMultiline, 'sql')).toBeNull();
  });

  it('reformats a long single-line SQL query into multi-line SQL', () => {
    const longSingleLine = "select id, total from orders where status = 'open' and total > 100";
    const beautified = beautifyValue(longSingleLine, 'sql');
    expect(beautified).not.toBeNull();
    expect(beautified).toContain('\n');
    expect(beautified?.replace(/\s+/g, ' ').trim().toLowerCase()).toBe(longSingleLine.toLowerCase());
  });

  it('reformats a long single-line JSON value into indented JSON', () => {
    const longSingleLine = '{"customerId": 1234, "status": "open", "items": [1, 2, 3], "total": 99.5}';
    const beautified = beautifyValue(longSingleLine, 'json');
    expect(beautified).not.toBeNull();
    expect(beautified).toContain('\n');
    expect(JSON.parse(beautified ?? '')).toEqual(JSON.parse(longSingleLine));
  });

  it('reformats a long single-line XML value into indented XML', () => {
    const longSingleLine = '<order id="1234"><customer id="99"/><items><item sku="A"/><item sku="B"/></items></order>';
    const beautified = beautifyValue(longSingleLine, 'xml');
    expect(beautified).not.toBeNull();
    expect(beautified).toContain('\n');
  });
});

describe('ParamValueModal', () => {
  let writeText: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
  });

  afterEach(cleanup);

  it('stays closed when there is no target', () => {
    render(<ParamValueModal target={null} onClose={() => undefined} />);
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('shows the full untruncated value and copies it on demand', async () => {
    const longValue = `x-${'a'.repeat(2000)}`;
    render(
      <AntdApp>
        <ParamValueModal target={{ key: 'node.name', value: longValue }} onClose={() => undefined} />
      </AntdApp>,
    );
    expect(screen.getByText('Full value — node.name')).toBeInTheDocument();
    expect(screen.getByText(longValue)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));
    expect(writeText).toHaveBeenCalledWith(longValue);
  });

  it('highlights SQL keywords only when the param key is sql', () => {
    const sql = 'SELECT * FROM orders';
    const { rerender } = render(
      <AntdApp>
        <ParamValueModal target={{ key: 'sql', value: sql }} onClose={() => undefined} />
      </AntdApp>,
    );
    const select = screen.getByText('SELECT');
    expect(select.tagName).toBe('SPAN');
    expect(select.className).toBe('hljs-keyword');

    rerender(
      <AntdApp>
        <ParamValueModal target={{ key: 'binds', value: sql }} onClose={() => undefined} />
      </AntdApp>,
    );
    // Non-sql keys render the raw text node, so 'SELECT' is not its own element.
    expect(screen.queryByText('SELECT')?.tagName).not.toBe('SPAN');
  });

  it('calls onClose when the modal is dismissed', () => {
    const onClose = vi.fn();
    render(
      <AntdApp>
        <ParamValueModal target={{ key: 'sql', value: 'SELECT 1' }} onClose={onClose} />
      </AntdApp>,
    );
    fireEvent.click(screen.getByRole('button', { name: /close/i }));
    expect(onClose).toHaveBeenCalled();
  });
});
