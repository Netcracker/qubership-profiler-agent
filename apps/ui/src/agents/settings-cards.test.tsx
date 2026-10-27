import { cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { useState } from 'react';
import { afterEach, describe, expect, it } from 'vitest';

import { DEFAULT_MIN_METHOD_SIZE, SettingsCards } from './settings-cards';
import type { AgentConfigSettings, AgentStartupValue } from './types';

afterEach(() => cleanup());

const PLUGINS = ['spring', 'tomcat_http', 'jackson', 'quartz'];

const STARTUP: AgentStartupValue[] = [{ label: 'Agent log level', value: 'INFO', source: 'ESC_LOG_LEVEL' }];

const SETTINGS: AgentConfigSettings = {
  enabled: true,
  min_duration_ms: 1,
  include: [{ pattern: 'com.shop.**', min_method_size: 20 }],
  exclude: ['com.shop.generated.**'],
  plugins: { spring: true, jackson: false },
  params: [{ name: 'request.id', index: true, list: false }],
  max_age: '8 days',
  max_size: '2 Gb',
};

/** Holds the settings the way the page does and exposes the latest value. */
function renderCards(): { current: () => AgentConfigSettings } {
  let latest = SETTINGS;
  function Harness() {
    const [settings, setSettings] = useState(SETTINGS);
    latest = settings;
    return <SettingsCards settings={settings} availablePlugins={PLUGINS} startup={STARTUP} onChange={setSettings} />;
  }
  render(<Harness />);
  return { current: () => latest };
}

const card = (title: string): HTMLElement => screen.getByRole('group', { name: title });

function add(group: HTMLElement, label: string, value: string): void {
  fireEvent.change(within(group).getByRole('textbox', { name: label }), { target: { value } });
  const column = within(group).getByRole('textbox', { name: label }).parentElement;
  if (column === null) throw new Error('the field has no row');
  fireEvent.click(within(column).getByRole('button', { name: 'Add' }));
}

describe('SettingsCards', () => {
  it('renders the six cards', () => {
    renderCards();
    for (const title of [
      'Profiling',
      'Local dump retention',
      'Packages',
      'Plugins',
      'Call parameters',
      'Set at startup',
    ]) {
      expect(card(title)).toBeInTheDocument();
    }
  });

  it('turns recording off and changes the minimum duration', () => {
    const cards = renderCards();
    const profiling = card('Profiling');
    expect(within(profiling).getByText('needs a restart')).toBeInTheDocument();

    fireEvent.click(within(profiling).getByRole('switch'));
    fireEvent.change(within(profiling).getByRole('spinbutton', { name: 'Skip method calls shorter than' }), {
      target: { value: '25' },
    });

    expect(cards.current()).toMatchObject({ enabled: false, min_duration_ms: 25 });
  });

  it('edits the two retention limits as text', () => {
    const cards = renderCards();
    const retention = card('Local dump retention');
    fireEvent.change(within(retention).getByRole('textbox', { name: 'Keep for at most' }), {
      target: { value: '3 days' },
    });
    fireEvent.change(within(retention).getByRole('textbox', { name: 'Keep at most' }), { target: { value: '500 Mb' } });
    expect(cards.current()).toMatchObject({ max_age: '3 days', max_size: '500 Mb' });
  });

  it('adds, resizes, and removes a pattern to profile', () => {
    const cards = renderCards();
    const packages = card('Packages');

    add(packages, 'Package or class pattern to profile', '  com.shop.cart.**  ');
    expect(cards.current().include).toEqual([
      { pattern: 'com.shop.**', min_method_size: 20 },
      { pattern: 'com.shop.cart.**', min_method_size: DEFAULT_MIN_METHOD_SIZE },
    ]);
    expect(within(packages).getByRole('textbox', { name: 'Package or class pattern to profile' })).toHaveValue('');

    fireEvent.change(within(packages).getByRole('spinbutton', { name: 'Minimum method size for com.shop.cart.**' }), {
      target: { value: '5' },
    });
    fireEvent.click(within(packages).getByRole('button', { name: 'Stop profiling com.shop.**' }));
    expect(cards.current().include).toEqual([{ pattern: 'com.shop.cart.**', min_method_size: 5 }]);
  });

  it('adds and removes an exclusion, and ignores a blank or repeated pattern', () => {
    const cards = renderCards();
    const packages = card('Packages');

    add(packages, 'Package or class pattern to exclude', '   ');
    add(packages, 'Package or class pattern to exclude', 'com.shop.generated.**');
    expect(cards.current().exclude).toEqual(['com.shop.generated.**']);

    add(packages, 'Package or class pattern to exclude', 'com.shop.proxy.**');
    fireEvent.click(within(packages).getByRole('button', { name: 'Remove the exclusion for com.shop.generated.**' }));
    expect(cards.current().exclude).toEqual(['com.shop.proxy.**']);
  });

  it('lists the plugins the config mentions until asked for all of them', () => {
    const cards = renderCards();
    const plugins = card('Plugins');
    expect(within(plugins).getByText(/1 of 4 are on\.$/)).toBeInTheDocument();
    expect(within(plugins).getAllByRole('checkbox')).toHaveLength(2);

    fireEvent.click(within(plugins).getByRole('button', { name: 'Show all 4 plugins' }));
    expect(within(plugins).getAllByRole('checkbox')).toHaveLength(4);

    fireEvent.click(within(plugins).getByRole('checkbox', { name: 'quartz' }));
    fireEvent.click(within(plugins).getByRole('checkbox', { name: 'spring' }));
    expect(cards.current().plugins).toEqual({ spring: false, jackson: false, quartz: true });
    expect(within(plugins).getByRole('button', { name: 'Show fewer plugins' })).toBeInTheDocument();
  });

  it('toggles, adds, and removes a call parameter', () => {
    const cards = renderCards();
    const params = card('Call parameters');

    fireEvent.click(within(params).getByRole('checkbox', { name: 'request.id listed' }));
    fireEvent.click(within(params).getByRole('checkbox', { name: 'request.id indexed' }));
    add(params, 'Parameter name', 'user.tenant');
    expect(cards.current().params).toEqual([
      { name: 'request.id', index: false, list: true },
      { name: 'user.tenant', index: true, list: false },
    ]);

    fireEvent.click(within(params).getByRole('button', { name: 'Remove the parameter request.id' }));
    expect(cards.current().params).toEqual([{ name: 'user.tenant', index: true, list: false }]);
  });

  it('shows the startup values without a way to edit them', () => {
    renderCards();
    const startup = card('Set at startup');
    expect(within(startup).getByText('Agent log level')).toBeInTheDocument();
    expect(within(startup).getByText('INFO')).toBeInTheDocument();
    expect(within(startup).getByText('ESC_LOG_LEVEL')).toBeInTheDocument();
    expect(within(startup).queryByRole('textbox')).not.toBeInTheDocument();
  });
});
