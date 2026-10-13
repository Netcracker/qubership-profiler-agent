import { Button, Card, Checkbox, Input, InputNumber, Switch } from 'antd';
import { useId, useState } from 'react';
import type { ReactNode } from 'react';

import styles from './settings-cards.module.css';
import type { AgentConfigSettings, AgentStartupValue } from './types';

/** The `minimum-method-size` a new pattern starts with. */
export const DEFAULT_MIN_METHOD_SIZE = 50;

function SettingsCard(props: { title: string; help?: ReactNode; muted?: boolean; children: ReactNode }) {
  const titleId = useId();
  return (
    <Card
      size="small"
      role="group"
      aria-labelledby={titleId}
      className={props.muted ? styles.muted : undefined}
      classNames={{ body: styles.card }}
    >
      <h2 id={titleId} className={styles.title}>
        {props.title}
      </h2>
      {props.help === undefined ? null : <div className={styles.help}>{props.help}</div>}
      {props.children}
    </Card>
  );
}

function RemoveButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <Button
      type="text"
      size="small"
      aria-label={label}
      title="Remove"
      onClick={onClick}
      icon={
        <svg className={styles.cross} width="10" height="10" viewBox="0 0 10 10" aria-hidden="true">
          <path d="M2 2l6 6M8 2 2 8" />
        </svg>
      }
    />
  );
}

/** A text field with an Add button. Enter adds too; a blank value adds nothing. */
function AddRow({ label, placeholder, onAdd }: { label: string; placeholder: string; onAdd: (value: string) => void }) {
  const [value, setValue] = useState('');
  const add = (): void => {
    const trimmed = value.trim();
    if (trimmed === '') return;
    onAdd(trimmed);
    setValue('');
  };
  return (
    <div className={styles.addRow}>
      <Input
        size="small"
        className={`pf-mono ${styles.grow}`}
        aria-label={label}
        placeholder={placeholder}
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onPressEnter={add}
      />
      <Button size="small" onClick={add}>
        Add
      </Button>
    </div>
  );
}

const without = <T,>(list: readonly T[], index: number): T[] => list.filter((_, i) => i !== index);

const replaceAt = <T,>(list: readonly T[], index: number, item: T): T[] => list.map((old, i) => (i === index ? item : old));

/** A whole number of at least zero; an emptied field reads as zero. */
const wholeNumber = (value: number | null): number => Math.max(0, Math.trunc(value ?? 0));

export interface SettingsCardsProps {
  settings: AgentConfigSettings;
  /** Every plugin the agents know, in display order. */
  availablePlugins: readonly string[];
  startup: readonly AgentStartupValue[];
  /** Receives the whole settings object with one change made. */
  onChange: (next: AgentConfigSettings) => void;
}

/**
 * The Settings tab: five cards that edit the settings and one that shows the
 * values fixed at JVM start. The cards hold no settings state of their own.
 */
export function SettingsCards({ settings, availablePlugins, startup, onChange }: SettingsCardsProps) {
  const [allPlugins, setAllPlugins] = useState(false);
  const minDurationId = useId();
  const patch = (change: Partial<AgentConfigSettings>): void => onChange({ ...settings, ...change });

  // Until asked for the rest, show the plugins the config mentions, on or off.
  const shownPlugins = allPlugins ? availablePlugins : availablePlugins.filter((name) => name in settings.plugins);
  const pluginsOn = availablePlugins.filter((name) => settings.plugins[name] === true).length;

  return (
    <div className={styles.cards}>
      <div className={styles.pair}>
        <SettingsCard title="Profiling">
          <label className={styles.inline}>
            <Switch size="small" checked={settings.enabled} onChange={(enabled) => patch({ enabled })} />
            <span className={styles.strong}>Record calls</span>
          </label>
          <div className={styles.help}>
            When off, the agent stays attached and connected but records nothing. Takes effect without a restart.
          </div>
          <div className={styles.inline}>
            <label htmlFor={minDurationId}>Skip method calls shorter than</label>
            <InputNumber
              id={minDurationId}
              size="small"
              className={`pf-mono ${styles.number}`}
              min={0}
              precision={0}
              value={settings.min_duration_ms}
              onChange={(value) => patch({ min_duration_ms: wholeNumber(value) })}
            />
            <span>ms</span>
            <span className={styles.restart}>needs a restart</span>
          </div>
        </SettingsCard>

        <SettingsCard
          title="Local dump retention"
          help="How long the agent keeps dump files on the pod's own disk."
        >
          <div className={styles.fields}>
            <label className={styles.inline}>
              Keep for at most
              <Input
                size="small"
                className={`pf-mono ${styles.short}`}
                value={settings.max_age}
                onChange={(e) => patch({ max_age: e.target.value })}
              />
            </label>
            <label className={styles.inline}>
              Keep at most
              <Input
                size="small"
                className={`pf-mono ${styles.short}`}
                value={settings.max_size}
                onChange={(e) => patch({ max_size: e.target.value })}
              />
            </label>
          </div>
        </SettingsCard>
      </div>

      <SettingsCard
        title="Packages"
        help={
          <>
            <span className="pf-mono">**</span> matches any subpackage. Methods smaller than the minimum size, in
            bytecode instructions, are not instrumented.
          </>
        }
      >
        <div className={styles.columns}>
          <div className={styles.column}>
            <div className={styles.columnHead}>
              <span className={styles.grow}>Profile</span>
              <span className={styles.sizeHead}>Min. method size</span>
            </div>
            {settings.include.map((rule, i) => (
              <div key={rule.pattern} className={styles.line}>
                <span className={`pf-mono ${styles.name}`}>{rule.pattern}</span>
                <InputNumber
                  size="small"
                  className={`pf-mono ${styles.number}`}
                  min={0}
                  precision={0}
                  aria-label={`Minimum method size for ${rule.pattern}`}
                  value={rule.min_method_size}
                  onChange={(value) =>
                    patch({ include: replaceAt(settings.include, i, { ...rule, min_method_size: wholeNumber(value) }) })
                  }
                />
                <RemoveButton
                  label={`Stop profiling ${rule.pattern}`}
                  onClick={() => patch({ include: without(settings.include, i) })}
                />
              </div>
            ))}
            <AddRow
              label="Package or class pattern to profile"
              placeholder="com.example.orders.**"
              onAdd={(pattern) => {
                if (settings.include.some((rule) => rule.pattern === pattern)) return;
                patch({ include: [...settings.include, { pattern, min_method_size: DEFAULT_MIN_METHOD_SIZE }] });
              }}
            />
          </div>
          <div className={styles.column}>
            <div className={styles.columnHead}>Do not profile</div>
            {settings.exclude.map((pattern, i) => (
              <div key={pattern} className={styles.line}>
                <span className={`pf-mono ${styles.name}`}>{pattern}</span>
                <RemoveButton
                  label={`Remove the exclusion for ${pattern}`}
                  onClick={() => patch({ exclude: without(settings.exclude, i) })}
                />
              </div>
            ))}
            <AddRow
              label="Package or class pattern to exclude"
              placeholder="com.example.generated.**"
              onAdd={(pattern) => {
                if (!settings.exclude.includes(pattern)) patch({ exclude: [...settings.exclude, pattern] });
              }}
            />
          </div>
        </div>
      </SettingsCard>

      <div className={styles.pair}>
        <SettingsCard
          title="Plugins"
          help={`Each plugin adds profiling rules for one library. ${pluginsOn} of ${availablePlugins.length} are on.`}
        >
          <div className={styles.plugins}>
            {shownPlugins.map((name) => (
              <Checkbox
                key={name}
                checked={settings.plugins[name] === true}
                onChange={(e) => patch({ plugins: { ...settings.plugins, [name]: e.target.checked } })}
              >
                <span className={`pf-mono ${styles.small}`}>{name}</span>
              </Checkbox>
            ))}
          </div>
          <div>
            <Button type="link" size="small" className={styles.link} onClick={() => setAllPlugins(!allPlugins)}>
              {allPlugins ? 'Show fewer plugins' : `Show all ${availablePlugins.length} plugins`}
            </Button>
          </div>
        </SettingsCard>

        <SettingsCard
          title="Call parameters"
          help="Values the agent attaches to a call. An indexed parameter can be used as a filter on the Calls screen. A listed parameter is shown in the calls list."
        >
          <div>
            {settings.params.map((param, i) => (
              <div key={param.name} className={styles.line}>
                <span className={`pf-mono ${styles.name}`}>{param.name}</span>
                <Checkbox
                  aria-label={`${param.name} indexed`}
                  checked={param.index}
                  onChange={(e) => patch({ params: replaceAt(settings.params, i, { ...param, index: e.target.checked }) })}
                >
                  Indexed
                </Checkbox>
                <Checkbox
                  aria-label={`${param.name} listed`}
                  checked={param.list}
                  onChange={(e) => patch({ params: replaceAt(settings.params, i, { ...param, list: e.target.checked }) })}
                >
                  Listed
                </Checkbox>
                <RemoveButton
                  label={`Remove the parameter ${param.name}`}
                  onClick={() => patch({ params: without(settings.params, i) })}
                />
              </div>
            ))}
            <AddRow
              label="Parameter name"
              placeholder="user.tenant"
              onAdd={(name) => {
                if (settings.params.some((param) => param.name === name)) return;
                patch({ params: [...settings.params, { name, index: true, list: false }] });
              }}
            />
          </div>
        </SettingsCard>
      </div>

      <SettingsCard
        muted
        title="Set at startup"
        help="These come from environment variables and JVM options on the pod. They are shown as the agents reported them and cannot be changed here."
      >
        <dl className={styles.startup}>
          {startup.map((value) => (
            <div key={value.label} className={styles.startupItem}>
              <dt>{value.label}</dt>
              <dd className={`pf-mono ${styles.small}`}>{value.value}</dd>
              <dd className={`pf-mono ${styles.source}`}>{value.source}</dd>
            </div>
          ))}
        </dl>
      </SettingsCard>
    </div>
  );
}
