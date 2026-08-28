import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import type { TreeWire } from '../msgpack/tree-wire';
import { HotspotsView } from './hotspots-view';
import { buildTreeModel } from './model';
import { applyCategories, parseCategoryConfig } from './transforms/categories';
import { computeFlatProfile } from './transforms/flat-profile';

// The Hotspots tab: category groups render as tree rows, and a method row
// expands in place into its incoming callers.

const METHODS = [
  'void com.acme.Main.run() (Main.java:1) [app.jar]',
  'void com.acme.db.Dao.select() (Dao.java:10) [app.jar]',
  'void com.acme.http.Client.get() (Client.java:5) [app.jar]',
];

function model() {
  const wire: TreeWire = {
    v: 1,
    methods: [...METHODS],
    params: [],
    root: {
      methodIdx: 0,
      durationMs: 850,
      selfDurationMs: 100,
      suspensionMs: 0,
      selfSuspensionMs: 0,
      executions: 4,
      selfExecutions: 1,
      children: [
        {
          methodIdx: 1,
          durationMs: 500,
          selfDurationMs: 500,
          suspensionMs: 0,
          selfSuspensionMs: 0,
          executions: 1,
          selfExecutions: 1,
        },
        {
          methodIdx: 2,
          durationMs: 250,
          selfDurationMs: 200,
          suspensionMs: 0,
          selfSuspensionMs: 0,
          executions: 2,
          selfExecutions: 1,
          children: [
            {
              methodIdx: 1,
              durationMs: 50,
              selfDurationMs: 50,
              suspensionMs: 0,
              selfSuspensionMs: 0,
              executions: 1,
              selfExecutions: 1,
            },
          ],
        },
      ],
    },
  };
  const m = buildTreeModel(wire);
  applyCategories(m, parseCategoryConfig('db.jdbc *Dao.select*'));
  return m;
}

describe('HotspotsView', () => {
  afterEach(cleanup);

  it('renders the dotted category groups and expands a hotspot into its callers', () => {
    const m = model();
    render(<HotspotsView model={m} profiles={computeFlatProfile(m)} />);

    // The dotted category shows as nested group rows.
    expect(screen.getByText('db')).toBeInTheDocument();
    expect(screen.getByText('jdbc')).toBeInTheDocument();

    // The hotspot row starts collapsed: Client.get appears once (its own
    // hotspot row in 'unsorted'), not yet as a caller of Dao.select.
    const hotspot = screen.getByTitle(METHODS[1]!);
    expect(screen.getAllByTitle(METHODS[2]!)).toHaveLength(1);

    // Expanding grafts the incoming callers in place.
    const toggle = hotspot.closest('div')!.querySelector('button')!;
    expect(toggle.textContent).toBe('+');
    fireEvent.click(toggle);
    expect(screen.getAllByTitle(METHODS[2]!)).toHaveLength(2);
    expect(screen.getAllByTitle(METHODS[0]!).length).toBeGreaterThan(1);
  });
});
