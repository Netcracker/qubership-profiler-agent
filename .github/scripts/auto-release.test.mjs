import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import autoRelease from './auto-release.cjs';

// The anniversary is 2026-10-28T12:00:00Z, and the monthly window opens 3.5 days before it
const RELEASED_AT = '2026-09-28T12:00:00Z';
// Two days before the anniversary on 2026-10-28, a run that does not release monthly
const MONDAY_BEFORE_MONTH = new Date('2026-10-19T06:00:00Z');

function dependabotAlert({ number = 1, fixedAt = '2026-10-10T00:00:00Z', scope = 'runtime', ghsa = 'GHSA-aaaa-bbbb-cccc' } = {}) {
  return {
    number,
    fixed_at: fixedAt,
    security_advisory: { ghsa_id: ghsa, severity: 'high' },
    dependency: { scope, manifest_path: 'settings.gradle.kts', package: { ecosystem: 'maven', name: 'com.example:lib' } },
  };
}

function codeScanningAlert({ number = 10, tool = 'CodeQL', fixedAt = '2026-10-18T03:20:35Z', rule = 'js/sql-injection' } = {}) {
  return {
    number,
    fixed_at: fixedAt,
    tool: { name: tool },
    rule: { id: rule, security_severity_level: 'high' },
    most_recent_instance: { category: 'dumps-collector' },
  };
}

// A fake of the Octokit calls the script makes, backed by the lists it is given
function fakeGitHub({
  commits = ['fix: something'],
  dependabotFixed = [], dependabotOpen = [],
  codeScanningFixed = [],
  runStatuses = ['completed'],
} = {}) {
  const dispatches = [];
  const github = {
    dispatches,
    async paginate(method, params, map) {
      const response = await method(params);
      return map ? map(response, () => {}) : response.data;
    },
    rest: {
      repos: {
        getLatestRelease: async () => ({ data: { tag_name: 'v4.0.7', published_at: RELEASED_AT } }),
        compareCommitsWithBasehead: async () => ({
          data: {
            ahead_by: commits.length,
            commits: commits.map(message => ({ commit: { message } })),
          },
        }),
      },
      dependabot: {
        listAlertsForRepo: async ({ state }) => ({ data: state === 'fixed' ? dependabotFixed : dependabotOpen }),
      },
      codeScanning: {
        listAlertsForRepo: async () => ({ data: codeScanningFixed }),
      },
      actions: {
        listWorkflowRuns: async () => ({ data: { workflow_runs: runStatuses.map(status => ({ status })) } }),
        createWorkflowDispatch: async params => { dispatches.push(params); },
      },
    },
  };
  return github;
}

const core = {
  info() {},
  summary: {
    addHeading() { return this; },
    addRaw() { return this; },
    addTable() { return this; },
    async write() {},
  },
};

function decide(github, { now = MONDAY_BEFORE_MONTH, dryRun = false } = {}) {
  return autoRelease({
    github, core, now, dryRun,
    context: { repo: { owner: 'Netcracker', repo: 'qubership-profiler-agent' } },
    branch: 'main',
    workflowId: 'release.yaml',
  });
}

const fixNumbers = decision => decision.fixes.map(f => f.number);

describe('autoRelease', () => {
  it('starts release.yaml on main when a Dependabot alert was fixed after the release', async () => {
    const github = fakeGitHub({ dependabotFixed: [dependabotAlert({ number: 338 })] });
    const decision = await decide(github);
    assert.deepStrictEqual(fixNumbers(decision), [338]);
    assert.deepStrictEqual(github.dispatches,
      [{ owner: 'Netcracker', repo: 'qubership-profiler-agent', workflow_id: 'release.yaml', ref: 'main' }]);
  });

  it('ignores a Dependabot alert fixed before the release', async () => {
    const decision = await decide(fakeGitHub({ dependabotFixed: [dependabotAlert({ fixedAt: '2026-09-27T00:00:00Z' })] }));
    assert.deepStrictEqual(fixNumbers(decision), []);
    assert.strictEqual(decision.release, false);
  });

  it('ignores a fixed Dependabot alert in a development dependency', async () => {
    const decision = await decide(fakeGitHub({ dependabotFixed: [dependabotAlert({ scope: 'development' })] }));
    assert.deepStrictEqual(fixNumbers(decision), []);
  });

  it('ignores a fixed Dependabot alert whose advisory is still open for the same package', async () => {
    const decision = await decide(fakeGitHub({
      dependabotFixed: [dependabotAlert({ number: 320 })],
      dependabotOpen: [dependabotAlert({ number: 350 })],
    }));
    assert.deepStrictEqual(fixNumbers(decision), []);
  });

  it('starts release.yaml when a code scanning alert was fixed after the release', async () => {
    const decision = await decide(fakeGitHub({ codeScanningFixed: [codeScanningAlert({ number: 12 })] }));
    assert.deepStrictEqual(fixNumbers(decision), [12]);
    assert.strictEqual(decision.release, true);
  });

  it('ignores a code scanning alert fixed before the release', async () => {
    const decision = await decide(fakeGitHub({
      codeScanningFixed: [codeScanningAlert({ fixedAt: '2026-09-27T00:00:00Z' })],
    }));
    assert.deepStrictEqual(fixNumbers(decision), []);
  });

  for (const tool of ['Grype', 'Trivy', 'Scorecard']) {
    it(`ignores a fixed ${tool} alert`, async () => {
      const decision = await decide(fakeGitHub({ codeScanningFixed: [codeScanningAlert({ number: 44, tool })] }));
      assert.deepStrictEqual(fixNumbers(decision), []);
    });
  }

  it('releases monthly when the run falls exactly 3.5 days before the anniversary', async () => {
    const decision = await decide(fakeGitHub(), { now: new Date('2026-10-25T00:00:00Z') });
    assert.strictEqual(decision.release, true);
  });

  it('skips the monthly release when the run falls more than 3.5 days before the anniversary', async () => {
    const decision = await decide(fakeGitHub(), { now: new Date('2026-10-24T23:59:59.999Z') });
    assert.strictEqual(decision.release, false);
  });

  it('skips when main has only version-bump commits since the release', async () => {
    const decision = await decide(fakeGitHub({
      commits: ['chore: bump version to 4.0.8'],
      dependabotFixed: [dependabotAlert()],
    }));
    assert.strictEqual(decision.release, false);
  });

  it('skips while a release.yaml run is in progress', async () => {
    const decision = await decide(fakeGitHub({ runStatuses: ['in_progress', 'completed'], dependabotFixed: [dependabotAlert()] }));
    assert.strictEqual(decision.release, false);
  });

  it('does not start release.yaml in a dry run', async () => {
    const github = fakeGitHub({ dependabotFixed: [dependabotAlert()] });
    const decision = await decide(github, { dryRun: true });
    assert.strictEqual(decision.release, true);
    assert.deepStrictEqual(github.dispatches, []);
  });
});
