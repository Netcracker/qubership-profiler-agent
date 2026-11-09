import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import autoRelease from './auto-release.cjs';

// The anniversary is 2026-10-28T12:00:00Z, and the monthly window opens 3.5 days before it
const RELEASED_AT = '2026-09-28T12:00:00Z';
// Two days before the anniversary on 2026-10-28, a run that does not release monthly
const MONDAY_BEFORE_MONTH = new Date('2026-10-19T06:00:00Z');

function dependabotAlert({
  number = 1, fixedAt = '2026-10-10T00:00:00Z', ghsa = 'GHSA-aaaa-bbbb-cccc',
  manifest = 'go.mod', scope = 'runtime', ecosystem = 'go', name = 'github.com/labstack/echo/v4',
} = {}) {
  return {
    number,
    fixed_at: fixedAt,
    security_advisory: { ghsa_id: ghsa, severity: 'high' },
    dependency: { scope, manifest_path: manifest, package: { ecosystem, name } },
  };
}

function codeScanningAlert({
  number = 10, tool = 'CodeQL', fixedAt = '2026-10-18T03:20:35Z', rule = 'js/sql-injection', severity = 'high',
} = {}) {
  return {
    number,
    fixed_at: fixedAt,
    tool: { name: tool },
    rule: { id: rule, security_severity_level: severity },
    most_recent_instance: { category: 'dumps-collector' },
  };
}

// The latest GitHub release was published at RELEASED_AT, 2026-09-28T12:00:00Z
function releaseRun(createdAt, conclusion = 'success', overrides = {}) {
  return {
    id: 7, run_attempt: 1, head_branch: 'main', head_sha: 'released-head', status: 'completed', conclusion,
    created_at: createdAt, run_started_at: createdAt, html_url: 'https://github.com/runs/7', ...overrides,
  };
}

// A fake of the Octokit calls the script makes, backed by the lists it is given
function fakeGitHub({
  commits = ['fix: something'],
  dependabotFixed = [], dependabotOpen = [],
  codeScanningFixed = [],
  releaseRuns = [],
  comparisons = {},
} = {}) {
  const dispatches = [];
  const github = {
    dispatches,
    async paginate(method, params) {
      // Pages through the method as Octokit does: until a page comes back shorter than per_page
      const data = [];
      for (let page = 1; ; page++) {
        const response = await method({ ...params, page });
        const items = response.data.workflow_runs || response.data;
        data.push(...items);
        if (items.length < params.per_page) break;
      }
      return data;
    },
    rest: {
      repos: {
        getLatestRelease: async () => ({ data: { tag_name: 'v4.0.7', published_at: RELEASED_AT } }),
        compareCommitsWithBasehead: async ({ basehead }) => {
          const messages = comparisons[basehead] || commits;
          return { data: { ahead_by: messages.length, commits: messages.map(message => ({ commit: { message } })) } };
        },
      },
      dependabot: {
        listAlertsForRepo: async ({ state }) => ({ data: state === 'fixed' ? dependabotFixed : dependabotOpen }),
      },
      codeScanning: {
        listAlertsForRepo: async () => ({ data: codeScanningFixed }),
      },
      actions: {
        listWorkflowRuns: async ({ branch, per_page, page = 1, status }) => ({ data: { workflow_runs: status === 'success'
          ? releaseRuns.filter(r => r.head_branch === branch && r.conclusion === status).slice(0, per_page)
          : releaseRuns.slice((page - 1) * per_page, page * per_page) } }),
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

// Returns the decision together with the messages the script passed to core.setFailed
async function decide(github, { now = MONDAY_BEFORE_MONTH, dryRun = false } = {}) {
  const failures = [];
  const decision = await autoRelease({
    github, now, dryRun,
    core: { ...core, setFailed: message => failures.push(message) },
    context: { repo: { owner: 'Netcracker', repo: 'qubership-profiler-agent' } },
    branch: 'main',
    workflowId: 'release.yaml',
  });
  return { ...decision, failures };
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

  for (const [name, alert] of [
    ['a development dependency',
      { manifest: 'apps/ui/package-lock.json', scope: 'development', ecosystem: 'npm', name: 'vite' }],
    // The Gradle submission sets no scope, and settings.gradle.kts also lists build tools such as Checkstyle
    ['the Gradle build',
      { manifest: 'settings.gradle.kts', scope: null, ecosystem: 'maven', name: 'com.google.guava:guava' }],
    ['an example application',
      { manifest: 'apps/examples/spring-boot-3-tomcat/pom.xml', ecosystem: 'maven', name: 'org.springframework:spring-web' }],
    ['an integration test', { manifest: 'apps/agent/it-e2e/package.json', ecosystem: 'npm', name: 'axios' }],
  ]) {
    it(`ignores a fixed Dependabot alert in ${name}`, async () => {
      const github = fakeGitHub({ dependabotFixed: [dependabotAlert({ number: 347, ...alert })] });
      const decision = await decide(github);
      assert.deepStrictEqual({ fixes: decision.fixes, dispatches: github.dispatches }, { fixes: [], dispatches: [] });
    });
  }

  it('ignores a fixed Dependabot alert whose advisory is open for the same package in a moved manifest', async () => {
    const decision = await decide(fakeGitHub({
      dependabotFixed: [dependabotAlert({ number: 320, manifest: 'backend/go.mod' })],
      dependabotOpen: [dependabotAlert({ number: 350, manifest: 'go.mod' })],
    }));
    assert.deepStrictEqual(fixNumbers(decision), []);
  });

  const npmAlert = { manifest: 'apps/agent/profiler-ui/yarn.lock', ecosystem: 'npm', name: 'braces' };
  for (const [name, fixed, open] of [
    ['the same advisory is open for another package', {}, { name: 'golang.org/x/net' }],
    ['another advisory is open for the same package', {}, { ghsa: 'GHSA-dddd-eeee-ffff' }],
    ['the same advisory is open in an integration test', npmAlert,
      { ...npmAlert, manifest: 'apps/agent/it-e2e/yarn.lock' }],
    ['the same advisory is open in a development dependency', npmAlert,
      { ...npmAlert, manifest: 'apps/ui/package-lock.json', scope: 'development' }],
  ]) {
    it(`starts release.yaml for a fixed Dependabot alert while ${name}`, async () => {
      const github = fakeGitHub({
        dependabotFixed: [dependabotAlert({ number: 348, ...fixed })],
        dependabotOpen: [dependabotAlert({ number: 350, ...open })],
      });
      const decision = await decide(github);
      assert.deepStrictEqual({ fixes: fixNumbers(decision), dispatches: github.dispatches.length },
        { fixes: [348], dispatches: 1 });
    });
  }

  it('starts release.yaml when a code scanning alert was fixed after the release', async () => {
    const decision = await decide(fakeGitHub({ codeScanningFixed: [codeScanningAlert({ number: 12 })] }));
    assert.deepStrictEqual(fixNumbers(decision), [12]);
    assert.strictEqual(decision.release, true);
  });

  it('ignores a fixed code scanning alert whose rule has no security severity', async () => {
    const github = fakeGitHub({
      codeScanningFixed: [codeScanningAlert({ number: 13, rule: 'js/unused-local-variable', severity: null })],
    });
    const decision = await decide(github);
    assert.deepStrictEqual({ fixes: decision.fixes, dispatches: github.dispatches }, { fixes: [], dispatches: [] });
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

  it('counts a Dependabot fix detected after the release run was created, even though the release was published later', async () => {
    const decision = await decide(fakeGitHub({
      releaseRuns: [releaseRun('2026-09-28T11:00:00Z')],
      dependabotFixed: [
        dependabotAlert({ number: 1, fixedAt: '2026-09-28T10:59:59Z' }),
        dependabotAlert({ number: 2, fixedAt: '2026-09-28T11:00:01Z' }),
      ],
    }));
    assert.deepStrictEqual(fixNumbers(decision), [2]);
  });

  it('counts a fix merged between a failed release attempt and its successful re-run', async () => {
    const decision = await decide(fakeGitHub({
      releaseRuns: [releaseRun('2026-09-28T11:00:00Z', 'success', { run_started_at: '2026-10-01T09:00:00Z' })],
      dependabotFixed: [dependabotAlert({ number: 3, fixedAt: '2026-09-30T00:00:00Z' })],
    }));
    assert.deepStrictEqual(fixNumbers(decision), [3]);
  });

  // A failed run may have published to Maven Central, tagged the release, or pushed the version bump
  for (const [shape, conclusion, commits] of [
    ['failed before the version bump', 'failure', ['fix: something']],
    ['failed after the version bump', 'failure', ['chore: bump version to 4.0.9', 'fix: something']],
    ['was cancelled after the version bump', 'cancelled', ['chore: bump version to 4.0.9', 'fix: something']],
  ]) {
    const releaseRuns = [
      releaseRun('2026-10-15T11:00:00Z', conclusion, { id: 8, html_url: 'https://github.com/runs/8' }),
      releaseRun('2026-09-28T11:00:00Z'),
    ];
    for (const [path, options, now] of [
      ['a Dependabot fix', { dependabotFixed: [dependabotAlert({ number: 348 })] }, MONDAY_BEFORE_MONTH],
      ['the monthly release', {}, new Date('2026-10-25T00:00:00Z')],
    ]) {
      it(`fails instead of starting release.yaml for ${path} when the newest release run ${shape}`, async () => {
        const github = fakeGitHub({ ...options, commits, releaseRuns });
        const decision = await decide(github, { now });
        assert.deepStrictEqual({ release: decision.release, reason: decision.reason, dispatches: github.dispatches },
          { release: false, reason: `release.yaml run 8 on main concluded ${conclusion}`, dispatches: [] });
        assert.match(String(decision.failures),
          new RegExp(`^Automatic releases are paused: release\\.yaml run 8 on main concluded ${conclusion} \\(https://github\\.com/runs/8\\)`));
      });
    }
  }

  for (const [name, failedRun] of [
    ['precedes the newest successful one', releaseRun('2026-09-20T11:00:00Z', 'failure', { id: 6 })],
    ['ran on another branch', releaseRun('2026-10-15T11:00:00Z', 'failure', { id: 8, head_branch: 'release/4.0' })],
  ]) {
    it(`starts release.yaml when a failed release run ${name}`, async () => {
      const github = fakeGitHub({
        releaseRuns: [failedRun, releaseRun('2026-09-28T11:00:00Z')].sort((a, b) => b.created_at.localeCompare(a.created_at)),
        dependabotFixed: [dependabotAlert({ number: 348 })],
      });
      const decision = await decide(github);
      assert.deepStrictEqual({ dispatches: github.dispatches.length, failures: decision.failures }, { dispatches: 1, failures: [] });
    });
  }

  it('fails when 20 newer release runs on another branch follow the failed run on main', async () => {
    const otherBranch = Array.from({ length: 20 }, (_, i) =>
      releaseRun(`2026-10-16T${String(i).padStart(2, '0')}:00:00Z`, 'success', { id: 100 - i, head_branch: 'release/4.0' }));
    const github = fakeGitHub({
      releaseRuns: [...otherBranch, releaseRun('2026-10-15T11:00:00Z', 'failure', { id: 8 }), releaseRun('2026-09-28T11:00:00Z')],
      dependabotFixed: [dependabotAlert({ number: 348 })],
    });
    const decision = await decide(github);
    assert.deepStrictEqual({ reason: decision.reason, dispatches: github.dispatches },
      { reason: 'release.yaml run 8 on main concluded failure', dispatches: [] });
  });

  it('waits for the re-run of an old release run on main that 20 newer runs on another branch follow', async () => {
    const otherBranch = Array.from({ length: 20 }, (_, i) =>
      releaseRun(`2026-10-16T${String(i).padStart(2, '0')}:00:00Z`, 'success', { id: 100 - i, head_branch: 'release/4.0' }));
    const rerun = releaseRun('2026-10-15T11:00:00Z', null, { id: 8, run_attempt: 2, status: 'in_progress' });
    const github = fakeGitHub({
      releaseRuns: [...otherBranch, rerun, releaseRun('2026-09-28T11:00:00Z')],
      dependabotFixed: [dependabotAlert({ number: 348 })],
    });
    const decision = await decide(github);
    assert.deepStrictEqual({ reason: decision.reason, dispatches: github.dispatches, failures: decision.failures },
      { reason: 'a release.yaml run is in progress', dispatches: [], failures: [] });
  });

  it('waits for a running release.yaml run without failing when the run before it failed', async () => {
    const decision = await decide(fakeGitHub({
      releaseRuns: [
        releaseRun('2026-10-18T11:00:00Z', null, { id: 9, status: 'in_progress' }),
        releaseRun('2026-10-15T11:00:00Z', 'failure', { id: 8 }),
      ],
      dependabotFixed: [dependabotAlert()],
    }));
    assert.deepStrictEqual({ reason: decision.reason, failures: decision.failures },
      { reason: 'a release.yaml run is in progress', failures: [] });
  });

  // The decision compares commits after the first read of the release runs and before the second one
  const released = releaseRun('2026-09-28T11:00:00Z');
  const olderFailed = releaseRun('2026-09-20T11:00:00Z', 'failure', { id: 6 });
  for (const [change, runsBefore, runsAfter] of [
    ['a release.yaml run starts', [released],
      [releaseRun('2026-10-19T06:00:30Z', null, { id: 9, status: 'in_progress' }), released]],
    ['the release.yaml run is re-run and fails', [released], [{ ...released, run_attempt: 2, conclusion: 'failure' }]],
    ['an older release.yaml run is re-run and succeeds', [released, olderFailed],
      [released, { ...olderFailed, run_attempt: 2, conclusion: 'success' }]],
  ]) {
    for (const [path, options, now] of [
      ['a Dependabot fix', { dependabotFixed: [dependabotAlert({ number: 348 })] }, MONDAY_BEFORE_MONTH],
      ['the monthly release', {}, new Date('2026-10-25T00:00:00Z')],
    ]) {
      it(`skips ${path} when ${change} during the decision`, async () => {
        const releaseRuns = [...runsBefore];
        const github = fakeGitHub({ ...options, releaseRuns });
        const compare = github.rest.repos.compareCommitsWithBasehead;
        github.rest.repos.compareCommitsWithBasehead = async params => {
          releaseRuns.splice(0, releaseRuns.length, ...runsAfter);
          return compare(params);
        };
        const decision = await decide(github, { now });
        assert.deepStrictEqual({ release: decision.release, reason: decision.reason, dispatches: github.dispatches },
          { release: false, reason: 'release.yaml runs changed while deciding', dispatches: [] });
      });
    }
  }

  it('counts commits since the commit of the successful release run, not since the latest tag', async () => {
    const decision = await decide(fakeGitHub({
      releaseRuns: [releaseRun('2026-08-28T11:00:00Z')],
      comparisons: {
        'v4.0.7...main': ['chore: bump version to 4.0.8'],
        'released-head...main': ['fix: something', 'chore: bump version to 4.0.8'],
      },
      dependabotFixed: [dependabotAlert()],
    }));
    assert.strictEqual(decision.reason, 'security fixes since the latest release');
  });

  it('releases monthly a month after the successful release run, although a later GitHub release exists', async () => {
    const decision = await decide(fakeGitHub({ releaseRuns: [releaseRun('2026-09-18T12:00:00Z')] }),
      { now: new Date('2026-10-19T06:00:00Z') });
    assert.strictEqual(decision.reason, 'a month has passed since the latest release');
  });

  for (const status of ['requested', 'waiting', 'pending', 'queued', 'in_progress', 'action_required']) {
    it(`skips while a release.yaml run is ${status}`, async () => {
      const github = fakeGitHub({
        releaseRuns: [releaseRun('2026-10-18T11:00:00Z', null, { id: 8, status }), releaseRun('2026-09-28T11:00:00Z')],
        dependabotFixed: [dependabotAlert()],
      });
      const decision = await decide(github);
      assert.deepStrictEqual({ reason: decision.reason, dispatches: github.dispatches },
        { reason: 'a release.yaml run is in progress', dispatches: [] });
    });
  }

  it('does not start release.yaml in a dry run', async () => {
    const github = fakeGitHub({ dependabotFixed: [dependabotAlert()] });
    const decision = await decide(github, { dryRun: true });
    assert.strictEqual(decision.release, true);
    assert.deepStrictEqual(github.dispatches, []);
  });
});
