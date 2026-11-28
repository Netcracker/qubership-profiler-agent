import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TextEncoder } from 'node:util';
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
  imageAnalyses = [], sarif = {},
  publishers = [],
  // The components of .qubership/docker-build-config.cfg at each commit; any other commit is not found
  buildConfigs = { 'current-head': ['qubership-profiler-backend', 'qubership-profiler-transfer'] },
  latestRelease = { tag_name: 'v4.0.7', published_at: RELEASED_AT },
  scanRuns = [{
    event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
    run_started_at: '2026-10-19T04:50:00Z', updated_at: '2026-10-19T05:30:00Z',
  }],
} = {}) {
  const dispatches = [];
  const github = {
    dispatches,
    async paginate(method, params, map) {
      if (typeof method === 'string') {
        assert.strictEqual(method, 'GET /repos/{owner}/{repo}/code-scanning/analyses');
        const data = imageAnalyses.filter(a => a.tool.name === params.tool_name);
        return map ? map({ data }, () => {}) : data;
      }
      // Pages through the method as Octokit does: until a page comes back shorter than per_page
      const data = [];
      for (let page = 1; ; page++) {
        const response = await method({ ...params, page });
        const items = response.data.workflow_runs || response.data;
        data.push(...items);
        if (items.length < params.per_page) break;
      }
      return map ? map({ data }, () => {}) : data;
    },
    async request(route, params) {
      assert.strictEqual(route, 'GET /repos/{owner}/{repo}/code-scanning/analyses/{analysis_id}');
      assert.strictEqual(params.headers.accept, 'application/sarif+json');
      assert.ok(Object.hasOwn(sarif, params.analysis_id));
      return { data: sarif[params.analysis_id] };
    },
    rest: {
      repos: {
        getLatestRelease: async () => ({ data: latestRelease }),
        getBranch: async () => ({ data: { commit: { sha: 'current-head' } } }),
        getContent: async ({ path, ref }) => {
          assert.strictEqual(path, '.qubership/docker-build-config.cfg');
          assert.ok(Object.hasOwn(buildConfigs, ref), `no build config at ${ref}`);
          const config = JSON.stringify({ components: buildConfigs[ref].map(name => ({ name })) });
          return { data: { encoding: 'base64', content: Buffer.from(config).toString('base64') } };
        },
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
        listWorkflowRuns: async ({ workflow_id, branch, per_page, page = 1, status }) => ({ data: { workflow_runs: status === 'success'
          ? releaseRuns.filter(r => r.head_branch === branch && r.conclusion === status).slice(0, per_page)
          : workflow_id === 'release.yaml' && !branch
          ? releaseRuns.slice((page - 1) * per_page, page * per_page)
          : workflow_id === 'security-scan.yml' ? scanRuns : publishers.filter(r => r.path === workflow_id) } }),
        createWorkflowDispatch: async params => { dispatches.push(params); },
      },
    },
  };
  return github;
}

// A job summary that records each call as [method, argument]
function recordingSummary() {
  const calls = [];
  const record = method => function (arg) { calls.push([method, arg]); return this; };
  return {
    calls,
    addHeading: record('addHeading'),
    addRaw: record('addRaw'),
    addList: record('addList'),
    addTable: record('addTable'),
    async write() {},
  };
}

// Returns the decision together with the messages the script passed to core.info, core.setFailed, and core.notice, and
// the calls it made on core.summary
async function decide(github, { now = MONDAY_BEFORE_MONTH, dryRun = false } = {}) {
  const logs = [];
  const failures = [];
  const notices = [];
  const summary = recordingSummary();
  const decision = await autoRelease({
    github, now, dryRun,
    core: {
      info: message => logs.push(message), summary, setFailed: message => failures.push(message), notice: message => notices.push(message),
    },
    context: { repo: { owner: 'Netcracker', repo: 'qubership-profiler-agent' } },
    branch: 'main',
    workflowId: 'release.yaml',
  });
  return { ...decision, logs, failures, notices, summary: summary.calls };
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

function imageScans({
  tool = 'Grype', image = 'qubership-profiler-backend', ids = [0, 1], latestFindings = [{}], mainFindings = [],
} = {}) {
  const imageAnalyses = ['latest', 'main'].map((tag, index) => ({
    id: ids[index],
    category: `${image}:${tag}@linux/amd64`,
    ref: 'refs/heads/main',
    commit_sha: 'current-head',
    created_at: '2026-10-19T05:00:00Z',
    error: '', warning: '',
    tool: { name: tool, version: '1.0.0' },
    analysis_key: `.github/workflows/security-scan.yml:${tool.toLowerCase()}-scan`,
    environment: '{}',
    results_count: (tag === 'latest' ? latestFindings : mainFindings).length,
  }));
  const sarif = Object.fromEntries(imageAnalyses.map((a, index) => {
    const input = index === 0 ? latestFindings : mainFindings;
    const results = input.map(({ pkg = 'example-package', version = '1.0', path = '/app', rule = 'CVE-example', number = 20 } = {}) => ({
      ruleId: tool === 'Grype' ? `${rule}-${pkg}` : rule,
      message: { text: tool === 'Grype'
        ? `A high vulnerability in go-module package: ${pkg}, version ${version} was found in image backend at: ${path}`
        : `Package: ${pkg}\nInstalled Version: ${version}\nVulnerability ${rule}` },
      properties: { 'github/alertNumber': number },
      locations: [{ physicalLocation: { artifactLocation: { uri: path } } }],
    }));
    return [a.id, { runs: [{
      automationDetails: { id: `${a.category}/` },
      tool: { driver: { ...a.tool, rules: input.map((f, i) => ({
        id: results[i].ruleId, properties: { 'security-severity': f.score || '8.0' },
      })) } },
      results,
    }] }];
  }));
  return { imageAnalyses, sarif };
}

const BACKEND_ONLY = { 'current-head': ['qubership-profiler-backend'] };

const publisher = (path, overrides = {}) => ({
  path, event: 'push', status: 'completed', conclusion: 'success', updated_at: '2026-10-18T12:00:00Z', ...overrides,
});

describe('autoRelease image findings', () => {
  for (const tool of ['Grype', 'Trivy']) {
    it(`releases for a fresh ${tool} finding present only in latest, even when no alert is globally fixed`, async () => {
      const github = fakeGitHub(imageScans({ tool }));
      const decision = await decide(github);
      assert.deepStrictEqual(fixNumbers(decision), [20]);
      assert.strictEqual(github.dispatches.length, 1);
    });

    it(`keeps a ${tool} advisory open when main has another vulnerable version at another path`, async () => {
      const decision = await decide(fakeGitHub(imageScans({ tool, mainFindings: [{ version: '2.0', path: '/moved' }] })));
      assert.deepStrictEqual({ release: decision.release, fixes: decision.fixes }, { release: false, fixes: [] });
    });

    it(`counts a ${tool} advisory once when latest contains the vulnerable package at multiple paths`, async () => {
      const decision = await decide(fakeGitHub(imageScans({ tool, latestFindings: [{}, { path: '/other' }] })));
      assert.deepStrictEqual(fixNumbers(decision), [20]);
    });

    it(`ignores ${tool} findings introduced only in main`, async () => {
      const decision = await decide(fakeGitHub(imageScans({ tool, latestFindings: [], mainFindings: [{}] })));
      assert.strictEqual(decision.release, false);
    });
  }

  it('compares Trivy packages separately when they share the same CVE', async () => {
    const decision = await decide(fakeGitHub(imageScans({ tool: 'Trivy', mainFindings: [{ pkg: 'another-package' }] })));
    assert.deepStrictEqual(fixNumbers(decision), [20]);
  });

  for (const [name, change] of [
    ['missing main analysis', s => s.imageAnalyses.pop()],
    ['legacy categories', s => { s.imageAnalyses.forEach(a => { a.category = 'qubership-profiler-backend'; }); }],
    ['another platform', s => { s.imageAnalyses[1].category = 'qubership-profiler-backend:main@linux/arm64'; }],
    ['another image', s => { s.imageAnalyses[1].category = 'qubership-profiler-transfer:main@linux/amd64'; }],
    ['another ref', s => { s.imageAnalyses[1].ref = 'refs/heads/other'; }],
    ['an older commit', s => { s.imageAnalyses[1].commit_sha = 'old-head'; }],
    ['a scanner error', s => { s.imageAnalyses[1].error = 'scan failed'; }],
    ['a scanner warning', s => { s.imageAnalyses[1].warning = 'incomplete analysis'; }],
    ['another workflow', s => { s.imageAnalyses[1].analysis_key = '.github/workflows/other.yml:scan'; }],
    ['different scanner versions', s => { s.imageAnalyses[1].tool.version = '2.0'; }],
    ['different configurations', s => { s.imageAnalyses[1].environment = '{"scope":"critical"}'; }],
    ['a future scan', s => { s.imageAnalyses[1].created_at = '2026-10-19T06:00:00.001Z'; }],
    ['an invalid timestamp', s => { s.imageAnalyses[1].created_at = 'invalid'; }],
    ['a scan older than 48 hours', s => { s.imageAnalyses[1].created_at = '2026-10-17T05:59:59.999Z'; }],
    ['scans more than 24 hours apart', s => { s.imageAnalyses[1].created_at = '2026-10-18T04:59:59.999Z'; }],
    ['a scan before the release', s => {
      s.latestRelease = { tag_name: 'v4.0.8', published_at: s.imageAnalyses[1].created_at };
    }],
    ['missing SARIF results', s => { delete s.sarif[1].runs[0].results; }],
    ['a truncated SARIF report', s => { s.imageAnalyses[1].results_count = 1; }],
    ['an unrecognized package', s => { s.sarif[0].runs[0].results[0].message.text = 'unknown format'; }],
    ['a mismatched SARIF category', s => { s.sarif[1].runs[0].automationDetails.id = 'another-category/'; }],
    ['a failed SARIF invocation', s => { s.sarif[1].runs[0].invocations = [{ executionSuccessful: false }]; }],
    ['a finding below HIGH severity', s => { s.sarif[0].runs[0].tool.driver.rules[0].properties['security-severity'] = '6.9'; }],
  ]) {
    it(`skips image fixes with ${name}`, async () => {
      const scans = imageScans();
      change(scans);
      const github = fakeGitHub(scans);
      const decision = await decide(github);
      assert.deepStrictEqual({ release: decision.release, fixes: decision.fixes, dispatches: github.dispatches },
        { release: false, fixes: [], dispatches: [] });
    });
  }

  it('accepts scans exactly 48 hours old and exactly 24 hours apart', async () => {
    const scans = imageScans();
    scans.imageAnalyses[0].created_at = '2026-10-17T06:00:00Z';
    scans.imageAnalyses[1].created_at = '2026-10-18T06:00:00Z';
    const decision = await decide(fakeGitHub({ ...scans, scanRuns: [{
      event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
      run_started_at: '2026-10-17T06:00:00Z', updated_at: '2026-10-19T05:30:00Z',
    }] }));
    assert.strictEqual(decision.release, true);
  });

  it('uses the newest analysis even when it failed and an older successful analysis exists', async () => {
    const scans = imageScans();
    scans.imageAnalyses.unshift({ ...scans.imageAnalyses[1], id: 3, created_at: '2026-10-19T05:30:00Z', error: 'failed' });
    const decision = await decide(fakeGitHub(scans));
    assert.strictEqual(decision.release, false);
  });

  for (const workflow of ['release.yaml', 'backend-dev-docker-build.yml']) {
    for (const conclusion of ['success', 'failure']) {
      it(`rejects scans predating a completed ${workflow} run with conclusion ${conclusion}`, async () => {
        const github = fakeGitHub({ ...imageScans(), publishers: [publisher(workflow, {
          conclusion, updated_at: '2026-10-19T05:00:01Z',
        })] });
        const decision = await decide(github);
        assert.strictEqual(decision.release, false);
      });
    }
    it(`rejects image fixes while ${workflow} is running`, async () => {
      const decision = await decide(fakeGitHub({ ...imageScans(), publishers: [publisher(workflow, { status: 'in_progress' })] }));
      assert.strictEqual(decision.release, false);
    });
  }

  it('ignores pull request builds that do not publish images', async () => {
    const decision = await decide(fakeGitHub({ ...imageScans(), publishers: [publisher('backend-dev-docker-build.yml', {
      event: 'pull_request', status: 'in_progress',
    })] }));
    assert.strictEqual(decision.release, true);
  });

  it('rejects image fixes if the branch moves during comparison', async () => {
    const github = fakeGitHub(imageScans());
    let reads = 0;
    github.rest.repos.getBranch = async () => ({ data: { commit: { sha: reads++ ? 'new-head' : 'current-head' } } });
    const decision = await decide(github);
    assert.strictEqual(decision.release, false);
  });

  it('rejects image fixes if a release appears during comparison', async () => {
    const github = fakeGitHub(imageScans());
    let reads = 0;
    github.rest.repos.getLatestRelease = async () => ({ data: {
      tag_name: reads++ ? 'v4.0.8' : 'v4.0.7', published_at: RELEASED_AT,
    } });
    const decision = await decide(github);
    assert.strictEqual(decision.release, false);
  });

  it('still permits a monthly release when image scans are stale', async () => {
    const scans = imageScans();
    scans.imageAnalyses[1].error = 'failed';
    const decision = await decide(fakeGitHub(scans), { now: new Date('2026-10-25T00:00:00Z') });
    assert.deepStrictEqual({ release: decision.release, fixes: decision.fixes }, { release: true, fixes: [] });
  });

  it('reports fresh image fixes without dispatching in a dry run', async () => {
    const github = fakeGitHub(imageScans());
    const decision = await decide(github, { dryRun: true });
    assert.deepStrictEqual({ release: decision.release, dispatches: github.dispatches }, { release: true, dispatches: [] });
  });
});

describe('autoRelease scan provenance', () => {
  for (const tool of ['Grype', 'Trivy']) {
    for (const tag of ['latest', 'main']) {
      it(`rejects a manual ${tool} ${tag} scan whose only-fixed filter is unknown`, async () => {
        // An unfixed vulnerability can disappear from main's report solely because only-fixed filters it out.
        const scans = imageScans({ tool });
        const scanRuns = scans.imageAnalyses.map((a, index) => {
          a.created_at = `2026-10-19T0${index + 4}:00:00Z`;
          return {
            event: a.category.includes(`:${tag}@`) ? 'workflow_dispatch' : 'schedule',
            head_sha: 'current-head', status: 'completed', conclusion: 'success',
            run_started_at: a.created_at, updated_at: a.created_at,
          };
        });
        const github = fakeGitHub({ ...scans, scanRuns });
        const decision = await decide(github);
        assert.deepStrictEqual({ release: decision.release, fixes: decision.fixes, dispatches: github.dispatches },
          { release: false, fixes: [], dispatches: [] });
      });
    }
  }

  for (const event of [undefined, 'push', 'workflow_dispatch']) {
    it(`rejects ambiguous scan provenance containing an overlapping ${event} run`, async () => {
      const scans = imageScans();
      const scheduled = {
        event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
        run_started_at: '2026-10-19T04:50:00Z', updated_at: '2026-10-19T05:30:00Z',
      };
      const github = fakeGitHub({ ...scans, scanRuns: [scheduled, { ...scheduled, event }] });
      const decision = await decide(github);
      assert.deepStrictEqual({ release: decision.release, fixes: decision.fixes, dispatches: github.dispatches },
        { release: false, fixes: [], dispatches: [] });
    });
  }

  it('rejects an expired scan workflow even when both analyses match its commit and time interval', async () => {
    const scans = imageScans();
    scans.imageAnalyses.forEach(a => { a.created_at = '2026-10-17T06:10:00Z'; });
    const github = fakeGitHub({ ...scans, scanRuns: [{
      event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
      run_started_at: '2026-10-17T05:45:00Z', updated_at: '2026-10-17T06:20:00Z',
    }] });
    const decision = await decide(github);
    assert.strictEqual(decision.release, false);
  });

  it('rejects fresh completed scans more than 24 hours apart', async () => {
    const scans = imageScans();
    scans.imageAnalyses[1].created_at = '2026-10-18T04:59:59Z';
    const github = fakeGitHub({ ...scans, scanRuns: scans.imageAnalyses.map(a => ({
      event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
      run_started_at: a.created_at, updated_at: a.created_at,
    })) });
    const decision = await decide(github);
    assert.strictEqual(decision.release, false);
  });

  it('reads the ArrayBuffer SARIF response returned by Octokit for application/json+sarif', async () => {
    const scans = imageScans();
    for (const id of [0, 1]) scans.sarif[id] = new TextEncoder().encode(JSON.stringify(scans.sarif[id])).buffer;
    const decision = await decide(fakeGitHub(scans));
    assert.deepStrictEqual(fixNumbers(decision), [20]);
  });

  it('rejects a stale latest scan even when the main scan is fresh', async () => {
    const scans = imageScans();
    scans.imageAnalyses[0].created_at = '2026-10-17T05:59:59Z';
    const decision = await decide(fakeGitHub(scans));
    assert.strictEqual(decision.release, false);
  });

  it('rejects image fixes if an image publisher starts during comparison', async () => {
    const github = fakeGitHub(imageScans());
    const list = github.rest.actions.listWorkflowRuns;
    let reads = 0;
    github.rest.actions.listWorkflowRuns = async params => {
      if (params.workflow_id === 'backend-dev-docker-build.yml' && ++reads > 1) {
        return { data: { workflow_runs: [publisher(params.workflow_id, { status: 'in_progress' })] } };
      }
      return list(params);
    };
    const decision = await decide(github);
    assert.deepStrictEqual({ release: decision.release, dispatches: github.dispatches }, { release: false, dispatches: [] });
  });

  it('rejects a scan that read the old image before publication but uploaded its analysis afterward', async () => {
    const github = fakeGitHub({ ...imageScans(), publishers: [publisher('release.yaml', {
      updated_at: '2026-10-19T04:55:00Z',
    })] });
    const decision = await decide(github);
    assert.strictEqual(decision.release, false);
  });

  it('compares fresh analyses from a scan run that failed in another job', async () => {
    const github = fakeGitHub({ ...imageScans(), scanRuns: [{
      event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'failure',
      run_started_at: '2026-10-19T04:50:00Z', updated_at: '2026-10-19T05:30:00Z',
    }] });
    const decision = await decide(github);
    assert.deepStrictEqual(fixNumbers(decision), [20]);
  });

  for (const [name, scanRuns] of [
    ['missing scan run', []],
    ['unfinished scan run', [{ status: 'in_progress', conclusion: null }]],
    ['scan run for another commit', [{ head_sha: 'other-head' }]],
    ['scan run outside the analysis interval', [{ updated_at: '2026-10-19T04:59:59Z' }]],
  ]) {
    it(`rejects image fixes with a ${name}`, async () => {
      const github = fakeGitHub({ ...imageScans(), scanRuns: scanRuns.map(r => ({
        event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
        run_started_at: '2026-10-19T04:50:00Z', updated_at: '2026-10-19T05:30:00Z', ...r,
      })) });
      const decision = await decide(github);
      assert.deepStrictEqual({ release: decision.release, dispatches: github.dispatches }, { release: false, dispatches: [] });
    });
  }
});

// Two scans merged into the options of one fakeGitHub
const mergeScans = (...scans) => ({
  imageAnalyses: scans.flatMap(s => s.imageAnalyses),
  sarif: Object.assign({}, ...scans.map(s => s.sarif)),
});

describe('autoRelease report', () => {
  it('compares only the images the build config builds, and lists the others as not built', async () => {
    const scans = mergeScans(
      imageScans({ latestFindings: [{ number: 20 }] }),
      imageScans({ image: 'qubership-profiler-maintenance', ids: [2, 3], latestFindings: [{ number: 21 }] }),
    );
    const decision = await decide(fakeGitHub({ ...scans, buildConfigs: BACKEND_ONLY }));
    // Two pairs: Grype on the backend, and Trivy on the backend, which has no analysis
    assert.deepStrictEqual(
      { fixes: fixNumbers(decision), total: decision.images.total, notBuilt: decision.images.notBuilt },
      { fixes: [20], total: 2, notBuilt: ['qubership-profiler-maintenance'] });
  });

  it('groups pairs skipped for one reason, naming the publisher run and its last update', async () => {
    const scans = mergeScans(imageScans(), imageScans({ tool: 'Trivy', ids: [2, 3] }));
    const decision = await decide(fakeGitHub({ ...scans, publishers: [publisher('backend-dev-docker-build.yml', {
      id: 5, updated_at: '2026-10-19T05:00:01Z',
    })], buildConfigs: BACKEND_ONLY }));
    assert.deepStrictEqual(decision.images.skipped, [{
      reason: 'the main analysis predates the last update of backend-dev-docker-build.yml run 5 at 2026-10-19T05:00:01Z',
      pairs: [
        'Grype qubership-profiler-backend@linux/amd64 (main analysis at 2026-10-19T05:00:00Z)',
        'Trivy qubership-profiler-backend@linux/amd64 (main analysis at 2026-10-19T05:00:00Z)',
      ],
    }]);
  });

  it('groups pairs that fail one condition with different values, and puts the values in the pair details', async () => {
    const scans = mergeScans(imageScans(), imageScans({ tool: 'Trivy', ids: [2, 3] }));
    scans.imageAnalyses[1].tool.version = '2.0';
    scans.imageAnalyses[3].tool.version = '3.0';
    const decision = await decide(fakeGitHub({ ...scans, buildConfigs: BACKEND_ONLY }));
    assert.deepStrictEqual(decision.images.skipped, [{
      reason: 'the main and latest analyses have different scanner versions',
      pairs: [
        'Grype qubership-profiler-backend@linux/amd64 (main 2.0, latest 1.0.0)',
        'Trivy qubership-profiler-backend@linux/amd64 (main 3.0, latest 1.0.0)',
      ],
    }]);
  });

  for (const [order, publishers, newest] of [
    ['the backend build run is newer', [
      publisher('release.yaml', { id: 4, updated_at: '2026-10-19T05:00:01Z' }),
      publisher('backend-dev-docker-build.yml', { id: 5, updated_at: '2026-10-19T05:10:00Z' }),
    ], 'backend-dev-docker-build.yml run 5'],
    ['the release run is newer', [
      publisher('release.yaml', { id: 4, updated_at: '2026-10-19T05:10:00Z' }),
      publisher('backend-dev-docker-build.yml', { id: 5, updated_at: '2026-10-19T05:00:01Z' }),
    ], 'release.yaml run 4'],
  ]) {
    it(`names the newer publisher run when ${order}`, async () => {
      const scans = mergeScans(imageScans(), imageScans({ tool: 'Trivy', ids: [2, 3] }));
      const decision = await decide(fakeGitHub({ ...scans, publishers, buildConfigs: BACKEND_ONLY }));
      assert.deepStrictEqual(decision.images.skipped.map(s => s.reason), [
        `the main analysis predates the last update of ${newest} at 2026-10-19T05:10:00Z`,
      ]);
    });
  }

  it('counts a built image that no scanner analyzed as pairs not compared', async () => {
    const decision = await decide(fakeGitHub(imageScans({ latestFindings: [] })));
    assert.deepStrictEqual({ total: decision.images.total, compared: decision.images.compared,
      skipped: decision.images.skipped }, { total: 4, compared: 1, skipped: [{
      reason: 'the scanner has no analysis of the image on main from the last 48 hours',
      pairs: ['Trivy qubership-profiler-backend', 'Grype qubership-profiler-transfer', 'Trivy qubership-profiler-transfer'],
    }] });
    assert.strictEqual(decision.reason,
      'no security fixes found, 3 of 4 image pairs not compared; the next monthly release is due around 2026-10-28');
  });

  it('counts a scanner whose analyses of an image all expired as one pair, whatever number of platforms', async () => {
    const amd64 = imageScans();
    const arm64 = imageScans({ ids: [2, 3] });
    arm64.imageAnalyses.forEach(a => { a.category = a.category.replace('linux/amd64', 'linux/arm64'); });
    const scans = mergeScans(amd64, arm64);
    scans.imageAnalyses.forEach(a => { a.created_at = '2026-10-16T05:00:00Z'; });
    const decision = await decide(fakeGitHub({ ...scans, buildConfigs: BACKEND_ONLY }));
    assert.deepStrictEqual({ total: decision.images.total, skipped: decision.images.skipped }, { total: 2, skipped: [{
      reason: 'the scanner has no analysis of the image on main from the last 48 hours',
      pairs: ['Grype qubership-profiler-backend', 'Trivy qubership-profiler-backend'],
    }] });
  });

  it('reads the image list at the main commit the scans are checked against', async () => {
    const scans = mergeScans(imageScans(), imageScans({ image: 'qubership-profiler-transfer', ids: [2, 3] }));
    const decision = await decide(fakeGitHub({ ...scans, buildConfigs: {
      'current-head': ['qubership-profiler-backend'],
      main: ['qubership-profiler-backend', 'qubership-profiler-transfer'],
    } }));
    assert.deepStrictEqual({ total: decision.images.total, notBuilt: decision.images.notBuilt },
      { total: 2, notBuilt: ['qubership-profiler-transfer'] });
  });

  // The fake returns these publisher runs from the second read of backend-dev-docker-build.yml on, which is the read
  // after the comparison
  function publisherAfterComparison(github, run) {
    const list = github.rest.actions.listWorkflowRuns;
    let reads = 0;
    github.rest.actions.listWorkflowRuns = async params => {
      if (params.workflow_id === 'backend-dev-docker-build.yml' && ++reads > 1) {
        return { data: { workflow_runs: [publisher(params.workflow_id, run)] } };
      }
      return list(params);
    };
  }

  it('names the status of a publisher run that appeared during the comparison', async () => {
    const github = fakeGitHub({ ...imageScans(), buildConfigs: BACKEND_ONLY });
    publisherAfterComparison(github, { id: 7, status: 'queued' });
    const decision = await decide(github);
    assert.deepStrictEqual(decision.images.skipped.map(s => s.reason), [
      'the scanner has no analysis of the image on main from the last 48 hours',
      'during the comparison backend-dev-docker-build.yml run 7 was queued',
    ]);
  });

  it('keeps the pairs skipped before a publisher run completed during the comparison, and lists the discarded ones',
    async () => {
      const scans = mergeScans(imageScans(), imageScans({ tool: 'Trivy', ids: [2, 3] }));
      scans.imageAnalyses[3].warning = 'incomplete analysis';
      const github = fakeGitHub({ ...scans, buildConfigs: BACKEND_ONLY });
      publisherAfterComparison(github, { id: 8, updated_at: '2026-10-19T05:50:00Z' });
      const decision = await decide(github);
      assert.deepStrictEqual({ fixes: decision.fixes, total: decision.images.total, compared: decision.images.compared,
        skipped: decision.images.skipped }, { fixes: [], total: 2, compared: 0, skipped: [{
        reason: 'the main analysis reports a warning',
        pairs: ['Trivy qubership-profiler-backend@linux/amd64 (incomplete analysis)'],
      }, {
        reason: 'during the comparison backend-dev-docker-build.yml run 8 was updated at 2026-10-19T05:50:00Z',
        pairs: ['Grype qubership-profiler-backend@linux/amd64'],
      }] });
    });

  it('names both publication times when the latest release keeps its tag during the comparison', async () => {
    const github = fakeGitHub({ ...imageScans(), buildConfigs: BACKEND_ONLY });
    let reads = 0;
    github.rest.repos.getLatestRelease = async () => ({ data: {
      tag_name: 'v4.0.7', published_at: reads++ ? '2026-09-28T13:00:00Z' : RELEASED_AT,
    } });
    const decision = await decide(github);
    assert.deepStrictEqual(decision.images.skipped.map(s => s.reason).slice(1), [
      'during the comparison the publication time of the latest release v4.0.7 changed from 2026-09-28T12:00:00Z to '
        + '2026-09-28T13:00:00Z',
    ]);
  });

  it('reports every built image as not compared when there are no analyses at all', async () => {
    const decision = await decide(fakeGitHub());
    assert.strictEqual(decision.reason,
      'no security fixes found, 4 of 4 image pairs not compared; the next monthly release is due around 2026-10-28');
  });

  const scheduledScan = {
    id: 3, event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
    run_started_at: '2026-10-19T04:50:00Z', updated_at: '2026-10-19T05:30:00Z',
  };
  const manualScan = { ...scheduledScan, event: 'workflow_dispatch' };
  const scanRun = 'a Security Scan run overlapping the main analysis';
  for (const [name, change, reason, detail] of [
    ['a scan before the release', s => { s.latestRelease = { tag_name: 'v4.0.8', published_at: '2026-10-19T05:00:00Z' }; },
      'the main analysis predates the latest release v4.0.8, published at 2026-10-19T05:00:00Z',
      'main analysis at 2026-10-19T05:00:00Z'],
    ['a running publisher', s => { s.publishers = [publisher('release.yaml', { id: 6, status: 'in_progress' })]; },
      'release.yaml run 6 is in_progress, so it may publish images'],
    ['a missing main analysis', s => { s.imageAnalyses.pop(); }, 'there is no main analysis from the last 48 hours'],
    ['a future scan', s => { s.imageAnalyses[1].created_at = '2026-10-19T06:00:00.001Z'; },
      'the main analysis was created after this run started at 2026-10-19T06:00:00.000Z',
      'main analysis at 2026-10-19T06:00:00.001Z'],
    ['an invalid timestamp', s => { s.imageAnalyses[1].created_at = 'invalid'; },
      'the main analysis has an invalid created_at', 'created_at "invalid"'],
    ['an older commit', s => { s.imageAnalyses[1].commit_sha = 'old-head'; },
      'the main analysis is for another commit than main at current-', 'main analysis for old-head'],
    ['a manual scan', s => { s.scanRuns = [manualScan]; },
      `${scanRun} was not started by the schedule, so its scan options are unknown`,
      'run 3 was started by workflow_dispatch'],
    ['a scanner error', s => { s.imageAnalyses[1].error = 'scan failed'; },
      'the main analysis reports an error', 'scan failed'],
    ['a missing scanner version', s => { s.imageAnalyses[1].tool = { name: 'Grype', version: '' }; },
      'the main analysis has no scanner version'],
    ['different scanner versions', s => { s.imageAnalyses[1].tool = { name: 'Grype', version: '2.0' }; },
      'the main and latest analyses have different scanner versions', 'main 2.0, latest 1.0.0'],
    ['a truncated SARIF report', s => { s.imageAnalyses[1].results_count = 1; },
      'the SARIF report of the main analysis is incomplete or unrecognized'],
    ['a missing latest analysis', s => { s.imageAnalyses.shift(); }, 'there is no latest analysis from the last 48 hours'],
    ['a scanner warning', s => { s.imageAnalyses[1].warning = 'incomplete analysis'; },
      'the main analysis reports a warning', 'incomplete analysis'],
    ['no result count', s => { s.imageAnalyses[1].results_count = undefined; }, 'the main analysis has no result count'],
    ['another workflow', s => { s.imageAnalyses[1].analysis_key = '.github/workflows/other.yml:scan'; },
      'the main analysis does not come from security-scan.yml', 'analysis_key .github/workflows/other.yml:scan'],
    ['different configurations', s => { s.imageAnalyses[1].environment = '{"scope":"critical"}'; },
      'the main and latest analyses differ in environment',
      'main analysis 1, latest analysis 0; environment: main {"scope":"critical"}, latest {}'],
    ['different analysis keys and configurations', s => {
      s.imageAnalyses[1].analysis_key = '.github/workflows/security-scan.yml:other-scan';
      s.imageAnalyses[1].environment = '{"scope":"critical"}';
    }, 'the main and latest analyses differ in analysis_key and environment',
    'main analysis 1, latest analysis 0; analysis_key: main .github/workflows/security-scan.yml:other-scan, latest '
      + '.github/workflows/security-scan.yml:grype-scan; environment: main {"scope":"critical"}, latest {}'],
    ['scans more than 24 hours apart', s => {
      s.imageAnalyses[1].created_at = '2026-10-18T04:00:00Z';
      s.scanRuns = s.imageAnalyses.map((a, i) => ({ ...scheduledScan, id: 3 + i, run_started_at: a.created_at,
        updated_at: a.created_at }));
    }, 'the main and latest analyses are more than 24 hours apart', '25.0 hours'],
    ['no scan run at the time of the analysis', s => { s.scanRuns = []; },
      'no Security Scan run on current- was running when the main analysis was created',
      'main analysis at 2026-10-19T05:00:00Z'],
    ['an unfinished scan run', s => { s.scanRuns = [{ ...scheduledScan, status: 'in_progress' }]; },
      `${scanRun} has not completed`, 'run 3 is in_progress'],
    ['a scan run that started before an image publication', s => {
      s.publishers = [publisher('release.yaml', { id: 6, updated_at: '2026-10-19T04:55:00Z' })];
    }, `${scanRun} started before the last update of release.yaml run 6 at 2026-10-19T04:55:00Z`,
    'run 3 started at 2026-10-19T04:50:00Z'],
    ['an expired scan run', s => {
      s.imageAnalyses.forEach(a => { a.created_at = '2026-10-17T06:10:00Z'; });
      s.scanRuns = [{ ...scheduledScan, run_started_at: '2026-10-17T05:45:00Z', updated_at: '2026-10-17T06:20:00Z' }];
    }, `${scanRun} started more than 48 hours before this run`, 'run 3 started at 2026-10-17T05:45:00Z'],
  ]) {
    it(`names the condition that skipped a pair with ${name}`, async () => {
      const scans = { ...imageScans(), scanRuns: [scheduledScan], buildConfigs: BACKEND_ONLY };
      change(scans);
      const decision = await decide(fakeGitHub(scans));
      const grype = decision.images.skipped.filter(s => s.pairs.some(pair => pair.startsWith('Grype ')));
      const pair = 'Grype qubership-profiler-backend@linux/amd64';
      assert.deepStrictEqual(grype, [{ reason, pairs: [detail ? `${pair} (${detail})` : pair] }]);
    });
  }

  it('keeps a multiline scanner warning in the summary item of its pair, and lists the images not built', async () => {
    const scans = mergeScans(imageScans(),
      imageScans({ image: 'qubership-profiler-maintenance', ids: [2, 3] }));
    scans.imageAnalyses[1].warning = 'line one\nline two';
    const decision = await decide(fakeGitHub({ ...scans, buildConfigs: BACKEND_ONLY }));
    const images = decision.summary.findIndex(([method, arg]) => method === 'addHeading' && arg === 'Images');
    assert.deepStrictEqual(decision.summary.slice(images + 1), [
      ['addRaw', 'Image comparison: 0 of 2 pairs compared.'],
      ['addList', [
        'Skipped 1, because the scanner has no analysis of the image on main from the last 48 hours: '
          + 'Trivy qubership-profiler-backend',
        'Skipped 1, because the main analysis reports a warning: '
          + 'Grype qubership-profiler-backend@linux/amd64 (line one\nline two)',
        'Not compared, because .qubership/docker-build-config.cfg does not build them: qubership-profiler-maintenance',
      ]],
    ]);
  });

  it('says how many image pairs were not compared when it finds no security fix', async () => {
    const scans = imageScans({ latestFindings: [] });
    scans.imageAnalyses[1].error = 'scan failed';
    const decision = await decide(fakeGitHub({ ...scans, buildConfigs: BACKEND_ONLY }));
    assert.strictEqual(decision.reason,
      'no security fixes found, 2 of 2 image pairs not compared; the next monthly release is due around 2026-10-28');
  });

  it('claims no image coverage gap when every image pair was compared', async () => {
    const scans = mergeScans(imageScans({ latestFindings: [] }),
      imageScans({ tool: 'Trivy', ids: [2, 3], latestFindings: [] }));
    const decision = await decide(fakeGitHub({ ...scans, buildConfigs: BACKEND_ONLY }));
    assert.strictEqual(decision.reason, 'no security fixes found; the next monthly release is due around 2026-10-28');
  });

  it('lists a Dependabot alert in the Gradle build as fixed but not counted, with the reason', async () => {
    const decision = await decide(fakeGitHub({ dependabotFixed: [dependabotAlert({
      number: 319, manifest: 'settings.gradle.kts', scope: null, ecosystem: 'maven',
      name: 'org.jetbrains.kotlin:kotlin-gradle-plugin', fixedAt: '2026-10-15T06:47:21Z',
    })] }));
    assert.deepStrictEqual(decision.notCounted, [{
      source: 'Dependabot', number: 319, severity: 'high',
      subject: 'org.jetbrains.kotlin:kotlin-gradle-plugin (settings.gradle.kts)', fixedAt: '2026-10-15T06:47:21Z',
      reason: 'the dependency scope is not set, not runtime',
    }]);
  });

  it('reports fixed Dependabot alerts whose advisory is still open only as a count', async () => {
    const decision = await decide(fakeGitHub({
      dependabotFixed: [dependabotAlert({ number: 320, manifest: 'backend/go.mod' })],
      dependabotOpen: [dependabotAlert({ number: 350, manifest: 'go.mod' })],
    }));
    const text = 'Not counted: 1 Dependabot alert fixed since the release, because the same advisory is still open '
      + 'for the same package in a shipped dependency; the Dependabot alerts of the repository list them';
    assert.deepStrictEqual({
      notCounted: decision.notCounted,
      stillOpen: decision.stillOpen,
      logs: decision.logs.filter(line => line.startsWith('Not counted')),
      summary: decision.summary.filter(([, arg]) => JSON.stringify(arg).includes('labstack')
        || JSON.stringify(arg).includes('Not counted')),
    }, { notCounted: [], stillOpen: 1, logs: [text], summary: [['addRaw', text]] });
  });

  it('reports a fixed Dependabot alert in an unshipped manifest only as a count when its advisory is still open',
    async () => {
      const decision = await decide(fakeGitHub({
        dependabotFixed: [dependabotAlert({ number: 321, manifest: 'apps/examples/x/go.mod' })],
        dependabotOpen: [dependabotAlert({ number: 350, manifest: 'go.mod' })],
      }));
      assert.deepStrictEqual({ notCounted: decision.notCounted, stillOpen: decision.stillOpen },
        { notCounted: [], stillOpen: 1 });
    });

  it('lists a fixed code scanning alert without a security severity as not counted', async () => {
    const decision = await decide(fakeGitHub({
      codeScanningFixed: [codeScanningAlert({ number: 13, rule: 'js/unused-local-variable', severity: null })],
    }));
    assert.deepStrictEqual(decision.notCounted.map(f => [f.number, f.reason]),
      [[13, 'the rule has no security severity']]);
  });

  it('writes None. under the counted fixes when nothing counts, and names the latest release by tag', async () => {
    const decision = await decide(fakeGitHub());
    const raw = decision.summary.filter(([method]) => method !== 'addTable');
    assert.deepStrictEqual(raw.slice(1, 4), [
      ['addRaw', 'Decision: skip: no security fixes found, 4 of 4 image pairs not compared; the next monthly release is '
        + 'due around 2026-10-28'],
      ['addList', ['Latest GitHub release: v4.0.7, published at 2026-09-28T12:00:00Z',
        'Counting from: v4.0.7, published at 2026-09-28T12:00:00Z', 'Commits since then, excluding version bumps: 1']],
      ['addHeading', 'Counted security fixes'],
    ]);
    assert.deepStrictEqual(raw[4], ['addRaw', 'None.']);
  });

  it('writes the counted fixes as a table, not None., when a fix counts', async () => {
    const decision = await decide(fakeGitHub({ dependabotFixed: [dependabotAlert({ number: 338 })] }));
    const afterHeading = decision.summary[decision.summary.findIndex(([, arg]) => arg === 'Counted security fixes') + 1];
    assert.deepStrictEqual(afterHeading[0], 'addTable');
    assert.deepStrictEqual(afterHeading[1][1],
      ['Dependabot', '#338', 'high', 'github.com/labstack/echo/v4 (go.mod)', '2026-10-10T00:00:00Z']);
  });

  it('writes the alerts fixed since the release but not counted as a table with the reason', async () => {
    const decision = await decide(fakeGitHub({
      codeScanningFixed: [codeScanningAlert({ number: 13, rule: 'js/unused-local-variable', severity: null })],
    }));
    const tables = decision.summary.filter(([method]) => method === 'addTable').map(([, rows]) => rows);
    assert.deepStrictEqual(tables, [[
      ['Source', 'Alert', 'Subject', 'Fixed at', 'Reason'].map(data => ({ data, header: true })),
      ['CodeQL', '#13', 'js/unused-local-variable (dumps-collector)', '2026-10-18T03:20:35Z',
        'the rule has no security severity'],
    ]]);
  });

  it('posts the decision as a notice', async () => {
    const decision = await decide(fakeGitHub({ dependabotFixed: [dependabotAlert()] }), { dryRun: true });
    assert.deepStrictEqual(decision.notices, ['release: security fixes since the latest release (dry run)']);
  });
});
