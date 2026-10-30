import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TextEncoder } from 'node:util';
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
  imageAnalyses = [], sarif = {},
  publishers = [],
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
      const response = await method(params);
      const data = response.data.workflow_runs || response.data;
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
        getLatestRelease: async () => ({ data: { tag_name: 'v4.0.7', published_at: RELEASED_AT } }),
        getBranch: async () => ({ data: { commit: { sha: 'current-head' } } }),
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
        listWorkflowRuns: async ({ workflow_id, per_page }) => ({ data: { workflow_runs: per_page === 20
          ? runStatuses.map(status => ({ status }))
          : workflow_id === 'security-scan.yml' ? scanRuns : publishers.filter(r => r.path === workflow_id) } }),
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

function imageScans({ tool = 'Grype', latestFindings = [{}], mainFindings = [] } = {}) {
  const imageAnalyses = ['latest', 'main'].map((tag, id) => ({
    id,
    category: `qubership-profiler-backend:${tag}@linux/amd64`,
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
    ['a scan before the release', s => { s.imageAnalyses[1].created_at = '2026-09-28T11:59:59Z'; }],
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
    scans.imageAnalyses.forEach(a => { a.created_at = '2026-10-17T05:50:00Z'; });
    const github = fakeGitHub({ ...scans, scanRuns: [{
      event: 'schedule', head_sha: 'current-head', status: 'completed', conclusion: 'success',
      run_started_at: '2026-10-17T05:45:00Z', updated_at: '2026-10-17T05:55:00Z',
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

  for (const [name, scanRuns] of [
    ['missing scan run', []],
    ['failed scan run', [{ status: 'completed', conclusion: 'failure' }]],
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
