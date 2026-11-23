'use strict';

const { TextDecoder } = require('node:util');

const DAY = 24 * 3600 * 1000;
const HOUR = 3600 * 1000;
const SCANNERS = ['Grype', 'Trivy'];
const CATEGORY = /^([a-z0-9._-]+):(main|latest)@(linux\/[a-z0-9_]+(?:\/[a-z0-9_]+)?)\/?$/;
const BUILD_CONFIG = '.qubership/docker-build-config.cfg';

/**
 * Compares the newest Grype and Trivy analyses of the `latest` and `main` tags of every built image, and returns the
 * HIGH and CRITICAL findings that `latest` has and `main` does not.
 *
 * The built images are the components of .qubership/docker-build-config.cfg at the branch commit the analyses are
 * checked against. Only analyses created in the 48 hours before `now` are read. A pair is compared only when both
 * analyses come from fresh scheduled scans of that commit, at most 24 hours apart, started after the latest release and
 * after the last update of every run of the release and backend image build workflows. Every other pair is skipped
 * with the condition that failed, and the values that differ between pairs go in the pair's details. Categories of
 * images outside the build config are reported as not built and never paired.
 *
 * @param {object} args
 * @param {object} args.github the Octokit client of actions/github-script
 * @param {string} args.branch the branch whose head commit the analyses and the build config are read at
 * @param {string} args.workflowId the file name of the release workflow
 * @param {{tag_name: string, published_at: string}} args.latest the latest GitHub release
 * @param {Date} args.now the time of this run
 * @returns {Promise<{fixes: object[], total: number, compared: number, skipped: {reason: string, pairs: string[]}[],
 *   notBuilt: string[], headline: string, details: string[], status: string}>} the fixes; the number of pairs, where a
 *   pair is a scanner, an image, and a platform with a recent analysis, or a scanner and a built image with none; the
 *   number compared; the skipped pairs grouped by condition, so that compared plus the skipped pairs equals total; the
 *   images analyzed but not built; and the report as a headline with detail lines, and as one text for the log
 */
module.exports = async function imageReleaseFixes({ github, owner, repo, branch, workflowId, latest, now }) {
  // The images, the analyses, and the publisher runs are all judged against this commit, and a move of the branch
  // during the comparison discards it
  const before = await publicationState({ github, owner, repo, branch, workflowId });
  const built = new Set(await builtImages({ github, owner, repo, ref: before.sha }));
  const cutoff = now.getTime() - 2 * DAY;
  const pairs = new Map();
  const notBuilt = new Set();
  for (const tool of SCANNERS) {
    const analyses = await github.paginate('GET /repos/{owner}/{repo}/code-scanning/analyses', {
      owner, repo, tool_name: tool, ref: `refs/heads/${branch}`, sort: 'created', direction: 'desc', per_page: 100,
    }, (response, done) => {
      if (response.data.some(a => Date.parse(a.created_at) < cutoff)) done();
      return response.data;
    });
    for (const analysis of analyses) {
      // The last page read can hold older analyses too; they are dropped, so the pairs do not depend on the page size.
      // An invalid created_at is kept, and the freshness check reports it.
      if (Date.parse(analysis.created_at) < cutoff) continue;
      const match = CATEGORY.exec(analysis.category);
      if (!match || analysis.ref !== `refs/heads/${branch}` || analysis.tool.name !== tool) continue;
      const [, image, tag, platform] = match;
      if (!built.has(image)) {
        notBuilt.add(image);
        continue;
      }
      const key = `${tool} ${image}@${platform}`;
      if (!pairs.has(key)) pairs.set(key, {});
      const pair = pairs.get(key);
      if (!pair[tag] || Date.parse(analysis.created_at) > Date.parse(pair[tag].created_at)) pair[tag] = analysis;
    }
  }
  const skipped = new Map();
  const skip = (name, { reason, detail }) => {
    if (!skipped.has(reason)) skipped.set(reason, []);
    skipped.get(reason).push(detail ? `${name} (${detail})` : name);
  };
  // A scanner with no recent analysis of a built image is one pair, whatever number of platforms it scans
  let total = pairs.size;
  for (const image of built) {
    for (const tool of SCANNERS) {
      if ([...pairs.keys()].some(key => key.startsWith(`${tool} ${image}@`))) continue;
      const reason = `the scanner has no analysis of the image on ${branch} from the last 48 hours`;
      skip(`${tool} ${image}`, { reason });
      total++;
    }
  }
  if (pairs.size === 0) return report({ branch, total, skipped, notBuilt });

  const scanRuns = await github.paginate(github.rest.actions.listWorkflowRuns, {
    owner, repo, workflow_id: 'security-scan.yml', branch, per_page: 100,
  });
  const fixes = [];
  const compared = [];
  const releasedAt = Date.parse(latest.published_at);
  const boundary = Math.max(releasedAt, before.publishedAt);
  const boundaryText = before.publishedAt > releasedAt
    ? `the last update of ${before.publisher} at ${before.publisherUpdatedAt}`
    : `the latest release ${latest.tag_name}, published at ${latest.published_at}`;
  for (const [name, pair] of pairs) {
    const problem = freshnessProblem({ pair, state: before, boundary, boundaryText, now, scanRuns, branch });
    if (problem) {
      skip(name, problem);
      continue;
    }
    const reports = await Promise.all([pair.latest, pair.main].map(async analysis => {
      const { data } = await github.request('GET /repos/{owner}/{repo}/code-scanning/analyses/{analysis_id}', {
        owner, repo, analysis_id: analysis.id, headers: { accept: 'application/sarif+json' },
      });
      return findings(data, analysis);
    }));
    const unreadable = ['latest', 'main'].filter((tag, i) => reports[i] === null);
    if (unreadable.length > 0) {
      const which = unreadable.join(' and ');
      skip(name, { reason: `the SARIF report of the ${which} analysis is incomplete or unrecognized` });
      continue;
    }
    compared.push(name);
    const [released, main] = reports;
    for (const [key, finding] of released) {
      if (main.has(key) || finding.score < 7) continue;
      fixes.push({
        source: pair.latest.tool.name,
        number: finding.number,
        severity: finding.score >= 9 ? 'critical' : 'high',
        subject: `${finding.rule} (${pair.latest.category})`,
        fixedAt: pair.main.created_at,
      });
    }
  }
  if (compared.length === 0) return report({ branch, total, skipped, notBuilt });

  const after = await publicationState({ github, owner, repo, branch, workflowId });
  const { data: currentRelease } = await github.rest.repos.getLatestRelease({ owner, repo });
  const changes = [];
  if (after.sha !== before.sha) changes.push(`${branch} moved from ${short(before.sha)} to ${short(after.sha)}`);
  if (after.running && after.running !== before.running) changes.push(`${after.running} was ${after.runningStatus}`);
  // A run that appeared during the comparison is named once, by its status
  if (after.publishedAt !== before.publishedAt && after.publisher !== after.running) {
    changes.push(`${after.publisher} was updated at ${after.publisherUpdatedAt}`);
  }
  if (currentRelease.tag_name !== latest.tag_name) {
    changes.push(`the latest release changed from ${latest.tag_name} to ${currentRelease.tag_name}`);
  } else if (currentRelease.published_at !== latest.published_at) {
    changes.push(`the publication time of the latest release ${latest.tag_name} changed from ${latest.published_at} `
      + `to ${currentRelease.published_at}`);
  }
  if (changes.length > 0) {
    // The pairs skipped before stay listed with their own conditions; the compared ones are discarded
    for (const name of compared) skip(name, { reason: `during the comparison ${changes.join(', ')}` });
    return report({ branch, total, skipped, notBuilt });
  }
  return report({ branch, fixes, total, compared: compared.length, skipped, notBuilt });
};

function report({ branch, fixes = [], total, compared = 0, skipped, notBuilt }) {
  const headline = total === 0
    ? `Image comparison: no Grype or Trivy analysis of a built image on ${branch}.`
    : `Image comparison: ${compared} of ${total} pairs compared.`;
  const details = [];
  for (const [reason, names] of skipped) {
    details.push(`Skipped ${names.length}, because ${reason}: ${names.join(', ')}`);
  }
  if (notBuilt.size > 0) {
    details.push(`Not compared, because ${BUILD_CONFIG} does not build them: ${[...notBuilt].join(', ')}`);
  }
  return {
    fixes,
    total,
    compared,
    skipped: [...skipped].map(([reason, pairs]) => ({ reason, pairs })),
    notBuilt: [...notBuilt],
    headline,
    details,
    status: [headline, ...details].join('\n'),
  };
}

const short = sha => String(sha).slice(0, 8);
const describeRun = r => `${String(r.path || r.name).split('/').pop()} run ${r.id}`;

async function builtImages({ github, owner, repo, ref }) {
  const { data } = await github.rest.repos.getContent({ owner, repo, path: BUILD_CONFIG, ref });
  return JSON.parse(Buffer.from(data.content, data.encoding).toString('utf8')).components.map(c => c.name);
}

async function publicationState({ github, owner, repo, branch, workflowId }) {
  const { data: head } = await github.rest.repos.getBranch({ owner, repo, branch });
  const runs = (await Promise.all([workflowId, 'backend-dev-docker-build.yml'].map(workflow => github.paginate(
    github.rest.actions.listWorkflowRuns, { owner, repo, workflow_id: workflow, branch, per_page: 100 },
  )))).flat().filter(r => r.event !== 'pull_request');
  const running = runs.find(r => r.status !== 'completed');
  // A failed build may already have pushed one component before another component failed, and a run that published
  // nothing, such as a dry run, is counted as well, since a run does not say which images it pushed
  const newest = runs.reduce((a, r) => (!a || Date.parse(r.updated_at) > Date.parse(a.updated_at) ? r : a), undefined);
  return {
    sha: head.commit.sha,
    running: running ? describeRun(running) : null,
    runningStatus: running ? running.status : null,
    publishedAt: Math.max(0, ...runs.map(r => Date.parse(r.updated_at))),
    publisher: newest ? describeRun(newest) : null,
    publisherUpdatedAt: newest ? newest.updated_at : null,
  };
}

// Returns the first condition that rules the pair out as {reason, detail}, or null when the pair is fresh. The reason
// names the condition and holds only values every pair shares, so that pairs skipped for one condition share one
// reason; the values of this pair go in detail.
function freshnessProblem({ pair, state, boundary, boundaryText, now, scanRuns, branch }) {
  if (state.running) return { reason: `${state.running} is ${state.runningStatus}, so it may publish images` };
  for (const tag of ['main', 'latest']) {
    if (!pair[tag]) return { reason: `there is no ${tag} analysis from the last 48 hours` };
  }
  if (!Number.isFinite(boundary) || !Number.isFinite(state.publishedAt)) {
    return { reason: 'the time of the latest release or of a publisher run update is not a valid date' };
  }
  for (const [tag, analysis] of [['main', pair.main], ['latest', pair.latest]]) {
    const time = Date.parse(analysis.created_at);
    const at = `${tag} analysis at ${analysis.created_at}`;
    if (!Number.isFinite(time)) {
      return { reason: `the ${tag} analysis has an invalid created_at`, detail: `created_at "${analysis.created_at}"` };
    }
    if (time > now.getTime()) {
      return { reason: `the ${tag} analysis was created after this run started at ${now.toISOString()}`, detail: at };
    }
    if (time <= boundary) return { reason: `the ${tag} analysis predates ${boundaryText}`, detail: at };
    if (analysis.commit_sha !== state.sha) {
      return {
        reason: `the ${tag} analysis is for another commit than ${branch} at ${short(state.sha)}`,
        detail: `${tag} analysis for ${short(analysis.commit_sha)}`,
      };
    }
    const runs = scanRuns.filter(r => r.head_sha === analysis.commit_sha
      && Date.parse(r.run_started_at) <= time && Date.parse(r.updated_at) >= time);
    if (runs.length === 0) {
      return {
        reason: `no Security Scan run on ${short(analysis.commit_sha)} was running when the ${tag} analysis `
          + 'was created',
        detail: at,
      };
    }
    // A run fails when any of its jobs fails; this analysis is judged by the checks below and by findings().
    const scan = `a Security Scan run overlapping the ${tag} analysis`;
    const unfinished = runs.find(r => r.status !== 'completed');
    if (unfinished) {
      return { reason: `${scan} has not completed`, detail: `run ${unfinished.id} is ${unfinished.status}` };
    }
    const early = runs.find(r => Date.parse(r.run_started_at) <= boundary);
    if (early) {
      return {
        reason: `${scan} started before ${boundaryText}`,
        detail: `run ${early.id} started at ${early.run_started_at}`,
      };
    }
    const expired = runs.find(r => Date.parse(r.run_started_at) < now.getTime() - 2 * DAY);
    if (expired) {
      return {
        reason: `${scan} started more than 48 hours before this run`,
        detail: `run ${expired.id} started at ${expired.run_started_at}`,
      };
    }
    // Scheduled scans use fixed options for both tags; manual inputs are absent from analysis metadata.
    const manual = runs.find(r => r.event !== 'schedule');
    if (manual) {
      return {
        reason: `${scan} was not started by the schedule, so its scan options are unknown`,
        detail: `run ${manual.id} was started by ${manual.event}`,
      };
    }
    if (analysis.error !== '') return { reason: `the ${tag} analysis reports an error`, detail: analysis.error };
    if (analysis.warning !== '') return { reason: `the ${tag} analysis reports a warning`, detail: analysis.warning };
    if (!Number.isInteger(analysis.results_count)) return { reason: `the ${tag} analysis has no result count` };
    if (!analysis.analysis_key.startsWith('.github/workflows/security-scan.yml:')) {
      return {
        reason: `the ${tag} analysis does not come from security-scan.yml`,
        detail: `analysis_key ${analysis.analysis_key}`,
      };
    }
    if (!analysis.tool.version) return { reason: `the ${tag} analysis has no scanner version` };
  }
  const apart = Math.abs(Date.parse(pair.main.created_at) - Date.parse(pair.latest.created_at));
  if (apart > DAY) {
    return {
      reason: 'the main and latest analyses are more than 24 hours apart',
      detail: `${(apart / HOUR).toFixed(1)} hours`,
    };
  }
  if (pair.main.tool.version !== pair.latest.tool.version) {
    return {
      reason: 'the main and latest analyses have different scanner versions',
      detail: `main ${pair.main.tool.version}, latest ${pair.latest.tool.version}`,
    };
  }
  const differing = ['analysis_key', 'environment'].filter(field => pair.main[field] !== pair.latest[field]);
  if (differing.length > 0) {
    return {
      reason: `the main and latest analyses differ in ${differing.join(' and ')}`,
      detail: `main analysis ${pair.main.id}, latest analysis ${pair.latest.id}; `
        + differing.map(field => `${field}: main ${pair.main[field]}, latest ${pair.latest[field]}`).join('; '),
    };
  }
  return null;
}

function findings(sarif, analysis) {
  if (sarif instanceof ArrayBuffer) sarif = JSON.parse(new TextDecoder().decode(sarif));
  if (!Array.isArray(sarif.runs) || sarif.runs.length !== 1) return null;
  const run = sarif.runs[0];
  if (run.tool?.driver?.name !== analysis.tool.name || run.tool.driver.version !== analysis.tool.version
    || run.automationDetails?.id?.replace(/\/$/, '') !== analysis.category.replace(/\/$/, '')
    || !Array.isArray(run.results) || run.results.length !== analysis.results_count
    || run.invocations?.some(i => i.executionSuccessful === false)) return null;
  const rules = new Map((run.tool.driver.rules || []).map(r => [r.id, r]));
  const result = new Map();
  for (const finding of run.results) {
    const rule = rules.get(finding.ruleId);
    const score = Number(rule?.properties?.['security-severity']);
    const message = finding.message?.text || '';
    const pkg = analysis.tool.name === 'Grype'
      ? / package: (.+?), version /.exec(message)?.[1]
      : /^Package: (.+)$/m.exec(message)?.[1];
    if (!rule || !Number.isFinite(score) || !pkg) return null;
    // A vulnerable package still present elsewhere in main is not an unreleased fix.
    const key = JSON.stringify([finding.ruleId, pkg]);
    result.set(key, { rule: finding.ruleId, score, number: finding.properties?.['github/alertNumber'] });
  }
  return result;
}
