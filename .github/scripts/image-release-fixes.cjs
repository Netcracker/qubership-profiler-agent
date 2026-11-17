'use strict';

const { TextDecoder } = require('node:util');

const DAY = 24 * 3600 * 1000;
const HOUR = 3600 * 1000;
const SCANNERS = ['Grype', 'Trivy'];
const CATEGORY = /^([a-z0-9._-]+):(main|latest)@(linux\/[a-z0-9_]+(?:\/[a-z0-9_]+)?)\/?$/;

/**
 * Compares the newest Grype and Trivy analyses of the `latest` and `main` tags of every built image, and returns the
 * HIGH and CRITICAL findings that `latest` has and `main` does not.
 *
 * A pair is compared only when both analyses come from fresh scheduled scans of the current branch commit, at most 24
 * hours apart, taken after the latest release and after every image publication. Every other pair is skipped with the
 * condition that failed and the values it compared. Categories of images outside `images` are reported as not built
 * and never paired.
 *
 * @param {object} args
 * @param {string[]} args.images the image names the repository builds, from .qubership/docker-build-config.cfg
 * @returns {Promise<{fixes: object[], total: number, compared: number, skipped: {reason: string, pairs: string[]}[],
 *   notBuilt: string[], status: string}>} the fixes; the number of pairs, counting a built image and scanner with no
 *   analysis as one; the number compared; the skipped pairs grouped by reason; and a text for the log
 */
module.exports = async function imageReleaseFixes({ github, owner, repo, branch, workflowId, latest, now, images }) {
  const built = new Set(images);
  const pairs = new Map();
  const notBuilt = new Set();
  for (const tool of SCANNERS) {
    const analyses = await github.paginate('GET /repos/{owner}/{repo}/code-scanning/analyses', {
      owner, repo, tool_name: tool, ref: `refs/heads/${branch}`, sort: 'created', direction: 'desc', per_page: 100,
    }, (response, done) => {
      if (response.data.some(a => Date.parse(a.created_at) < now.getTime() - 2 * DAY)) done();
      return response.data;
    });
    for (const analysis of analyses) {
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
  const skip = (name, reason, detail) => {
    if (!skipped.has(reason)) skipped.set(reason, []);
    skipped.get(reason).push(detail ? `${name} (${detail})` : name);
  };
  // A built image a scanner has not analyzed is a pair that cannot be compared. Pagination reads every analysis newer
  // than 48 hours, so such a scanner has no analysis of the image in that window.
  let total = pairs.size;
  for (const image of built) {
    for (const tool of SCANNERS) {
      if ([...pairs.keys()].some(key => key.startsWith(`${tool} ${image}@`))) continue;
      skip(`${tool} ${image}`, `the scanner has no analysis of the image on ${branch} from the last 48 hours`);
      total++;
    }
  }
  if (pairs.size === 0) return report({ branch, total, skipped, notBuilt });

  const before = await publicationState({ github, owner, repo, branch, workflowId });
  const scanRuns = await github.paginate(github.rest.actions.listWorkflowRuns, {
    owner, repo, workflow_id: 'security-scan.yml', branch, per_page: 100,
  });
  const fixes = [];
  let compared = 0;
  const releasedAt = Date.parse(latest.published_at);
  const boundary = Math.max(releasedAt, before.publishedAt);
  const boundaryText = before.publishedAt > releasedAt
    ? `the images ${before.publisher} published by ${before.publisherUpdatedAt}`
    : `the latest release ${latest.tag_name}, published at ${latest.published_at}`;
  for (const [name, pair] of pairs) {
    const problem = freshnessProblem({ pair, state: before, boundary, boundaryText, now, scanRuns, branch });
    if (problem) {
      const { reason, detail } = typeof problem === 'string' ? { reason: problem } : problem;
      skip(name, reason, detail);
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
      skip(name, `the SARIF report of the ${unreadable.join(' and ')} analysis is incomplete or unrecognized`);
      continue;
    }
    compared++;
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

  const after = await publicationState({ github, owner, repo, branch, workflowId });
  const { data: currentRelease } = await github.rest.repos.getLatestRelease({ owner, repo });
  const changes = [];
  if (after.sha !== before.sha) changes.push(`${branch} moved from ${short(before.sha)} to ${short(after.sha)}`);
  if (after.running !== before.running && after.running) changes.push(`${after.running} started`);
  if (after.publishedAt !== before.publishedAt) {
    changes.push(`${after.publisher} updated at ${after.publisherUpdatedAt}`);
  }
  if (currentRelease.tag_name !== latest.tag_name || currentRelease.published_at !== latest.published_at) {
    changes.push(`the latest release changed from ${latest.tag_name} to ${currentRelease.tag_name}`);
  }
  if (changes.length > 0) {
    return report({ branch, total, notBuilt, invalidated: `${changes.join(', ')} during the comparison` });
  }
  return report({ branch, fixes, total, compared, skipped, notBuilt });
};

function report({ branch, fixes = [], total = 0, compared = 0, skipped = new Map(), notBuilt, invalidated }) {
  const lines = [];
  if (invalidated) {
    lines.push(`Image comparison: no image fixes counted, because ${invalidated}.`);
  } else if (total === 0) {
    lines.push(`Image comparison: no Grype or Trivy analysis of a built image on ${branch}.`);
  } else {
    lines.push(`Image comparison: ${compared} of ${total} pairs compared.`);
    for (const [reason, names] of skipped) {
      lines.push(`Skipped ${names.length}, because ${reason}: ${names.join(', ')}`);
    }
  }
  if (notBuilt.size > 0) {
    const names = [...notBuilt].join(', ');
    lines.push(`Not compared, because .qubership/docker-build-config.cfg does not build them: ${names}`);
  }
  return {
    fixes: invalidated ? [] : fixes,
    total,
    compared: invalidated ? 0 : compared,
    skipped: [...skipped].map(([reason, pairs]) => ({ reason, pairs })),
    notBuilt: [...notBuilt],
    status: lines.join('\n'),
  };
}

const short = sha => String(sha).slice(0, 8);
const describeRun = r => `${String(r.path || r.name).split('/').pop()} run ${r.id}`;

async function publicationState({ github, owner, repo, branch, workflowId }) {
  const { data: head } = await github.rest.repos.getBranch({ owner, repo, branch });
  const runs = (await Promise.all([workflowId, 'backend-dev-docker-build.yml'].map(workflow => github.paginate(
    github.rest.actions.listWorkflowRuns, { owner, repo, workflow_id: workflow, branch, per_page: 100 },
  )))).flat().filter(r => r.event !== 'pull_request');
  const running = runs.find(r => r.status !== 'completed');
  // A failed build may already have pushed one component before another component failed.
  const newest = runs.reduce((a, r) => (!a || Date.parse(r.updated_at) > Date.parse(a.updated_at) ? r : a), undefined);
  return {
    sha: head.commit.sha,
    running: running ? describeRun(running) : null,
    publishedAt: Math.max(0, ...runs.map(r => Date.parse(r.updated_at))),
    publisher: newest ? describeRun(newest) : null,
    publisherUpdatedAt: newest ? newest.updated_at : null,
  };
}

// Returns the first condition that rules the pair out, with the values it compared, or null when the pair is fresh. The
// condition is a string, or {reason, detail} when it carries a per-pair value: the value goes in detail, so that pairs
// skipped for the same condition share one reason.
function freshnessProblem({ pair, state, boundary, boundaryText, now, scanRuns, branch }) {
  if (state.running) return `${state.running} may be publishing images`;
  for (const tag of ['main', 'latest']) {
    if (!pair[tag]) return `there is no ${tag} analysis`;
  }
  if (!Number.isFinite(boundary) || !Number.isFinite(state.publishedAt)) {
    return 'the time of the latest release or image publication is not a valid date';
  }
  for (const [tag, analysis] of [['main', pair.main], ['latest', pair.latest]]) {
    const time = Date.parse(analysis.created_at);
    if (!Number.isFinite(time)) return `the ${tag} analysis has an invalid created_at "${analysis.created_at}"`;
    if (time > now.getTime()) {
      return `the ${tag} analysis was created at ${analysis.created_at}, `
        + `after this run started at ${now.toISOString()}`;
    }
    if (time <= boundary) {
      return {
        reason: `the ${tag} analysis predates ${boundaryText}`,
        detail: `${tag} analysis at ${analysis.created_at}`,
      };
    }
    if (analysis.commit_sha !== state.sha) {
      return `the ${tag} analysis is for commit ${short(analysis.commit_sha)}, and ${branch} is at ${short(state.sha)}`;
    }
    const runs = scanRuns.filter(r => r.head_sha === analysis.commit_sha
      && Date.parse(r.run_started_at) <= time && Date.parse(r.updated_at) >= time);
    if (runs.length === 0) {
      return `no Security Scan run on ${short(analysis.commit_sha)} was running when the ${tag} analysis was created `
        + `at ${analysis.created_at}`;
    }
    // A run fails when any of its jobs fails; this analysis is judged by the checks below and by findings().
    const unfinished = runs.find(r => r.status !== 'completed');
    if (unfinished) return `Security Scan run ${unfinished.id} is ${unfinished.status}`;
    const early = runs.find(r => Date.parse(r.run_started_at) <= boundary);
    if (early) return `Security Scan run ${early.id} started at ${early.run_started_at}, before ${boundaryText}`;
    const expired = runs.find(r => Date.parse(r.run_started_at) < now.getTime() - 2 * DAY);
    if (expired) {
      return `Security Scan run ${expired.id} started at ${expired.run_started_at}, more than 48 hours before this run`;
    }
    // Scheduled scans use fixed options for both tags; manual inputs are absent from analysis metadata.
    const manual = runs.find(r => r.event !== 'schedule');
    if (manual) return `Security Scan run ${manual.id} was started by ${manual.event}, so its scan options are unknown`;
    if (analysis.error !== '') return `the ${tag} analysis reports an error: ${analysis.error}`;
    if (analysis.warning !== '') return `the ${tag} analysis reports a warning: ${analysis.warning}`;
    if (!Number.isInteger(analysis.results_count)) return `the ${tag} analysis has no result count`;
    if (!analysis.analysis_key.startsWith('.github/workflows/security-scan.yml:')) {
      return `the ${tag} analysis comes from ${analysis.analysis_key}, not from security-scan.yml`;
    }
  }
  const apart = Math.abs(Date.parse(pair.main.created_at) - Date.parse(pair.latest.created_at));
  if (apart > DAY) return `the main and latest analyses are ${(apart / HOUR).toFixed(1)} hours apart, more than 24`;
  if (!pair.main.tool.version || pair.main.tool.version !== pair.latest.tool.version) {
    return `the scanner versions differ: main ${pair.main.tool.version || 'unknown'}, `
      + `latest ${pair.latest.tool.version || 'unknown'}`;
  }
  if (pair.main.analysis_key !== pair.latest.analysis_key || pair.main.environment !== pair.latest.environment) {
    return 'the main and latest analyses ran with different scan configurations';
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
