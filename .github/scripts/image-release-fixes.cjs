'use strict';

const { TextDecoder } = require('node:util');

const DAY = 24 * 3600 * 1000;
const SCANNERS = ['Grype', 'Trivy'];
const CATEGORY = /^([a-z0-9._-]+):(main|latest)@(linux\/[a-z0-9_]+(?:\/[a-z0-9_]+)?)\/?$/;

module.exports = async function imageReleaseFixes({ github, owner, repo, branch, workflowId, latest, now }) {
  const pairs = new Map();
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
      const key = `${tool} ${image}@${platform}`;
      if (!pairs.has(key)) pairs.set(key, {});
      const pair = pairs.get(key);
      if (!pair[tag] || Date.parse(analysis.created_at) > Date.parse(pair[tag].created_at)) pair[tag] = analysis;
    }
  }
  if (pairs.size === 0) return { fixes: [], status: 'No image categories found; image fixes were not counted.' };

  const before = await publicationState({ github, owner, repo, branch, workflowId });
  const scanRuns = await github.paginate(github.rest.actions.listWorkflowRuns, {
    owner, repo, workflow_id: 'security-scan.yml', branch, per_page: 100,
  });
  const notes = [];
  const fixes = [];
  let compared = 0;
  const boundary = Math.max(Date.parse(latest.published_at), before.publishedAt);
  for (const [name, pair] of pairs) {
    const reason = freshnessProblem(pair, before, boundary, now, scanRuns);
    if (reason) {
      notes.push(`${name}: ${reason}`);
      continue;
    }
    const reports = await Promise.all([pair.latest, pair.main].map(async analysis => {
      const { data } = await github.request('GET /repos/{owner}/{repo}/code-scanning/analyses/{analysis_id}', {
        owner, repo, analysis_id: analysis.id, headers: { accept: 'application/sarif+json' },
      });
      return findings(data, analysis);
    }));
    if (reports.some(r => r === null)) {
      notes.push(`${name}: incomplete or unrecognized SARIF`);
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
  if (JSON.stringify(before) !== JSON.stringify(after)
    || currentRelease.tag_name !== latest.tag_name || currentRelease.published_at !== latest.published_at) {
    return { fixes: [], status: 'Images, branch, or release changed during comparison; image fixes were not counted.' };
  }
  return {
    fixes,
    status: `Image comparison: ${compared} pairs compared, ${notes.length} skipped.${notes.length ? `\n${notes.join('\n')}` : ''}`,
  };
};

async function publicationState({ github, owner, repo, branch, workflowId }) {
  const { data: head } = await github.rest.repos.getBranch({ owner, repo, branch });
  const runs = (await Promise.all([workflowId, 'backend-dev-docker-build.yml'].map(workflow => github.paginate(
    github.rest.actions.listWorkflowRuns, { owner, repo, workflow_id: workflow, branch, per_page: 100 },
  )))).flat().filter(r => r.event !== 'pull_request');
  return {
    sha: head.commit.sha,
    publishing: runs.some(r => r.status !== 'completed'),
    // A failed build may already have pushed one component before another component failed.
    publishedAt: Math.max(0, ...runs.map(r => Date.parse(r.updated_at))),
  };
}

function freshnessProblem(pair, state, boundary, now, scanRuns) {
  if (state.publishing) return 'an image publisher is running';
  if (!pair.main || !pair.latest) return 'missing main or latest analysis';
  for (const analysis of [pair.main, pair.latest]) {
    const time = Date.parse(analysis.created_at);
    if (!Number.isFinite(time) || !Number.isFinite(boundary) || !Number.isFinite(state.publishedAt)
      || time > now.getTime() || time <= boundary) {
      return 'analysis timestamp is invalid or predates image publication';
    }
    if (analysis.commit_sha !== state.sha) return 'analysis is for an older branch commit';
    const runs = scanRuns.filter(r => r.head_sha === analysis.commit_sha
      && Date.parse(r.run_started_at) <= time && Date.parse(r.updated_at) >= time);
    if (runs.length === 0 || runs.some(r => r.status !== 'completed' || r.conclusion !== 'success'
      || Date.parse(r.run_started_at) <= boundary || Date.parse(r.run_started_at) < now.getTime() - 2 * DAY)) {
      return 'scan workflow failed, is incomplete, or started before the freshness boundary';
    }
    // Scheduled scans use fixed options for both tags; manual inputs are absent from analysis metadata.
    if (runs.some(r => r.event !== 'schedule')) return 'scan options are unknown for a non-scheduled run';
    if (analysis.error !== '' || analysis.warning !== '' || !Number.isInteger(analysis.results_count)
      || !analysis.analysis_key.startsWith('.github/workflows/security-scan.yml:')) {
      return 'analysis failed, has warnings, or came from another workflow';
    }
  }
  if (Math.abs(Date.parse(pair.main.created_at) - Date.parse(pair.latest.created_at)) > DAY) {
    return 'main and latest analyses are more than 24 hours apart';
  }
  if (!pair.main.tool.version || pair.main.tool.version !== pair.latest.tool.version
    || pair.main.analysis_key !== pair.latest.analysis_key || pair.main.environment !== pair.latest.environment) {
    return 'scanner versions or analysis configurations differ';
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
