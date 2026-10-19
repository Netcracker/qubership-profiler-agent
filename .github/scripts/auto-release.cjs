'use strict';

const DAY = 24 * 3600 * 1000;

/**
 * Decides whether to release, and starts the release workflow when it should.
 *
 * A release is due when a security fix has landed on the branch since the latest release, or on the weekly run
 * nearest to one month after it. Nothing is released while the branch has only version-bump commits since the
 * release tag, or while a run of the release workflow is not completed.
 *
 * @param {object} args
 * @param {object} args.github the Octokit client of actions/github-script
 * @param {object} args.context the workflow context of actions/github-script
 * @param {object} args.core the @actions/core module of actions/github-script
 * @param {Date} args.now the time of this run
 * @param {string} args.branch the branch to release from
 * @param {string} args.workflowId the file name of the release workflow
 * @param {boolean} args.dryRun reports the decision without starting the release workflow
 * @returns {Promise<{release: boolean, reason: string, fixes: object[]}>} the decision and the alerts it counted
 */
module.exports = async function autoRelease({ github, context, core, now, branch, workflowId, dryRun }) {
  const { owner, repo } = context.repo;

  const { data: latest } = await github.rest.repos.getLatestRelease({ owner, repo });
  const releasedAt = new Date(latest.published_at);
  core.info(`Latest release: ${latest.tag_name}, published at ${latest.published_at}`);

  // The release workflow pushes "chore: bump version to x.y.z" after the tag, so that commit alone is no change
  const { data: comparison } = await github.rest.repos.compareCommitsWithBasehead({
    owner, repo, basehead: `${latest.tag_name}...${branch}`,
  });
  const changes = comparison.commits.filter(c => !c.commit.message.startsWith('chore: bump version to '));
  const hasChanges = changes.length > 0 || comparison.ahead_by > comparison.commits.length;
  core.info(`Commits on ${branch} since ${latest.tag_name}: ${comparison.ahead_by}, excluding version bumps: ${changes.length}`);

  const fixes = [
    ...await dependabotFixes({ github, owner, repo, releasedAt }),
    ...await codeScanningFixes({ github, owner, repo, branch, releasedAt }),
  ];

  // A weekly run falls at most 3.5 days from the anniversary. Waiting for the first run after it instead would
  // stretch the interval towards five weeks.
  const anniversary = new Date(releasedAt);
  anniversary.setUTCMonth(anniversary.getUTCMonth() + 1);
  const monthElapsed = now.getTime() >= anniversary.getTime() - 3.5 * DAY;

  const { data: runs } = await github.rest.actions.listWorkflowRuns({ owner, repo, workflow_id: workflowId, per_page: 20 });
  const releaseRunning = runs.workflow_runs.some(r => r.status !== 'completed');

  let release = false;
  let reason;
  if (!hasChanges) {
    reason = `${branch} has no changes since ${latest.tag_name}`;
  } else if (releaseRunning) {
    reason = `a ${workflowId} run is in progress`;
  } else if (fixes.length > 0) {
    release = true;
    reason = 'security fixes since the latest release';
  } else if (monthElapsed) {
    release = true;
    reason = `a month has passed since ${latest.tag_name}`;
  } else {
    reason = `no security fixes, next monthly release is due around ${anniversary.toISOString().slice(0, 10)}`;
  }
  const decision = `${release ? 'release' : 'skip'}: ${reason}`;
  core.info(`Decision: ${decision}`);

  await core.summary
    .addHeading('Auto release', 2)
    .addRaw(`Latest release: ${latest.tag_name}, published at ${latest.published_at}`, true)
    .addRaw(`Commits since the release, excluding version bumps: ${changes.length}`, true)
    .addRaw(`Decision: ${decision}${dryRun ? ' (dry run)' : ''}`, true)
    .addTable([
      ['Source', 'Alert', 'Severity', 'Subject', 'Fixed at'].map(data => ({ data, header: true })),
      ...fixes.map(f => [f.source, `#${f.number}`, f.severity, f.subject, f.fixedAt]),
    ])
    .write();

  if (release && !dryRun) {
    await github.rest.actions.createWorkflowDispatch({ owner, repo, workflow_id: workflowId, ref: branch });
    core.info(`Started ${workflowId} on ${branch}`);
  }
  return { release, reason, fixes };
};

// A Dependabot alert is fixed once the manifest on the default branch no longer has the vulnerable version.
// Moving a manifest also fixes its alerts and opens the same ones at the new path, so an advisory that is still
// open for the same package does not count. Development dependencies do not ship.
async function dependabotFixes({ github, owner, repo, releasedAt }) {
  const fixed = await github.paginate(github.rest.dependabot.listAlertsForRepo,
    { owner, repo, state: 'fixed', per_page: 100 });
  const open = await github.paginate(github.rest.dependabot.listAlertsForRepo,
    { owner, repo, state: 'open', per_page: 100 });
  const key = a => `${a.security_advisory.ghsa_id} ${a.dependency.package.ecosystem}:${a.dependency.package.name}`;
  const stillOpen = new Set(open.map(key));
  return fixed
    .filter(a => new Date(a.fixed_at) > releasedAt
      && a.dependency.scope !== 'development'
      && !stillOpen.has(key(a)))
    .map(a => ({
      source: 'Dependabot',
      number: a.number,
      severity: a.security_advisory.severity,
      subject: `${a.dependency.package.name} (${a.dependency.manifest_path})`,
      fixedAt: a.fixed_at,
    }));
}

// security-scan.yml scans the latest and main images of each component, and the reusable workflow uploads both under
// one category, so whichever upload finishes last decides the state of an alert. A fixed Grype or Trivy alert
// therefore does not show that main has the fix and the released image lacks it, and is not counted until the
// categories carry the tag: https://github.com/Netcracker/qubership-workflow-hub/issues/1012
const IMAGE_SCANNERS = new Set(['Grype', 'Trivy']);

async function codeScanningFixes({ github, owner, repo, branch, releasedAt }) {
  const fixed = await github.paginate(github.rest.codeScanning.listAlertsForRepo,
    { owner, repo, state: 'fixed', ref: `refs/heads/${branch}`, per_page: 100 });
  // Scorecard reports repository settings, and a rule without security_severity_level is no vulnerability
  return fixed
    .filter(a => new Date(a.fixed_at) > releasedAt
      && a.tool.name !== 'Scorecard'
      && !IMAGE_SCANNERS.has(a.tool.name)
      && a.rule.security_severity_level)
    .map(a => ({
      source: a.tool.name,
      number: a.number,
      severity: a.rule.security_severity_level,
      subject: `${a.rule.id} (${a.most_recent_instance.category})`,
      fixedAt: a.fixed_at,
    }));
}
