'use strict';

const DAY = 24 * 3600 * 1000;

/**
 * Decides whether to release, and starts the release workflow when it should.
 *
 * A release is due when a security fix has landed on the branch since the latest release, or on the weekly run
 * nearest to one month after it. Nothing is released while the branch has only version-bump commits since the
 * release, or while a run of the release workflow is not completed. The latest release is the newest successful run
 * of the release workflow on the branch, or the latest GitHub release when the branch has no such run.
 *
 * When the newest completed run of the release workflow on the branch did not succeed, nothing is released and the
 * job fails, because that run may have stopped after publishing some artifacts or after the version bump. The runs of
 * the release workflow are read again right before the dispatch, and any change since the first read skips the
 * release.
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

  const runs = await releaseRuns({ github, owner, repo, workflowId });
  const releaseRunning = runs.some(r => r.status !== 'completed');
  const lastCompleted = runs.find(r => r.status === 'completed' && r.head_branch === branch);
  // A running release run may be the re-run of a failed one, so it waits rather than fails
  const failedRun = !releaseRunning && lastCompleted && lastCompleted.conclusion !== 'success'
    ? lastCompleted
    : undefined;

  const { data: latest } = await github.rest.repos.getLatestRelease({ owner, repo });
  // The release workflow builds the branch tip it checks out after the run is created, and publishes the GitHub
  // release before the images, so a run that fails after publishing may ship only some images. Creation is the latest
  // moment known to be released; a re-run moves run_started_at but keeps created_at.
  const { data: { workflow_runs: [releaseRun] } } = await github.rest.actions.listWorkflowRuns({
    owner, repo, workflow_id: workflowId, branch, status: 'success', per_page: 1,
  });
  const releasedAt = new Date(releaseRun ? releaseRun.created_at : latest.published_at);
  const releasedCommit = releaseRun ? releaseRun.head_sha : latest.tag_name;
  const releaseSource = releaseRun
    ? `${workflowId} run ${releaseRun.id} created at ${releaseRun.created_at} on ${releaseRun.head_sha}`
    : `${latest.tag_name}, published at ${latest.published_at}`;
  core.info(`Latest release: ${releaseSource}`);

  // The release workflow pushes "chore: bump version to x.y.z" commits, so those alone are no change
  const { data: comparison } = await github.rest.repos.compareCommitsWithBasehead({
    owner, repo, basehead: `${releasedCommit}...${branch}`,
  });
  const changes = comparison.commits.filter(c => !c.commit.message.startsWith('chore: bump version to '));
  const hasChanges = changes.length > 0 || comparison.ahead_by > comparison.commits.length;
  core.info(`Commits on ${branch} since ${releasedCommit}: ${comparison.ahead_by}, excluding version bumps: ${changes.length}`);

  const fixes = [
    ...await dependabotFixes({ github, owner, repo, releasedAt }),
    ...await codeScanningFixes({ github, owner, repo, branch, releasedAt }),
  ];

  // A weekly run falls at most 3.5 days from the anniversary. Waiting for the first run after it instead would
  // stretch the interval towards five weeks.
  const anniversary = new Date(releasedAt);
  anniversary.setUTCMonth(anniversary.getUTCMonth() + 1);
  const monthElapsed = now.getTime() >= anniversary.getTime() - 3.5 * DAY;

  let release = false;
  let reason;
  if (releaseRunning) {
    reason = `a ${workflowId} run is in progress`;
  } else if (failedRun) {
    reason = `${workflowId} run ${failedRun.id} on ${branch} concluded ${failedRun.conclusion}`;
  } else if (!hasChanges) {
    reason = `${branch} has no changes since ${releasedCommit}`;
  } else if (fixes.length > 0) {
    release = true;
    reason = 'security fixes since the latest release';
  } else if (monthElapsed) {
    release = true;
    reason = `a month has passed since the latest release`;
  } else {
    reason = `no security fixes, next monthly release is due around ${anniversary.toISOString().slice(0, 10)}`;
  }
  // A release run that started while the alerts were read may already publish these fixes, and one that finished
  // changed the latest release or the pause this decision relied on
  if (release && runState(await releaseRuns({ github, owner, repo, workflowId })) !== runState(runs)) {
    release = false;
    reason = `${workflowId} runs changed while deciding`;
  }
  const decision = `${release ? 'release' : 'skip'}: ${reason}`;
  core.info(`Decision: ${decision}`);

  await core.summary
    .addHeading('Auto release', 2)
    .addRaw(`Latest release: ${releaseSource}`, true)
    .addRaw(`Commits since the release, excluding version bumps: ${changes.length}`, true)
    .addRaw(`Decision: ${decision}${dryRun ? ' (dry run)' : ''}`, true)
    .addTable([
      ['Source', 'Alert', 'Severity', 'Subject', 'Fixed at'].map(data => ({ data, header: true })),
      ...fixes.map(f => [f.source, f.number ? `#${f.number}` : '', f.severity, f.subject, f.fixedAt]),
    ])
    .write();

  if (failedRun) {
    core.setFailed(`Automatic releases are paused: ${workflowId} run ${failedRun.id} on ${branch} concluded `
      + `${failedRun.conclusion} (${failedRun.html_url}). Depending on where it stopped, a new run fails at the same `
      + 'step or publishes the same commits again under the next version. Re-run its failed jobs, or start '
      + `${workflowId} once the cause is fixed; automatic releases resume after a ${workflowId} run on ${branch} `
      + 'succeeds.');
  }
  if (release && !dryRun) {
    await github.rest.actions.createWorkflowDispatch({ owner, repo, workflow_id: workflowId, ref: branch });
    core.info(`Started ${workflowId} on ${branch}`);
  }
  return { release, reason, fixes };
};

// Every run of the release workflow on every branch, newest first. A re-run keeps the creation time of its run, so
// only the whole list shows an old run that runs again.
function releaseRuns({ github, owner, repo, workflowId }) {
  return github.paginate(github.rest.actions.listWorkflowRuns, { owner, repo, workflow_id: workflowId, per_page: 100 });
}

const runState = runs => JSON.stringify(runs.map(r => [r.id, r.run_attempt, r.status, r.conclusion, r.updated_at]));

// Example applications and integration tests never ship
const UNSHIPPED_MANIFEST = /^apps\/examples\/|(^|\/)it-[^/]+\//;

// Only runtime dependencies ship. The Gradle dependency submission sets no scope, and its one manifest,
// settings.gradle.kts, also covers build tools such as Checkstyle and Error Prone, so its alerts do not count.
const ships = a => a.dependency.scope === 'runtime' && !UNSHIPPED_MANIFEST.test(a.dependency.manifest_path);

// A Dependabot alert is fixed once the manifest on the default branch no longer has the vulnerable version.
// Moving a manifest also fixes its alerts and opens the same ones at the new path, so an advisory that is still
// open for the same package in a shipped manifest does not count.
async function dependabotFixes({ github, owner, repo, releasedAt }) {
  const fixed = await github.paginate(github.rest.dependabot.listAlertsForRepo,
    { owner, repo, state: 'fixed', per_page: 100 });
  const open = await github.paginate(github.rest.dependabot.listAlertsForRepo,
    { owner, repo, state: 'open', per_page: 100 });
  const key = a => `${a.security_advisory.ghsa_id} ${a.dependency.package.ecosystem}:${a.dependency.package.name}`;
  const stillOpen = new Set(open.filter(ships).map(key));
  return fixed
    .filter(a => new Date(a.fixed_at) > releasedAt
      && ships(a)
      && !stillOpen.has(key(a)))
    .map(a => ({
      source: 'Dependabot',
      number: a.number,
      severity: a.security_advisory.severity,
      subject: `${a.dependency.package.name} (${a.dependency.manifest_path})`,
      fixedAt: a.fixed_at,
    }));
}

// An alert of Grype or Trivy covers the latest and main scans of an image together, so whether it is open or fixed does
// not show that main has a fix the released image lacks
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
