'use strict';

const DAY = 24 * 3600 * 1000;
const imageReleaseFixes = require('./image-release-fixes.cjs');

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
 * The decision is posted as a notice. The log and the job summary list the alerts it counted, the Dependabot and source
 * code scanning alerts fixed since the release that it did not count with the reason, and the image pairs it could not
 * compare with the condition that failed. Grype and Trivy alerts are judged only by the image comparison, and Scorecard
 * alerts are left out of both lists. A fixed Dependabot alert whose advisory is still open in a shipped dependency is
 * reported only as a count, because the log and the summary of this public repository are visible to readers who cannot
 * see its Dependabot alerts.
 *
 * @param {object} args
 * @param {object} args.github the Octokit client of actions/github-script
 * @param {object} args.context the workflow context of actions/github-script
 * @param {object} args.core the @actions/core module of actions/github-script
 * @param {Date} args.now the time of this run
 * @param {string} args.branch the branch to release from
 * @param {string} args.workflowId the file name of the release workflow
 * @param {boolean} args.dryRun reports the decision without starting the release workflow
 * @returns {Promise<{release: boolean, reason: string, fixes: object[], notCounted: object[], stillOpen: number,
 *   images: object}>} the decision; the alerts it counted; the alerts fixed since the release that it did not count,
 *   except those whose advisory is still open in a shipped dependency; the number of those; and the image comparison
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
  core.info(`Latest GitHub release: ${latest.tag_name}, published at ${latest.published_at}`);
  core.info(`Counting from: ${releaseSource}`);

  // The release workflow pushes "chore: bump version to x.y.z" commits, so those alone are no change
  const { data: comparison } = await github.rest.repos.compareCommitsWithBasehead({
    owner, repo, basehead: `${releasedCommit}...${branch}`,
  });
  const changes = comparison.commits.filter(c => !c.commit.message.startsWith('chore: bump version to '));
  const hasChanges = changes.length > 0 || comparison.ahead_by > comparison.commits.length;
  core.info(`Commits on ${branch} since ${releasedCommit}: ${comparison.ahead_by}, excluding version bumps: ${changes.length}`);

  const dependabot = await dependabotFixes({ github, owner, repo, releasedAt });
  const codeScanning = await codeScanningFixes({ github, owner, repo, branch, releasedAt });
  const fixes = [...dependabot.fixes, ...codeScanning.fixes];
  const notCounted = [...dependabot.notCounted, ...codeScanning.notCounted];

  // A weekly run falls at most 3.5 days from the anniversary. Waiting for the first run after it instead would
  // stretch the interval towards five weeks.
  const anniversary = new Date(releasedAt);
  anniversary.setUTCMonth(anniversary.getUTCMonth() + 1);
  const monthElapsed = now.getTime() >= anniversary.getTime() - 3.5 * DAY;

  const images = await imageReleaseFixes({ github, owner, repo, branch, workflowId, latest, now });
  core.info(images.status);
  fixes.push(...images.fixes);
  for (const f of fixes) core.info(`Counted: ${describeAlert(f)}`);
  for (const f of notCounted) core.info(`Not counted: ${describeAlert(f)}: ${f.reason}`);
  const stillOpenLine = dependabot.stillOpen > 0
    ? `Not counted: ${dependabot.stillOpen} Dependabot ${dependabot.stillOpen === 1 ? 'alert' : 'alerts'} fixed since `
      + 'the release, because the same advisory is still open for the same package in a shipped dependency; the '
      + 'Dependabot alerts of the repository list them'
    : null;
  if (stillOpenLine) core.info(stillOpenLine);

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
    const uncompared = images.total - images.compared;
    reason = 'no security fixes found'
      + (uncompared > 0 ? `, ${uncompared} of ${images.total} image pairs not compared` : '')
      + `; the next monthly release is due around ${anniversary.toISOString().slice(0, 10)}`;
  }
  // A release run that started while the alerts and images were read may already publish these fixes, and one that
  // finished changed the latest release or the pause this decision relied on
  if (release && runState(await releaseRuns({ github, owner, repo, workflowId })) !== runState(runs)) {
    release = false;
    reason = `${workflowId} runs changed while deciding`;
  }
  const decision = `${release ? 'release' : 'skip'}: ${reason}${dryRun ? ' (dry run)' : ''}`;
  core.info(`Decision: ${decision}`);
  core.notice(decision, { title: 'Auto release decision' });

  const summary = core.summary
    .addHeading('Auto release', 2)
    .addRaw(`Decision: ${decision}`, true)
    .addList([
      `Latest GitHub release: ${latest.tag_name}, published at ${latest.published_at}`,
      `Counting from: ${releaseSource}`,
      `Commits since then, excluding version bumps: ${changes.length}`,
    ])
    .addHeading('Counted security fixes', 3);
  if (fixes.length > 0) {
    summary.addTable([
      ['Source', 'Alert', 'Severity', 'Subject', 'Fixed at'].map(data => ({ data, header: true })),
      ...fixes.map(f => [f.source, f.number ? `#${f.number}` : '', f.severity, f.subject, f.fixedAt]),
    ]);
  } else {
    summary.addRaw('None.', true);
  }
  if (notCounted.length > 0 || stillOpenLine) summary.addHeading('Fixed since the release, not counted', 3);
  if (notCounted.length > 0) {
    summary.addTable([
      ['Source', 'Alert', 'Subject', 'Fixed at', 'Reason'].map(data => ({ data, header: true })),
      ...notCounted.map(f => [f.source, `#${f.number}`, f.subject, f.fixedAt, f.reason]),
    ]);
  }
  if (stillOpenLine) summary.addRaw(stillOpenLine, true);
  summary.addHeading('Images', 3).addRaw(images.headline, true);
  if (images.details.length > 0) summary.addList(images.details);
  await summary.write();

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
  return { release, reason, fixes, notCounted, stillOpen: dependabot.stillOpen, images };
};

const describeAlert = f => `${f.source}${f.number ? ` #${f.number}` : ''} ${f.subject}, fixed at ${f.fixedAt}`;

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
function unshipped(a) {
  if (a.dependency.scope !== 'runtime') {
    return `the dependency scope is ${a.dependency.scope ?? 'not set'}, not runtime`;
  }
  if (UNSHIPPED_MANIFEST.test(a.dependency.manifest_path)) return `${a.dependency.manifest_path} does not ship`;
  return null;
}

// A Dependabot alert is fixed once the manifest on the default branch no longer has the vulnerable version.
// Moving a manifest also fixes its alerts and opens the same ones at the new path, so an advisory that is still
// open for the same package in a shipped manifest does not count. Such alerts are returned as a count in stillOpen,
// not in notCounted, so that no report names a vulnerability that still ships. Advisories open only in manifests that
// do not ship are not withheld.
async function dependabotFixes({ github, owner, repo, releasedAt }) {
  const fixed = await github.paginate(github.rest.dependabot.listAlertsForRepo,
    { owner, repo, state: 'fixed', per_page: 100 });
  const open = await github.paginate(github.rest.dependabot.listAlertsForRepo,
    { owner, repo, state: 'open', per_page: 100 });
  const key = a => `${a.security_advisory.ghsa_id} ${a.dependency.package.ecosystem}:${a.dependency.package.name}`;
  const openKeys = new Set(open.filter(a => !unshipped(a)).map(key));
  const recent = fixed.filter(a => new Date(a.fixed_at) > releasedAt);
  // A fixed alert in a manifest that does not ship is withheld too: its advisory and package are those of an open alert
  const stillOpen = recent.filter(a => openKeys.has(key(a)));
  return {
    ...split(recent.filter(a => !stillOpen.includes(a)), a => ({
      source: 'Dependabot',
      number: a.number,
      severity: a.security_advisory.severity,
      subject: `${a.dependency.package.name} (${a.dependency.manifest_path})`,
      fixedAt: a.fixed_at,
      reason: unshipped(a),
    })),
    stillOpen: stillOpen.length,
  };
}

// Splits alerts into the fixes that count and the ones whose reason says why not
function split(alerts, describe) {
  const fixes = [];
  const notCounted = [];
  for (const { reason, ...fix } of alerts.map(describe)) {
    if (reason) notCounted.push({ ...fix, reason });
    else fixes.push(fix);
  }
  return { fixes, notCounted };
}

// Image alerts can stay globally open while latest is vulnerable and main is fixed; compare their analyses separately.
const IMAGE_SCANNERS = new Set(['Grype', 'Trivy']);

async function codeScanningFixes({ github, owner, repo, branch, releasedAt }) {
  const fixed = await github.paginate(github.rest.codeScanning.listAlertsForRepo,
    { owner, repo, state: 'fixed', ref: `refs/heads/${branch}`, per_page: 100 });
  // Scorecard reports repository settings, not vulnerabilities, so its alerts are left out of both lists
  const candidates = fixed.filter(a => new Date(a.fixed_at) > releasedAt
    && a.tool.name !== 'Scorecard'
    && !IMAGE_SCANNERS.has(a.tool.name));
  return split(candidates, a => ({
    source: a.tool.name,
    number: a.number,
    severity: a.rule.security_severity_level,
    subject: `${a.rule.id} (${a.most_recent_instance.category})`,
    fixedAt: a.fixed_at,
    // A rule without security_severity_level is no vulnerability
    reason: a.rule.security_severity_level ? null : 'the rule has no security severity',
  }));
}
