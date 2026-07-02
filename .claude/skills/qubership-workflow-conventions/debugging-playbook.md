# Debugging playbook

Use this when the user provides a broken workflow, failed run, or logs.

## First response goal

Find the first wrong step, not the loudest later error.

## Failure categories

### Trigger mismatch

Symptoms:

- workflow does not start;
- starts on wrong branch;
- does not run on tags;
- path filters skip unexpectedly.

Check:

- `on:` block;
- branch filters;
- tag filters;
- path filters;
- job-level `if:`;
- event name and ref.

### Permission denial

Symptoms:

- 403;
- permission denied;
- cannot push tag;
- cannot publish package/image;
- cannot create release;
- cloud auth rejected.

Check:

- job `permissions:`;
- selected action README;
- repository/org settings;
- protected branch/tag rules;
- environment approvals;
- token source.

### Missing or wrong input

Symptoms:

- action fails with missing input;
- action silently no-ops;
- output is empty;
- config not recognized.

This is the application of the *Anti-hallucination* rule (see
`SKILL.md` → *Mandatory conventions*) to a broken workflow. Re-read
the action's README via `qubership-actions-guide`, diff every `with:`
key against the documented inputs, and remove or correct any that
don't appear in the README.

### Path or artifact problem

Symptoms:

- file not found;
- artifact empty;
- release has no assets;
- Docker context missing.

Check:

- `working-directory`;
- checkout path;
- build output directory;
- upload/download artifact names;
- action-specific output paths.

### Toolchain drift

Symptoms:

- works locally, fails in GitHub Actions;
- dependency install fails;
- version mismatch.

Check:

- runner OS;
- setup action version;
- lockfile;
- Node/Python/Java/Maven versions;
- cache key.

### Rate limits and registry throttling

Symptoms:

- `429 Too Many Requests`;
- `toomanyrequests: You have reached your pull rate limit` (Docker Hub);
- `403` from GHCR/ECR/registry under load;
- npm/PyPI/Maven Central transient `503`;
- GitHub API `secondary rate limit` or `API rate limit exceeded`.

Check:

- which registry/API is throttling — log line shows the host;
- whether the workflow runs anonymously against Docker Hub (use a
  logged-in pull or a mirror);
- `GITHUB_TOKEN` vs PAT — `GITHUB_TOKEN` has higher API limits inside
  Actions than an unauthenticated request;
- matrix size and parallel job count — many concurrent jobs hitting the
  same registry trigger throttling;
- whether retries are configured for the failing step.

Fixes:

- log in to the registry before pulling (Docker Hub, GHCR);
- replace anonymous public pulls with authenticated ones;
- reduce matrix breadth or stagger jobs with `concurrency:`;
- add a retry wrapper for transient network failures (e.g. shell
  `for i in 1 2 3; do ... && break; sleep $((i*5)); done`) — only on
  idempotent steps.

### Boolean inputs evaluated as strings

Symptoms:

- `dry-run: true` is set, but the workflow still publishes;
- `if: inputs.flag == false` runs the step even when `flag` is unchecked;
- `${{ inputs.flag && 'a' || 'b' }}` always returns the same branch.

Cause:

- `workflow_dispatch` inputs always arrive as strings, even when
  declared `type: boolean`. The string `"false"` is truthy in
  expressions like `${{ inputs.flag }}`.
- `github.event.inputs.X` is also always a string, regardless of input
  type.
- Mixing the two access forms in the same workflow leads to inconsistent
  comparisons.

Fixes:

- Compare against string literals when the input came from a UI form:

  ```yaml
  if: ${{ github.event.inputs.dry-run == 'true' }}
  ```

- Use `inputs.X` (not `github.event.inputs.X`) inside `workflow_call`
  reusable workflows where the type is honoured.
- For Qubership actions that accept `dry-run`, pass the value as a
  quoted string to be safe:

  ```yaml
  with:
    dry-run: "${{ inputs.dry-run }}"
  ```

- Validate the conversion in a debug step before relying on it:

  ```yaml
  - run: echo "dry-run='${{ inputs.dry-run }}' (type-coerced)"
  ```

### Network and DNS failures

Symptoms:

- `dial tcp: lookup ... no such host`;
- `connection reset by peer`;
- `i/o timeout` during package install or image push;
- intermittent failure on the same step.

Check:

- whether the failure repeats on rerun (transient vs structural);
- runner image version — recent image bumps occasionally drop CA certs
  or change resolver behaviour;
- self-hosted runner egress rules (firewall, proxy, VPN);
- whether the step uses a hardcoded mirror that is currently down;
- IPv6/IPv4 dual-stack issues — some registries fail over IPv6 only.

Fixes:

- rerun once to rule out transient failure;
- pin the runner image (`runs-on: ubuntu-22.04` instead of
  `ubuntu-latest`) when a recent image change correlates with the break;
- for self-hosted runners, verify outbound allow-list covers the
  registry/API host;
- add explicit timeouts on long network calls so a hang fails fast
  instead of consuming the whole job timeout.

## Useful GitHub CLI commands

```bash
gh run list --limit 10
gh run view <run-id> --log-failed
gh run download <run-id> -D /tmp/gha-run
gh workflow view <workflow-name>
```

## Debugging output format

When debugging, return:

1. Probable root cause.
2. Evidence from the workflow/log.
3. Minimal patch.
4. Full corrected workflow if useful.
5. How to rerun and verify.
