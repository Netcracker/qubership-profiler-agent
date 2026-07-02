# Security model

This file covers patterns that go beyond the baseline rules in
`SKILL.md` (*Mandatory conventions → Permissions* and *→ Secrets and PR
safety*). The baseline rules are not repeated here.

## Pull request safety

Treat pull requests from forks as **untrusted code execution**. Beyond
the baseline rule "do not expose secrets to untrusted PR code", apply
these patterns:

- For workflows that must run on PRs from forks, split them: a safe job
  on `pull_request` (no secrets, read-only checkout) and a privileged
  job on `pull_request_target` only when strictly necessary.
- When using `pull_request_target`, **never** check out the PR head
  ref into a step that executes scripts, build tools, or installs
  dependencies. Doing so runs untrusted code with access to repo
  secrets.
- The CLA assistant template is one of the few legitimate uses of
  `pull_request_target` because it does not check out PR code at all.

## Checkout credentials

`actions/checkout` persists `GITHUB_TOKEN` in the local git config by
default (`persist-credentials: true`). Any later step in the same job
can use that token to push, even if the job's intent is read-only.
This is a frequent source of accidental writes from compromised
build-tool plugins or malicious dependencies.

For read-only jobs (validate, build, test, scan), set
`persist-credentials: false`:

```yaml
- uses: actions/checkout@<sha>  # vX.Y.Z
  with:
    persist-credentials: false
```

Keep the default (`persist-credentials: true`) only when a later step
in the same job genuinely needs the token to perform git operations
(push tags, push commits, create branches). Even then, prefer using a
short-lived GitHub App token (see *GitHub App tokens* below) over the
persisted `GITHUB_TOKEN` for write operations.

This pattern is followed by every Netcracker workflow template's
checkout step — preserve it when forking.

## Environment-scoped secrets

For production deployment workflows, prefer **environment-scoped
secrets** over repository-wide secrets. Environment scoping limits
exposure to jobs that opt into the environment and lets you require
manual approval before the secret is materialised:

```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production  # secrets only available here
    steps:
      - run: deploy.sh
        env:
          REGISTRY_TOKEN: ${{ secrets.PROD_REGISTRY_TOKEN }}
```

Configure environment-scoped secrets at:

```text
GitHub → Repository → Settings → Environments → <name> → Secrets
```

## GitHub App tokens for protected branches

`GITHUB_TOKEN` cannot push to a branch protected with required reviews
or required status checks. When a release workflow needs to commit
version bumps or tags to a protected default branch (Maven release,
Python release, etc.), authenticate via a GitHub App token instead.

```yaml
- name: Prepare app token
  id: app-token
  uses: actions/create-github-app-token@<sha>  # vX.Y.Z
  with:
    app-id: ${{ vars.GH_BUMP_VERSION_APP_ID }}
    private-key: ${{ secrets.GH_BUMP_VERSION_APP_KEY }}

- name: Checkout
  uses: actions/checkout@<sha>  # vX.Y.Z
  with:
    token: ${{ steps.app-token.outputs.token }}
```

Rules:

- The App must have `Contents: write` and any other permissions the
  release step requires (e.g. `Pull requests: write` for auto-PR).
- Add the App to the branch protection bypass list — otherwise the
  token gets the same restrictions as `GITHUB_TOKEN`.
- Store `app-id` as an org/repo variable (`vars.GH_BUMP_VERSION_APP_ID`)
  and the private key as a secret (`secrets.GH_BUMP_VERSION_APP_KEY`).
- Fall back to `secrets.GITHUB_TOKEN` if the App vars are unset, so
  unprotected repos still work:

  ```yaml
  token: ${{ steps.app-token.outputs.token || secrets.GITHUB_TOKEN }}
  ```

This pattern is used in `maven-release-v2.yaml` and `python-release.yaml`
templates.

## Environments

For production release/deploy operations, prefer GitHub Environments:

```yaml
environment: production
```

Environments add three things on top of plain secrets:

1. **Approvals** — require named reviewers before the job starts.
2. **Wait timer** — delay the job for N minutes after approval.
3. **Branch restrictions** — only allow specific branches to deploy.

Configure at:

```text
GitHub → Repository → Settings → Environments → <name>
```

Explain required environment approvals to the user when generating a
deployment workflow.
