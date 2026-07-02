# Zizmor rules — apply at workflow generation time

Apply every rule below while writing or editing a workflow.

---

## template-injection

**What to avoid:** `github.event.*` user-controlled values interpolated directly inside
`run:` scripts or `actions/github-script` `script:` input.

**Attacker-controlled sources — never use these inside `${{ }}` in a `run:` block:**

- `github.event.pull_request.title`
- `github.event.pull_request.body`
- `github.event.pull_request.head.ref`
- `github.event.pull_request.head.label`
- `github.event.issue.title`
- `github.event.issue.body`
- `github.event.comment.body`
- `github.event.review.body`
- `github.event.discussion.title`
- `github.event.discussion.body`
- `github.head_ref`
- `github.event.workflow_run.head_branch`
- `github.event.workflow_run.head_commit.message`

**Correct pattern — pass through `env:` var:**

```yaml
env:
  TITLE: ${{ github.event.pull_request.title }}
run: |
  echo "$TITLE"
```

---

## excessive-permissions

**What to avoid:** Overly broad permissions at workflow level or per job.

**Never write:**

- `permissions: write-all`
- `contents: write` at the top-level workflow (not inside a job)
- Any `write` permission a job does not actually need

**Correct pattern:**

```yaml
# Top of workflow — restrict everything
permissions: {}

jobs:
  build:
    permissions:
      contents: read
      packages: write   # only what this job actually needs
```

Start every job from `contents: read` and elevate only where the action's README requires it.

---

## unpinned-uses

**What to avoid:** Any `uses:` not pinned to a full 40-character SHA.

**Never write:**

```yaml
uses: actions/checkout@main          # branch ref
uses: actions/checkout@v4            # mutable tag
uses: actions/checkout@v4.2.2        # mutable tag
```

**Always write:**

```yaml
uses: actions/checkout@<40-char-sha>  # vX.Y.Z
```

Use the Pin table in `qubership-actions-guide` → *Pin table* for known SHAs.
For actions not in the Pin table — fetch the SHA via the API before writing.

---

## artipacked

**What to avoid:** `actions/checkout` without `persist-credentials: false` in a job
that also uploads artifacts — Git credentials can be leaked into the artifact archive.

**Flag if:** same job has both `actions/checkout` and `actions/upload-artifact`.

**Correct pattern:**

```yaml
- uses: actions/checkout@<sha>  # vX.Y.Z
  with:
    persist-credentials: false
```

Apply `persist-credentials: false` on every checkout unless the job explicitly needs
Git push access (e.g. tag creation, branch push).

---

## secrets-inherit

**What to avoid:** Passing all secrets to a reusable workflow via `secrets: inherit`.

**Never write:**

```yaml
jobs:
  call:
    uses: org/repo/.github/workflows/re-deploy.yml@<sha>
    secrets: inherit
```

**Correct pattern — pass only what the callee needs:**

```yaml
jobs:
  call:
    uses: org/repo/.github/workflows/re-deploy.yml@<sha>
    secrets:
      REGISTRY_TOKEN: ${{ secrets.REGISTRY_TOKEN }}
```

---

## dangerous-triggers

**What to avoid:** `pull_request_target` or `workflow_run` combined with checkout or
execution of untrusted PR head code.

**Flag if** the workflow uses `pull_request_target` or `workflow_run` AND any step:

- Checks out with `ref: ${{ github.event.pull_request.head.sha }}` or similar PR head ref
- Runs shell commands using `github.event.pull_request.*` input directly

**Correct pattern — safe checkout in privileged trigger:**

```yaml
- uses: actions/checkout@<sha>  # vX.Y.Z
  with:
    ref: ${{ github.sha }}           # base ref, not PR head
    persist-credentials: false
```

CLA assistant is one of the few legitimate uses of `pull_request_target` — it is safe
because it does not checkout or execute PR head code.

---

## github-env

**What to avoid:** Writing untrusted event data to `GITHUB_ENV` or `GITHUB_PATH`
in privileged trigger contexts (`pull_request_target`, `workflow_run`).

**Never write:**

```yaml
run: echo "BRANCH=${{ github.event.pull_request.head.ref }}" >> $GITHUB_ENV
```

**Correct pattern — use `GITHUB_OUTPUT` for inter-step data:**

```yaml
run: echo "branch=$BRANCH" >> $GITHUB_OUTPUT
env:
  BRANCH: ${{ github.event.pull_request.head.ref }}
```

---

## unredacted-secrets

**What to avoid:** Secret values transformed or concatenated in shell — GitHub's
automatic redaction may miss the transformed form.

**Never write:**

```yaml
run: |
  TOKEN="${{ secrets.MY_TOKEN }}"
  curl -H "Authorization: Bearer ${TOKEN:0:10}..."   # substring bypasses redaction
```

**Correct pattern:**

```yaml
run: |
  curl -H "Authorization: Bearer $MY_TOKEN"
env:
  MY_TOKEN: ${{ secrets.MY_TOKEN }}
```

If transformation is unavoidable, mask the result explicitly with `::add-mask::`.

---

## concurrency-limits

**What to avoid:** Missing `concurrency:` block on `push`- or `pull_request`-triggered
workflows — parallel runs can cause race conditions or wasted compute.

**Flag if:** workflow is triggered by `push` or `pull_request` and has no `concurrency:` block.

**Correct patterns — see `workflow-patterns.md` → *Concurrency* for full details:**

For CI (push / pull_request):

```yaml
concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

For release / publish (never cancel):

```yaml
concurrency:
  group: release-${{ github.ref }}
  cancel-in-progress: false
```

---

## github-app

**What to avoid:** GitHub App token passed directly via shell interpolation in a `run:` step.

**Never write:**

```yaml
run: |
  gh api ... --header "Authorization: Bearer ${{ steps.generate-token.outputs.token }}"
```

**Correct pattern — always via env var:**

```yaml
run: |
  gh api ... --header "Authorization: Bearer $APP_TOKEN"
env:
  APP_TOKEN: ${{ steps.generate-token.outputs.token }}
```

---

## secrets-outside-env

**What to avoid:** Jobs that use `${{ secrets.* }}` without a `environment:` block —
secrets should be scoped to GitHub Environments with protection rules for production workflows.

**Flag if:** a deployment or publish job uses secrets but has no `environment:` declaration.

**Correct pattern:**

```yaml
jobs:
  deploy:
    environment: production
    permissions:
      contents: read
    steps:
      - run: deploy.sh
        env:
          DEPLOY_TOKEN: ${{ secrets.DEPLOY_TOKEN }}
```

Note: not every workflow needs an environment — apply judgment. Release and deployment
workflows publishing to production registries or environments should always declare one.

---

## bot-conditions

**What to avoid:** Checking `github.actor` by string comparison to detect bots —
anyone can create an account named `dependabot[bot]`.

**Never write:**

```yaml
if: github.actor == 'dependabot[bot]'
```

**Correct pattern:**

```yaml
if: github.actor_id == '49699333'        # dependabot's stable numeric ID
# or
if: github.event.sender.type == 'Bot'
```

---

## unsound-contains

**What to avoid:** Using `contains()` with attacker-controlled input as the haystack
in security-sensitive conditions — an attacker can craft a label name that satisfies
the check.

**Never write:**

```yaml
if: contains(github.event.label.name, 'safe-to-run')
```

**Correct pattern — use exact equality for security gates:**

```yaml
if: github.event.label.name == 'safe-to-run'
```

---

## misfeature

**What to avoid:** GitHub Actions features that are unsafe or misleading:

- `continue-on-error: true` on security-sensitive steps (scan, lint, auth) — failures are silently swallowed
- `workflow_dispatch` inputs used unsafely in shell without sanitisation
- `actions/github-script` with untrusted input in `script:`

**Never write:**

```yaml
- name: Security scan
  continue-on-error: true   # failure is silently ignored
```

**Correct pattern — only use `continue-on-error` on genuinely optional steps,
never on steps whose failure should block the workflow:**

```yaml
- name: Security scan
  continue-on-error: false  # default — omit the line entirely
```

For `workflow_dispatch` inputs in shell — always pass through `env:`, same as template-injection rule.

---

## unsound-condition

**What to avoid:** `if:` conditions that are inadvertently always `true` due to
YAML/expression type coercion — e.g. comparing a string context value to a non-string.

**Flag if:** an `if:` expression compares `github.event_name` or a similar string
context value using `==` where type coercion could make it always evaluate to `true`.

**Common mistake:**

```yaml
if: ${{ github.event.inputs.dry-run }}   # always true if input is the string "false"
```

**Correct pattern — explicit string comparison:**

```yaml
if: ${{ github.event.inputs.dry-run == 'true' }}
```

For boolean inputs from `workflow_dispatch`, always compare to the string `'true'` or `'false'`.
