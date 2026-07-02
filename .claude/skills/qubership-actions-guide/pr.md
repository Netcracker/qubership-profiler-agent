# PR automation — assigners and commit messages

## Clarifying questions

Extract answers from the user's message first. Ask only what is missing.
Trigger and runner are inferred per `workflow-patterns.md` → *Trigger rules* — do not ask.

Before asking — check whether a CODEOWNERS file exists in the repo. The action searches three locations in order: `.github/CODEOWNERS`, `CODEOWNERS` (repo root), `docs/CODEOWNERS`. If any of these exists, use CODEOWNERS mode and do not ask. Only ask if none of the three exist or the user explicitly requests a config-file override.

| Question | What it controls |
| --- | --- |
| How many reviewers to assign? | Maps to `shuffle` input. Default: 1 — apply without asking unless user specifies otherwise. |

---

## `pr-assigner`

Auto-assigns reviewers to PRs. **Prefer CODEOWNERS** — it is the zero-config path and is already the standard for most repos. Use the config file only when CODEOWNERS doesn't exist or you need to override the reviewer pool for a specific repo.

**Trigger:** `pull_request` (opened, reopened, synchronize) — NOT `pull_request_target`.
`pull_request_target` is only needed when secrets are required from fork PRs. `pr-assigner` only needs `pull-requests: write` which is available on `pull_request`.

**Permissions:** `pull-requests: write`, `contents: read`

**Required env:**

```yaml
env:
  GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### CODEOWNERS mode (preferred)

No config file needed. The action searches three locations in order and uses the first file found:

1. `.github/CODEOWNERS`
1. `CODEOWNERS` (repo root)
1. `docs/CODEOWNERS`

Selection logic within the file:

- Prefers the `*` (wildcard) rule if present.
- Falls back to the last non-empty, non-comment line if no wildcard rule exists.

Use `shuffle` input to control how many reviewers are assigned.

### Config file mode (override)

Use only when CODEOWNERS is absent or its reviewer pool needs a per-repo override.

Default path: `.github/pr-assigner-config.yml`. Pass custom path via `configuration-path` input.

```yaml
assignees:
  - username1
  - username2
count: 2
```

| Field | Description |
| --- | --- |
| `assignees` | **Required.** List of GitHub usernames to assign from |
| `count` | **Required.** Number of reviewers to assign |

### Key inputs

| Input | Description |
| --- | --- |
| `shuffle` | Number of assignees to assign — overrides config `count` |
| `configuration-path` | Path to config file. Default: `.github/pr-assigner-config.yml` |
| `self-assign` | `true` — allow PR author to be assigned if they appear in the assignee list. Default: `false` |

### Pipeline

```text
pull_request → pr-assigner
               reads config or CODEOWNERS, assigns reviewers
```

Action ref (from Pin table):

```yaml
uses: netcracker/qubership-workflow-hub/actions/pr-assigner@<sha>  # vX.Y.Z
```

### Fork safety note

For fork PRs the action skips silently — write permissions are not available on fork PRs with `pull_request` trigger. This is expected behaviour. If assignment for fork PRs is needed, use `pull_request_target` with an explicit fork check step before the action.

---

## `pr-add-messages`

Appends commit messages from the PR to the PR description. No inputs required.

**Trigger:** `pull_request` (opened, synchronize)
**Permissions:** `pull-requests: write`

### Pipeline

```text
pull_request → pr-add-messages
               collects commit messages, appends to PR description
```

No config file needed. No inputs needed.
