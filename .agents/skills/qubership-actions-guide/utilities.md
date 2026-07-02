# Utilities — apm-packages-update, wait-for-workflow, custom-event, store-input-params

## apm-packages-update

Runs `apm update --yes` in the current repository and opens a pull request with the
resulting changes. Designed to be placed in each consumer repository and triggered on a
schedule or manually.

### When to use

- A repository uses APM-managed skill packages and needs periodic updates.
- You want to automate APM package bumps without touching the repository manually.

### Prerequisites

- `apm.yml` must exist at the repository root.
- The specified `target` must be configured in `apm.yml` (the action adds it automatically
  if missing).

### Permissions

```yaml
permissions:
  contents: read
```

The `token` input must have permission to push branches and open pull requests.
Use the org-level secret `APM_UPDATE_TOKEN`.

### Inputs

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `branch` | No | `main` | Target branch to update |
| `target` | No | `claude` | APM target name in `apm.yml` |
| `token` | Yes | — | Use `secrets.APM_UPDATE_TOKEN` |

### Usage pattern

```yaml
name: Update APM packages

on:
  workflow_dispatch:
  schedule:
    - cron: "0 6 * * 1"

jobs:
  update:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - name: Update APM packages
        uses: netcracker/qubership-workflow-hub/actions/apm-packages-update@<resolved-sha>  # <resolved-tag>
        with:
          token: ${{ secrets.APM_UPDATE_TOKEN }}
```

---

## wait-for-workflow

Polls the GitHub API until a target workflow run starts and completes. Use when a
workflow must gate on another workflow that runs in parallel or in a different repo.

### When to use

- A caller workflow triggers a downstream workflow via `repository_dispatch` and needs
  its result before continuing.
- A PR workflow needs to wait for a separate build workflow to finish on the same commit.
- A `workflow_dispatch` job needs to wait for a specific run by ID.

### Matching logic

The action resolves which run to wait for using this precedence:

1. `workflow` is a numeric value → treated as a direct workflow **run ID**.
2. `workflow` ends with `.yml` or `.yaml` → treated as a **workflow filename**;
   the action then searches for the most recent run matching either:
   - `pr-number` (when provided) — looks for PR-triggered runs
   - `sha` (default: `github.sha`) — looks for runs on the same commit

### Permissions

```yaml
permissions:
  contents: read
  actions: read   # required to query workflow runs via API
```

### Inputs

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `workflow` | Yes | — | Workflow filename (e.g. `build.yml`) or numeric run ID |
| `token` | Yes | — | `secrets.GITHUB_TOKEN` is sufficient for same-repo queries |
| `sha` | No | `${{ github.sha }}` | Commit SHA used to match runs when `pr-number` not set |
| `pr-number` | No | `${{ github.event.pull_request.number }}` | PR number; takes precedence over `sha` for matching |
| `timeout` | No | `30` | Minutes to wait after the run starts before failing |
| `max-wait` | No | `10` | Minutes to wait for the run to appear before failing |
| `poll-interval` | No | `10` | Seconds between API polls |

### Outputs

| Output | Description |
| --- | --- |
| `conclusion` | `success`, `failure`, `cancelled`, etc. |
| `run-id` | The numeric ID of the run that was waited on |

The action exits with status code 1 if the run fails, is cancelled, or times out —
the job fails automatically without needing an explicit `if:` check.

### Usage pattern

```yaml
jobs:
  trigger:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - name: Dispatch downstream workflow
        uses: netcracker/qubership-workflow-hub/actions/custom-event@cabbb90e9471163cfac84bd50ff0296b2803b44c  # v2.3.0
        with:
          event-type: run-integration-tests
          client-payload: '{"ref": "${{ github.sha }}"}'
          github-token: ${{ secrets.GITHUB_TOKEN }}

  wait:
    needs: trigger
    runs-on: ubuntu-latest
    permissions:
      contents: read
      actions: read
    steps:
      - name: Wait for integration tests
        uses: netcracker/qubership-workflow-hub/actions/wait-for-workflow@cabbb90e9471163cfac84bd50ff0296b2803b44c  # v2.3.0
        with:
          workflow: integration-tests.yml
          token: ${{ secrets.GITHUB_TOKEN }}
          timeout: 60
          poll-interval: 15
```

---

## custom-event

Dispatches a `repository_dispatch` event to the same or a different repository with a
JSON payload. The receiving workflow must declare `on: repository_dispatch` with a
matching `types` filter.

### Token requirements

| Target | Token |
| --- | --- |
| Same repository | `secrets.GITHUB_TOKEN` is sufficient |
| Different repository | PAT or GitHub App token with `repo` scope on the target |

### Permissions (caller job)

```yaml
permissions:
  contents: read  # no additional permissions needed for same-repo dispatch
```

### Inputs

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `event-type` | Yes | — | Becomes `github.event.event_type` in the receiving workflow |
| `github-token` | Yes | — | Falls back to `GITHUB_TOKEN` env var if not set |
| `client-payload` | No | `{}` | Must be valid JSON string; max 10 KB; available as `github.event.client_payload` |
| `owner` | No | current owner | Target repo owner — provide both `owner` and `repo` for cross-repo dispatch |
| `repo` | No | current repo | Target repository name |

### Output

| Output | Description |
| --- | --- |
| `status` | HTTP status code — `204` on success |

### Receiving workflow pattern

```yaml
on:
  repository_dispatch:
    types: [deploy-staging]

jobs:
  handle:
    runs-on: ubuntu-latest
    steps:
      - run: echo "Version ${{ github.event.client_payload.version }}"
```

### Same-repo dispatch

```yaml
- uses: netcracker/qubership-workflow-hub/actions/custom-event@cabbb90e9471163cfac84bd50ff0296b2803b44c  # v2.3.0
  with:
    event-type: deploy-staging
    client-payload: '{"version": "1.2.3", "environment": "staging"}'
    github-token: ${{ secrets.GITHUB_TOKEN }}
```

### Cross-repo dispatch

```yaml
- uses: netcracker/qubership-workflow-hub/actions/custom-event@cabbb90e9471163cfac84bd50ff0296b2803b44c  # v2.3.0
  with:
    event-type: deploy-staging
    client-payload: '{"version": "1.2.3"}'
    github-token: ${{ secrets.PAT_TOKEN }}
    owner: my-org
    repo: my-other-repo
```

---

## store-input-params

Persists `workflow_dispatch` inputs as a JSON artifact at the start of a release
workflow. Downstream jobs download the artifact to reconstruct the original inputs
without relying on environment variables or job outputs.

**Do not pass secrets** via `workflow_dispatch` inputs — they appear in the GitHub UI
and in the artifact file.

### Inputs

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `input` | No | `{}` | JSON string of params to save — pass `${{ toJSON(inputs) }}` |
| `stored_file_name` | No | `input_params.json` | Filename inside the artifact |
| `artifact_name` | No | `input_params` | Artifact name used for download in downstream jobs |

### Pattern

Always place this step first in the `workflow_dispatch` job, before any side-effecting steps:

```yaml
on:
  workflow_dispatch:
    inputs:
      release:
        description: Release version
        required: true
        type: string
      environment:
        required: false
        type: string
        default: staging

jobs:
  store-params:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - uses: netcracker/qubership-workflow-hub/actions/store-input-params@cabbb90e9471163cfac84bd50ff0296b2803b44c  # v2.3.0
        with:
          input: ${{ toJSON(inputs) }}
```

Download in a downstream job:

```yaml
  downstream:
    needs: store-params
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c  # v8.0.1
        with:
          name: input_params
      - name: Read params
        run: cat input_params.json
```
