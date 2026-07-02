# Cleanup — stale container images and Maven packages

## Template coverage

| Package type | Starting point |
| --- | --- |
| Container images | Fetch `cleanup-old-docker-container.yaml` from `Netcracker/.github/workflow-templates` and adapt it. |
| Maven packages | No template exists — build from scratch using the pipeline shape and inputs reference below. |

---

## Clarifying questions

Extract answers from the user's message first. Ask only what is missing.
Trigger and runner are inferred per `workflow-patterns.md` → *Trigger rules* — do not ask.

| Question | What it controls |
| --- | --- |
| Container images or Maven packages? | Determines which pipeline shape and filter logic to use |
| How many days to keep? | Maps to `threshold-days`. Default: `7` — apply without asking unless user specifies otherwise |
| Any tags/versions that must never be deleted? | Maps to `excluded-tags` (container) or `excluded-patterns` (Maven) |

---

## Action: `container-package-cleanup`

Auto-discovers all packages for the repo via `GITHUB_REPOSITORY` — no package list needed.

**Required env on the action step:**

```yaml
env:
  PACKAGE_TOKEN: ${{ secrets.GH_RWD_PACKAGE_TOKEN }}
```

`PACKAGE_TOKEN` must have `read:packages` + `delete:packages` scopes.
`GITHUB_TOKEN` does **not** have `delete:packages` — a dedicated PAT or GitHub App token is required.

**Permissions:** `contents: read` — the action operates only on the package registry, not repo contents.

---

## Container images

### Tag filter logic

Filters are applied in this order:

1. Candidates: all versions older than `threshold-days`
1. If `included-tags` is set — keep only versions whose tags match (wildcard `*` / `?` supported)
1. Remove any version whose tags match `excluded-tags` — these are never deleted regardless of age

**`excluded-tags` recommended value:**

```text
release*,semver,main,latest,*.*,*.*.*
```

- `release*` — any tag starting with "release"
- `semver` — literal semver marker tag
- `main`, `latest` — floating tags always kept
- `*.*`, `*.*.*` — semver-like tags (`1.2`, `1.2.3`, `v1.2.3`) — protects versioned release images

Do not drop the `*.*` / `*.*.*` patterns without a specific reason.

### Pipeline shape (container)

When adapting the template, the canonical `workflow_dispatch` inputs block is:

```yaml
on:
  schedule:
    - cron: "0 0 * * 0"  # every Sunday at midnight
  workflow_dispatch:
    inputs:
      threshold-days:
        description: "Number of days to keep container versions"
        required: false
        default: "7"
      included-tags:
        description: "Tags to include for deletion"
        required: false
        default: "*"
      excluded-tags:
        description: "Tags to exclude from deletion"
        required: false
        default: "release*,semver,main,latest,*.*,*.*.*"  # semver patterns protect versioned release images
      dry-run:
        description: "Enable dry-run mode"
        required: false
        default: true
        type: boolean
      debug:
        description: "Enable debug mode"
        required: false
        default: false  # action default is true — set false to reduce log noise
        type: boolean
```

**Always set `dry-run: true` as the `workflow_dispatch` default.** Operators preview before committing to deletion. Scheduled runs can use `false` after filters are validated.

---

## Maven packages

No org template — build from scratch.

### Pattern logic

- `included-patterns` **not set**: action automatically targets `*SNAPSHOT*` only — safe default for most repos.
- `included-patterns` **set**: only versions matching the pattern are candidates.
- `excluded-patterns`: versions matching these are never deleted.
- `threshold-versions`: keep this many newest versions per package (default: `1`).

### Common Maven use cases

| Goal | Configuration |
| --- | --- |
| Delete old SNAPSHOTs (default) | No patterns needed — `*SNAPSHOT*` is the automatic default |
| Delete old SNAPSHOTs but keep last 3 | `threshold-versions: 3` |
| Delete specific release versions | `included-patterns: "1.0.*"` |
| Protect a specific version | `excluded-patterns: "1.2.3-SNAPSHOT"` |

### Pipeline shape (Maven)

```yaml
on:
  schedule:
    - cron: "0 0 * * 0"  # every Sunday at midnight
  workflow_dispatch:
    inputs:
      threshold-days:
        description: "Number of days to keep versions"
        required: false
        default: "7"
      threshold-versions:
        description: "Number of newest versions to keep per package"
        required: false
        default: "1"
      included-patterns:
        description: "Patterns to include for deletion (default: *SNAPSHOT*)"
        required: false
        default: ""
      excluded-patterns:
        description: "Patterns to exclude from deletion"
        required: false
        default: ""
      dry-run:
        description: "Enable dry-run mode"
        required: false
        default: true
        type: boolean
      debug:
        description: "Enable debug mode"
        required: false
        default: false  # action default is true — set false to reduce log noise
        type: boolean
```

---

## Inputs reference

| Input | Action default | Description |
| --- | --- | --- |
| `package-type` | `container` | `container` or `maven` |
| `threshold-days` | `7` | Delete versions older than this many days |
| `threshold-versions` | `1` | *(Maven only)* Keep this many newest versions per package |
| `included-tags` | `""` | *(Container)* Only delete versions matching these tags. Wildcard `*`/`?` supported |
| `excluded-tags` | `""` | *(Container)* Never delete versions matching these tags |
| `included-patterns` | `""` | *(Maven)* Candidates for deletion. When empty: `*SNAPSHOT*` applied automatically |
| `excluded-patterns` | `""` | *(Maven)* Never delete versions matching these patterns |
| `dry-run` | `false` | Print what would be deleted without actually deleting |
| `debug` | `true` | Extra debug logs — set `false` in production to reduce noise |
| `batch-size` | `15` | Parallel deletion threads per package |
| `max-errors` | `5` | Stop after this many errors |
