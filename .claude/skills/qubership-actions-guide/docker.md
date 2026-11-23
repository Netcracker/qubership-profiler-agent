# Docker — config, pipelines, and security

> **Note:** `docker-config-resolver` is deprecated — superseded by `config-resolver`.
> In docker mode, `config-resolver` keeps the same output shape for valid Docker configs and
> adds stricter validation (`components` is required), plus an optional `schema` input for
> non-docker configs. Use `config-resolver` when the pinned release contains
> `actions/config-resolver`; for older releases `docker-config-resolver` remains valid.
> All pipeline shapes in this guide apply to both.

## Migrating an existing workflow

1. Read the workflow file (ask user for path or let them paste it).
2. Find and read any config files referenced in the workflow — component list,
   registries, and build settings may already be defined there.
3. Extract all existing information (images, Dockerfiles, registry, platforms)
   before asking the user anything — do not ask for information that can be read.
4. Replace non-Qubership patterns using the table below, then go to *Collect requirements* for anything still missing.

| Existing pattern | Replace with |
| --- | --- |
| `docker/build-push-action` | `docker-action` |
| Manual `docker build` + `docker push` shell steps | `docker-action` |
| `docker/metadata-action` for tags | `metadata-action` |
| Manual tag string construction in shell | `metadata-action` |
| Hardcoded component list in matrix or env vars | `config-resolver` + config file, or inline JSON array |
| Non-Qubership config file format (custom YAML/JSON) | Migrate into `.qubership/docker.cfg` format |
| `docker login` steps | Built into `docker-action` via `registry` + `GITHUB_TOKEN` env |
| `actions/download-artifact` before Docker build | `download-artifact: true` input on `docker-action` |

---

## Collect requirements

Extract answers from the user's message first. Ask only what is missing and required to generate the workflow.
Trigger and runner are inferred per `workflow-patterns.md` → *Trigger rules* — do not ask.

If the user's request contains "release", "publish", or "tag" — load `release.md` immediately, do not ask.

| # | Question | What it controls |
| - | --- | --- |
| 1 | Should there be a dry-run mode? | `dry-run` input + conditional — builds without pushing |
| 2 | Registry: GHCR / Docker Hub / both? | `registry` input and auth method |

Config file — do not ask, determine from path:

- **Path A** (existing workflow/configs): config file was already found when reading the workflow — use it.
- **Path B** (from scratch): no config file exists — generate it (see *Config file generation*). Never ask the user to fill in a template.
- Never put image name, Dockerfile path, platforms, or build-context into workflow inputs — always use config file.

Defaults — do not ask, apply automatically:

- Platforms: `linux/amd64,linux/arm64` — set in `defaults` of config file (see *Config file schema*)
- `extra-tags` input always included in `workflow_dispatch` — user removes if not needed
- Image names / Dockerfiles: placeholder `"name": "your-image"` in generated config — user fills in
- Build step before Docker: only ask if user explicitly mentions Maven/npm/Go/etc

---

## Decide and generate

Use collected answers to pick the pipeline and generate output in this order:

1. **Path B only:** no config file exists → generate it first (see *Config file generation* below), write it, show it, ask user to confirm or adjust.
2. Then generate the workflow.

### Pipeline selection

**Release workflows MUST include `tag-action` (check) and `tag-action` (create) before any build step. Never omit tag creation in a release workflow — even if the user didn't mention it explicitly. "Release" implies tag.**

| Config file? | Release? | Pipeline |
| --- | --- | --- |
| No | No | `metadata-action` → `docker-action` (inline component) |
| Yes | No | `config-resolver` → `metadata-action` → `docker-action` (matrix) |
| No | Yes | `tag-action` (check) → `tag-action` (create) → `docker-action` → `github-release` |
| Yes | Yes | `tag-action` (check) → `config-resolver` → `tag-action` (create) → `docker-action` (matrix) → `github-release` |

If release assets needed → append `assets-action` after `github-release` in any release pipeline.

If build step in separate job → prepend build job with `upload-artifact`, add `download-artifact: true` to `docker-action`.

For release details (tag-action inputs, assets-action patterns, permissions) — see `release.md`.

**Checkout:** `docker-action` checks out internally (`checkout: "true"` by default). Only set `ref` in release workflows — pass `refs/tags/v${{ inputs.release }}` so the build uses the tag created in the previous job. For CI builds (push/PR), omit `ref` entirely — the action uses the current commit. A separate explicit checkout is still needed in any job that reads files before the build (e.g. `resolve-config` job reading the config file).

### Tag strategy per trigger

Use different `metadata-action` templates depending on the trigger — they serve different purposes:

| Trigger | `default-template` | Result |
| --- | --- | --- |
| `push` to branch / `pull_request` | `"{{ref-name}},{{short-sha}}"` | Branch tag + short commit SHA — temporary, identifies the build |
| `workflow_dispatch` release | `"{{ref-name}},{{major}}.{{minor}},{{major}},latest"` | Full semver set — `v1.2.3`, `1.2`, `1`, `latest` |

When the workflow handles both triggers (push + workflow_dispatch), use two separate `metadata-action` steps with `if:` conditions and merge the outputs:

```yaml
- id: metadata-release
  if: github.event_name == 'workflow_dispatch'
  uses: netcracker/qubership-workflow-hub/actions/metadata-action@<sha>  # vX.Y.Z
  with:
    ref: refs/tags/v${{ inputs.release }}
    default-template: "{{ref-name}},{{major}}.{{minor}},{{major}},latest"
    extra-tags: ${{ inputs.extra-tags }}

- id: metadata-push
  if: github.event_name == 'push'
  uses: netcracker/qubership-workflow-hub/actions/metadata-action@<sha>  # vX.Y.Z
  with:
    ref: ${{ github.ref }}
    default-template: "{{ref-name}},{{short-sha}}"
```

Then pass to `docker-action`:

```yaml
tags: ${{ steps.metadata-release.outputs.result || steps.metadata-push.outputs.result }}
```

### Key action inputs reference

**`config-resolver` / `docker-config-resolver` (deprecated):**

- `file-path` — path to config file
- `schema` — `config-resolver` only, optional: empty or `docker/v1` for docker mode; any
  other value flattens the file generically (see the action README)

**`metadata-action`:**

- `ref` — the ref to render tags from; for release use `refs/tags/v${{ inputs.release }}`, for push use `${{ github.ref }}`
- `default-template` — tag template (see *Tag strategy* above)
- `extra-tags` — additional tags from `workflow_dispatch` input

**`docker-action`:**

- `component` — `${{ toJson(matrix.component) }}` (with config) or inline JSON array (without config)
- `platforms` — `${{ matrix.component.platforms }}` (with config) or direct value
- `tags` — from `metadata-action` output or direct
- `registry` — e.g. `ghcr.io`
- `dry-run` — `"true"` / `"false"`
- `ref` — set only in release workflows: `refs/tags/v${{ inputs.release }}`; omit for CI builds
- `checkout` — default `"true"`; set `"false"` only if repo already checked out in the same job
- `download-artifact` / `download-artifact-name` / `download-artifact-path` — for separate build job

---

## Config file generation

When config file is wanted but doesn't exist — generate it from collected answers.
Do not ask the user to fill in a template. Write the file, show it, say "adjust if anything looks wrong."

Minimal generated example (two images, GHCR):

```json
{
  "registry": "ghcr.io",
  "defaults": {
    "platforms": "linux/amd64,linux/arm64",
    "dockerfile": "Dockerfile",
    "build_context": "."
  },
  "components": [
    {
      "name": "my-service"
    },
    {
      "name": "my-worker",
      "dockerfile": "worker/Dockerfile",
      "build_context": "worker"
    }
  ]
}
```

Default path: `.qubership/docker.cfg`. Confirm with user before writing.
Fields not set in a component are inherited from `defaults`.

---

## Config file schema

The config file is the central component registry for the repo. `docker-config-resolver`
reads it and outputs a JSON matrix used by any downstream job — build, scan, cleanup, deploy, etc.
The filename is arbitrary — path is passed via `file-path` input. Convention: `docker.cfg`.

```json
{
  "registry": "ghcr.io",
  "security": {
    "scan": true,
    "tag": "latest",
    "only_high_critical": true,
    "trivy_scan": true,
    "grype_scan": true,
    "only_fixed": true,
    "continue_on_error": true
  },
  "defaults": {
    "platforms": "linux/amd64,linux/arm64",
    "dockerfile": "Dockerfile",
    "build_context": "."
  },
  "components": [
    {
      "name": "my-service",
      "dockerfile": "Dockerfile",
      "build_context": ".",
      "platforms": "linux/amd64",
      "arguments": "NODE_ENV=production",
      "security": {
        "scan": true,
        "tag": "latest",
        "only_high_critical": false
      }
    }
  ]
}
```

### Top-level fields

| Field | Description |
| --- | --- |
| `registry` | Base registry URL, e.g. `"ghcr.io"` |
| `security` | Global security settings — inherited by all components unless overridden |
| `defaults` | Default build settings inherited by all components |
| `components` | **Required.** Array of component definitions |

### Component fields (all optional except `name`)

| Field | Description |
| --- | --- |
| `name` | **Required.** Image path becomes `{registry}/{owner}/{name}` |
| `dockerfile` | Path to Dockerfile. Default: `"Dockerfile"` |
| `build_context` | Docker build context path. Default: `"."` |
| `platforms` | Comma-separated platforms. Default: `"linux/amd64,linux/arm64"` |
| `arguments` | Build args, comma-separated or newline-delimited |
| `security` | Component-level security overrides (merged with global) |

Note: `context` is a deprecated alias for `build_context` — always use `build_context`.

### `security` object fields

| Field | Description |
| --- | --- |
| `scan` | `true` — include this component in security scan |
| `tag` | Image tag to use when scanning |
| `only_high_critical` | Scan only HIGH + CRITICAL vulnerabilities |
| `trivy_scan` | Enable Trivy scanner |
| `grype_scan` | Enable Grype scanner |
| `only_fixed` | Ignore unfixed vulnerabilities |
| `continue_on_error` | Don't fail the job if vulnerabilities found |

### Merge and flatten rules

`docker-config-resolver` processes each component in two steps:

1. **Merge:** component-level `security` fields override global `security` fields.
2. **Flatten:** `security.scan` → `security_scan`, `security.tag` → `security_tag`, etc.

The workflow reads `matrix.component.security_scan`, `matrix.component.security_trivy_scan`, and so on.
