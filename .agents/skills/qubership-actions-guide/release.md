# Release — tags, GitHub Releases, and assets

This guide applies to any domain (Docker, Helm, Maven, npm, Python, etc.).
Load it whenever the workflow needs to create a Git tag, a GitHub Release, or upload release assets.

## Clarifying questions

Extract answers from the user's message first. Ask only what is missing and cannot be defaulted.

Default: the workflow always creates a Git tag. If the tag already exists the `check-tag` step fails fast. Only skip tag creation if the user explicitly says the tag already exists.

| # | Question | What it controls |
| - | --- | --- |
| 1 | Minimal GitHub Release or with auto-generated changelog from PR history? | Minimal → `tag-action` with `create-release: true`. Changelog → `release-drafter` (requires `.github/release-drafter-config.yml`). |

Default: `upload-assets` job always generated with a comment `# remove if not needed`. Build artifacts need `upload-artifact` → `download-artifact`; repo files need only `checkout`.

---

## Critical rule: tag before build

**Always create the Git tag before any build or publish step.** Never create the tag after.

Reason: if the build pushes artifacts (Docker images, Maven jars, npm packages) but the tag step fails afterwards, artifacts exist in the registry with release version tags but no corresponding Git tag — the release is in an inconsistent state.

Correct order:

```text
check-tag → create-tag → [build/publish] → github-release
```

Wrong order (never do this):

```text
[build/publish] → create-tag   ← tag created after artifacts already pushed
```

---

## Patterns

### Tag only

```text
tag-action (check)  →  tag-action (create)
verify tag absent      creates vX.Y.Z tag
```

Use when only a Git tag is needed, no GitHub Release page.

---

### Tag + minimal GitHub Release

```text
tag-action (check)  →  tag-action (create, create-release: true)
verify tag absent      creates tag and empty GitHub Release
```

`tag-action` with `create-release: true` creates both in one step.
Release body will be empty — edit manually afterwards if needed.

---

### Tag + GitHub Release with changelog (release-drafter)

```text
tag-action (check)  →  tag-action (create)  →  release-drafter
verify tag absent      creates tag              generates changelog, publishes release
```

`release-drafter` reads PR titles and labels to build the changelog automatically.
**Requires** `.github/release-drafter-config.yml` in the repo.
Ask the user if it exists. If not — generate the default config below and write it to `.github/release-drafter-config.yml`.

`release-drafter` inputs:

- `config-name: release-drafter-config.yml`
- `publish: true`
- `name: ${{ inputs.release }}`
- `tag: v${{ inputs.release }}`
- `version: ${{ inputs.release }}`
- `commitish:` — branch or ref the release points to

Action ref: `netcracker/release-drafter@<sha>  # v1.0.0`

### Default `release-drafter-config.yml`

Generate and write to `.github/release-drafter-config.yml` if the file doesn't exist:

```yaml
name-template: 'v$RESOLVED_VERSION'
tag-template: 'v$RESOLVED_VERSION'

categories:
  - title: '💥 Breaking Changes'
    labels:
      - breaking-change
  - title: '💡 New Features'
    labels:
      - feature
      - enhancement
  - title: '🐞 Bug Fixes'
    labels:
      - bug
      - fix
      - bugfix
  - title: '⚙️ Technical Debt'
    labels:
      - refactor
  - title: '📝 Documentation'
    labels:
      - documentation

change-template: "- (#$NUMBER) $TITLE by @$AUTHOR"

no-changes-template: 'No significant changes'

template: |
  ## 🚀 Release

  ### What's Changed
  $CHANGES

  ---

  **Full Changelog**: https://github.com/$OWNER/$REPOSITORY/compare/$PREVIOUS_TAG...v$RESOLVED_VERSION

version-resolver:
  major:
    labels:
      - major
  minor:
    labels:
      - minor
  patch:
    labels:
      - patch
  default: patch
```

Version bump is determined by PR labels: `major`, `minor`, `patch`. Default is `patch` if no label set.

---

### Tag + GitHub Release + assets from repo files

```text
tag-action (check)  →  tag-action (create, create-release: true)  →  checkout (ref: tag)  →  assets-action
verify tag absent      creates tag and release                        fetch repo at tag       upload files
```

Use when assets are files committed to the repo (scripts, configs, docs).
`checkout` with `ref: v${{ inputs.release }}` fetches the tagged state.

---

### Tag + GitHub Release + assets from build artifacts

```text
[producer job]                   [release job]                         [upload-assets job]
build  →  upload-artifact    →   tag-action (check)                →   download-artifact  →  assets-action
           saves to GHA           tag-action (create + release)         restores files        uploads to release
```

Use when assets are produced during the workflow (jars, .tgz chart packages, zips, binaries).

- `upload-artifact` in the producer job saves files to GitHub Actions storage
- `download-artifact` in the upload-assets job restores them — **no `checkout` needed**
- Pass `artifact-ids` output from the producer job to `download-artifact` for precision

This is the pattern used in Helm chart releases — `.tgz` packages produced by `charts-values-update-action`
are saved as artifacts and downloaded in the `github-release` job before `assets-action`.

---

## Key action inputs

### `tag-action`

| Input | Description |
| --- | --- |
| `ref` | Branch or commit to tag |
| `tag-name` | Tag name, e.g. `v${{ inputs.release }}` |
| `check-tag` | `true` — fail if tag already exists |
| `create-tag` | `true` — create the tag |
| `create-release` | `true` — also create a minimal GitHub Release |
| `tag-message` | Annotated tag message, e.g. `Release v1.2.3` |
| `dry-run` | `true` — simulate without pushing anything |
| `skip-checkout` | `true` — skip checkout if repo already checked out in a previous step |

**Always run check and create as separate steps or jobs.** Never combine `check-tag: true` and `create-tag: true` in one step.

### `assets-action`

| Input | Description |
| --- | --- |
| `tag` | Release tag to upload assets to |
| `item-path` | Comma-separated file paths or glob patterns, e.g. `./*.tgz` |
| `archive-type` | `zip` (default), `tar`, `tar.gz` — used when item is a directory |
| `retries` | Upload retry attempts, default `3` |

Requires `permissions: contents: write`.

---

## Permissions

| Job | Minimum permissions |
| --- | --- |
| Tag check only | `contents: read` |
| Create tag / release | `contents: write` |
| Upload assets | `contents: write` |
| release-drafter | `contents: write` |
