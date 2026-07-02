---
name: qubership-templates-guide
description: Navigation-only skill for the Netcracker workflow-templates catalog at Netcracker/.github/workflow-templates. Use when the user asks for a CI/CD workflow that may already exist as a curated template (Docker build/release, Maven/npm/Python release, Helm chart release, security scan, SBOM, PR hygiene, license/lint, GHCR cleanup). Points to the canonical template files and explains how to fork. All rules (pinning, permissions, anti-hallucination, naming) live in qubership-workflow-conventions — this skill does not restate them.
---

# qubership-templates-guide

Navigator for the official Netcracker workflow-templates catalog at
[`Netcracker/.github/workflow-templates`](https://github.com/Netcracker/.github/tree/main/workflow-templates).

These templates are the canonical, production-tested workflows used
across the org. The catalog is the source of truth — this skill helps
you locate the right template and adapt it.

## How to use

**Run this step when creating a new workflow or restructuring an existing one** (changing jobs, triggers, or pipeline shape). Skip for small targeted edits (timeout, branch filter, input value).

If a matching template is found → tell the user which template, fetch it, use it as the base.
If no match → return to `qubership-workflow-conventions` Workflow design process Step 2 and build from scratch.

1. **Read the user's task.** Identify the CI/CD operation (release,
   build, scan, lint, cleanup, etc.).

1. **Match against the catalog table below.** Pick the closest template
   by purpose, not by name resemblance. Then verify it still exists before using it:

   ```bash
   gh api repos/Netcracker/.github/contents/workflow-templates/<file> --jq '.name'
   ```

   If the file returns 404 — inform the user, show the current list, and pick the nearest
   alternative or fall back to building from scratch.

1. **Fetch the template ONCE.** Two equivalent ways:

   ```text
   gh api repos/Netcracker/.github/contents/workflow-templates/<file> --jq '.content' | base64 -d
   ```

   ```text
   WebFetch → https://raw.githubusercontent.com/Netcracker/.github/main/workflow-templates/<file>
   ```

   Use a commit SHA or a tag for `<ref>` in production; `main` is
   acceptable for exploration.

1. **Read the paired `.properties.json`** when present (same basename,
   `.properties.json` extension). It contains the GitHub UI metadata —
   description, categories, `filePatterns` — useful for confirming the
   template's intent.

1. **Adapt the template.** See *What to adapt vs. keep* below.

1. **Apply `qubership-workflow-conventions`** (*Mandatory conventions*)
   to every adapted line. Replace template SHAs with values from the
   *Pin table* in `qubership-actions-guide`.

1. **Tell the user which template you started from**, including the
   raw URL or repo path, so they can compare and re-fetch updates.

## Catalog: task → template

The table below is a snapshot for fast lookup; verify the file exists
in the current
[`Netcracker/.github/workflow-templates`](https://github.com/Netcracker/.github/tree/main/workflow-templates)
directory before using it (templates are added, renamed, and removed):

```bash
gh api repos/Netcracker/.github/contents/workflow-templates --jq '.[].name'
```

### Docker (CI builds)

| Task | Template | Trigger | Key details |
| --- | --- | --- | --- |
| Single Docker image, no config file | `dev-docker-build-single-image.yml` | push + pull_request + workflow_dispatch | `metadata-action` → `docker-action`; inline image name; dry-run input |
| Multiple Docker images from `.qubership/docker.cfg` | `dev-docker-build-multiple-images.yml` | push + pull_request + workflow_dispatch | `docker-config-resolver` → `metadata-action` → `docker-action` (matrix); dry-run input |
| Build only changed images (changeset detection) | `dev-docker-build-selective.yml` | push + pull_request + workflow_dispatch | like multiple-images but filters components by changed files via `tj-actions/changed-files` |
| Build Maven project then Docker image | `dev-mvn-docker-build.yml` | push + pull_request + workflow_dispatch | Maven build → upload-artifact → docker-action with download-artifact |

### Releases

| Task | Template | Trigger | Key details |
| --- | --- | --- | --- |
| Release Docker images + GitHub release | `docker-release.yaml` | workflow_dispatch | check-tag → `docker-config-resolver` (`.qubership/docker.cfg`) → create-tag → docker-action (matrix) → release-drafter |
| Release Maven artifact (Central or GH Packages) | `maven-release-v2.yaml` | workflow_dispatch | dry-run-build → deploy (maven-release) → docker-build (optional) → github-release; needs GPG secrets + GH App token for protected branch |
| Deploy Maven SNAPSHOT | `maven-snapshot-deploy.yaml` | workflow_dispatch | maven-snapshot-deploy action; no tag/release created |
| Release npm package | `npm-release.yaml` | workflow_dispatch | npm publish to npmjs or GitHub Packages |
| Publish npm package (lightweight) | `npm-publish.yaml` | workflow_dispatch | lightweight npm publish, no release drafter |
| Release Python package via Poetry | `python-release.yaml` | workflow_dispatch | poetry-publisher → PyPI; dry-run support |
| Release Helm charts + Docker images | `helm-charts-release.yaml` | workflow_dispatch | check-tag → docker dry-run → charts-values-update-action → docker-action (matrix) → release-drafter + assets-action (.tgz); needs `.qubership/helm-charts-release-config.yaml` + `.qubership/docker-build-config.cfg` |

### Security and supply chain

| Task | Template | Trigger | Key details |
| --- | --- | --- | --- |
| Security scan via docker.cfg config | `security-scan-with-config.yml` | workflow_dispatch + schedule (Sunday) | `docker-config-resolver` → filter `security_scan==true` → `re-security-scan` (matrix); supports manual overrides per-input |
| Security scan — discover from GHCR | `security-scan.yml` | workflow_dispatch + schedule (Sunday) | `ghcr-discover-repo-packages` → `re-security-scan` (matrix); no config file needed |
| Security scan for APIHUB | `security-scan-apihub.yml` | workflow_dispatch + schedule | APIHUB-specific variant |
| CVE scan via Docker Scout | `scout-cves.yml` | workflow_dispatch + schedule | Docker Scout instead of Trivy/Grype |
| Generate SBOM and attach to release | `sbom-to-release.yaml` | workflow_dispatch | `cdxgen` → attach to GitHub release |
| Native GitHub dependency review on PR | `dependency-review.yaml` | pull_request | GitHub native dependency-review action |
| OSSF Scorecard | `ossf-scorecard.yaml` | schedule + push to main | OSSF scorecard upload |

### Code quality and linting

| Task | Template |
| --- | --- |
| Lint and test Helm charts | `lint-test-chart.yaml` |
| Lint markdown / source via super-linter | `super-linter.yaml` |
| Check third-party licenses | `check-license.yaml` |
| Add license headers to source files | `license-header.yml` |
| Check broken links | `link-checker.yaml` |
| Lint Renovate config | `renovate-config-lint.yaml` |
| Profanity filter for issues/PRs | `profanity-filter.yaml` |

### PR hygiene

| Task | Template |
| --- | --- |
| Conventional Commits PR check | `pr-conventional-commits.yaml` |
| Lint PR title | `pr-lint-title.yaml` |
| Auto-label PRs | `automatic-pr-labeler.yaml` |
| Auto-assign reviewers | `pr-assigner.yml` |
| Auto-assign issues to project board | `auto-assign-project-to-issue.yml` |
| CLA assistant | `cla.yaml` |

### Build and maintenance

| Task | Template |
| --- | --- |
| Build Go project | `go-build.yaml` |
| Cleanup old container versions in GHCR | `cleanup-old-docker-container.yaml` |
| Bump test workflow versions | `bump-test-workflows-version.yaml` |

## What to adapt vs. keep

When forking a template into a user's repo:

**Adapt** (safe, expected):

- Workflow `name:` and `run-name:` for repo-specific labels.
- Image names, package names, registry URLs that are repo-specific.
- Secret names if the user's repo uses different conventions.
- Path filters in `paths-ignore:` to match repo layout.
- `inputs:` defaults (versions, tags) for repo-specific norms.
- Trigger filters (branches, tags) for the repo's branching model.

**Keep** (do not "improve" without a reason):

- Job-level `permissions:` blocks — the templates already follow
  least-privilege.
- Job structure and `needs:` order — typical templates implement
  dry-run → deploy → release stages for a reason.
- Config-file references (`.qubership/docker.cfg`, etc.) — these are
  the org-standard locations.

**Always tell the user about**:

- Required secrets the template uses (e.g. `GH_BUMP_VERSION_APP_ID`,
  `MAVEN_GPG_PRIVATE_KEY`, `PYPI_API_TOKEN`).
- Required config files (`.qubership/docker.cfg`,
  `.qubership/helm-charts-release-config.yaml`,
  `.github/release-drafter-config.yml`) and where to find examples.
- Required org/repo variables (`vars.GH_BUMP_VERSION_APP_ID` etc.).

## What this skill does NOT do

- It does not generate workflows from scratch — for that, follow
  `qubership-workflow-conventions`.
- It does not duplicate template content — templates are fetched on
  demand from the catalog.
- It does not cover reusable workflows (`re-*.yml`) — those are not
  part of the workflow-templates catalog.
