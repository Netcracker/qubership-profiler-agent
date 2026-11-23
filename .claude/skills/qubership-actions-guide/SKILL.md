---
name: qubership-actions-guide
description: Navigation-only skill for individual actions in netcracker/qubership-workflow-hub. Use when a workflow needs to consume a specific Qubership action (Docker build/push, version/tag rendering, Maven/npm/Python publishing, package cleanup, Helm charts, security scans, etc.) and you need to find the right action and read its authoritative README. All rules (pinning, permissions, anti-hallucination, naming) live in qubership-workflow-conventions — this skill does not restate them.
---

# qubership-actions-guide

## Step 1 — identify the operation and load the right guide

Before picking any action, identify what the workflow needs to do and load
the relevant supporting document:

| Operation | Load |
| --- | --- |
| Docker build, push, image migration | `docker.md` |
| Security scan (images, source/deps, k8s cluster) | `security.md` |
| Email notifications | `notifications.md` |
| Helm chart release, values update | `helm.md` |
| Git tag, GitHub Release, release assets | `release.md` |
| PR automation (assigners, commit messages) | `pr.md` |
| CLA / DCO signing | `cla.md` |
| Maven build, SNAPSHOT deploy, release | `maven.md` |
| npm, Python publish | catalog below — no guide file needed |
| Cleanup (container images, Maven packages) | `cleanup.md` |
| APM packages update | `utilities.md` |
| Utilities (wait-for-workflow, custom-event, store-input-params) | `utilities.md` |
| Repository config file resolution without Docker (workflow policy, custom JSON configs) | `utilities.md` |
| JSON Schema validation (validate a JSON file against a schema, fail the pipeline on violations) | `utilities.md` |

Each guide contains: clarifying questions for the user, config file schemas,
and pipeline patterns for that domain. Read it before picking actions or
asking questions.

Reusable workflows (`re-*.yml` in `netcracker/qubership-workflow-hub`) are out of scope —
consume them as documented, do not redesign them here.

## Step 2 — pick actions from the catalog

Use the catalog to match each step in the workflow to a Qubership action.
For full input/output details fetch the action README on demand (see *Pin table*).

### Docker

| Action | Purpose |
| --- | --- |
| `config-resolver` | Schema-aware config resolver: docker mode by default (same output shape for valid Docker configs, stricter validation), generic flattening for any other schema |
| `docker-config-resolver` | **Deprecated — use `config-resolver`.** Read docker config file, validate, output JSON array for matrix builds |
| `docker-action` | Build & push multi-platform Docker images |

### Versioning & tagging

| Action | Purpose |
| --- | --- |
| `metadata-action` | Extract GitHub context and produce a version string / tags |
| `tag-action` | Create / delete / check Git tags; optional GitHub release creation |
| `branch-action` | Create a new branch from a tag or another branch |

### Publishing

Ask only what is missing after inferring from context:

- npm: registry (npmjs / GitHub Packages)
- Python: target (PyPI / GitHub Packages)

| Action | Purpose |
| --- | --- |
| `maven-release` | Maven artifact release with version bumping and GPG signing |
| `maven-snapshot-deploy` | Build and deploy Maven SNAPSHOT to Central or GitHub Packages |
| `poetry-publisher` | Build, test & publish Python package via Poetry to PyPI |

### Helm charts

| Action | Purpose |
| --- | --- |
| `chart-version` | Update `version` / `appVersion` in Helm Chart.yaml |
| `charts-values-update-action` | Update image versions in Helm values files; optionally create release branch |

### Security & compliance

| Action | Purpose |
| --- | --- |
| `cdxgen` | Generate SBOM + CycloneDX vulnerability report from source/deps |
| `k8s-hardening-scan` | Validate Kubernetes container hardening compliance (Kubescape + Trivy) |

### Cleanup

| Action | Purpose |
| --- | --- |
| `container-package-cleanup` | Remove stale container or Maven package versions from registry |

### PR & collaboration

| Action | Purpose |
| --- | --- |
| `cla-assistant` | CLA / DCO signing via PR comments |
| `pr-assigner` | Auto-assign reviewers based on config / CODEOWNERS |
| `pr-add-messages` | Append commit messages to PR description |

### Notifications

| Action | Purpose |
| --- | --- |
| `email-action` | Send email notifications via SMTP with fallback to repository variables for connection settings |

### Utilities

| Action | Purpose |
| --- | --- |
| `config-resolver` | Resolve any JSON config file to flat JSON (also listed under Docker) — generic mode for non-Docker configs, e.g. workflow policy |
| `apm-packages-update` | Run `apm update --yes` and open a PR with the changes in the current repo |
| `ghcr-discover-repo-packages` | Discover all GHCR packages for a repo — feeds security scan, cleanup, or any step needing the image list |
| `custom-event` | Emit `repository_dispatch` event with JSON payload |
| `smart-download` | Download workflow artifacts by name, IDs, or glob pattern |
| `store-input-params` | Persist `workflow_dispatch` inputs as artifact |
| `wait-for-workflow` | Wait for a specific GitHub Actions workflow run to complete |
| `verify-json` | Validate a JSON file against a JSON Schema |
| `assets-action` | Upload files/dirs to a GitHub release, auto-archives directories |

Deprecated (do not use): `commit-and-push`, `pom-updater`, `tag-checker`, `archive-and-upload-assets`.

## Pin table

Use these exact SHAs in `uses:` lines. For SHA pinning rules see `qubership-workflow-conventions` → *Pinning*.
Update this table manually when intentionally upgrading to a new release.

### netcracker/qubership-workflow-hub

Always resolve the latest release SHA dynamically:

```text
1. WebFetch → https://api.github.com/repos/netcracker/qubership-workflow-hub/releases/latest
   → extract .tag_name (e.g. v2.3.0)
2. WebFetch → https://api.github.com/repos/netcracker/qubership-workflow-hub/git/ref/tags/<tag_name>
   → extract .object.sha and .object.type
   - if .object.type == "commit" → use .object.sha directly
   - if .object.type == "tag" (annotated tag) → WebFetch .object.url → extract .object.sha (the commit SHA)
3. Use that commit SHA in uses: lines. Never use .target_commitish — it is a branch name, not a SHA.
```

If the GitHub API is unavailable, fall back to the last known SHA: `cabbb90e9471163cfac84bd50ff0296b2803b44c` (v2.3.0).

```yaml
uses: netcracker/qubership-workflow-hub/actions/docker-action@<resolved-sha>  # <resolved-tag>
```

### Third-party actions (update manually on upgrade)

| Repo | Latest tag | SHA |
| --- | --- | --- |
| `netcracker/release-drafter` | `v1.0.0` | `86f4276a3894b5af70480e826c32fe3648ac6a70` |
| `actions/checkout` | `v6.0.3` | `df4cb1c069e1874edd31b4311f1884172cec0e10` |
| `actions/upload-artifact` | `v7.0.1` | `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` |
| `actions/download-artifact` | `v8.0.1` | `3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c` |
| `actions/setup-python` | `v6.2.0` | `a309ff8b426b58ec0e2a45f0f869d46889d02405` |
| `actions/create-github-app-token` | `v3.2.0` | `bcd2ba49218906704ab6c1aa796996da409d3eb1` |

When the catalog purpose line is not enough to write the `with:` block — fetch the action README. Never write inputs from memory.

```text
1. WebFetch → https://api.github.com/repos/netcracker/qubership-workflow-hub/releases/latest
   → extract .tag_name
2. Resolve commit SHA via /git/ref/tags/<tag_name> (see Pin table above for the exact procedure)
3. WebFetch → https://raw.githubusercontent.com/netcracker/qubership-workflow-hub/<commit-sha>/actions/<name>/README.md
```
