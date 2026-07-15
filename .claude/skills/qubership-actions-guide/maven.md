# Maven — build, snapshot deploy, and release

## Template coverage

| Task | Starting point |
| --- | --- |
| Full Maven release (version bump + tag + publish) | Fetch `maven-release-v2.yaml` from `Netcracker/.github/workflow-templates` and adapt |
| Deploy SNAPSHOT on push | Fetch `maven-snapshot-deploy.yaml` and adapt |
| Maven build + Docker image (CI) | Fetch `dev-mvn-docker-build.yml` and adapt |

---

## Clarifying questions

Extract answers from the user's message first. Ask only what is missing.
Trigger is inferred per `workflow-patterns.md` → *Trigger rules* — do not ask.

| Question | What it controls |
| --- | --- |
| Release or SNAPSHOT deploy? | Determines which action and pipeline to use |
| Target registry: Maven Central or GitHub Packages? | Maps to `profile` / `target-store` / `server-id` inputs and which credentials are needed |
| Java version? | Maps to `java-version`. Default: `21` — apply without asking unless user specifies otherwise |
| Is the default branch protected? | Release only — determines whether GitHub App token is needed for `maven-release` |
| Should Docker image be built after release? | Release only — adds docker-build + check-dockerfile jobs |

---

## Two actions

### `maven-release` — full release via Maven Release Plugin

Bumps version (major/minor/patch), creates Git tag `v<version>`, runs `mvn release:prepare` + `mvn release:perform`, deploys signed artifact.

**Critical behaviour:**

- Checks out `<org>/<module>` — a **different repo** from the one running the workflow. `module` input must match the target repository name exactly.
- `token` must have write access to `<org>/<module>`. `GITHUB_TOKEN` only covers the calling repo — use a PAT or GitHub App token for cross-repo releases or protected branches.
- `dry-run` default is `'true'` (string). Only passes `dry-run: 'false'` (exact string) to trigger an actual release. Any other value including omission → dry-run.
- `gpg-private-key` and `gpg-passphrase` are **required even for dry-run** — the action deploys a SNAPSHOT artifact.
- Tag is created **inside the action** via `mvn release:prepare` — do not add a separate `tag-action` step.
- `server-id` must match the Maven `<server><id>` in `settings.xml`. Pass the same value as `profile` (e.g. `central` or `github`).

**Output:** `release-version` — the released version string (e.g. `2.1.0`).

### `maven-snapshot-deploy` — SNAPSHOT deploy or any Maven command

Deploys current SNAPSHOT version or runs any Maven command. Does not bump version or create tags.

**Critical behaviour:**

- `maven-token` is the only required input.
- When `maven-command: deploy` — reads `pom.xml` to check for `-SNAPSHOT` suffix. If not a SNAPSHOT, automatically falls back to `mvn install` without deploying.
- When `maven-command` is anything other than `deploy` (e.g. `clean verify`, `test`) — SNAPSHOT check is skipped, command runs as-is. GPG not needed.
- Profile activation: if `target-store` value matches a `<profile><id>` in `pom.xml`, activates `-P<target-store>` automatically.
- For GitHub Packages: pass `maven-token: ${{ github.token }}` — no `maven-username` needed, action falls back to `github.actor`.
- `upload-artifact: 'true'` — uploads `**/target` dirs as GitHub Actions artifact, enabling Docker build in a downstream job.

---

## Credentials by registry

| Registry | `profile` / `target-store` | `maven-user` / `maven-username` | `maven-password` / `maven-token` |
| --- | --- | --- | --- |
| Maven Central | `central` | `secrets.MAVEN_USER` | `secrets.MAVEN_PASSWORD` |
| GitHub Packages | `github` | `github.actor` | `github.token` or `secrets.GITHUB_TOKEN` |

GPG secrets (org-level): `MAVEN_GPG_PRIVATE_KEY`, `MAVEN_GPG_PASSPHRASE`.

---

## Protected branch — GitHub App token

`mvn release:prepare` pushes a version-bump commit and tag to the default branch. If the branch is protected, `GITHUB_TOKEN` cannot push — use a GitHub App token:

```yaml
- name: "Prepare app token"
  uses: actions/create-github-app-token@<sha>  # vX.Y.Z
  id: app-token
  with:
    app-id: ${{ vars.GH_BUMP_VERSION_APP_ID }}
    private-key: ${{ secrets.GH_BUMP_VERSION_APP_KEY }}
```

Then pass `token: ${{ steps.app-token.outputs.token }}` to `maven-release`.

If the default branch is **not** protected — `token: ${{ secrets.GITHUB_TOKEN }}` is sufficient.

---

## Pipelines

### Release pipeline

```text
dry-run-build → deploy → [check-dockerfile → docker-build] → github-release
```

- `dry-run-build`: runs `maven-release` with `dry-run: 'true'` — validates build and deploy without bumping version or pushing
- `deploy`: runs `maven-release` with `dry-run: 'false'` — bumps version, creates tag, deploys artifact; outputs `release-version`; uploads `**/target` via `actions/upload-artifact` for downstream Docker build
- `check-dockerfile` + `docker-build`: conditional on `build-docker` input; `docker-action` with `download-artifact: true` restores the artifact uploaded in `deploy` job
- `github-release`: calls `release-drafter` reusable workflow with `release-version` from deploy

Tag ordering: tag is created **inside `maven-release`** during `release:prepare` — before artifacts are pushed. This is the correct order.

**Permissions per job:**

| Job | Permissions |
| --- | --- |
| `dry-run-build` | `contents: read`, `packages: write` |
| `deploy` | `contents: write`, `packages: write` |
| `check-dockerfile` | `contents: read` |
| `docker-build` | `contents: read`, `packages: write` |
| `github-release` | `contents: write` |

### SNAPSHOT deploy pipeline

```text
push (non-main branches) → checkout → maven-snapshot-deploy
```

Trigger: `push` with `branches-ignore: [main, **release*, dependabot/**]` + `paths-ignore` for docs.

**Permissions (job level — not workflow level):**

| Target | Permissions |
| --- | --- |
| GitHub Packages | `contents: read`, `packages: write` |
| Maven Central | `contents: read` |

### Maven + Docker CI pipeline

Canonical one-job flow from `dev-mvn-docker-build.yml` — no artifact upload/download:

```text
checkout → maven-snapshot-deploy → metadata-action → docker-action
           (build only, no deploy)   generates tags     builds image
```

`maven-snapshot-deploy` is used with a non-deploy `maven-command` (e.g. `--batch-mode package -Dgpg.skip=true`) — no credentials needed, no SNAPSHOT check. All steps run in the same job, so the built `target/` is available on disk for the Docker build without artifact transfer.

`docker-action` with `checkout: 'false'` — repo already checked out in the same job.

---

## Inputs reference

### `maven-release`

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `version-type` | Yes | `patch` | `major`, `minor`, or `patch` |
| `module` | Yes | — | Target repo name (not full slug) |
| `token` | Yes | — | PAT or GitHub App token with write access to `<org>/<module>` |
| `gpg-private-key` | Yes | — | Required even for dry-run |
| `gpg-passphrase` | Yes | — | Required even for dry-run |
| `dry-run` | No | `'true'` | String `'false'` to release; any other value → dry-run |
| `ref` | No | `main` | Branch to release from |
| `server-id` | No | `github` | Must match Maven `<server><id>` — pass same value as `profile` |
| `profile` | No | `''` | Maven profile to activate (`central` or `github`) |
| `maven-user` | No | `''` | Registry username |
| `maven-password` | No | `''` | Registry password/token |
| `java-version` | No | `21` | JDK version |
| `maven-version` | No | `''` | Pin a specific Maven version; omit to use runner's pre-installed Maven |
| `maven-args` | No | `-DskipTests=true -Dmaven.javadoc.skip=true -B` | Extra Maven args |
| `bump-dependencies-after-release` | No | `'false'` | String `'true'` to bump `org.qubership.*` deps to next SNAPSHOT after release |

### `maven-snapshot-deploy`

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `maven-token` | Yes | — | GitHub token or Maven Central token |
| `target-store` | No | `central` | `central` or `github` |
| `maven-command` | No | `deploy` | Any Maven command; SNAPSHOT check only applies to `deploy` |
| `java-version` | No | `21` | JDK version |
| `maven-version` | No | `''` | Pin a specific Maven version; omit to use runner's pre-installed Maven |
| `maven-username` | No | — | Registry username; falls back to `github.actor` for GitHub Packages |
| `gpg-private-key` | No | — | Only needed for signed deploys |
| `gpg-passphrase` | No | — | Only needed for signed deploys |
| `additional-mvn-args` | No | `''` | Extra Maven args |
| `upload-artifact` | No | `'false'` | String `'true'` to upload `**/target` as artifact |
| `artifact-id` | No | `maven-snapshot-deploy-artifact` | Artifact name for upload |
| `working-directory` | No | `''` | Run Maven from this directory |
| `pom-file` | No | `pom.xml` | Path to pom.xml relative to working-directory |
| `sonar-token` | No | — | Passed as `SONAR_TOKEN` to Maven process |

---

## Secrets reference

| Secret | Where set | Used by |
| --- | --- | --- |
| `MAVEN_GPG_PRIVATE_KEY` | Org level | `maven-release`, `maven-snapshot-deploy` |
| `MAVEN_GPG_PASSPHRASE` | Org level | `maven-release`, `maven-snapshot-deploy` |
| `MAVEN_USER` | Org level | `maven-release` (Central) |
| `MAVEN_PASSWORD` | Org level | `maven-release` (Central) |
| `GH_BUMP_VERSION_APP_KEY` | Org level | `create-github-app-token` for protected branch push |
| `GH_BUMP_VERSION_APP_ID` | Org variable (`vars.`) | `create-github-app-token` |

---

## pom.xml requirements

Both actions require `pom.xml` to be prepared according to the org guide:
[maven-publish-pom-preparation_doc.md](https://github.com/Netcracker/.github/blob/main/docs/maven-publish-pom-preparation_doc.md)

Key requirements:

- `<profile><id>` matching `target-store` value must exist for profile activation
- Maven Central deploy requires Central Publishing Maven Plugin configured in the profile
- Forbidden `groupId` values: `org.qubership` and `com.netcracker` exactly (subpackages like `org.qubership.cloud` are allowed)
