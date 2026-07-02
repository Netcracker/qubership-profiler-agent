---
name: qubership-workflow-conventions
description: Single source of truth for Qubership GitHub Actions workflows. Use when designing, writing, reviewing, or debugging .github/workflows/*.yml that consume actions or templates from netcracker/qubership-workflow-hub or Netcracker/.github.
---

# qubership-workflow-conventions

Single source of truth for any GitHub Actions workflow that consumes the
Qubership ecosystem (`netcracker/qubership-workflow-hub` actions,
`Netcracker/.github` templates).

## Companion skills (navigators)

- `qubership-templates-guide` — fork-able Netcracker workflow-templates catalog.
- `qubership-actions-guide` — actions catalog, domain guides, and the Pin table for SHAs.

All rules live in this file.

## Clarify before acting

Before designing any workflow, establish context with the minimum necessary
questions. Never ask for information that can be read from a file.

### Step 0 — one question first

Ask exactly one question:

> "Do you have an existing workflow and/or `.qubership/` config files, or
> are we starting from scratch?"

If the user has already provided a file or it is open in the IDE — skip
this question and go straight to *Path A*.

### Path A — existing workflow or configs

Read the workflow and any config files it references. Load the relevant domain guide from
`qubership-actions-guide` (`docker.md`, `helm.md`, `security.md`, `release.md`, `notifications.md`,
`maven.md`, `cleanup.md`, `utilities.md`, `pr.md`, `cla.md`) and use its migration table. Ask only about what
is missing after reading. Return the full updated workflow file — preserve existing structure, change only what needs changing.

### Path B — starting from scratch

Hand off to `qubership-actions-guide` Step 1 — it routes to the relevant domain guide
(`docker.md`, `helm.md`, `security.md`, `release.md`, `notifications.md`, `maven.md`,
`cleanup.md`, `utilities.md`, `pr.md`, `cla.md`) or returns to the catalog for operations
without a guide (npm, Python).
Clarifying questions for catalog-only operations live in the catalog (see `qubership-actions-guide` → *Publishing*).

**Infer the trigger from the request** — do not ask unless truly ambiguous.
See `workflow-patterns.md` → *Trigger rules* for the three standard patterns.

## Workflow design process

After *Clarify before acting*:

1. **Read `qubership-templates-guide/SKILL.md` and search the catalog for a matching template.** Do this when creating a new workflow or restructuring an existing one (changing jobs, triggers, or pipeline shape). Skip for small targeted edits (adding a timeout, changing a branch filter, updating an input value). If a match is found: show the user which template, use it as the base. If no match: proceed from scratch using actions-guide.
2. **Hand off to `qubership-actions-guide` Step 1** — it loads the domain
   guide and picks actions. Use Pin table for SHAs. Fall back to standard
   actions only when no Qubership action fits.
3. **Apply *Mandatory conventions*** to every step — including *Security checklist* below.
4. **Return the workflow** per *Preferred answer style*.

## Mandatory conventions

### Anti-hallucination

LLMs invent plausible-sounding identifiers when they don't have the
exact spelling at hand. This applies to **any** identifier: action
names, input names, output names, fields inside JSON config payloads,
environment variable names, step IDs, and tag formats.

Rules:

- Never write a Qubership action identifier or `with:` block from memory.
- Before writing any reference to an identifier from the action library,
  read the action's README via `qubership-actions-guide`, locate the
  relevant **Inputs** / **Outputs** / config-schema section, and copy
  the identifier verbatim.
- If the identifier you expect is not in the README — it does not
  exist. Pick a different action, restructure the workflow, or tell
  the user the action does not support that capability.
- This applies even when:
  - you remember the identifier from earlier in the session;
  - a similar action has a similar-looking identifier;
  - the identifier appears in another action's **output** JSON —
    outputs and inputs are different namespaces.
- When in doubt, re-read the README. The cost of an extra lookup is
  negligible; the cost of a hallucinated identifier is a broken
  workflow that silently no-ops or fails on first run.

### Pinning

Pin every action — Qubership and third-party — as a full 40-character
commit SHA with a trailing `# vX.Y.Z` comment showing the release:

```yaml
uses: netcracker/qubership-workflow-hub/actions/<name>@<sha>  # vX.Y.Z
```

For actions listed in the Pin table (`qubership-actions-guide` → *Pin table*) —
always use it.

For actions **not in the Pin table** (including `actions/*` and all third-party actions),
resolve the SHA from a verifiable source (GitHub API/release metadata), then add it to the
Pin table or ask the user to provide the SHA. Do not write SHAs from memory.

Forbidden: `@main`, short SHAs, bare tags (`@v6`, `@v1.2.3`). Always full SHA.

### Permissions

- Set `permissions:` at the **job level**, not the workflow level.
- Start every job from `contents: read` and elevate only where the action's
  README says it needs more (e.g. `packages: write` for GHCR pushes,
  `pull-requests: write` for PR comments, `id-token: write` for OIDC,
  `contents: write` for tag/release creation).

### Naming and structure

- Inputs are `kebab-case` (`dry-run`, `force-create`).
- Outputs are short singular nouns (`version`, `tag`, `digest`).
- Org name in `uses:` is always lowercase `netcracker`, never
  `Netcracker`.
- Boolean inputs default `false`; the input name describes the feature
  when enabled.

### Configuration file placement

When a Qubership-authored action accepts an external config file
(e.g. `configuration-path`, `config-path`):

- Place the file under **`.qubership/`**, not `.github/`. The `.qubership/`
  directory is the conventional home for Qubership-specific configuration
  in consumer repos.
- Pass the path explicitly via the action's input — do not rely on the
  action's default path, which may point to `.github/`.
- **Only create a config file when one of these conditions is met**:
  the user explicitly asks for it; or the action's inline inputs are
  not sufficient (e.g. multiple branch patterns, reuse across workflows).
  If inline inputs cover the use case — use them directly in the step,
  do not create a file.

This rule applies **only to actions authored by Netcracker/Qubership**.
Third-party actions (e.g. `actions/checkout`, `helm/kind-action`) keep
their configs wherever their own documentation prescribes.

### Security (zizmor rules)

Read `zizmor-rules.md` and apply all rules while writing the workflow.
Violations must not appear in the generated output.

### Secrets and PR safety

- Never put secret values in YAML. Use `${{ secrets.SECRET_NAME }}`.
- Never echo secrets; never use `set -x` near secret usage.
- Treat pull requests from forks as **untrusted**. Do not expose
  secrets, deployment credentials, package publish tokens, registry
  tokens, or release tokens to untrusted PR code.
- Avoid `pull_request_target` unless the workflow is intentionally
  designed for it and does not check out or execute untrusted code
  unsafely. CLA assistant is one of the few legitimate uses.
- For production deployment workflows, prefer **environment-scoped
  secrets** (declared on a GitHub Environment) over repository-wide
  secrets — see `security-model.md`.

## Supporting documents

Read these files when relevant:

- `workflow-patterns.md` — workflow structure, triggers, concurrency,
  timeouts, matrix, config-driven matrix, dry-run gating, reusable
  workflow contracts, artifacts.
- `zizmor-rules.md` — full zizmor security rules to apply at generation time.
- `security-model.md` — checkout credentials, environment-scoped
  secrets, GitHub App tokens for protected branches, environments.
- `debugging-playbook.md` — failure categories and the diagnosis
  procedure for broken workflows.

## Preferred answer style

Match the user's language. Translate section headers; keep YAML, paths,
action identifiers unchanged.

Always return a complete, copy-paste-ready workflow file — not a partial snippet, unless the user explicitly asks for one.

Structure:

```text
File: .github/workflows/<name>.yml

<full YAML>

Actions used: <which Qubership actions or templates and why>
What to configure: <secrets / vars / environments / config files>
How to trigger: <push / PR / tag / workflow_dispatch>
How to verify: <Actions tab, expected artifact/image/release>
```
