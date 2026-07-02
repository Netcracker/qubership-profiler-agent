# CLA assistant — contributor license agreement signing

## Clarifying questions

Extract answers from the user's message first. Ask only what is missing.

| Question | What it controls |
| --- | --- |
| Where should signatures be stored — in this repo or a separate private repo? | Same repo → only `GITHUB_TOKEN` needed. Separate repo → also needs `PERSONAL_ACCESS_TOKEN` + `remote-repository-name` + `remote-organization-name` inputs |
| Which branch should signatures be committed to? | Maps to `branch` input — **must not be a protected branch** |
| Which bots/users should be exempt from signing? | Maps to `allowlist` input — comma-separated, supports `bot*` wildcard |
| CLA or DCO mode? | DCO → add `use-dco-flag: true` |

---

## Pipeline

```text
pull_request_target (opened, synchronize, closed) + issue_comment (created) → cla-assistant
                                                                                checks signature, requests signing, records to JSON
```

Full trigger block:

```yaml
on:
  issue_comment:
    types: [created]
  pull_request_target:
    types: [opened, closed, synchronize]
```

`closed` is required — when the PR merges the action locks the conversation to prevent contributors from retroactively modifying or deleting their signatures.

**Trigger must be `pull_request_target`** — CLA requires write permissions on fork PRs, which `pull_request` does not provide. Security here is a combination of three factors: (1) `pull_request_target` runs in the base repo context without checking out PR head code, (2) the action does not execute untrusted code from the PR, (3) signature recording only happens when the comment matches the signing phrase. Do not add `actions/checkout` with the PR head ref in this workflow.

---

## Key inputs

| Input | Description |
| --- | --- |
| `path-to-document` | **Required.** URL to the CLA document contributors must sign |
| `path-to-signatures` | Path to signatures JSON file. Default: `signatures/version1/cla.json` |
| `branch` | Branch where signatures are committed. Must not be protected. Default: `master` — **change to `main` if the repo's default branch is `main`** |
| `allowlist` | Comma-separated users/bots exempt from signing. Supports `bot*` wildcard |
| `remote-repository-name` | Repo name for remote signature storage (separate repo mode) |
| `remote-organization-name` | Org name for remote signature storage (separate repo mode) |
| `use-dco-flag` | `true` — use DCO mode instead of CLA |

---

## Permissions

```yaml
permissions:
  actions: write
  contents: write
  pull-requests: write
  statuses: write
```

---

## Secrets

| Secret | When needed |
| --- | --- |
| `GITHUB_TOKEN` | Always — passed as `env.GITHUB_TOKEN` (not `with:`), see example below |
| `PERSONAL_ACCESS_TOKEN` (e.g. `CLA_ACCESS_TOKEN`) | Only when storing signatures in a separate remote repo |

`GITHUB_TOKEN` must be set as an environment variable on the step, not as a `with:` input:

```yaml
- uses: netcracker/qubership-workflow-hub/actions/cla-assistant@<sha>  # vX.Y.Z
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    PERSONAL_ACCESS_TOKEN: ${{ secrets.CLA_ACCESS_TOKEN }}  # only for remote repo storage
  with:
    path-to-document: https://example.com/CLA.md
```

---

## Signing phrase

Contributors sign by posting this exact comment on the PR:

```text
I have read the CLA Document and I hereby sign the CLA
```

Re-check is triggered by posting: `recheck`

---

## Signature storage

Signatures are stored as a JSON file committed to the specified branch. Default path: `signatures/version1/cla.json`.

For org-wide CLA storage — use a dedicated private repo (`remote-repository-name` + `remote-organization-name`). This avoids polluting each repo's history with signature commits.
