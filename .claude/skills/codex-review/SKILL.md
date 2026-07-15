---
name: codex-review
description: Run a code review through the Codex CLI and apply the fixes — Codex reports findings, you classify each one (real issue vs false positive), fix the real ones, and re-review until the review is clean. Use only when the user asks for a Codex review (cross-review or second opinion) of a branch diff, the uncommitted changes, or a specific commit.
---

# Codex code review

Use the Codex CLI as a second reviewer: it reads a diff and reports findings, you decide which
ones are real, fix those, and re-review until Codex comes back clean. The work that needs care is
the judgement — classifying each finding and writing the fix — so the steps below are mostly about
driving Codex and keeping a clear record of what was fixed and what was skipped.

Reply in the language the user writes in. Keep code identifiers, paths, and tool names as they are.

## Prerequisites

The [Codex CLI](https://github.com/openai/codex) is installed and authenticated, with `codex` on
the `PATH`. If `codex` is missing, stop and report it — the rest of the skill depends on it.

## Step 1. Pick the review target

Find out what to review. The target maps to one Codex scope flag:

- `--base <branch>` — the diff against a branch (the common case).
- `--uncommitted` — the working-tree changes that are not yet committed.
- `--commit <SHA>` — one specific commit.

If the user already named a target, use it. Otherwise ask with `AskUserQuestion`. Hold the chosen
flag for the Codex calls below:

```bash
REVIEW_SCOPE="--base main"   # or "--uncommitted", or "--commit <SHA>"
```

## Step 2. Pick the review mode

Ask the user which mode to run (`AskUserQuestion`):

- **Automatic** — you classify every finding, then show a summary of what you will fix and what you
  will skip (with reasons) before touching any code.
- **Interactive** — for each finding, ask the user to fix it as suggested, skip it, or fix it a
  different way.

## Step 3. Set up a scratch directory

Keep the intermediate files out of the repository so the review never shows up in the diff it is
reviewing:

```bash
CODEX_REVIEW_DIR="$(mktemp -d "${TMPDIR:-${TEMP:-/tmp}}/codex-review-XXXXXX")"
echo "$CODEX_REVIEW_DIR"
```

Every later step reads and writes under `$CODEX_REVIEW_DIR`.

## Step 4. Run the first review

```bash
codex exec review $REVIEW_SCOPE --json -o "$CODEX_REVIEW_DIR/review-output.md" 2>/dev/null
```

`--json` streams JSONL to stdout, one JSON object per line. Find the line with
`"type": "session_meta"` and read its `session_id` — step 7 needs it to resume the same Codex
session. Record the run state in `$CODEX_REVIEW_DIR/state.json`:

```json
{
  "session_id": "<session_id from session_meta>",
  "iteration": 1,
  "findings": [],
  "fixed": [],
  "skipped": []
}
```

## Step 5. Classify the findings

Read `$CODEX_REVIEW_DIR/review-output.md` and sort each finding into one of two buckets:

- **Real** — a bug, a missing error path, a security concern, or a style issue worth fixing.
- **False positive** — irrelevant, debatable, or already handled by surrounding code.

In **automatic** mode, show one summary: the findings you will fix (short descriptions) and the
ones you will skip (with a reason for each), then proceed without asking per finding.

In **interactive** mode, ask the user per finding: fix as suggested, skip, or fix differently.

Record the findings and decisions in `state.json`.

## Step 6. Apply the fixes

Read the surrounding code first, then edit it — prefer `Edit` over `Write`. When the fixes are in,
move each finding to the `fixed` or `skipped` array in `state.json`.

## Step 7. Re-review (iterations 2–3)

Resume the same Codex session so it judges the fixes in context rather than starting fresh:

```bash
codex exec resume "$SESSION_ID" --json -o "$CODEX_REVIEW_DIR/review-output.md" \
  "I addressed these findings: <fixed items>. I deliberately skipped these, with reasons: \
<skipped items>. Please re-review."
```

`$SESSION_ID` is the `session_id` from `state.json`. Increment `iteration`, then repeat steps 5–6
for any new findings.

Stop the loop when any of these holds:

- Codex reports no new findings.
- `iteration` reaches 3.
- The user chooses to stop (in interactive mode, offer this after each iteration).

## Step 8. Final report

Summarise the review. For each finding, show what Codex reported and what you did about it:

```text
## Review report

Iterations: N

### Finding 1 — [severity] (fixed | skipped)
- File: path/to/file:Lines
- Issue: what Codex found, and why it matters.
- Resolution: the concrete change, or why it was skipped. For a fix, name the before and after
  (for example, was `MergeLabels(...)`, now `resourceLabelsFromBase(...)`, which also folds in
  the component labels).

### Verdict: clean | items remaining
```

Always pair the original finding with its resolution. For a skipped finding, say why (false
positive, design decision, out of scope).

## Step 9. Clean up

```bash
rm -rf "$CODEX_REVIEW_DIR"
```
