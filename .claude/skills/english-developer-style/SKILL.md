---
name: english-developer-style
description: 'American-English-first style guidance for developer-facing text of any length — README and docs, code comments and docstrings, Javadoc / KDoc / TSDoc / JSDoc, commit messages, PR descriptions and review replies, design docs, changelog entries, UI strings (buttons, labels, placeholders, tooltips), error and log messages, and English-side localization files (.po, .properties, JSON i18n). Use whenever the task involves English developer text — writing, editing, rewriting, translating, localizing, reviewing, proofreading, verifying, auditing, double-checking, or cross-checking. Length does not matter: a one-line `msgstr`, a `// FIXME` comment, or a three-word button label is in scope. Also covers choice of English identifier names (methods, variables, classes, files, tests) read by humans. Covers em-dash usage, hyphen stacking, sentence structure, that/which, gerunds after rather than, hedging, inclusive language, and British/American dialect consistency. Do not use for casual chat replies to the user.'
---

# English developer style (American)

This skill governs natural-language text written for developers and for users of developer tools: README and reference
pages, code comments and docstrings, commit messages, PR descriptions, changelogs, UI strings, error and log messages,
and localization files. Length does not matter — a one-line `msgstr` or a three-word button label goes through the same
checklist as a multi-page README. It does not govern code itself, marketing copy, or general end-user product copy
outside developer surfaces.

Defaults: American English, second person, present tense, sentence-case headings, serial (Oxford) comma. Voice model: a
knowledgeable colleague who has done this before.

## 1. When to apply

**Engage whenever the task touches English developer-facing text, regardless of length.** The verb in the request — not
the file size or the "this is just one word" feeling — decides.

Trigger actions:

- **Authoring:** writing new pages, comments, commit messages, PR descriptions, release notes, UI strings, or error and
  log messages.
- **Translation and localization:** translating to or from English; editing `.po`, `.pot`, `.properties`, `.resx`, JSON
  i18n, Fluent, or ARB files.
- **Editing:** rewriting an LLM-generated draft before commit (primary use), polishing someone else's prose.
- **Review and verification:** "check the wording", "does this read well", "is this English natural", "make this sound
  less AI", "proofread these strings", "verify the translation", "audit the changelog", "double-check the error
  messages", "cross-check the docstrings against the code".

Covered surfaces:

- Markdown: README, reference docs, design docs, ADR, runbooks, changelog, release notes.
- Source files (`.go`, `.js`, `.ts`, `.py`, `.java`, `.rs`, `.kt`, `.cs`, `.cpp`, `.rb`, `.swift`, `.scala`, `.php`,
  ...): all English text inside them — code comments (`//`, `/* … */`, `#`, `--`), docstrings (Javadoc, KDoc, TSDoc,
  JSDoc, Python docstrings, Rust doc-comments), and the English identifier names you choose for new functions, types,
  fields, files, and tests. A one-line `// TODO: handle retry` belongs here just as much as a multi-line Javadoc block.
- Localization files (English source or English target): `.po`, `.pot`, `.properties`, `.resx`, JSON i18n, `.ftl`,
  `.arb`. A one-line `msgstr` is in scope.
- UI strings: buttons, labels, placeholders, tooltips, empty states, confirmations.
- Error, validation, warning, and log messages.
- Commit messages, PR / MR descriptions, code-review replies.

Skip *existing* code identifiers, generated API references, third-party quotes, vendor product names, and license text —
do not translate or rename them. (When you choose a *new* identifier, the choice itself is in scope; see the bullet on
source files.) Yield to a more specific style guide if the repository already has one and existing prose is consistent
with it.

Anti-pattern: "I'm a fluent English speaker, I do not need the skill." Dialect policy, AI-tell catalog, em-dash and
hyphen rules, hedging policy, and per-surface templates live here — not in general fluency. If the request touches
English developer text and the skill is not loaded, stop and load it before answering.

Mechanical checks belong in tooling, not in this skill body:

- spelling and dialect substitutions (Vale plus Hunspell `en_US` / `en_GB`);
- terminology lists, future-tense, `currently`, `please`/`sorry`, sentence-length warnings (Vale: `Google`, `Microsoft`,
  GitLab-derived rules);
- inclusive-language substitutions with per-project allowlists (alex.js or Vale, not in prompts);
- commit-message grammar (commitlint with Conventional Commits).

Use this skill for the judgment-based work — tone, structure, AI tells, hedging, error-message empathy — that tooling
cannot enforce reliably.

## 2. Dialect policy

Default to American English: `-ize`, `color`, `organization`, `behavior`; double quotation marks in body prose;
`Month DD, YYYY` dates; `while` not `whilst` unless quoting. Keep the serial (Oxford) comma in both dialects — the
Chicago Manual of Style mandates it too.

Detect and yield. If a repository's existing prose is more than roughly 5% UK-spelled, treat it as `en-GB` and match.
Never mix dialects within a file. If a project pins a dialect in `CONTRIBUTING.md` or a Vale config, that wins.

Contractions (`don't`, `it's`, `you'll`) are welcome and reduce stiffness. Do not ban them, whatever a stricter house
style says.

## 3. Voice and tone

Write as a knowledgeable colleague. Address the reader as `you`. Use `we` only for a decision the team has actually
made, not as a default narrator.

- **Direct, not chatty.** No `Let's`, `Simply`, `It's that easy!`, or exclamation marks outside literal log output.
- **Specific, not promotional.** Say what the thing does, not how powerful, seamless, or robust it is. Drop adjectives
  that do not pay rent.
- **Empathetic, not apologetic.** `Sorry, something went wrong` makes errors look worse. Tell the reader what happened
  and what to do.
- **Confident, not hedged.** State the rule; add a caveat only when one is real.

Before / after:

- *Before:* "This powerful guide will simply walk you through everything you need to seamlessly set up the SDK."
- *After:* "Set up the SDK in four steps. You need a project ID and an API key."

## 4. Sentence and paragraph craft

- **One idea per sentence.** Aim for ≤ 25 words; split anything over 30 unless the structure genuinely needs the length.
- **Lead with the action or the answer (BLUF).** Put the verb the reader will run, or the conclusion they need, in the
  first clause.
- **Limit modifier stacks.** Three or more adjectives stacked before a noun
  (`a robust scalable cloud-native message broker`) is a rewrite signal.
- **One idea per paragraph.** First sentence carries the point; the rest supports it.
- **Active voice by default,** passive when the actor is unknown, uninteresting, or the sentence reads better that way
  (`The container is restarted on exit`).
- **Parallel lists.** Every bullet starts with the same part of speech; every step uses the same imperative form.

Before / after:

- *Before:* "With a carefully considered, well-architected configuration strategy, your deployment will run more
  smoothly."
- *After:* "A clear configuration layout makes deployments easier to debug."

## 5. Punctuation and AI-tell avoidance

LLM drafts have recognizable habits. Treat the list below as patterns to *reduce*, not tokens to ban — any one signal in
isolation is weak. Scan for them on every editing pass.

- **Em-dashes.** Use sparingly, and only when a comma, colon, or parenthesis genuinely will not do. Never as a paragraph
  connector or definition-list separator.
- **`It's not X, it's Y` / `not only X but also Y`.** Mechanically balanced parallels read like marketing copy. Collapse
  to the part you actually mean.
- **Tail `-ing` clauses that add `significance`.** "…enabling teams to deliver value at scale" almost never carries
  information. Cut.
- **Formulaic connectors.** Trim `moreover`, `furthermore`, `additionally`, `on the other hand` when an ordinary
  sentence break does the job.
- **Vague attributions.** `Widely regarded as`, `has been described as`, `experts agree`. Cite or delete.
- **Promotional closers.** `…unlocking new possibilities`, `…paving the way for the future of X`. Stop at the technical
  fact.
- **Rigid section scaffolding.** `Introduction / Challenges / Future Prospects` lifted onto a technical page. Use the
  section headings the content needs.
- **Bullet lists with a bold inline header plus colon on every item.** Fine for genuine term/definition pairs; an AI
  tell when used to format ordinary prose.

Hyphens, en-dashes, and em-dashes are three different characters. Number ranges take en-dashes (`10–20 requests`);
compound modifiers take hyphens (`high-throughput pipeline`).

## 6. Hedging and certainty

- **One hedge per sentence at most.** `May possibly sometimes fail` carries no information; pick one.
- **Present tense for current behavior.** `The handler retries three times`, not `The handler will retry`.
- **Reserve `will` for the actual future.** `The build will fail if the secret is missing` is fine because the future is
  the subject.
- **Avoid `currently`.** It dates the sentence the moment it is written. Either give a version, or drop it.
- **No page-topic self-description.** Skip `This guide explains how to…`. Start with the thing.

Before / after:

- *Before:* "This document currently describes how you can potentially configure the optional retry behavior, which may
  sometimes be useful."
- *After:* "Configure retries with `retry.max_attempts`. The default is three."

## 7. Domain modules

### Docs and README

Open with the answer: what the thing is, who it is for, the minimal command to try it. Procedures use numbered steps;
each step is one discrete action with a verifiable outcome. Link text names the destination (`the apm.yml reference`),
never `click here`. Inline code in backticks; code blocks language-tagged.

### Javadoc, docstrings, code comments

Document the *why* and the contract: invariants, side effects, ownership, units, thread-safety, error conditions. Skip
restating the signature — the type system does that already. One short summary sentence first, then details. Avoid
comments that narrate the next line of code. Comments age: if removing one would not confuse a future reader, do not
write it.

### Commit messages

Follow Conventional Commits: `type(scope): summary`. Summary is imperative present (`add`, `fix`, `remove`), ≤ 72
characters, no trailing period. Body, after a blank line, explains *why* the change was needed and notes anything
non-obvious (migration risk, perf trade-off, related incident). Reference issues by ID; do not paraphrase the diff.

- *Before:* `Updated some files to fix the thing that was broken sometimes.`
- *After:* `fix(auth): refresh token before retry on 401` plus a body explaining the race that caused the original
  failure.

### PR descriptions

Three short sections, in order: **Why** (the user-visible problem or constraint that forced the change), **What** (one
paragraph or bulleted change list), **How to verify** (commands, screenshots, or test names). Lead with the motivation:
a reviewer reads the change list and verification in light of it. Call out breaking changes, migrations, and follow-up
work explicitly. A reviewer should not have to read the diff to know whether to engage.

### Changelog and release notes

User-facing voice: name the change in terms of what the reader can now do, or what behavior has changed. Group under
`Added`, `Changed`, `Fixed`, `Removed`, `Deprecated`, `Security` (Keep a Changelog). One sentence per entry; link to the
PR or commit for detail. Breaking changes get their own block at the top of the version.

- *Before:* `Refactored the resolver pipeline for better performance.`
- *After:* `Resolver now caches DNS lookups for 30 s, reducing cold-start latency on connection-heavy workloads.`

### Error and log messages

Adapt the Atlassian pattern for developer surfaces: **reason + action + consequence**, in that order, in one or two
short sentences. No `please`, no `sorry`. Second person if you address the user; third person if you describe state.
Name the offending input or resource; include identifiers a developer can grep. Logs add structured fields rather than
padding the message with context.

- *Before:* `Sorry! Something went wrong, please try again later.`
- *After:* `Cannot open config file '/etc/foo.yaml': permission denied.`
  `Run as a user with read access, or set FOO_CONFIG to a readable path.`

Warning versus error: warnings describe a situation the program is handling; errors describe one it is not. Match
severity to prose — do not promote a recoverable retry to an error string.

## 8. Final-pass checklist

Run this once before commit. Each item should take seconds.

- BLUF — does the page, paragraph, or message open with the answer?
- Length — any sentence over 30 words that does not earn it?
- Modifiers — any noun carrying three or more adjectives?
- Em-dashes — count them; if they outnumber commas in a paragraph, rewrite.
- AI tells — `it's not X, it's Y`, `-ing` significance tails, `moreover`/`furthermore`, vague attributions, promotional
  closers?
- Hedging — at most one per sentence; no `currently`; no bare `will`?
- Self-description — any `This guide explains…` opener?
- Dialect — spelling and quotation style match the rest of the file?
- Lists — bullets parallel, steps imperative, headings sentence case?
- Code — inline identifiers in backticks; code blocks language-tagged; placeholders in `<angle-brackets>`?
- Domain format — commit follows Conventional Commits; error follows reason + action + consequence; release note is
  user-visible?

## 9. When to break the rules

Borrowing from Orwell via Google: break any of these rules sooner than write anything outright barbarous. The rules
exist to make prose clearer; if applying one in a specific spot makes prose worse, do not apply it. In particular:

- Passive voice is correct when the actor is unknown or uninteresting.
- A long sentence is fine when the idea genuinely is long and splitting it would obscure the logic.
- An em-dash is the right punctuation when a comma would mis-bind and parentheses would interrupt.
- A hedge is honest when the underlying behavior really is conditional.
- `We` is appropriate for a team decision; `I` is appropriate in a personal release note.

Reach for a deliberate exception consciously, in service of the reader. Drift into a habitual one and the prose stops
being yours.
