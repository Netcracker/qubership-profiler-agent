---
name: english-developer-style
description: >-
  Load before writing or rewording any English text that lands in a repository or in front of a developer: a code
  comment or doc comment, a commit message, a pull request title or description, a changelog entry, a README or docs
  page, an error or log message, a UI string, an English localization file. A coding task triggers it too: "fix the
  bug", "add the feature", and "write a test" each end in comments, a commit, and a description, so load it before the
  first comment is written, not when the description is due. Also load to review, proofread, translate, or check such
  text, however short: a one-line comment or a three-word label counts. Governs wording: sentence shape, vocabulary,
  punctuation, hedging, dialect, and the tells of machine-written prose. What each artifact contains belongs to the
  authoring skill for that artifact (a doc comment, a change description, a docs page); load it as well. Not for chat
  replies to the user.
---

# English developer style (American)

This skill governs natural-language text written for developers and for users of developer tools: README and reference
pages, code comments and docstrings, commit messages, PR descriptions, changelogs, UI strings, error and log messages,
and localization files. Length does not matter: a one-line `msgstr` or a three-word button label goes through the same
checklist as a multi-page README. It does not govern code itself, marketing copy, or general end-user product copy
outside developer surfaces.

Defaults: American English, second person, present tense, sentence-case headings, serial (Oxford) comma. Voice: a
knowledgeable colleague who has done this before.

What an artifact contains is another skill's job, and this one does not repeat it: `javadoc-authoring` for a doc comment
on the JVM, `godoc-authoring` for Go, `rustdoc-authoring` for Rust, `jsdoc-authoring` for JavaScript and TypeScript,
`pythondoc-authoring` for Python, `change-description-authoring` for a commit message, a pull request description, or a
changelog entry, and `docs-page-authoring` for a documentation page. Load the one for the artifact in front of you
together with this skill; this skill writes the sentences, that one decides which sentences there are.

## 1. When to apply

Apply this skill whenever the task touches English developer-facing text, regardless of length. The verb in the request
decides, not the file size and not the feeling that it is just one word.

Trigger actions:

- **Authoring:** new pages, comments, commit messages, PR descriptions, release notes, UI strings, error and log
  messages.
- **Translation and localization:** to or from English; edits to `.po`, `.pot`, `.properties`, `.resx`, JSON i18n,
  Fluent, or ARB files.
- **Editing:** rewriting an LLM-generated draft before commit (the primary use), polishing someone else's prose.
- **Review and verification:** "check the wording", "does this read well", "is this English natural", "make this sound
  less AI", "proofread these strings", "verify the translation", "audit the changelog", "double-check the error
  messages", "cross-check the docstrings against the code".

Covered surfaces:

- Markdown: README, reference docs, design docs, ADR, runbooks, changelog, release notes.
- Source files (`.go`, `.js`, `.ts`, `.py`, `.java`, `.rs`, `.kt`, `.cs`, `.cpp`, `.rb`, `.swift`, `.scala`, `.php`,
  ...): all English text inside them: code comments (`//`, `/* … */`, `#`, `--`), docstrings (Javadoc, KDoc, TSDoc,
  JSDoc, Python docstrings, Rust doc comments), and the English identifier names you choose for new functions, types,
  fields, files, and tests. A one-line `// TODO: handle retry` is in scope as much as a multi-line Javadoc block.
- Localization files, English source or English target: `.po`, `.pot`, `.properties`, `.resx`, JSON i18n, `.ftl`,
  `.arb`. A one-line `msgstr` is in scope.
- UI strings: buttons, labels, placeholders, tooltips, empty states, confirmations.
- Error, validation, warning, and log messages.
- Commit messages, PR and MR descriptions, code-review replies.

Skip *existing* code identifiers, generated API references, third-party quotes, vendor product names, and license text:
do not translate or rename them. Choosing a *new* identifier is in scope, per the source-files bullet. Yield to a more
specific style guide where the repository has one and its existing prose already follows it.

Anti-pattern: "I'm a fluent English speaker, I do not need the skill." The dialect policy, the AI-tell catalog, the
em-dash and hyphen rules, the hedging policy, and the per-surface templates live here, not in general fluency. If the
request touches English developer text and this skill is not loaded, stop and load it before continuing.

Mechanical checks belong in tooling, not here:

- spelling and dialect substitutions: Vale with Hunspell `en_US` or `en_GB`;
- terminology lists, future tense, `currently`, `please` and `sorry`, sentence-length warnings: Vale with the `Google`,
  `Microsoft`, or GitLab-derived rules. A fixed list of banned or preferred words is a linter's job. Judging whether a
  word the list has never seen is a defined term or a coinage is §4a's job;
- inclusive-language substitutions with per-project allowlists: alex.js or Vale, not prompts;
- commit-message grammar: commitlint with Conventional Commits.

Use this skill for the judgment work that tooling cannot enforce reliably: tone, structure, AI tells, hedging,
error-message empathy.

## 2. Dialect policy

Default to American English: `-ize`, `color`, `organization`, `behavior`; double quotation marks in body prose; `Month
DD, YYYY` dates; `while`, not `whilst`, unless quoting. Keep the serial (Oxford) comma in both dialects; the Chicago
Manual of Style mandates it too.

Detect and yield. Where a repository's existing prose is more than roughly 5% UK-spelled, treat it as `en-GB` and match.
Never mix dialects within a file. A dialect pinned in `CONTRIBUTING.md` or a Vale config wins over detection.

Contractions (`don't`, `it's`, `you'll`) are welcome and reduce stiffness. Do not ban them, whatever a stricter house
style requires.

## 3. Voice and tone

Write as a knowledgeable colleague. Address the reader as `you`. Use `we` only for a decision the team has actually
made, not as a default narrator.

- **Direct, not chatty.** No `Let's`, `Simply`, `It's that easy!`, or exclamation marks outside literal log output.
- **Specific, not promotional.** State what the thing does, not how powerful, seamless, or robust it is. Drop an
  adjective that adds no meaning.
- **Empathetic, not apologetic.** `Sorry, something went wrong` makes an error look worse. Tell the reader what happened
  and what to do.
- **Confident, not hedged.** State the rule; add a caveat only where one is real.

Before / after:

- *Before:* "This powerful guide will simply walk you through everything you need to seamlessly set up the SDK."
- *After:* "Set up the SDK in four steps. You need a project ID and an API key."

## 4. Sentence and paragraph craft

- **One idea per sentence.** Aim for 25 words or fewer; split anything over 30 unless the structure genuinely needs the
  length.
- **Lead with the action or the conclusion (BLUF).** Put the verb the reader will run, or the conclusion they need, in
  the first clause.
- **State what happens, not what does not happen differently.** `The method does not treat an uppercase name specially`
  is true, and still leaves the reader to derive the behavior from a rule stated elsewhere; `An uppercase name is
  rejected` names it. The negative form earns its place only where a documented expectation exists to deny (a superclass
  contract, a sibling member, a specification that does special-case the input), and then as a second clause that cites
  the expectation: `An uppercase name is rejected, where the constructor lowercases it.` A negative aimed at no
  expectation denies nothing.

  An expectation is what the other party *does*: the supertype's own implementation, the sibling's documented result,
  the special case the specification prescribes. A latitude is not one. `InputStream.skip` returns `0` for a negative
  count and adds that a subclass may handle one differently; an override that also returns `0` has denied nothing.
- **A sentence about what another API allows is not about this one.** `InputStream#skip(long) lets a subclass handle
  a negative count differently` is a true fact about `InputStream`, and a caller of this method still has to be told
  what this one does. Where the direct statement gives the whole contract, the permission sentence is provenance and
  goes. The deletion test: cut the clause and read what is left. If a caller could still act correctly on it, the
  clause carried nothing they needed. Keep it only where the reader arrives holding the other behavior as a contract
  they may rely on, and then the news is the deviation, not the permission.
- **A contrast that raises a question is cut, not answered.** `…and this one does not` makes the reader ask why not.
  The usual repair, a clause of rationale after it, keeps the contrast and adds a sentence; the repair that works
  drops the contrast, and the question is not asked. Measured on one comment: four drafts kept the clause and
  differed only in how they answered it, and a fifth, written with no earlier text in view, carried the whole
  contract in one line.
- **Keep a truth condition in the sentence it limits.** A later paragraph does not repair an earlier sentence that made
  the claim without its condition; a reader quotes the earlier sentence, and a future edit shortens it. `Could` and
  `may` do not carry the condition either, because they name no case. Put the condition in the same sentence, or point
  at the sentence that carries it with an explicit anaphor such as `for that caller`.
  - *Before:* "Summing therefore changes no answer."
  - *After:* "Summing returns the same zero-or-nonzero answer the predicate reads."
- **Give the action to the component that performs it.** A caller configures, invokes, and observes; the class under
  discussion reads the flag, records the progress, throws the exception. A sentence with the caller as subject because
  its configuration decides the outcome is wrong about the code, not merely vague. Where the caller really is the actor,
  it is the right subject.
  - *Before:* "A caller that opens the session with retries enabled sees them applied there."
  - *After:* "For such a caller, the retry policy applies them until the deadline passes."
- **An equivalence claim names the dimension it holds on.** Two implementations can return equal values and still differ
  in what they throw, how long they block, what they allocate, and what they leave behind; `changes no answer` claims
  all of those at once. Name the equal dimension, and give the differing one its own sentence.
  - *Before:* "Both forms agree, so summing changes no answer."
  - *After:* "Both forms agree on the comparison with zero. Summing also throws where the current form does not."
- **Limit modifier stacks.** Three or more adjectives before a noun (`a robust scalable cloud-native message broker`) is
  a rewrite signal. Nouns used as adjectives count: `a fixed-length protocol message length prefix` and `as envelope
  arithmetic input` carry no adjective and are worse, because the reader has to guess which noun is the head.
- **Unpack an over-compressed sentence, and spend the words to do it.** Technical prose fails far more often by being
  too dense than by being too long, and the failure is invisible to a length rule, because the offending sentence is
  usually short. Each shape below costs the reader an extra pass.

  | Shape | Before | After |
  | --- | --- | --- |
  | Stacked relative, pronoun dropped | `a message for a ceiling no connection property raises` | `…for a ceiling that no connection property raises` |
  | Elliptical nominal opener, a labeled fragment with no verb | `Hard cap, enforced unconditionally by readMessageLength.` | `The protocol fixes this maximum, so readMessageLength enforces it unconditionally.` |
  | Noun pile | `a fixed-length protocol message length prefix` | `the length prefix of a message whose length the protocol fixes` |
  | Colon splicing two independent claims | `it cannot be larger than the startup packet: every option in it is a name the driver chose` | `…than the startup packet, and every option it names is a name the driver chose` |
  | Purpose clause trailing a long sentence | `…names the property, so an operator is not left reading "expected between 5 and 1048576" and guessing` | `…names the property. Otherwise an operator would see "expected between 5 and 1048576" and have to guess` |
  | Imperative where a rule is meant | `Keep this limit at or below X, and keep every other one there too.` | `This limit, and every other limit that applies before authentication, must stay at or below X.` |
  | Vague deictic | `there too`; `8 MiB clears every plausible one` | the noun itself: `8 MiB leaves room for any realistic backend` |
  | Hypothetical in the indicative | `a fork that raises both limits reaches 2.6 MiB` | `a fork that raised both limits would reach 2.6 MiB` |
  | Adverb stranded after a comma | `Values match the lowercase token of each constant, case-insensitively.` | `Its value is the lowercase name of one of the constants, matched case-insensitively.` |
  | Nominalization where a verb exists | `avoids collision with unrelated software` | `keeps it from clashing with unrelated software` |
  | A new fact spliced into a sentence that already worked | `Accepts the same value forms as maxResultBuffer: a byte count, a decimal suffix (64M is 64000000 bytes) or a percent of the maximum heap (10p).` | `Sizes use decimal suffixes, so 64M means 64000000 bytes. A percent of the maximum heap, such as 10p, is accepted too.` |
  | A new fact wedged in as an appositive | `Largest GSS token, in bytes, that the driver accepts` | `Largest GSS token the driver accepts, in bytes` |

  Unpacking adds words and no information, so a rewrite budget tends to forbid it. Spend the words anyway; the payment
  is the re-read you remove. This does not license explaining more, hedging, or a sentence of rationale, which earn
  their place on other grounds.

  The last two rows apply when you *add* a fact rather than compress: the fact gets its own sentence, or the end of the
  one it belongs to, never the middle. The fact is real and the sentence still true, so the writer rarely notices the
  splice (measured: 3 of 4 rewrites that lost to a plainer draft on language alone had gained a genuine fact this way).

  Unpacking can itself create a re-read, and that is its limit. Opening a parenthesis into a subordinate clause loses
  when the clause lands inside a correlative pair: `feeds neither the row-size estimate (no fetchSize makes an
  over-sized row fit) nor the counter` becomes `is left out of both the row-size estimate, where no fetchSize would make
  an over-sized row fit, and the counter, since…`, and only the word count improved. If the repair puts more between
  `both` and `and`, or between a subject and its verb, than the original put in brackets, keep the brackets.

  The hypothetical row has one exception. In the comment on a regression test the defect is history, and the past tense
  is the honest tense for it: `A negative count used to be reported as available, and the caller then failed with
  StringIndexOutOfBoundsException.` A chain of `would` clauses makes the reader hold a hypothetical open across the
  whole paragraph, and narrating it in the present tense describes a path the code no longer has. What the comment on
  a regression test carries, and in which order, is `javadoc-authoring` §1.
- **Shorten by cutting sentences, never by cutting qualifiers.** A text gets shorter by dropping a sentence whose fact
  you can name and defend dropping, or by moving the fact to the tag or the member it belongs to, never by thinning a
  sentence that stays. An adverb, an adjective, or a verb of origin usually carries a claim, and removing it changes the
  contract while the sentence still reads well:
  - `the slot stays implicitly non-null` became `the slot stays non-null`: *implicitly* carried the claim: the slot is
    non-null by default, not because an annotation did anything.
  - `any unannotated slot defaults to @NonNull` became `a slot with no annotation is @NonNull`: *defaults to* named
    where the non-nullness comes from; *is* does not.
  - `protocol data from a stream it can no longer make progress on` became `stale protocol data`: the clause gave the
    reason the bytes are unusable; *stale* only reports that they are.

  After you shorten, re-read the old sentence word by word. For every qualifier that is gone, name the claim it carried
  and where it now lives, or put the word back. A word limit is a signal to cut a sentence, not a license to compress
  one.
- **One idea per paragraph.** The first sentence carries the point; the rest supports it.
- **Active voice by default,** passive where the actor is unknown or uninteresting, or where the sentence reads better
  that way (`The container is restarted on exit`).
- **Parallel lists.** Every bullet starts with the same part of speech; every step uses the same imperative form.

Before / after:

- *Before:* "With a carefully considered, well-architected configuration strategy, your deployment will run more
  smoothly."
- *After:* "A clear configuration layout makes deployments easier to debug."

## 4a. Use the field's word

A reader searches for the word they already know, so word choice decides whether the text can be found at all. Three
cases; only the third calls for a rewrite.

**The field has a published definition: keep it.** `idempotent`, `memoize`, `hoist`, `monotonic`, `backpressure`,
`quorum`. The test is whether the word has a fixed definition in the sense you mean, not whether it sounds like jargon.
A term points at a concept the reader can look up; the paraphrase is longer, vaguer, and points at nothing. This holds
hardest for text an LLM or a specialist reads (an `AGENTS.md`, a design doc, an internal comment) and relaxes for a page
written for someone outside the field, where a term earns its place by being defined once.

Synonym rotation is the opposite failure and also collapses to one word: `check`, `verify`, `confirm`, and `validate`
across four sentences read as four operations.

**The code has its own word: match it.** Name the thing the way the field, parameter, or setting names it, and the way
the documentation names it. A reader given `exceeds the ceiling of 1048576 bytes` searches for `ceiling`, and a codebase
that calls it a limit everywhere else has cost that reader a search for nothing: the message should have said `limit`.
The rule binds in the direction people rarely check: when a rename or a house word changes what the code calls
something, the prose and the strings change with it.

This covers verbs, not only names. Where the code or its comments already name a relation (a model *applies to* a
method, a lock *guards* a field), use that verb rather than a fresh one. An invented verb hides better than an invented
noun because it looks like precision: `the contract binds every override` asserts an enforcement the code does not
perform, where `applies to` states what the code does.

Where the code's own name is the wrong word, the repair is a rename, not a comment that quietly uses another word. Name
the identifier as the code spells it today and report the name; a sentence that avoids the word to avoid the problem has
obeyed the rule and lost the reader.

**The word is yours: replace it.** A coinage looks like a term and defines nothing: `envelope arithmetic`, `version
gating`, `the inventory of messages`. If defining it takes a paragraph, the field has a word already, and that paragraph
is the search you should have run. Where the field genuinely has none, define the term once, in the class or page it
belongs to, before anything else uses it, and expect that to be rare.

A metaphor is the coinage's usual disguise. `Bytes ride along with the ticket` and `the envelope the row travels in`
name no mechanism a reader can look up, which is the same defect as *personified components* in §5, seen from the
vocabulary side rather than the verb side. Describe code in the vocabulary of software engineering and computer
science, and borrow a word from another domain only where the code works with that domain: a payments module posts a
ledger entry because bookkeeping is its domain. Construction is the usual source of borrowed words: `critical`, not
`load-bearing`; `the key part`, not `the cornerstone`; `a check`, not `a guardrail`.

A nickname is the other disguise. *The goodbye* for the Terminate message, *every way in* for the methods that read,
*one frame up* for the calling method: a reader who greps for `Terminate` or for the method name finds nothing, and a
nickname that sounds apt to its author is opaque to everyone else. Use the name the code or the protocol uses.

## 5. Punctuation and AI-tell avoidance

LLM drafts have recognizable habits. Treat the list below as patterns to reduce, not tokens to ban; any one signal in
isolation is weak. Scan for them on every editing pass.

- **Em-dashes.** Use them sparingly, and only where a comma, a colon, or a parenthesis genuinely will not do. Never as a
  paragraph connector, and never as the separator in a `term — definition` list item: `- retries — how many times the
  client tries again` is a definition list written with a dash; a colon or a full sentence serves better. That
  construction is the forbidden one; a dash doing ordinary parenthetical work in a list item falls under the sparing
  rule like any other. Inheriting the pattern from the text you are editing does not justify keeping it, and removing it
  alone does not justify a rewrite. A double hyphen is not an em-dash: in Javadoc and in most renderers `--` renders as
  two hyphens, so it fails as punctuation and under this rule. Use a comma, a colon, or a new sentence.
- **`It's not X, it's Y` and `not only X but also Y`.** Mechanically balanced parallels read like marketing copy. Keep
  the part you mean.
- **Tail `-ing` clauses that add significance.** "…enabling teams to deliver value at scale" almost never carries
  information. Cut it.
- **Formulaic connectors.** Trim `moreover`, `furthermore`, `additionally`, `on the other hand` where a sentence break
  does the job.
- **Vague attributions.** `Widely regarded as`, `has been described as`, `experts agree`. Cite or delete.
- **Promotional closers.** `…unlocking new possibilities`, `…paving the way for the future of X`. Stop at the technical
  fact.
- **Rigid section scaffolding.** `Introduction / Challenges / Future Prospects` on a technical page. Use the headings
  the content needs.
- **Bullet lists with a bold inline header plus colon on every item.** Fine for genuine term and definition pairs; an AI
  tell when used to format ordinary prose.
- **Gapped verbs.** `…which governs the direction this ceiling does not` makes the reader reconstruct `govern` from the
  first half. Repeat the verb, or split the sentence.
- **Personified components and data.** The verb decides, not the noun. Whatever the subject is (a stream, a check, a
  limit, a message, a flag, a handler, a pool, a test, or the driver), it takes a verb that names a mechanism: reads,
  returns, throws, closes, skips, copies, allocates, checks, reports, sends, receives, rejects, accepts, records,
  bounds, marks. A verb that needs a mind is the wrong verb for a component. Verbs flagged in review by one codebase's
  maintainers: *answer*, *ask*, *say*, *tell*, *know*, *notice*, *want*, *agree*, *promise*, *honor*, *owe*, *play
  along*, *leave alone*, *hand out*, *hand back*, *hang off*, *come through*, *go away*, *say goodbye*, *prove*, *stand
  for*. Bytes likewise do not *travel*, *ride along*, or *run to* a size. Standard terms stay: *reject*, *refuse*, and
  *accept* for validation (`a negative count is refused`, `connection refused`), *trust* for a trust boundary
  (`untrusted input`), *expect* in `expected between 5 and 1048576`, *see* in `see {@link ...}`, and the protocol's own
  vocabulary (`the server refuses the connection`, `the backend sends`).
  - *Before:* `Fails when ensureBytes answers a negative count instead of refusing it.`
  - *After:* `Fails when ensureBytes accepts a negative count instead of throwing.`
  - *Before:* `the ceiling answers to -Dpgjdbc.protocolHardeningMode=disable`
  - *After:* `-Dpgjdbc.protocolHardeningMode=disable turns the limit off`
  - *Before:* `Saying goodbye is best-effort, closing is not.`
  - *After:* `The Terminate message is best-effort, but the close is not.`
- **Cleft sentences.** `X is what makes Y`, `which is why`, `which is how`, `is where`, `the one X that`, `that is
  what`, `exactly the case this exists for`: each makes the reader hold a frame open until the end of the sentence, and
  a paragraph built from them is an argument rather than a description. State the fact in plain order: cause then
  effect, or effect then `because`.
  - *Before:* `The goodbye failing is what says the connection went wrong; closing after that failing is what closing a
    broken connection does.`
  - *After:* `The failed write reports what went wrong with the connection. The failure after it is only what closing a
    broken connection produces.`
- **Coined shorthand.** `reads as`, `surfaces as`, `off the wire`, `wraps positive`, `in both directions`, `both axes`,
  `load-bearing`, `the wire-level outcome`, `names the failures apart`: each stands in for a plain phrase the writer did
  not stop to find. Write the plain phrase: `is reported as`, `arrives in a backend message`, `overflows to a positive
  value`, `in two ways`, `gives each failure a distinct message`. This is §4a's coinage rule at the phrase level.
- **A caption where a sentence is meant.** `The wire-level outcome the guard exists for.`, `Not what isClosed()
  reports.`, `Not volatile, and deliberately so:`, `The socket, not whether close() has run:` open a comment with a
  label that has no subject and no verb. This is the elliptical opener from §4's table, and the shape LLM drafts produce
  most often at the start of a doc comment. Give it a subject and a verb.
  - *Before:* `Not what {@link #isClosed()} reports. That asks the socket, which ...`
  - *After:* `{@link #isClosed()} reports something else: it reports the socket, which ...`
  - *Before:* `Not volatile, and deliberately so: this class is unsynchronized throughout, and ...`
  - *After:* `The field is deliberately not volatile. This class is unsynchronized throughout, and ...`
- **Contrast as the default sentence shape.** `A merge conflict is a property of the queue, not of the change`,
  `the order is a fair question; which one to keep is not`, `not a symptom of numbers I could not defend` each make
  the reader decode a contrast before they get the fact. State the fact. Keep a contrast only where the reader would
  otherwise assume the alternative, and then name the alternative once, in a second clause. A paragraph that ends on
  an antithesis is arguing; the reader wants the finding.
- **Do not write in the register of this skill.** The skill compresses each rule into a short sentence so that it
  can be remembered. The text you produce is read once, by someone with less context, so use the plainer sentence.
  If a sentence you wrote could be pasted into this skill as a rule, rewrite it.
- **Notation standing in for a word.** In running prose write the connective out: `+` as *and*, `/` as *or*, `~` as
  *about*, `≥` as *at least*. The test is whether the symbol replaces a word, not whether a symbol appears: the `+` in
  `Capture name + declared length + envelope endpoint`, the `/` in `forks such as CockroachDB/YugabyteDB/Redshift`, and
  the `~` in `a ~1.7 GB allocation` all do. Keep the notation where it is the subject rather than the punctuation: a
  byte layout (`4 (length) + 2 (field count) + 4 per field`), a range, a version constraint, an expression the reader
  will type, and **a comparison against a named setting, limit, version, or threshold**. `pool.maxIdle > 0` is the
  predicate the code tests and the value the reader will look up, and a grep finds it in both places; *a non-zero
  `pool.maxIdle`* is longer and matches neither. The symbol is right where you would read it aloud as itself.
- **Digits a reader will grep stay unseparated.** Write `65535`, not `65,535` or `65 535`. A number in a comment, an
  error message, or a config example gets pasted into a test or matched against the code, and the grouping character is
  itself locale-dependent (`65 535` in French, `65.535` in German). Group digits only in prose about a magnitude nobody
  will look up, such as `about 2 million rows`, and prefer the word there. Leave locale-aware grouping to the runtime
  formatter that renders a number for an end user.
  - **A locale-aware formatter groups them for you.** `MessageFormat.format("length {0} exceeds {1}", len, max)` passes
    each argument through the locale's `NumberFormat`, so one reader files a bug quoting `length 65,535` and another
    `length 65 535`, and neither string matches the source. Pass the digits as text: `Integer.toString(len)`,
    `Long.toString(len)`, or `Long.toUnsignedString(len)` for a value the signed type cannot hold. Where the argument
    has to stay a number, the `{0,number,#}` format element suppresses grouping. `String.format` groups only when asked
    with `%,d`, but under the default locale it can still replace the digits themselves (`٦٥٥٣٥` under
    `ar-EG-u-nu-arab`), so pass `Locale.ROOT` for technical output. Go's `fmt` and Rust's `format!` do not localize
    numbers, so a plain `%d` or `{}` is enough there.
  - **A size or duration a reader has to judge gets both forms: `64 MB (64000000 bytes)`, `about 25 days (2147484
    seconds)`.** The unit shows whether the limit is anywhere near their workload; the digits are what they grep, paste
    into a config, or compare against the source. A parenthesis is the cheapest way to carry both. Below roughly 100000
    the digits stand on their own (`8008`, `30000`, `65535`); a short form invented for them either lies or adds
    nothing. Above that, apply the pair to a value the reader weighs against their own configuration, not to every large
    number a sentence contains.
    - **Pick the unit that makes the short form exact.** `64000000` is `64 MB` and `8388608` is `8 MiB`; swapping the
      two misleads the reader about whether `64M` in a config matches the limit.
    - **Spell an inexact short form out: `about 1 GiB (1073741823 bytes)`.** An unmarked short form claims to be exact,
      so the hedge carries information rather than softening a claim, and it belongs even where §6 would otherwise cut
      one. Write the word, not `~`, per the notation rule above. Where the direction matters, as with a limit a reader
      could configure straight past, `just under 1 GiB` names the side the value falls on and `about` does not.
    - **A value a formatter fills in at runtime** prints as digits, so the unit belongs in the surrounding text:
      `exceeds the limit of {0} bytes`.
- **An apostrophe inside a format pattern is an operator, not punctuation.** In `java.text.MessageFormat`, which every
  `MessageFormat.format` call and every `Logger.log` with an `Object[]` goes through, a single quote opens a quoted
  literal, and the placeholders inside it stop being placeholders. Reword `the driver's own limits … report it at {2}`
  to `the limits the driver chose for itself … report it at {2}` and the argument substitutes again; with the apostrophe
  left in, the user is told to report it at `{2}`. The escape is a doubled `''`, but prefer the rewording: a doubled
  quote in a `msgid` is a trap for the next translator, and nothing in the build fails either way. The same rule catches
  `don't`, `can't`, and `the server's` in any string that carries a placeholder. Python `str.format`, Rust `format!`,
  and Go `fmt` have no such rule, so this trap is specific to the JVM's formatter and to ICU.
- **Treat a quoted diagnostic as a literal.** Put an error or log message in inline code and reproduce its
  capitalization, spacing, and final punctuation exactly. `Large Objects may not be used in auto-commit mode.` carries
  its period in the source, and moving it outside the quotation marks to satisfy prose punctuation makes the text no
  longer the string a reader will grep for.
- **A fenced block carries its language.** Open every fence with the tag that names what is inside: `bash`, `java`,
  `json`, `diff`, or `text` for output that is none of these. The tag turns on highlighting, selects whether a doc build
  compiles or lints the block, and labels the block as a command or as its output. An untagged fence is the default in
  most editors and carries no information, so use `text` rather than leaving it bare.

Hyphens, en-dashes, and em-dashes are three different characters. Number ranges take en-dashes (`10–20 requests`);
compound modifiers take hyphens (`high-throughput pipeline`).

## 6. Hedging and certainty

- **One hedge per sentence at most.** `May possibly sometimes fail` carries no information; pick one.
- **Present tense for current behavior.** `The handler retries three times`, not `The handler will retry`.
- **Reserve `will` for the actual future.** `The build will fail if the secret is missing` is fine, because the future
  is the subject.
- **Avoid `currently`.** It dates the sentence the moment it is written. Give a version, or drop it.
- **No page-topic self-description.** Skip `This guide explains how to…`. Start with the thing.

Before / after:

- *Before:* "This document currently describes how you can potentially configure the optional retry behavior, which may
  sometimes be useful."
- *After:* "Configure retries with `retry.max_attempts`. The default is three."

## 7. Domain modules

**Edit content before prose.** On an editorial pass, decide which facts belong in the artifact before improving any
sentence. A pass that only rewrites sentences keeps every paragraph, including the ones that should not be there.

**Before is not instead.** The sentences that survive the cut, and any that replace what went, still go through §4 and
§5. Run §8 over the final text, not over the draft you started from.

**The text you are editing is provenance, not evidence.** A claim's presence shows that it was made, not that it is
true. Verify every claim about behavior or mechanism (what the code reads, sets, returns, what a caller observes)
against the code, the tests, or an authoritative contract, and verify a claim you carry over unchanged the same way. A
claim supported only by the version you are editing is unsupported.

**An unchanged value is not a known value.** *The call no longer resets the flag* is a claim about the code; *the flag
stays enabled for the duration* is a claim about every caller, and it holds only where something checkable fixes the
incoming value. Where callers choose the value, name the condition (*for callers that leave it at the default*) or make
the claim about the operation.

**Name the layer that permits or refuses.** A check passed is not an operation performed. *The client-side guard now
permits the call* is checkable against the guard; *the operation now works* also claims that the far side accepts it.
Use the end-to-end wording only for a result established end to end, such as by a test that exercises the whole path.

**Name the actor whose behavior the verb describes.** *The guard admits the call*, *the method returns*, *the server
rejects the request* each name the actor, and the layer follows. A pronoun does not: *they refuse only when the flag is
set* is true at whichever layer the reader supplies. Where a claim holds at one layer and not the next, the subject is
the part that carries that boundary, and a broader subject silently widens the claim. This is not a ban on pronouns.
*It* is fine where the previous sentence names one actor at the same layer; the test is whether a reader could resolve
the pronoun to a different actor, or to the same actor at a different layer.

**A rewrite that removes an antecedent must repair the reference it orphaned.** *This*, *that*, *such a*, a pronoun, or
a definite noun phrase left behind resolves to whatever now precedes it. Repair it in the same pass. An ambiguity the
text already had is worth fixing and is not urgent; one the rewrite created is.

**A private helper is an implementation artifact.** Name it only where its name or its boundary changes what a reviewer
decides: the identifier they will grep for, or an extraction that is itself under review. Otherwise write *the guard*,
*the check*, or *the parser*.

**A code expression keeps its operators.** §5's notation rule is about a symbol standing in for a word. It does not
reach inside an expression the reader may copy, paste, or grep: `retries > 0 && !closed` is the condition the code tests
and the string a reader searches for, and *retries above zero and the handle still open* is longer, no clearer, and
matches neither. Quote the expression, or describe the behavior without a paraphrase shaped like code.

**A counter is a ceiling, not a target.** Where tooling counts long sentences, dashes, or untagged fences, a passing
number is finished. Lowering it at the cost of length or clarity trades what a reader notices for what only a script
sees.

**Label a numbered reference when the numbers around it mean different things.** In a paragraph that also cites an
issue, a release, or an RFC, write `PR #123` and `issue #456`. Where every number in view is the same kind, the bare
form is fine.

**Words that quantify need their boundary named.** *Only*, *always*, *never*, *any*, *every*, *permits*, *refuses* each
range over a set, and one escaping member falsifies the sentence. Name the set (the states of an enum, the versions in a
range, the callers a contract admits), check its least convenient member (the one the sentence was drafted without), and
either name the boundary in the sentence or drop the quantifier.

**An identifier you do not have is not invented.** A pull request number, an issue number, a commit hash, or a
version that does not exist yet is written as a placeholder the reader can see (`[PR #NNNN]`, `TBD`) or left out,
never guessed from the last one in view, and the pull request description says which placeholder is to be filled in
before the merge. A wrong number sends the reader to someone else's change, which is worse than no number (measured:
six of nine Opus runs on one day wrote the next number after the latest one in the changelog).

**A bug report is provenance too.** The report, the task statement, and another change's description show what someone
observed or meant; the code shows what happens. Verify the symptom, the condition, and every number against the source
before any of them reaches a comment, a commit message, or a changelog entry, and where the two disagree, write what the
code does (measured: two of three bug reports in one experiment carried a false claim, and the runs that copied it into
their comments were the ones marked down).

### Docs and README

Link text names the destination (`the apm.yml reference`), never `click here`. Procedures use numbered steps; each step
is one discrete action with a verifiable outcome. Inline code in backticks; code blocks language-tagged. Which reader a
section serves, what an option entry carries, and what a change owes the documentation are `docs-page-authoring`.

### Javadoc, docstrings, code comments

**Write in the register of the JDK's own Javadoc, not of an essay.** The reader is a maintainer scanning a class in an
IDE, reading the comment once in the middle of doing something else. A sentence they have to read twice has failed,
however elegant it is. §5's rules on personified components, clefts, coined shorthand, and captions bind hardest here,
because a comment has no surrounding prose to carry the reader past a bad sentence.

Do not narrate the next line of code, and do not restate the signature; the type system does that already. Which slots a
doc comment has, what the first sentence must do, when a member needs no comment, what a test class or an override says,
and how a rewrite is budgeted are `javadoc-authoring` (Go: `godoc-authoring`; Rust: `rustdoc-authoring`; JavaScript and
TypeScript: `jsdoc-authoring`; Python: `pythondoc-authoring`). Comments age: if removing one would not confuse a future
reader, do not write it.

### Commit messages, pull request titles, and pull request descriptions

The subject line is imperative present (`add`, `fix`, `remove`), with no trailing period, and a noun phrase is not an
imperative: `docs: changelog for the EOFException` labels the change; `docs: add the EOFException to the changelog`
states what the commit does. Where the repository prefixes subjects (`type(scope):`), the imperative is the first word
after the prefix.

- *Before:* `Updated some files to fix the thing that was broken sometimes.`
- *After:* `fix(auth): refresh token before retry on 401`, plus a body explaining the race behind the original failure.

**Each section of a description opens at its own level.** BLUF applies to the document and again to every section:
state the observable defect or the violated contract before the arithmetic, the call path, or the mechanism that
proves it. Where a lead sentence above the sections already names the defect, the first section may open with the
mechanism.

What the body of a commit carries, which sections a pull request description has and what each one owes the reviewer,
whether a prefix or a length limit applies, which trailers a reader greps, and what a squash merge copies into the
history are `change-description-authoring`.

### Review replies

The reader is the reviewer. They wrote their comment days ago, the thread has scrolled off screen, and they read
your reply once, on GitHub, between two other tasks. Write so that one read is enough.

- **One section per point the reviewer made, in the reviewer's order.** Open the section by restating the point in
  one sentence, in the reviewer's words or as a short quote, so they do not scroll up to find it. Then give the
  answer in one sentence. Then the evidence: the constant and its value, the file, the test class, the quote with
  its date and link.
- **Restate a list by quoting it or repeating its items, never by counting it.** Where the reviewer listed five
  numbers or four citations, write `the five limits you list` and then the five, or quote the sentence. `Two of the
  five values you list are off` and `the three citations you list` are new claims, and a count is the claim most
  likely to be off by one. Check the count against the reviewer's text before you write it, or do not write it.
- **One piece of evidence per sentence.** A sentence carries the constant and its value, or the file, or the quote
  with its date. A link replaces a commit hash and a date, so give the link and not all three. Put the rest of the
  evidence in a table under the sentence, not in the sentence.
- **Quote what you cite.** `Item 3 of your review` sends the reviewer up the thread even with a link beside it.
  Quote the item in one line, or leave the citation out.
- **A heading names the reviewer's point, not your conclusion.** Write `The GSS limits` or `Tests that need a
  server`, not `Both changes use the same two libpq constants`. The conclusion is the first sentence under the
  heading, next to the evidence for it.
- **Say what is right before what is wrong.** Where the reviewer's point holds, say so first, in that section. A
  reply that only disputes is read as a defense, even when every sentence in it is true.
- **`I` and `you` are the normal subjects.** The author checked, rebased, chose, proposes. The reviewer wrote,
  cites, proposes. A sentence whose subject is an abstraction and whose verb is `is` (`the properties are not a
  symptom of`, `the rest of the comparison is checkable`) hides who did what. Name the person or the component,
  and use a verb that names an action.
- **Every referent is in the reply or linked from it.** `Your earlier review`, `that commit`, `the compromise you
  propose`, `the same path`, `here`, `above` each send the reviewer up the thread. Link the comment, name the commit
  by hash, or restate the point in half a sentence.
- **Ask for a decision as a question, with the options named.** `Do you want the driver to close the connection,
  or to skip the row and fail the query? I will implement either.` A section that describes a contradiction and
  stops has not asked anything.
- **Quote the reviewer exactly**, punctuation included, with the date or a link. Do not paraphrase a sentence you
  are about to disagree with.
- **State the fact and what it means for the decision, and stop.** Do not end a section on a line written to land
  (`Which one to keep is not.`, `a property of the queue, not of the change`). The reviewer is deciding what to
  merge, not scoring a debate.
- **Length.** A comment with N points gets about N short sections. A section past five sentences moves its evidence
  into a table or a link to the code.

Before and after, from one reply:

- *Before:* `A merge conflict is a property of the queue, not of the change. It should not enter a comparison of
  two changes, and it should never produce "this PR is not mergeable".`
- *After:* `The conflict came from #4336 touching the same files, and I resolved it in a minute. Any PR that waits
  long enough conflicts with something, so I would not count it against either PR.`
- *Before:* `The properties are therefore not a symptom of numbers I could not defend. Each one sets a limit whose
  right value only the user knows.`
- *After:* `So I did not add the properties to cover for numbers I could not defend. They exist because only the
  user knows how large their rows and COPY messages get.`
- *Before:* `The order they land in is a fair question. Which one to keep is not a fair question.`
- *After:* `Merge order is worth discussing. Dropping one of the two is not, because outside GSS they change
  different things.`

### Changelog and release notes

User-facing voice: name the change as what the reader can now do, or what behavior has changed, and name the symptom a
reader will search the release notes for, such as the exception class or the error text, even at the cost of a second
sentence. Which parts an entry carries, in which order, and where a breaking change is marked are
`change-description-authoring`.

- *Before:* `Refactored the resolver pipeline for better performance.`
- *After:* `Resolver now caches DNS lookups for 30 seconds. Cold-start latency drops on connection-heavy workloads.`

### Error and log messages

Adapt the Atlassian pattern for developer surfaces: **reason + action + consequence**, in that order, in one or two
short sentences. No `please`, no `sorry`. Second person where you address the user; third person where you describe
state. Name the offending input or resource, and include identifiers a developer can grep. Logs add structured fields
rather than padding the message with context. The double-quote default of §2 covers prose: a message may quote a path or
an identifier the way its platform does (`'/etc/foo.yaml'`), as long as one codebase picks one style.

- *Before:* `Sorry! Something went wrong, please try again later.`
- *After:* `Cannot open config file '/etc/foo.yaml': permission denied.` `Run as a user with read access, or set
  FOO_CONFIG to a readable path.`

**A message is bound to the vocabulary of the code it reports on** (§4a), and the binding costs most here: the reader
holds a string and a search box, with no context around either. Where the message is translated, the English text is the
source the other locales follow, so one drifting word reaches every one of them.

Warning versus error: a warning describes a situation the program is handling; an error describes one it is not. Match
severity to prose; do not promote a recoverable retry to an error string.

## 8. Final-pass checklist

Run this once before commit. Each item should take seconds.

- BLUF: does the page, paragraph, or message open with the conclusion?
- Length: any sentence over 30 words that does not earn it?
- Modifiers: any noun carrying three or more adjectives?
- Em-dashes: count them; if they outnumber commas in a paragraph, rewrite.
- AI tells: `it's not X, it's Y`, `-ing` significance tails, `moreover` or `furthermore`, vague attributions,
  promotional closers?
- Register: does any component *answer*, *ask*, *say*, *owe*, *know*, *hand back*, or take any other verb that needs a
  mind? Any nickname (`the goodbye`, `every way in`) where the code has a name?
- Clefts: any `is what`, `which is why`, `which is how`, `the one X that`?
- Captions: does any comment open with a label that has no subject and no verb?
- Qualifiers: did any sentence get shorter, and if so, which claim did each dropped adverb, adjective, or verb of origin
  carry, and where does it live now?
- History: is a past defect in one past-tense sentence, rather than narrated in the present tense or as a chain of
  `would` clauses?
- Hedging: at most one per sentence; no `currently`; no bare `will`?
- Self-description: any `This guide explains…` opener?
- Dialect: spelling and quotation style match the rest of the file?
- Lists: bullets parallel, steps imperative, headings sentence case?
- Code: inline identifiers in backticks; code blocks language-tagged; placeholders in `<angle-brackets>`?
- Density: run §4's table. Any of the twelve shapes left compressed, any new fact spliced into a working sentence, and
  did any repair create a re-read of its own?
- Notation: any `+`, `/`, `~`, or `≥` standing in for a word where nothing is being computed?
- Numbers: any digit grouping in a value a reader would grep, paste, or compare with the code? Does every size or
  duration a reader has to judge carry both forms, `64 MB (64000000 bytes)`, with the unit either exact or hedged in
  words?
- Behavior: does any sentence state what does not happen differently, where naming the behavior would do?
- Provenance: does any sentence state what another API allows, where the rule this one follows is what the reader
  needs? Does any contrast raise a why-not that the next clause then answers?
- Conditions: is every truth condition in the sentence it limits, or pointed at by an explicit anaphor?
- Actors: does each action belong to the component that performs it?
- Equivalence: does every claim of sameness name the dimension it holds on?
- Vocabulary: does every sentence name the thing the way the code and the docs name it, is every term here one the field
  has already defined, and did you coin a word or a metaphor in this text?
- Content: did you decide what belongs here before polishing it, and did the skill for this artifact settle which
  paragraphs exist?
- Diagnostics: is every quoted error or log message a literal, punctuation included?
- Domain format: subject line imperative; error follows reason + action + consequence; release note is user-visible?
- Identifiers: is every issue, PR, commit, or version number one that exists, and every claim from the bug report
  checked against the code?

## 9. When to break the rules

Break any of these rules sooner than write anything outright barbarous (Orwell, via Google). The rules exist to make
prose clearer; where applying one in a specific spot makes the prose worse, do not apply it. In particular:

- Passive voice is correct when the actor is unknown or uninteresting.
- A long sentence is fine when the idea genuinely is long and splitting it would obscure the logic.
- An em-dash is the right punctuation when a comma would mis-bind and parentheses would interrupt.
- A hedge is honest when the underlying behavior really is conditional.
- `We` is appropriate for a team decision; `I` is appropriate in a personal release note.

One habit that is not a rule: read the draft aloud, as if to a colleague across the desk, before you apply it. Rephrase
on the page any sentence you would rephrase while saying it.

Make a deliberate exception consciously, in service of the reader. Drift into a habitual one and the prose stops being
yours.
