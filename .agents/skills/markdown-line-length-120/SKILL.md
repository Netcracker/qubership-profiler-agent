---
name: markdown-line-length-120
description: Apply Markdown drafting rules an autofixer cannot fix — wrap body lines at 120 chars (MD013), one H1 per document (MD025), named links instead of bare URLs (MD034), language tag on every fenced code block (MD040). Use when editing `*.md` in a repository that pins markdownlint `MD013.line_length` to 120.
---

# Markdown drafting rules an autofixer cannot fix

Build every `.md` file with these four rules in mind from the first draft.
They are not autofixable, so pasting in a long paragraph and rewrapping it
afterwards is wasted work. Run `markdownlint-cli2 --fix` before commit for
the rest (`MD022`, `MD032`, `MD060`, `MD029`, `MD026`, `MD047`, ...).

## Wrap body lines at 120 characters (`MD013`)

Plan the sentence around the wrap, not the other way round. Break at word
boundaries; never split `` `inline code` `` or a `[link](url)` span across
lines. List-item continuations indent by two spaces to line up with the
bullet content. Exempt from the limit: YAML frontmatter, fenced code blocks,
and pipe tables — do not reformat them to fit 120.

## One H1 per document (`MD025`)

The first heading is the title (`#`). All later sections are `##` or deeper.
A section break never gets promoted back to `#`, even when the document is
long.

## Named links, not bare URLs (`MD034`)

Write `[the SDK reference](https://example.com/sdk)`, not
`https://example.com/sdk` on its own. When the URL really is the link text
(citations, release notes), wrap it in angle brackets:
`<https://example.com/sdk>`.

## Language tag on every fenced code block (`MD040`)

`` ```yaml ``, `` ```bash ``, `` ```text ``, `` ```diff `` — always pick
one. Use `text` for plain output, directory listings, ASCII diagrams, or
fixtures with no syntax to highlight.
