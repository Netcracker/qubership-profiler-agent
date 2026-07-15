// Groups similar SQL/bind texts by a normalised signature (08 R11): strip
// commas and quoted literals, drop digits, and abbreviate every remaining
// word to its first character. Two texts differing only in literals and
// identifier tails collapse to the same signature. Ported from the backend's
// `sqlSignature` (backend/libs/calltree/params.go), itself carried over from
// the old UI's similarity key (profiler-ui/src/profiler.mjs:3469) — kept in
// lock-step with the Go implementation so cross-node grouping in the
// Parameters tab (params-summary.ts) agrees with the per-node grouping the
// backend already applies (02-read-contract.md §2.5.3).

const LITERAL_RE = /'(?:''|[^'])*'/g;

export function sqlSignature(sql: string): string {
  const stripped = sql.replaceAll(',', '').replace(LITERAL_RE, '');
  let out = '';
  let inWord = false;
  for (const ch of stripped) {
    if (ch >= '0' && ch <= '9') {
      // Digits vanish without ending the word: "ab1cd" stays one word, "a".
      continue;
    }
    if (ch === '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
      if (!inWord) {
        out += ch;
        inWord = true;
      }
      continue;
    }
    if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' || ch === '\f' || ch === '\v') {
      inWord = false;
      continue;
    }
    out += ch;
    inWord = false;
  }
  return out;
}
