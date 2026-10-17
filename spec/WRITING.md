# Writing the spec

This is the spec's writing standard. Every edit to `spec/` follows it: [coven.md](coven.md),
[format.md](format.md), [api.md](api.md) and the proof write-ups under [proofs/](proofs/).

The spec is read by people deciding whether coven is right, and by people changing it. It has to
explain, not just list rules. Read this before you change a sentence.

## Each change
- **Put the rule where the story needs it.** Find the section where a reader meets the situation, and add the
  rule there, in the order things happen. Never append a rule to the end of a section because it's convenient.
- **Give the reason.** Say what breaks without the rule, in one sentence.
- **Show an example when the rule is tricky,** before the general statement. Use the cast: Ana, Ben, Carol, Dan;
  the Household store on Dropbox; the Gifts circle. Use real slot numbers, table and column names.
- **Use the defined terms.** Each term is defined once, where it first appears: slot, claim, floor, journal,
  audience, circle, write, row change, cell, generation, lost value, taken out, snapshot, coverage, reload.
  No synonyms, no new coined words. If you truly need a new term, define it where it first appears.
- **Plain words.** If a word would need a parenthetical, the parenthetical is the word.
- **Short, not dense.** Three plain sentences beat one packed one. No noun piles. One idea per paragraph.

## Each section keeps its shape
1. What this is and why it exists, in a few plain sentences.
2. An example.
3. The main rules.
4. **Edge cases**: exceptions and fine points.
5. Reference material last, or in an appendix.
A reader can stop at the depth they need. Signpost where you are and what comes next ("§16 covers deletion").

## Where things go
- **Proof citations** go in Appendix G ("What's proved"): claim, theorem, file. Keep a citation in the body only
  where it helps the reader, e.g. a counterexample that shows why a rule exists.
- **Lookup material** (coven's tables, record fields, provider profiles, error variants, crate conventions) goes
  in the appendices. The body tells the story and points to the appendix.
- **Byte layouts** go in format.md; the app-facing API in api.md.
- Choose the form that explains each detail best: prose for the story, an example for a tricky case, a table for
  comparisons and variants, an appendix for lookup. Mix them.

## Completeness
Nothing hard-won is dropped. You may merge duplicates and reword, but never change meaning. If two sentences
contradict, keep both facts and raise the contradiction instead of choosing.

## Review
Every spec diff is read for exposition before it lands, not only for correctness: is the rule in the right
place, with its reason, an example where needed, in the defined terms, and does the section still read in
order? A change that breaks the flow is reworked before landing. After each batch of changes, the whole
document gets an editing pass.
