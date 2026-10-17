# Queued originals, conversion and data verdicts

The Lean package in [queue](queue/) models immutable authored records,
per-change data verdicts, schema conversion and atomic local installation.
The [sequence package](sequence.md) models claim settlement, shared-slot
publication, captured raises and reload scheduling.

## Records and boundaries

[Model.lean](queue/CovenQueue/Model.lean) separates a write's identity,
version, disposition and row changes. A row change retains its generation,
operation, generation source and replaced setters. The history enumeration
is timestamp order for well-founded source lookup; it is not a storage path.
Expanded read sets and covered-write sets are inputs derived from the applied
slot prefix and selected snapshot. They are not per-device position objects.

`recorded_reads_do_not_exclude` proves that reads alone cannot change a write's
admission. Authority, reset coverage and schema eligibility are supplied
separately. [Verdicts.lean](queue/CovenQueue/Verdicts.lean) uses the merge
package's data-dependency rule for each row change, including changes in the
same write that have different outcomes. `changes_converge` and
`change_value_accounted` cover agreement and preservation of original values.

`capture_records_loss`, `snapshot_drop_stays_dropped` and
`snapshot_loss_preserves_cause` preserve source verdicts with snapshots,
independently of dismissible loss records. The examples in
[DataExamples.lean](queue/CovenQueue/DataExamples.lean) check mixed writes,
implicit own reads, actual setter evidence, chains through partially lost
writes, schema losses and empty-valued deletes.

## Conversion preserves originals

[Pending.lean](queue/CovenQueue/Pending.lean) distinguishes an untried original
from one with fixed preparation metadata. It makes no provider requests.
[Conversion.lean](queue/CovenQueue/Conversion.lean) keeps these originals
separate from the interpreted history used by the merge.

`conversions_preserve_uploads`, `conversions_preserve_attempt` and
`conversions_preserve_untried` prove that repeated conversions cannot rewrite
queued originals. `reload_matches_receiver`, `receive_matches_evaluation`
and `receive_agreement` give the same interpretation to the original author
and another receiver under the same selected boundaries and evidence.

Conversion first determines admission and data dependence. A dependent change
keeps its original values as a frozen loss; an independent change can be
converted. `dependent_change_not_converted`, `independent_change_converted`
and `converted_change_matches_receiver` prove these cases and their agreement
with receipt. `missing_converter_preserves_values`, `lost_never_reconverted`
and `conversions_preserve_frozen` preserve losses through later schema changes.
Callbacks here are successful validated functions; executing application SQL
and handling a failed transaction are outside this conversion model.

[ConversionChain.lean](queue/CovenQueue/ConversionChain.lean) selects a chain
from migration statements. `statement_plans_agree` proves that devices select
the same chain. `row_classification_diverges` shows why classifying a migration
from the rows it happens to affect can give different answers.
`one_step_is_conversion` connects a one-step chain to the conversion above.
`additions_pass_unchanged` composes a range with no breaking steps by the
identity; `newest_breaking_write_admitted` proves admission with unchanged
values at the newest breaking version under a later additions-only raise.
`chain_available_iff` requires a converter for every crossed breaking step,
and `retained_step_required` proves that an older write uses that converter.
`below_breaking_needs_converter` proves exclusion whenever a crossed breaking
step lacks one. `addition_raise_cases` checks both
schema versions below an additions-only raise to 3.

## Merge and migration

[RowMerge.lean](queue/CovenQueue/RowMerge.lean) composes per-change verdicts
with the existing merge. `mergeInput_valid`, `rows_and_losses_converge` and
`row_value_accounted` cover surviving generation witnesses, agreement and
original-value accounting. [Migration.lean](queue/CovenQueue/Migration.lean)
authors ordinary deletes for the supplied hidden rows and freezes their
original values and setters. Converted edits keep their original generation;
they cannot overwrite a later incarnation.

[Keys.lean](queue/CovenQueue/Keys.lean) checks key-selection decisions given
membership, exposure and delivery facts. These facts are supplied by the
applied sequence; the queue model does not prove their selection or the
cryptography of the resulting ciphertext.

## Availability and commit refusals

`reloading_blocks_commit` in [Verdicts.lean](queue/CovenQueue/Verdicts.lean)
proves that a write touching an audience being reloaded is refused. A failed
reload leaves that audience unavailable; a completed load supplies its whole
materialized state. Ordinary reads and writes outside the affected audience
are governed by the API and sequence reload model.

[AtomicReceive.lean](queue/CovenQueue/AtomicReceive.lean) models a local
transaction: changes are staged before the consumed number advances.
`never_half_visible` proves that observers see either the previous state or
the complete committed result. `crash_discards_transaction` discards only
uncommitted work. These theorems assume transaction semantics; they do not
prove SQLite's implementation.

## Checking and Rust comparison

Run `lake build` in `spec/proofs/queue`. Every retained module is imported by
the root and [Axioms.lean](queue/CovenQueue/Axioms.lean) audits the results.
`scripts/check.sh` rejects unfinished steps and custom axioms.

`authoredRunner` calls [Checked.lean](queue/CovenQueue/Checked.lean) and the
merge package's authored-source validation.
`write_sources::tests::authored_sources_differential` compares Rust's actual
source checks using `COVEN_AUTHORED_LEAN`.
`migration_receive::tests::conversion_chain_matches_lean` uses
`COVEN_CONVERSION_LEAN` to compare `conversionRunner` with actual database
receipt: additions after a breaking raise, missing converters, unchanged
values, and ordered conversion across additions, through buffered and streamed
receipt and reopening. Row-merge and migration examples are checked in Lean;
these runners do not compare those entire subsystems. The sequence runner
covers the shared-slot rules separately.
