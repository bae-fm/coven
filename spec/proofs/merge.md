# Proof of convergence

Owner tables restrict direct writes. Schema-derived foreign-key changes are
verified against authorized direct causes; ordinary constraint losses still
apply to owned rows. [B18](#b18-owner-tables-and-row-authenticity) states the
checked permissions, causal records, restoration, and SQL boundary.

- This document proves the convergence guarantee of [§2.2](../coven.md#22-convergence) for the merge of [§8](../coven.md#8-merging)
  and the audiences of [§15](../coven.md#15-circles).
- The proof is checked by machine: a Lean 4 development in
  `spec/proofs/merge/`, which has no unproven step and uses no axiom beyond
  Lean's own.
- Each claim names the Lean theorem that checks it;
  [B11](#b11-what-lean-checks-and-what-is-outside-the-model) lists the model's limits.

### B1 The claim

- Two devices that read the same audiences and applied the same writes and
  sequence entries, each in an order that respects causality, hold the same
  database:
  - the same `_coven_rows` and `_coven_cells`;
  - the same rows in the app's tables, with the same values;
  - the same `_coven_lost` rows, for lost values and removed rows alike.
- This holds whatever order each device ran the removal rules in.
- Two devices that read different circles agree on every row both read,
  except which row shows for a key present in two audiences, and the rows
  taken out with it ([B9](#b9-audiences)).
- `_coven_writes` holds the same writes on both, under row ids of each
  device's own ([§8.1](../coven.md#81-cells)).

### B2 Terms

- *Write*, *row change*, *cell*, *generation*, *audience*: as in [§4](../coven.md#4-writing-locally), [§8](../coven.md#8-merging),
  [§8.3](../coven.md#83-deletes) and [§15](../coven.md#15-circles).
- *Row*: one table, key and audience. The store's note 1 and a circle's note
  1 are two rows, each with its own generations ([§15.4](../coven.md#154-moving-rows)).
- *Had read*: the recorded slot prefix includes `a` before making
  `w`, including its own earlier writes. Passing a position does not mean
  applying its data ([§7.1](../coven.md#71-causality)). Actual generation and
  replaced-cell sources are separate retained evidence in B17.
- *Causal order*: an order of applying writes in which each write comes after
  every write it had read. B17 derives this order from receive steps, with
  consumed inputs distinguished from retained effects at a boundary.
- *Closed set*: a set of writes that holds every write any of them had read.
  The ordinary applied set is closed. After exclusions, closure refers to
  retained inputs in the original boundary model. B17 keeps consumed causal
  metadata and filters individual changes by their data sources.
- *Incarnation*: one lifetime of a row. An insert made at generation `g`
  starts incarnation `g + 1`; an update or delete made at generation `g` acts
  on incarnation `g`.
- E.g. Ana creates note 43 at generation 0, so it lives in incarnation 1;
  she deletes it, and re-adds it at generation 2, starting incarnation 3.
- *Setter*: a write that sets a cell. Its value is the cell's value in that
  write.
- *Replacer* of a value: a write that sets the same cell of the same
  incarnation with a larger timestamp, or deletes that incarnation.
- *Merged state*: each row's generation, the write `_coven_rows` names for
  each generation, each cell's winning write and value, and the lost values
  ([§8](../coven.md#8-merging)).
- *Removed row*: a row present in the merged state that a removal rule takes
  out of the app's table.

### B3 Model and assumptions

- A write is an identifier with:
  - its timestamp, a number;
  - which writes it had read;
  - for each row it changes, one row change: insert, update or delete, the
    generation it was made at, and which columns it sets.
- A cell's value is identified with the write that set it, since a write
  sets one value per cell.
- The writes satisfy, by [§7.1](../coven.md#71-causality), [§7.2](../coven.md#72-timestamps) and [§8.3](../coven.md#83-deletes):
  - **assumption 1**: timestamps are unique;
  - **assumption 2**: a write's timestamp is larger than that of every write
    it had read;
  - **assumption 3**: a change's generation was reached on the authoring
    device: it is 0, or some write the author had read moved the row to it;
  - **assumption 4**: an insert is made at an even generation, an update or
    delete at an odd one.
- What a device stores for the merged state:
  - each row's current generation;
  - per generation, the write `_coven_rows` names: of the writes that moved
    the row there, the one with the smallest timestamp;
  - per cell, the write whose value won;
  - per value, its `_coven_lost` row: the incarnation it was set in, and the
    write recorded as replacing it.
- In Lean: `Writes`, `Valid`, `St`, in `Model.lean`.

### B4 The merged state as a function of the writes

For a closed set `S` of writes, each part of the merged state is defined from
`S` alone, with no order of application:

- **Generation** of a row: the largest generation any write in `S` moved it
  to by an insert or delete; 0 if none.
- **Write per generation**: of the writes in `S` that moved the row to that
  generation, the one with the smallest timestamp ([§8.3](../coven.md#83-deletes)).
- **Cell**: of the writes in `S` that set the cell in the current
  incarnation, the one with the largest timestamp; nothing while the row is
  deleted ([§8.1](../coven.md#81-cells)).
- **Lost**: a value is lost when some write in `S` replaced it and no write
  in `S` that replaced it had read it ([§8.2](../coven.md#82-lost-values)).
- **Replaced by**, for a lost value of incarnation `k`:
  - if `k` was deleted: its earliest delete, which is the write `_coven_rows`
    names for generation `k + 1`;
  - otherwise: the cell's current winning write.
- E.g. the writes of [§8.1](../coven.md#81-cells) on note 42's title:

  ```
  write             value               stamp          had read
  Ana's write 3     "Grocery list"      12:00:00 #0
  Ana's write 4     "Groceries"         13:01:00 #0    Ana 3
  Ben's write 9     "Weekly groceries"  13:01:00 #1    Ana 3, Ana 4
  Carol's write 2   "Shopping"          14:00:00 #0    Ana 3
  ```

  - the title: Carol's write 2, the largest stamp;
  - "Grocery list": replaced by Ana 4, Ben 9 and Carol 2, which all had read
    it; not lost;
  - "Groceries": replaced by Ben 9 and Carol 2, and Ben 9 had read it; not
    lost;
  - "Weekly groceries": replaced by Carol 2 only, which hadn't read it; lost,
    replaced by Carol's write 2.
- In Lean: `IsSpec` in `Model.lean`.

### B5 Applying one write

A device applies an arriving write `w` to each row `r` it changes. Let `G` be
`r`'s generation on the device, and `g` the generation `w`'s change was made
at.

- **Generation**: an insert or delete with `g = G` moves the row to `G + 1`.
  Any other change leaves it.
- **Write per generation**: an insert or delete made at `g` competes for
  generation `g + 1`'s `_coven_rows` row: the smaller timestamp stays.
- **Cells**:
  - a delete with `g = G` empties the row's cells;
  - an insert with `g = G` starts a new incarnation with its values;
  - an update at `G`, or an insert at `G - 1` concurrent with the one that
    started the current incarnation, competes per cell with the cell's
    winning write: the larger timestamp stays ([§8.1](../coven.md#81-cells), [§8.3](../coven.md#83-deletes));
  - a change made at an older incarnation changes no cell.
- **`_coven_lost`**, for each value `a` of each cell of `r`:
  - `w`'s own value:
    - made at an incarnation already deleted: lost, replaced by the write
      `_coven_rows` names for that incarnation's delete ([§8.3](../coven.md#83-deletes));
    - made at the current incarnation with a smaller stamp than the cell's
      value: lost, replaced by the cell's winning write;
    - otherwise not lost;
  - a value already lost that `w` replaces:
    - if `w` had read it: no longer lost ([§8.2](../coven.md#82-lost-values));
    - otherwise still lost, and "replaced by" becomes the earlier delete or
      the later setter, by [B4](#b4-the-merged-state-as-a-function-of-the-writes);
  - the cell's current value, which `w` replaces without having read it:
    lost, replaced by `w`;
  - anything else: unchanged.
- The step reads only the device's state, the timestamps of writes it has
  applied, and the arriving write's record.
- In Lean: `step` in `Model.lean`.

### B6 Proof for the merged state

- **Step lemma**: let `S` be closed, the state be [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result for `S`, and
  `w` a write not in `S` whose had-read writes are all in `S`. Then applying
  `w` gives [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result for `S` plus `w`. Lean: `step_spec`.
- The step lemma rests on these facts:
  - no write in `S` had read `w`, since `S` is closed and `w` is not in it;
    so any replacer already applied hadn't read `w`;
  - `g ≤ G`, by assumption 3;
  - every generation from 1 to `G` was reached by a write in `S`, so with
    assumption 4 an incarnation was deleted exactly when it is older than
    `G`;
  - a value nobody replaced is the cell's current value;
  - the largest or smallest of a set, by timestamp, changes as the step
    does when `w` joins the set.
- **Induction**: applying a causal order write by write keeps [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result,
  from the empty database or from a snapshot. Lean: `foldl_isSpec`,
  `run_isSpec`.
- **Uniqueness**: two states that are both [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result for one set are equal.
  Lean: `isSpec_unique`.
- **Convergence**: two causal orders of the same writes give the same merged
  state. Lean: `merge_converges`.
- **Snapshots** ([§16.5](../coven.md#165-coming-back-after-a-long-absence)): loading a snapshot and applying the writes after it,
  with the covered writes counting as applied, gives the same merged state
  as applying every write from the start. Lean: `snapshot_converges`.
- **Timestamp order** is one causal order, by assumption 2, so every causal
  order gives the state applying the writes in timestamp order gives. Lean:
  `causalOrder_of_ts_sorted`.
- **A write made after seeing another wins over it** ([§7.2](../coven.md#72-timestamps), [§8.1](../coven.md#81-cells)):
  `write_after_read_wins` quantifies over every causal application order.
  When a write sets the same cell in the same incarnation, or deletes that
  incarnation, having read the earlier setter, the earlier value is neither
  current nor an unread loss. `cell_after_read_wins` and
  `delete_after_read_wins` also apply directly to any `IsSpec` state,
  including one reached from a snapshot. A third write may beat both;
  `cell_after_all_reads_wins` gives the exact winner when it read every
  competing setter in the current incarnation.
- **Deletion and re-insertion**: `deleted_row_has_no_cells` proves all cells
  absent at an even generation. `reinsert_after_delete_wins` proves that an
  insert after seeing the delete starts the next incarnation, retains the
  earlier delete's generation record, and excludes every old-incarnation
  setter regardless of timestamp. `older_incarnation_not_current` states
  that last fact for every later generation.
- **The storage clock supplies the causal stamp inequality**:
  `stamp_supplies_past_ts` invokes the storage package's
  `CovenStorage.Clocks.stamp_after_every_read`. `packed_stamp_before` maps
  its timestamp order to the merge model's natural numbers using D2's
  64-bit device ids. A successful stamp after the greatest read timestamp
  therefore satisfies assumption 2, including when the wall clock is behind.
- E.g. Lean runs the step on the writes of [§8.1](../coven.md#81-cells) and [§8.3](../coven.md#83-deletes), in the
  arrival orders they describe:
  - Ben's phone and Carol's tablet both end with "Shopping" and one
    `_coven_lost` row, "Weekly groceries" replaced by Carol's write 2. Lean:
    `example_8_1`.
  - On Carol's tablet, "Groceries" is lost after Ana's write 4 arrives, and
    no longer lost after Ben's write 9. Lean: `example_8_2`.
  - Ben's edit of note 43 is lost, replaced by Ana's write 7, whether it
    arrives before Ana's delete or after her re-add, and generation 2's
    `_coven_rows` row names write 7. Lean: `example_8_3`.

### B7 The removal rules

- The removal rules' inputs come from the merged state and the applied sequence,
  never from which rows are removed:
  - which rows are present: an odd generation;
  - each row's references: the parent each points at, and whether it is
    *stale*: the parent's generation it carries has been deleted since
    ([§9.1](../coven.md#91-foreign-keys));
  - whether the row's merged values fail a CHECK ([§9.3](../coven.md#93-check-constraints));
  - the kept entry that deleted the row's circle, if any ([§15.9](../coven.md#159-deleting-a-circle));
  - each row's unique claims, ordered by the greatest `(slot, write number)`
    among the setters of its current constraint inputs, including a partial
    index's WHERE columns and the stored columns a generated column is
    computed from ([§9.2](../coven.md#92-keys-and-unique-values));
  - each key present in two audiences on the device ([§15.4](../coven.md#154-moving-rows)).
- **Set null** for a child whose parent's generation was deleted is part
  of the merged state, not the rules ([§9.1](../coven.md#91-foreign-keys)):
  - under set null, the cell holds null, which CHECK and unique constraints
    see like any value;
  - later writes to the cell compete with the stamp of the write whose
    reference won, as with any cell.
  - Lost values show what their setters wrote; these substitutions apply
    only to the app's rows, including a removed row when it comes back.
  - E.g. at 16:00 Ana deletes note 43 while Ben, offline, adds link 6
    pointing at it, under set null. Every device stores link 6 with
    `note_id` null, whichever write arrived first. Lean: `example_8_4`.
  - E.g. Ben adds link 6 at 16:00 pointing at note 43; Dan, having read
    Ben's write, points it at note 44 at 16:10; Ana, having read neither,
    deletes note 43 at 16:20.
  - A device that gets Ana's delete and Ben's insert first reads link 6 as
    null, with the cell naming Ben's write; Dan's write then wins over
    Ben's, so every device ends with link 6 on note 44. Lean:
    `example_8_4_later`.
  - Where setting the reference to null would fail a CHECK, the reference
    stays and counts as stale.
- The rules other than unique values and keys in two audiences take a
  present row out when:
  - **foreign keys**: one of its references is stale, or its parent is
    absent or taken out;
  - **CHECK**: its merged values fail;
  - **deleted circles**: it is in a circle the applied sequence has deleted.
- **Unique values**: a row loses when a row still present claims the same
  value of the same constraint with an earlier claim, or an equal claim and a
  smaller primary key. The removal model's numeric `stamp` input represents
  this claim order; [the sequence model](sequence.md) derives it from counted
  setters and proves release to the earliest surviving live claimant.
- **Keys in two audiences**: of two present rows with one key, the store's
  wins over a circle's, and of two circles' rows, the one whose current
  generation started with the smaller timestamp.
- [§8.4](../coven.md#84-rows-taken-out-and-rows-that-come-back)'s three steps:
  1. apply the other rules until none fires;
  2. judge unique values and keys in two audiences among the rows still
     present;
  3. apply the other rules again until none fires.
- A removed row's `_coven_lost` row names every rule that holds for it once
  the steps end, and the unique or other-audience rule from step 2.
- In Lean: `EntryInputs` and `entryView` carry the entry identity. They use
  `fires`, `rivalBefore`, `removal` and `view` in `Removal.lean` for the same
  three passes. `Inputs` retains Boolean circle inputs for the Boolean Rust
  differential; `EntryInputs.erase` supplies those Boolean tests from the
  entry, so there is only one removal algorithm. The entry-aware runner calls
  `entryView` and preserves the entry in its returned rules.

### B8 Proof for the removal rules

- **Monotone**: a rule is monotone when it keeps firing for a row as more
  rows are removed.
- Every rule other than unique values and keys in two audiences is
  monotone:
  - a stale reference stays stale, and a parent taken out stays out when
    more rows are removed;
  - CHECK and deleted-circle results don't depend on removals.
  - Lean: `fires_monotone`.
- For monotone rules over a finite set of rows:
  - **termination**: each step removes a row, so applying rules stops. Lean:
    `killStep_terminating`;
  - **local confluence**: two steps from one state, removing `x` and `y`,
    meet again: after removing `x`, the rule for `y` still fires, by
    monotonicity, and the reverse. Lean: `killStep_locallyConfluent`;
  - **one result**: by Newman's lemma, a terminating, locally confluent
    system ends in one state from each start, whatever order the steps take.
    Lean: `newman`, `kill_unique_normal`;
  - **least fixpoint**: that state is the smallest set of removed rows that
    contains the start and leaves no rule firing. Lean: `normal_least`.
- No pair of rules needs checking on its own: monotonicity covers every
  pair, and any rule added later that is monotone.
- **Why unique values are judged once**: the unique rule isn't monotone,
  since taking the winner out stops it firing for the loser. Applied as one
  more rule, it can end two ways:
  - Ana deletes folder 0; note 1 is in it, under cascade; note 2 is not; both
    are titled "Plan", note 1's claim first;
  - cascade first takes note 1 out, and note 2 then has no rival: note 2
    stays;
  - unique first takes note 2 out, then cascade takes note 1 out: neither
    stays.
  - Lean: `unique_with_others`.
- **[§8.4](../coven.md#84-rows-taken-out-and-rows-that-come-back)'s three steps have one result**: steps 1 and 3 have one result each
  by monotonicity, and step 2 is a function of step 1's result. In the
  example above, step 1 takes note 1 out, so note 2 keeps "Plan" on every
  device. Lean: `stratified_unique`, `judged_once`.
- **Any order**: coven's run of the rules is one of the orders the three
  steps allow, so every order gives the same removed rows. Lean:
  `removal_stratified`, `any_order_removal`.
- **A unique loser whose value is unchanged comes back** only when the
  winner is deleted, or taken out before unique values are judged.
  - E.g. notes are unique by title, and a sub-note points at its parent
    under cascade. Note 1 is "Ideas"; Ana adds note 2, "Plan", as a sub-note
    of note 1 in slot 1000; Ben's rename of note 1 to "Plan" claims slot 1100.
    - Step 2: note 2's earlier slot keeps "Plan"; note 1 is taken out.
    - Step 3: note 2 goes with its parent, by cascade.
    - Note 1 stays out: back, it would bring note 2 back, and lose to it
      again. Lean: `example_8_5_subnote`.
  - E.g. the same, but note 2 is a sub-note of note 3, which claims "Ideas"
    at slot 1200, and note 4 claims "Ideas" at slot 900.
    - Step 2: note 1 loses "Plan" to note 2, and note 3 loses "Ideas" to note
      4.
    - Step 3: note 2 goes with note 3.
    - Note 1 stays out: note 2 was present when unique values were judged.
      Lean: `example_8_5_step3`.
- **Every removed row names a rule**: a row removed by a run of monotone
  rules still meets a rule once the run ends, and a step 2 loser counts its
  rule from that step. Lean: `star_fires`, `removed_has_rule`.
- E.g. todos need `start <= end`, and todo 7 is in list 3. Ana deletes
  list 3 while Ben moves todo 7's start past its end. Todo 7's `_coven_lost`
  row names the foreign key and the CHECK, on every device, whichever rule a
  device ran first. Lean: `example_8`.
- **End to end**: the merged state converges ([B6](#b6-proof-for-the-merged-state)), and the rules, and
  therefore the app's tables and all active loss records, are functions of
  it and the applied sequence. `LossRecord` gives cells and removed rows one shape:
  row, generation, optional column, written values/setters and cause. Lean:
  `lossRecord`, `device_converges`,
  `rule_order_converges`.
- E.g. Lean runs the writes of [§9.1](../coven.md#91-foreign-keys), [§9.2](../coven.md#92-keys-and-unique-values), [§9.3](../coven.md#93-check-constraints) and [§15.9](../coven.md#159-deleting-a-circle), with the
  rules reading the merged state it computes, in more than one arrival
  order:
  - [§9.1](../coven.md#91-foreign-keys): Carol's tablet applies Ana's delete of note 43, then Ben's
    attachment 9 on it: attachment 9 is taken out, and its `_coven_lost` row
    names the foreign key.
  - Ben's move of attachment 9 to note 44 then arrives: attachment 9 is
    back. Ben's phone, which gets Ana's delete last, never takes it out.
    Lean: `example_8_4`.
  - [§9.2](../coven.md#92-keys-and-unique-values): Ana renames the tag "urgent" to "important" while Ben tags note 44
    "urgent". Note 42 ends tagged "important", and Ben's `(44, "urgent")` is
    taken out, naming the foreign key. Lean: `example_8_5_key`.
  - [§9.3](../coven.md#93-check-constraints): Ana sets start 10 while Ben sets end 8: the row is taken out,
    naming the CHECK. Ben's later end of 20 brings it back. Lean:
    `example_8_6`.
  - [§15.9](../coven.md#159-deleting-a-circle): Ben's entry deletes Gifts while Ana adds note 9.
    Notes 7, 8 and 9 stay at generation 1, hidden with that entry named in
    their losses. There is no circle deletion write. Lean: `example_14_7`.

### B9 Audiences

- A row is one table, key and audience, with its own generations ([§15.4](../coven.md#154-moving-rows)).
  Each row change sits in the part of its row's audience; a device applies
  the parts it can read and counts the rest as applied ([§15.6](../coven.md#156-writes-into-circles)).
- E.g. Ana moves note 1 into her circle; Ben, outside it, sees it deleted
  and re-adds note 1 in the store; then Carol, in the circle, edits the
  circle's note 1.
  - The store's note 1 is deleted by Ana's move and re-added by Ben: it ends
    at generation 3 with Ben's values, on Carol's device and on Dan's, who
    is outside the circle.
  - The circle's note 1 is a row of its own, at generation 1 with Carol's
    edit; only circle members have it.
  - Lean: `Moved.agree`.
- **Same audiences**: the writes as a device sees them still meet [B3](#b3-model-and-assumptions)'s
  assumptions, since a change's generation was reached by a change to the
  same row, in the same audience.
- So two devices that read the same audiences hold the same merged state.
  Lean: `valid_project`, `audience_converges`.
- **Different audiences**: a row's merged state depends only on the changes
  to that row, so devices agree on the merged state of every row both read.
  Lean: `fold_atRow`, `audiences_agree`.
- **Removals on devices with different rows**: two devices take out the same
  rows among any set of rows both have that holds, for each of its rows,
  every parent its references point at and every row whose claim rivals its
  own. Lean: `removal_local`.
- [§15.3](../coven.md#153-references-stay-readable) gives the parents: a row points only at rows every reader of it can
  read.
- [§15.2](../coven.md#152-unique-values-dont-cross-audiences) gives the unique rivals: a unique constraint on a root table
  includes the audience column, and one on a descendant table includes the
  foreign key it takes its audience from, so rivals share an audience;
  constraints that could span audiences are refused. Lean: `rivals_closed`.
- Every member's devices read the applied sequence, so they agree on which circles
  are deleted.
- **Keys in two audiences** are the one rule that reads rows another device
  may not have:
  - E.g. in the move above, Carol's device has note 1 in the store and in
    the circle. The store's shows; the circle's is taken out, naming the
    other-audience rule. Dan's device has only the store's, which shows.
    Lean: `Moved.store_wins`.
  - The store's row never loses, so devices agree on every store row.
  - A device in two circles can show a different circle row for one key
    than a device in only one of them, since each reads different rows.

### B10 The spec's examples, checked

- [§8.4](../coven.md#84-rows-taken-out-and-rows-that-come-back): todo 7 names both rules. Lean: `example_8`.
- [§8.1](../coven.md#81-cells): both orders end with "Shopping" and one lost value. Lean:
  `example_8_1`.
- [§8.2](../coven.md#82-lost-values): "Groceries" is lost on Carol's tablet until Ben's write 9 arrives.
  Lean: `example_8_2`.
- [§8.3](../coven.md#83-deletes): Ben's edit is replaced by Ana's write 7 in every order. Lean:
  `example_8_3`.
- [§9.1](../coven.md#91-foreign-keys): attachment 9 is taken out, then back when Ben's move arrives;
  link 6 holds null, then ends on note 44 once Dan's write arrives. Lean:
  `example_8_4`, `example_8_4_later`.
- [§9.2](../coven.md#92-keys-and-unique-values): a key change leaves Ben's `(44, "urgent")` taken out;
  a loser whose winner leaves in step 3 stays out. Lean:
  `example_8_5_key`, `example_8_5_subnote`,
  `example_8_5_step3`.
- [§9.3](../coven.md#93-check-constraints): the row is taken out, then back when Ben sets end 20. Lean:
  `example_8_6`.
- [§15.4](../coven.md#154-moving-rows): the store's note 1 wins over the circle's. Lean: `Moved.agree`,
  `Moved.store_wins`.
- [§15.9](../coven.md#159-deleting-a-circle): existing and late circle rows keep their
  generations and setters and are hidden by the named entry. Lean: `example_14_7`.

### B11 What Lean checks, and what is outside the model

The merge proofs assume valid authored changes, authenticated inputs, supplied
schema and admission verdicts, and a sound snapshot. They prove convergence,
causal winners, rule-order independence and value accounting under those
assumptions. The [sequence model](sequence.md) supplies ordered prefixes,
claim settlement, cut-off, first-slot ownership and unique claims, snapshot
selection and retention. This package does not prove provider behavior,
cryptography, SQL extraction or atomic SQLite installation.

Values are represented by their setters and cells. Frozen records retain old
column identities and original causes; they are not fed back into live removal
rules. References, CHECK results, claim ranks and schema consequences are inputs.
The foreign-key proofs below state the corresponding SQL extraction boundary.

`CovenMerge/Axioms.lean` audits the retained results. `scripts/check.sh` builds
all modules, rejects unfinished proofs and custom axioms, and runs the current
Rust differentials described in B16 and B18. Only `propext`, `Classical.choice`
and `Quot.sound` are allowed. A green model build establishes these Lean
statements; Rust agreement is a separate check.

### B12 Circle entries, migration deletes and frozen losses

For [§15.9](../coven.md#159-deleting-a-circle), `EntryInputs` holds the
responsible entry, and `entryView` records `deletedCircleEntry entry`.
`deleted_rule_names_entry` proves the row is hidden and its loss names that
entry. `circle_entries_preserve_merge` proves observation changes no merge
record. `row_returns` proves a present row returns when no reason remains.
`entry_device_converges` includes the named losses; `entry_device_rule_order`
allows every order within §8.4's three passes.

For [§18.7](../coven.md#187-lost-values-across-a-migration), `migrationRemoves` uses the
current three-pass removal result. Circle deletions in the applied sequence
are permanent; migration selection has no finality input.

`withHiddenDeletes` gives the migration write an ordinary delete at each
selected row's generation. `hidden_generation_delete`, `hidden_delete_even`
and `migration_keeps_generation_record` prove the next even generation,
empty live cells and retained delete identity. `migration_removes_live_loss`
proves the old active removed-row record disappears. `captureHidden` holds
one frozen whole-row record per deleted row, naming the breaking version and
migration write and keeping the original setters. `hidden_value_frozen`
proves this for every captured value, even when the migration had read it.

`late_edit_recorded` proves an eligible edit at the old generation changes
neither the generation nor the live cell, and records the arriving value
against the generation's delete. `migration_and_late_edit` checks the three
writes—insert, migration delete, concurrent edit—in both application orders.
The migration's known input is kept in its frozen whole-row loss; the unseen
edit is a separate cell loss. If several writes delete that incarnation,
§8.3 chooses the earliest delete. An uncovered old-schema edit excluded by
§18.6 instead gets the frozen schema loss proved by `excluded_values_recorded`.

`migration_converges` covers every valid causal order of the authored
migration and concurrent edits. The migration is authored from fixed inputs;
receivers do not recompute its delete generations from their arrival prefix.
`migration_capture_converges` proves equal author input sets also produce
equal frozen records.

Dropping a table or column calls `freezeDropped` on each affected pending
loss. `drop_freezes_affected`, `drop_preserves_pending` and
`drop_preserves_all_pending` prove the affected records become frozen and
that no record, setter, old name or original cause is removed. A whole-row
loss keeps the whole row. `dropped_schema_keeps_losses` checks both a pending
color value and a whole-row loss.

`droppedBy` is the affected predicate, by table and column identity: the
migration drops the loss's table, the column of a cell loss, or a column a
whole-row loss's values name. `dropSchema` is the merge state the drop
leaves: a dropped table's rows lose their merge history, a dropped column
its cells and cell losses. During this schema-drop substep, a hidden row keeps its live removed-row record. `hidden_row_after_drop`
proves that record is the old one without the dropped columns' values, and
`dropped_column_frozen_whole` that a record naming a dropped column is
frozen whole, that value included, while the live record no longer holds
it. The completed breaking migration then deletes hidden rows and replaces
their active records with the frozen records proved above.

### B13 Reset contents on every device

For [§19.4](../coven.md#194-resetting-an-audience), `ResetBoundary` distinguishes
writes consumed by the snapshot's slot prefix from writes that contributed to its merge
state. Ignored and excluded writes can have consumed slots without
contributing values, as [format D7](../format.md#d7-snapshots) requires.
`consumed_is_not_input` checks that distinction through a later reload.

`reloadReset` starts with the snapshot and applies only uncovered writes
whose `read_through` includes the reset slot. `reset_exact` gives that
complete state equality. `reset_covered_not_reapplied` and
`reset_ignored_no_effect` prove that covered writes and unread pre-reset
writes add nothing, including no merge losses. `post_reset_ignores_old_dependency`
proves that a post-reset write can read discarded history without inheriting
its effects. `reset_causal` proves that filtering those parts preserves the
causal order needed by merge.

`reset_every_device` replaces the target audience regardless of its previous
local state, including the author's. `reset_other_audience` preserves other
audiences. `reset_author_and_remote` checks a snapshot covering write 0,
ignored writes 1 and 2, and a post-reset write 3 that had read write 2. Both
author and remote end with write 3 and no losses for writes 1 or 2.

`reset_converges` allows different arrival orders and even different sets of
ignored writes, provided the eligible sets agree. `boundary_converges` and
`boundary_rule_order` use the shared `Snapshot.lean` proofs to extend this to
the app view, entry-named removed rows, cell losses, snapshot-frozen records
and schema-excluded records. Reset
eligibility is checked before schema exclusion: `boundary_reset_no_loss`
and `reset_ignored_never_schema_loss` prove that ignored parts do not gain
schema losses. Frozen records in the chosen snapshot remain included.

A boundary can have no reset entry. Then only the schema verdict filters
uncovered writes; `no_reset_cannot_ignore` proves none is reset-ignored.
`migration_without_reset` checks an eligible converted edit losing to the
snapshot's generation delete, an excluded edit becoming a frozen schema
loss, and a losing migration's computed value disappearing without a new
loss. Each uses the same selected snapshot and incoming write.

### B14 Where every value goes

For [§2.2](../coven.md#22-convergence), `value_accounted` quantifies over every
setter in the effective merge history. Each value is either still shown,
replaced by a write that read it, recorded as a lost cell, or captured in a
removed row with a rule that holds. `boundary_value_accounted` applies that
proof to the actual state produced by `boundaryDevice`.

`hidden_value_frozen` accounts for rows a migration deletes.
`drop_preserves_all_pending` accounts for losses whose schema disappears.
`excluded_values_recorded` keeps excluded changes, including an empty-valued
delete's identity. `boundary_accounted` proves the only other dispositions
are the snapshot, ordinary merge, or exactly the two stated exceptions:
reset-ignored writes and values computed by a losing migration. This is a
proof about the model's values and supplied schema verdicts, not a proof of
an arbitrary application's SQL migration.

`same_key_shows_once` proves two rows with ordered other-audience claims
cannot both be shown. Other removal rules can hide both.

### B15 Checked counterexamples and readings

`UniqueOnce.unique_with_others` shows why uniqueness cannot run in an
unrestricted loop with foreign-key removal: rule order can change the result.
The three passes in §8.4 avoid that ambiguity. Timestamps choose cells; supplied
claim ranks choose unique winners. These are different orders.

Snapshot coverage represents consumed inputs, which can include writes with
no data effects. Reset eligibility is checked before schema exclusion.
Original generation and setter evidence determines data dependence; a write's
recorded reads alone never make another change count.

### B16 Rust differentials

`MergeRunner.lean` compares the core merge, Boolean and entry-named removal,
frozen losses, and foreign-key permissions. `ReceiveRunner.lean` compares the
shared per-change data classifier. The check script names each existing Rust
test exactly and fails if any named test is absent.

The merge crate's `lean_differential`, `entry_differential_histories` and
`entry_differential_agreement` compare merge fields and removal causes.
`dependency::tests::mg_differential_data_dependency` compares retained and
excluded changes, source chains and repeated sources. The database's
`composite_reference_differential` compares captured whole-tuple setters and
losses; `freeze_differential_histories` and `freeze_differential_agreement`
compare records preserved when a migration drops schema, after an earlier
breaking migration has frozen hidden rows. B18 describes the
foreign-key comparisons. The sequence package owns first-slot creation,
cut-off and claim settlement.

Build the runners with `lake build` in `spec/proofs/merge`. Rust tests read
`COVEN_MERGE_LEAN` for `mergeRunner` and `COVEN_RECEIVE_LEAN` for
`receiveRunner`; `scripts/check.sh` supplies their absolute paths.

### B17 Receive order, progress, and dropping by data

`DataDependency.dependsOnDropped` examines a change's original generation
source and replaced-cell setters at that row. `Authored` requires those
sources to exist, precede the change and match the generation, incarnation
and cell they support. `dependencyDecision` implements that predicate;
`depends_iff` proves its meaning. Missing or malformed evidence cannot become
an independent change. SQL capture and snapshot encoding must preserve it.

`effective` leaves causal metadata intact and removes non-retained changes.
`dropped_stays_dropped`, `dependent_not_retained` and `independent_retained`
prove the decisions. `effective_valid` derives valid surviving generations:
a retained change's required generation source survived too. `Data.loss`
records dependent changes with their original values, generation and sources,
including empty-valued deletes. `dependent_recorded`,
`dependent_loss_preserves` and `Data.value_accounted` prove that accounting.

`Receive.lean` is an abstract downloaded/consumed-input model over a supplied
past relation, without per-device download constraints. `passed_causal` derives causal application;
`complete_past_eventually_passed`, `arriving_past_eventually_passed` and
`receive_eventually_applied` require eventual inputs, successful prerequisites
and fair retry scheduling. The concrete slot-prefix past and reader progress
are proved in [the sequence package](sequence.md), not by a per-device
frontier adapter.

`ReceiveMerge.Policy.admitted` supplies the sequence's base admission decision.
`unadmitted_no_effects` and `unadmitted_not_applied` keep refused inputs out of
the data merge. Schema and reset decisions remain separate inputs.
`receive_converges` and `receive_result_converges` compose causal receipt,
per-change classification, merge records and losses. Their premises include
valid authored evidence, equal admission facts and consumed inputs, and a
sound snapshot. `receive_value_accounted` includes original setters removed
before the merge. These are completed atomic results; SQLite transactions
are outside the Lean model.

### B18 Owner tables and row authenticity

Owner tables are a write permission. They do not promise that another member
cannot cause an owned row to disappear. A foreign key, CHECK, unique contest
or audience rule can remove it under the ordinary schema and merge rules.
There is no owner-specific delete trial, removal-set veto or schema predicate.

**Identity and direct permission.** `Policy` opts tables in and derives each
row's owner from its original creating write's authenticated member.
The creator is supplied by the applied sequence's first counted creation,
ordered by slot and then write number; [the sequence ownership
proof](sequence/CovenSequence/Owners.lean) establishes that choice and its
preservation. `owner_fixed` retains the authenticated creator across copies
and reinsertions. The cell merge's generation setter does not select ownership.

`Admission.statement_authorized` proves the direct-target permission, including
a row whose direct and derived effects coalesce;
`every_applied_statement_authorized` proves it for every change that enters the
filtered history, on every device and for every valid authored history.
Shared-trigger writes have direct origin, even when an FK action fires the
trigger. A derived label grants no permission by itself. `Evidence` separates the
proposed payload from the receiver's `Execution`, which supplies actual direct
targets, schema consequences and each row's complete derivation. The latter
is reconstructed, never taken from an unverified sender label.

**Exact foreign-key consequences.** `ForeignKey.evaluate` starts at a direct
change in this same write, checked against its target's owner. Each subsequent
step selects an effect from the receiver's schema interpretation. It produces
the expected payload without reading the proposed target payload.
`verifies` compares the entire result, including the row. With `Effect`, that
includes the incarnation, operation, columns and actual scalar values. A
payload holds no generated cell: no row change or step carries one, and a
receiver refuses a write that names one before judging it (format.md D5.5).
So the comparison needs no exception for them, and
`Admission.derived_payload_unique` needs no assumption about the values SQLite
computes.
`evaluate_sound` and `evaluate_complete` identify successful certificates with
finite consequences of this schema; `evaluate_root` proves every certificate
has a direct root. `together` combines exact effects in authored order and
retains both sets of contributing roots. `combineEffects` checks row and
generation identity before coalescing complete scalar patches. A finite certificate cannot bootstrap an unrooted cycle.

`Admission.derived_payload_unique` proves that a fixed receiver execution
admits only one complete payload for a derived row. Neither a partial
consequence nor a different order of coalescing can replace it.
`Admission.derived_exact`, `derived_roots_authorized` and
`every_applied_derived_exact` compose that check with the real per-change
filter. `applied_payload_exact` additionally requires `Capture`: the checked
payload projects to the optional net row change actually passed to the core
merge. This prevents validating one operation and merging another. Direct
execution remains authenticated evidence even when its net projection is
absent; its additional generation/setter source roles remain in `Direct`.
`direct_source_dropped` proves that they still propagate a dropped source. Authentic scalar
interpretation uses that same original payload, not a second values map.

`resolvedSchema` computes DELETE CASCADE, SET NULL and referenced-key UPDATE
actions from resolved references. RESTRICT and NO ACTION supply no derived
change. `keyAfter` reads changed parent columns from its patch and retains
the original value for every unchanged component; the child receives the
whole compound tuple. The existing SET DEFAULT refusal is unchanged.

The general schema interpreter is an input, like the SQL-derived references,
CHECK results and claims in B7. It must reconstruct the authenticated authoring
execution, including statement order, trigger targets, multiple actions on
one row, and primary-key delete/insert normalization. Lean does not implement
a SQL parser or prove that SQLite capture produces that interpreter.
`resolvedSchema` is a checked concrete instance, not an implementation of
arbitrary SQL execution. Complete signed evidence, original schema versions,
resolved row identities and row/payload correspondence are admission
obligations. A supplied function unrelated to the schema would not establish
a SQL authorization claim. Missing evidence must wait or fail; it cannot
become a permissive result.

**Causes, losses and restoration.** `Admission.dependencies` combines the
existing generation and replaced-setter sources, additional source roles
retained by direct executions, and the root and intermediate rows recomputed from an FK certificate. The extra sources are in the same
write and name rows as well as writes. They preserve the cause of an actual
schema effect; they do not protect visibility or infer dependence from reads.
The change list must contain every original net change and direct execution
target, including targets with no net row change. Validation of those
additional execution sources belongs to the same authenticated SQL-extraction
boundary.

`seed_dropped`, `source_dropped` and `dropped_contributing_root_no_effect` prove that a
refused direct cause and its dependent FK changes have no merge effect.
The closure also carries prior per-change refusals and ordinary data losses.
`DataDependency.effective_valid` still establishes valid surviving generations.
`replay_restores_row` relates replay to the history with the dropped effects
absent, including generations, cells and cell losses. A remaining independent
delete or removal rule can still keep a row out.

`Ownership.excluded_recorded` retains every excluded change's identity,
generation and original values, including empty deletes. A refused change names its target's owner or a failed FK derivation; a
dependent change names its failed sources. `applied_derived_recorded` gives
every accepted derived change its payload, original write, target and all verified direct
causes. A local cascade is a recorded derived delete even when it knowingly
replaced the old values and no lost-cell record is required.

`constraint_effect_recorded` uses the existing ordinary loss recorder: every
hidden owned row has its values and a nonempty set of current causes. This
covers references, CHECKs, unique contests and audiences, without an additional
owner policy. `constraint_effect_reversed` uses `row_returns`: when the last
cause disappears, the row is shown and its active row loss disappears. Stale
SET NULL substitutions retain their original setter/reference as in B7; they
are not new authored edits or fabricated lost values.

**Agreement and termination.** `Admission.result_converges` includes the
merged state, final view, ordinary losses, rejected changes and derived causal
records in every causal order of the same authenticated inputs. Permission and
FK verification use those inputs, not an arrival prefix. Certificate order is
part of the canonical reconstructed evidence. `dropped_order_independent`
proves that changing the enumeration of the same change set cannot change
which causes propagate. `no_rejection_cycle` and the existing `close_normal`
prove termination without restoring a rejected change during the computation.
The core B6 and three-pass termination/convergence results remain unchanged.
No bound for SQL reconstruction or complete history replay is claimed.

**Checked cases.** `OwnerCascadeAuthorization.lean` retains the two-row,
three-write witness: Ben creates a track, Ana creates its review, and Ben
reads the review and deletes the track. `valid`, `provenance` and `causal`
check the authored history. `captured` connects its exact scalar payloads to
the original merge changes. `cascade_checked`, `cascade_applied` and
`cascade_recorded` accept both deletes and retain the track deletion as the
review deletion's cause. `cascade_reversed` drops that cause, restores both
original generations and Ana's setter, shows the review, and records the
excluded derived deletion as a dependency loss.

`restrict_no_derivation` denies the foreign child change under RESTRICT or
NO ACTION; SQLite's local constraint check refuses the parent deletion while
referenced. `trigger_is_direct` rejects the same target as a shared-trigger
write. `forged_payloads_refused` rejects an arbitrary edit, a different
generation and an extra scalar on a purported cascade. An unauthorized root
cannot authorize its child (`no_cause_no_permission`).

`OwnerForeignKeyExamples` checks both columns of a SET NULL action, refusal of
omitted or additional columns and wrong scalar values, referenced-key UPDATE
with an unchanged tuple component, stale incarnations and a transitive
cascade. `coalesced_actions_keep_both_causes` checks two SET NULL actions on
one child and the loss of the combined result when either cause drops.
`incomplete_coalescing_refused` rejects either partial patch while accepting
the reconstructed combined patch on another owner's row.
Its removal-input examples permit a shared CHECK to hide an owned
dependent and a SET NULL unique expression to hide an owned rival, record the
ordinary causes, and return the rows when the causes disappear. These examples
exercise supplied removal inputs; they do not execute SQL histories.
`OwnerFilterExamples` retains mixed-write rejection: the unauthorized row and
its dependent change lose while unrelated unowned changes apply, in both
causal arrival orders.

`OwnerTransientForeignKey` checks an UPDATE SET NULL case: the parent key
changes from 7 to 8 and back to 7 while the child remains NULL. `captured`
projects the direct parent execution to no net merge change while retaining
its authenticated payload and sources. `absent_net_cause_authorizes` and
`applied` accept the exact child effect, reject an arbitrary replacement value
and retain the direct cause. `changed_trace_refused` checks that the entire
captured execution is compared. `dropped_root_reverses` restores the child's
original setter when that cause drops. The same SQL statement sequence was
also executed against SQLite in memory; the Lean example uses the resulting
execution inputs, not a proved SQL interpreter.

**Telling actions from direct targets without labels.** A write lists its
SQL operations as steps and never says what made one (D5).
`ForeignKeyClassification.lean` models one write's steps in execution order
together with the references each row holds just before each step: the
authoring view, replayed through the write's earlier steps. `action_iff`
states the receiver's rule: a step is a foreign-key action exactly when it
equals the schema's consequence of an earlier step through a reference its
row holds at that moment. `cause` names the nearest such step. The rule reads
only the steps, the replayed references and the schema.

`Faithful` states what SQLite does at the extraction boundary: each action it
runs is the consequence of its cause through the reference the child holds
when the action reaches it. Under it, `action_classified` proves every real
action is classified as one, so an honest author's cascade is never refused
as a direct edit, and `direct_was_direct` proves that a step classified as
direct was made by a statement or a trigger. `certificate` builds, for every
step, a finite derivation in B18's model that `evaluate` maps exactly to that
step, rooted only in steps classified as direct; `edge_exact` ties each edge
to a real schema consequence. `every_step_rooted` composes this with the
direct permission: when every step classified as direct is on a row its
writer may change, every step of the write, also one on another member's row,
is an exact chain of consequences from such steps.
`forbidden_row_needs_action` is the converse view: a step on a row the writer
may not change is admitted only as an action.

Four checked histories bound the rule; each was also run on SQLite.
`TriggerFirst`: Ben deletes his track; the cascade deletes Ana's first review,
and a shared AFTER DELETE trigger fired by that deletion deletes her second
review before the cascade reaches it. `trigger_classified_as_action` shows
the trigger's step is classified as the track deletion's action. Its payload
is exactly the action's, so no rule reading only steps can tell them apart; a
receiver accepts nothing the schema would not have done. So "foreign-key
action" means "the schema's exact consequence" on every device, the
author's included, and a shared trigger's step stays direct whenever it
differs from every consequence. `FollowBack`: Ben changes his track's
referenced code and back, and Ana's review follows both times. `start_view_refuses_cascade` shows that
references read from the view before the write began classify the second
cascade as a direct edit, refusing an honest write; `replayed_classifies`
accepts both with references replayed through the earlier steps. `Retarget`:
after the follow-back Ben inserts another track holding the first code and
then sets Ana's review to that code himself. `values_accept_retarget` shows
that comparing values alone classifies his edit as the first cascade, one the
review had already taken, though it points her review at another track.
`parents_refuse_retarget` compares each step's named parents too (D5
`ColumnValue.parents`) and keeps the edit direct. `follows_cause` proves the
general fact: with named parents compared, a classified action names in
every reference it writes exactly the row and incarnation whose step caused
it, so an action cannot point a row at another parent. Like `consequence`,
these histories update a referenced key in place. Coven references only
primary keys and turns a key change into a delete and an insert (§9.2), so the
action names the inserted row, and a reference to the old key names an
incarnation that ended; that refuses `Retarget`'s edit even before parents are
compared. `FollowBack` holds either way: the second cascade's cause is a step
on the inserted row, which the view at the write's start never names.
`Affinity`: Ben's code has the INTEGER key 7, which Ana's tag holds in a TEXT
column as '7'; changing the code to 8 makes SQLite store the text '8' in the
tag. `unstored_refuses_cascade` shows that comparing the parent's new key as
the parent holds it calls that cascade a direct edit; `stored_classifies`
compares values as the child's columns store them (`storedRule`), as coven
does.

**What an author checks, and which net changes must end where their steps
do.** `ForeignKeyAuthoring.lean` states the author's check before a write
leaves (`LocalAccept`): the receivers' label-free rule, run on its own steps.
`local_accept_iff` proves the author accepts exactly the writes receivers
accept: those whose every step the rule calls direct is on a row its writer
may change. So no device keeps an effect the others refuse
(`rebuild_refusal_refuses`), and `accepted_rooted` composes the check with
`every_step_rooted`: every step of an accepted write has a certificate rooted
in direct steps its writer may change. `TriggerFirst.author_accepts`: the
trigger's step that equals the cascade's exact effect is accepted by its
author as by every receiver.

A row's net change is the payload that merges; its steps are the execution
classification reads. `checkSteps` is the receiver's check as coven runs it:
a direct row's net change must equal where its steps end only when its steps
cause actions, since only then do they justify effects on other rows.
`checkSteps_relaxed` reduces it to `Admission.check` over evidence in which
every other direct row proposes where its steps end, so every theorem above
holds for it, and the `receive` mode decides it. `applied_root_ends_where_steps_end`
proves what the condition buys: every root of an applied derived change has
its merged net change exactly where its steps end. `InconsistentCause` is the
counterexample without it: Ben's track whose steps delete it while its net
change only renames it would still set Ana's note's reference NULL
(`accepted_without_condition`), though the merge keeps the track; with it,
the track's change is refused and the note's change drops with it
(`refused_with_condition`).

**Actions through shared columns.** A cascade into a compound key also
writes the columns another reference of the child shares with it, and its
recorded step names that reference's parent too (D5).
`ForeignKeyOverlap.lean` checks when the write determines that parent.
`Overlap`: Ben changes his album's id; the cascade rewrites Ana's track's
artist column with the same value, so its artist reference keeps its parent
(`kept_classified`), and without that parent SQLite's action would be refused
(`unknown_refuses`). `Retargeted`: Ben inserts an artist and moves his album
to it; the cascade points the track's artist reference at that artist, whose
incarnation the insert made (`retargeted_classified`). `Untouched` and
`Stale` are why the receiver refuses the rest. When the cascade points the
reference at an artist the write never touched, or carries a reference whose
parent may have ended before the write, the step SQLite records depends on
that row's incarnation in the authoring view. Two authoring views with the
same write make different steps (`views_differ`), and `one_view_misjudged`
proves a receiver reading only the write accepts at most one of them: it
refuses SQLite's action in one view or accepts in the other a step that
points Ana's row at an ended incarnation. The author runs the receivers'
check before its write leaves, so it refuses such a write too
(`rebuild_refusal_refuses`).

**Moving owned rows.** `OwnerMoves.lean` treats every row a move carries as
a copy: a direct change of the mover's write, made without SQL, keeping its
original creation. `refused_iff` proves a move is permitted exactly when its
writer may change every present row the declared audience parent carries,
the root included; `carried_foreign_refuses` refuses Ben's move of his own
album while it carries Ana's track (`Album.foreign_track_refuses`).
`copy_refused` proves such a copy fails `Admission.check` whatever origin a
sender proposes, and its loss names the original owner; `admitted_permitted`
proves admitted copies mean a permitted move.

**Authorship a snapshot carries.** `OwnerSnapshotAuthors.lean` models what a
loader knows about a snapshot's author: its device, the key its signature
verified with and, once registered, the device's member. A device belongs to
one member for good and its authenticated key history is retained (§10, §10.4). D7 section 7
needs evidence for a write unless it is from the snapshot's own device or,
with the signer's member known, from that member's device.
`own_writes_signers` proves such a write's author is the snapshot's signer and
its signing key binds that same member, also when it differs from the
snapshot's key after a replacement (`Replaced.own_write_after_replacement`).
`owner_true` proves the owner a loader computes for an accepted creation is
the write's true author, and unless that owner is the signer, the write's
signed body contains this row's creation (`owned_row_true_author`). A snapshot
can attribute rows without evidence only to its own signer.

**Retained guarantees and scope.** `Ownership.value_accounted` extends B14
to every original setter: it has an ordinary accounting outcome or its original
value in a rejected-change loss. `originalCell`, `setter_has_cell_leaf` and
`authenticated_value_accounted` retain authentication of that exact original
cell and its disposition. `accepted_owner_matches_filter` connects a verified
creation opening to the permission's owner. The unchanged
[storage proofs](storage.md#signed-roots-and-row-authorship) retain authenticity,
opening completeness, hash minimality and exclusion of unrequested old scalar
openings. They do not establish SQL semantics or snapshot freshness.

Audience-move destination readability remains separately proved by
`move_safe`, using the complete merged descendant graph and replayed
membership. It confers no permission to forge a child's values. Owner identity
travels with copies. Transporting snapshots, discovering unreadable evidence,
SQL capture, local commit rollback, schema conversion, and atomic installation
remain implementation obligations. So do the replayed references and the
parents each step names: `Trace.refs` and `Step.parents` are inputs, and the
receiver must check that a step's named parent holds the written key at that
moment, as B18 already requires resolved row identities.

**Rust differentials.** The `effects` mode evaluates `resolvedSchema` derivations with `evaluate` and
`verifies`. `captured_actions_are_the_models_consequences` runs SQL deletes,
referenced-key updates and compound and transitive actions on SQLite,
rebuilds each captured foreign-key action's `Reference` from the schema and
its cause's own captured step, and requires the model's exact consequence.
That checks the author's capture against B18's effects.
The `receive` mode decides `checkSteps` and `Admission.guard` for a
received write from the receiver's reconstructed execution: the references
behind each action the receiver found, each row's direct effect, each derived
row's paths combined with `together`, its proposed net change, and the
generation, setter and execution sources of every change with those that do
not survive. `store` gives a cascaded key as the child's column stores it.
`judge_view::tests::receipt_matches_the_model` receives direct,
derived, transitive, coalesced, trigger and unauthorized-root writes, a forged
action payload, actions across affinities, through references recorded by
value alone and through columns another reference shares, an action whose
cause's net change differs from its steps, an own row's net change that
differs from its steps but causes nothing, a set-NULL and a replaced parent's
cascade on rows with generated columns, and such a set-NULL whose net change
differs from its action,
and requires each recorded verdict to be the model's cause, with the roots of
each derived change (`applied_derived_recorded`) and the failed sources of each
dependent one (`dependencies`, `dropped_contributing_root_no_effect`).
For a delete, `Effect.cells` carries D5.6's `old` map. The resolved reference
supplies the replaced setters' authenticated written values, independently
of that proposed map and of the displayed row. `delete_old_mismatch_refused`
refuses any different map; `cascade_delete_keeps_written_values` and
`changed_delete_old_refused` check unchanged values, omissions, additions
and a displayed NULL substituted for a written reference.
`judge_view::tests::cascade_delete_old_matches_the_model` compares the real
receiver's acceptance and refusal after a concurrent parent deletion has
made that reference display NULL.
The `classify` mode compares the receiver's classification itself, which
supplies those references. It takes every step of a received write as D5
records it, with the references the receiver replays for each step's row,
and returns `Classification.cause` under `heldRule`
([`ForeignKeyRecords.lean`](merge/CovenMerge/ForeignKeyRecords.lean)): a
`Held` reference carries what an action through it records besides its own
columns, the other cells sharing a reference with them and the parents the
other references name, and `heldConsequence` builds the whole recorded step,
named parents included. `held_follows_cause` is `follows_cause` for it.
`judge_view::tests::classification_matches_the_model` requires the
receiver's cause of every step of the same writes to be the model's. The
replayed references stay inputs, as `Trace.refs` is.
The `admission` mode decides `Admission.check` for one local write, given
each changed row's table and owner. `local_admission_matches_the_model` runs
Ben's statements against Ana's rows on SQLite: an authorized cascade, a key
changed 7 to 8 and back, direct edits and deletes, a direct edit coalesced
with an authorized cascade, a shared trigger fired by a statement and one
fired by a cascade, and new rows. The real write commits exactly when the
model passes every row it changed (`statement_authorized`,
`derived_roots_authorized`); otherwise it fails with `DbError::NotOwner` and
leaves no row or upload. The storage
model's comparison is in [storage.md](storage.md#signed-roots-and-row-authorship).

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-database -- --ignored --exact \
  execution_capture::tests::lean::captured_actions_are_the_models_consequences \
  author_judgement::tests::local_admission_matches_the_model \
  judge_view::tests::receipt_matches_the_model \
  judge_view::tests::classification_matches_the_model \
  judge_view::tests::cascade_delete_old_matches_the_model
```

### B19 Local keys into values coven updates

Coven applies a change to a synced non-key column with an `UPDATE`, and
SQLite then runs every local foreign key's `ON UPDATE` action. Local rows
exist on one device only, so no write accounts for them.
`LocalUpdateAction.lean` checks the rule of [§9.1](../coven.md#91-foreign-keys).

**NO ACTION stops the apply for good.** `Stuck` is synced `parents.code = 'a'`
with a local pin on `'a'` under the default `ON UPDATE NO ACTION`; Ben's write
renames the code to `'b'`. `stuck_forever` proves that the commit check fails,
the apply rolls back to the same state, and every retry fails again.
`restrict_fails` shows RESTRICT failing at once. The Rust test
`a_local_key_that_does_not_follow_stops_every_apply` reproduces both attempts.

**CASCADE and SET NULL keep every key valid.** `rename_holds` and
`follow_holds` prove that a child renamed with its parent, to the new value or
to NULL, still references existing values. `chain_holds` extends this by
induction along a chain of local keys, each into the column the previous
key's action rewrites, which is the set of columns the schema check follows.
`accepted_apply` reruns the pin under both accepted actions.

**Outside the model.** A column is a list of values. Keys act independently,
as SQLite applies each key's action to its own columns; the model follows one
chain at a time and does not cover a local column two keys rewrite at once.
Primary-key changes are deletes and inserts (§9.2) and fall under the delete
rule, which this file does not model.

### B20 Installing a snapshot

A reload installs a snapshot's merge records as the snapshot holds them, and
then runs the removal rules only over a region of rows
([§16.5](../coven.md#165-coming-back-after-a-long-absence)). Every present row
outside the region is shown as its merged values, with no rule recorded.
Running the rules over every row would hold facts for each row of a large
store in memory; a consistent store gives the rules no row to start from.
`Install.lean` checks that the region gives the device replay gives.

**An example.** Release 0 is gone. Track 1 references it, and file 2
references track 1. Rows 3 and 4 claim one value, row 3's claim first.
Track 1 has a reason of its own: its parent is absent. File 2 has none, but
goes with its track. Row 4 loses to row 3. `Releases.removed` checks that
`view` removes track 1, file 2 and row 4 and shows row 3, and
`Releases.installed` that the install with the region `{1, 2, 3, 4}` gives the
same view.

**What the region holds.** `Region` states three things:

- every present row with a reason of its own: a failing CHECK, a deleted
  circle, or a reference that is stale or names an absent parent;
- both rows of every rival claim, unique or other-audience;
- every row that references a row of the region.

`within` runs the rules over the region's rows alone. Every row outside it
keeps what its generation says: present, and never taken out.

**The rules over the region remove what the rules over every row remove.**
`install_removal` proves it from §8.4's three steps. No rule fires for a
present row outside the region: it has no reason of its own, and its parents
are outside the region too, so they are present and stay in. Step 1 over the
region therefore ends where step 1 over every row ends, and so does step 3.
Step 2 reads only rival claims, and both rows of each are in the region. So
the region's run is one of the orders `Stratified` allows over every row, and
`any_order_removal` makes it `removal`.

A region row's parents need not be in the region. A parent outside it is
present and never taken out, which is the answer the rules over every row
give for it.

**The install shows what replay shows.** `installView` takes the region's
view inside the region and shows every present row outside it.
`install_view` proves it is `view`; `outside_shown` states that a present row
outside the region is shown. `install_replays` composes this with B6: a sound
snapshot of a closed set of writes, the writes after it applied in a causal
order, and the rules run over a region give the same merged state, view and
loss records as `device` for any causal order of the same writes.
`install_entry_replays` does the same with deleted-circle rules naming their
entries (B12).

**Edge cases.** A region without file 2, which has no reason of its own,
shows the file though its track is taken out (`without_children`). A region
without row 3 shows row 4, which loses to it (`without_rival`). So children
and both rivals are needed, and a row's reasons alone are not enough.

**Outside the model.** Which rows the region holds is an input, as
references, CHECK results and claims are in B7. `snapshot_conflicts::rows`
selects the rows with a reason in SQL, and `coven_merge::recompute` reaches
their children, parents and rivals; that the result meets `Region` is the
implementation's obligation. The copy of the merge records and the
snapshot's soundness are as in B11.
