## Appendix B. Proof of convergence

Owner tables restrict direct writes. Schema-derived foreign-key changes are
verified against authorized direct causes; ordinary constraint losses still
apply to owned rows. [B18](#b18-owner-tables-and-row-authenticity) states the
checked permissions, causal records, restoration, and SQL boundary.

- This appendix proves the convergence guarantee of [§3](../coven.md#3-guarantees) for the merge of [§8](../coven.md#8-merge)
  and the audiences of [§14](../coven.md#14-audiences).
- The proof is checked by machine: a Lean 4 development in
  `spec/proofs/merge/`, which has no unproven step and uses no axiom beyond
  Lean's own.
- Each claim names the Lean theorem that checks it;
  [B11](#b11-what-lean-checks-and-what-is-outside-the-model) lists the model's limits.

### B1 The claim

- Two devices that read the same audiences and applied the same writes and
  store log entries, each in an order that respects causality, hold the same
  database:
  - the same `_coven_rows` and `_coven_cells`;
  - the same rows in the app's tables, with the same values;
  - the same `_coven_lost` rows, for lost values and removed rows alike.
- This holds whatever order each device ran the removal rules in.
- Two devices that read different circles agree on every row both read,
  except which row shows for a key present in two audiences, and the rows
  taken out with it ([B9](#b9-audiences)).
- `_coven_writes` holds the same writes on both, under row ids of each
  device's own ([§8.1](../coven.md#81-example)).

### B2 Terms

- *Write*, *row change*, *cell*, *generation*, *audience*: as in [§5](../coven.md#5-local-database), [§8](../coven.md#8-merge),
  [§8.3](../coven.md#83-deletes) and [§14](../coven.md#14-audiences).
- *Row*: one table, key and audience. The store's note 1 and a circle's note
  1 are two rows, each with its own generations ([§14.2](../coven.md#142-moving-rows)).
- *Had read*: the recorded position says the device passed `a` before making
  `w`, including its own earlier writes. Passing a position does not mean
  applying its data ([§7.1](../coven.md#71-causality)). Actual generation and
  replaced-cell sources are separate retained evidence in B17.
- *Causal order*: an order of applying writes in which each write comes after
  every write it had read. B17 derives this order from receive steps, with
  consumed positions distinguished from retained effects at a boundary.
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
  ([§8](../coven.md#8-merge)).
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
  deleted ([§8.1](../coven.md#81-example), [§8.2](../coven.md#82-concurrent-writes-to-one-row)).
- **Lost**: a value is lost when some write in `S` replaced it and no write
  in `S` that replaced it had read it ([§8](../coven.md#8-merge)).
- **Replaced by**, for a lost value of incarnation `k`:
  - if `k` was deleted: its earliest delete, which is the write `_coven_rows`
    names for generation `k + 1`;
  - otherwise: the cell's current winning write.
- E.g. the writes of [§8.1](../coven.md#81-example) on note 42's title:

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
    winning write: the larger timestamp stays ([§8.2](../coven.md#82-concurrent-writes-to-one-row), [§8.3](../coven.md#83-deletes));
  - a change made at an older incarnation changes no cell.
- **`_coven_lost`**, for each value `a` of each cell of `r`:
  - `w`'s own value:
    - made at an incarnation already deleted: lost, replaced by the write
      `_coven_rows` names for that incarnation's delete ([§8.3](../coven.md#83-deletes));
    - made at the current incarnation with a smaller stamp than the cell's
      value: lost, replaced by the cell's winning write;
    - otherwise not lost;
  - a value already lost that `w` replaces:
    - if `w` had read it: no longer lost ([§8.2](../coven.md#82-concurrent-writes-to-one-row));
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
- **Snapshots** ([§15](../coven.md#15-snapshots)): loading a snapshot and applying the writes after it,
  with the covered writes counting as applied, gives the same merged state
  as applying every write from the start. Lean: `snapshot_converges`.
- **Timestamp order** is one causal order, by assumption 2, so every causal
  order gives the state applying the writes in timestamp order gives. Lean:
  `causalOrder_of_ts_sorted`.
- **A write made after seeing another wins over it** ([§3](../coven.md#3-guarantees)):
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
- E.g. Lean runs the step on the writes of [§8.1](../coven.md#81-example), [§8.2](../coven.md#82-concurrent-writes-to-one-row) and [§8.3](../coven.md#83-deletes), in the
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

- The removal rules' inputs come from the merged state and the store log,
  never from which rows are removed:
  - which rows are present: an odd generation;
  - each row's references: the parent each points at, and whether it is
    *stale*: the parent's generation it carries has been deleted since
    ([§8.4](../coven.md#84-foreign-keys));
  - whether the row's merged values fail a CHECK ([§8.6](../coven.md#86-check-constraints));
  - the kept entry that deleted the row's circle, if any ([§14.7](../coven.md#147-deleting-a-circle));
  - each row's unique claims, each with a stamp: the timestamp of the latest
    write that set any column the constraint reads in it, including a partial
    index's WHERE columns, and generated columns as set by each write that
    changed their value ([§8.5](../coven.md#85-keys-and-uniqueness));
  - each key present in two audiences on the device ([§14.2](../coven.md#142-moving-rows)).
- **Set null** for a child whose parent's generation was deleted is part
  of the merged state, not the rules ([§8.4](../coven.md#84-foreign-keys)):
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
  - **deleted circles**: it is in a circle the store log has deleted.
- **Unique values**: a row loses when a row still present claims the same
  value of the same constraint with a smaller stamp, or an equal stamp and a
  smaller primary key.
- **Keys in two audiences**: of two present rows with one key, the store's
  wins over a circle's, and of two circles' rows, the one whose current
  generation started with the smaller timestamp.
- [§8](../coven.md#8-merge)'s three steps:
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
- **[§8](../coven.md#8-merge)'s three steps have one result**: steps 1 and 3 have one result each
  by monotonicity, and step 2 is a function of step 1's result. In the
  example above, step 1 takes note 1 out, so note 2 keeps "Plan" on every
  device. Lean: `stratified_unique`, `judged_once`.
- **Any order**: coven's run of the rules is one of the orders the three
  steps allow, so every order gives the same removed rows. Lean:
  `removal_stratified`, `any_order_removal`.
- **A unique loser whose value is unchanged comes back** only when the
  winner is deleted, or taken out before unique values are judged.
  - E.g. notes are unique by title, and a sub-note points at its parent
    under cascade. Note 1 is "Ideas"; at 10:00 Ana adds note 2, "Plan", as a
    sub-note of note 1; at 11:00 Ben, not having seen it, renames note 1 to
    "Plan".
    - Step 2: note 2's claim from 10:00 keeps "Plan"; note 1 is taken out.
    - Step 3: note 2 goes with its parent, by cascade.
    - Note 1 stays out: back, it would bring note 2 back, and lose to it
      again. Lean: `example_8_5_subnote`.
  - E.g. the same, but note 2 is a sub-note of note 3, "Ideas" since 12:00,
    and note 4 has been "Ideas" since 09:00.
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
  it and the store log. `LossRecord` gives cells and removed rows one shape:
  row, generation, optional column, written values/setters and cause. Lean:
  `lossRecord`, `device_converges`,
  `rule_order_converges`.
- E.g. Lean runs the writes of [§8.4](../coven.md#84-foreign-keys), [§8.5](../coven.md#85-keys-and-uniqueness), [§8.6](../coven.md#86-check-constraints) and [§14.7](../coven.md#147-deleting-a-circle), with the
  rules reading the merged state it computes, in more than one arrival
  order:
  - [§8.4](../coven.md#84-foreign-keys): Carol's tablet applies Ana's delete of note 43, then Ben's
    attachment 9 on it: attachment 9 is taken out, and its `_coven_lost` row
    names the foreign key.
  - Ben's move of attachment 9 to note 44 then arrives: attachment 9 is
    back. Ben's phone, which gets Ana's delete last, never takes it out.
    Lean: `example_8_4`.
  - [§8.5](../coven.md#85-keys-and-uniqueness): Ana renames the tag "urgent" to "important" while Ben tags note 44
    "urgent". Note 42 ends tagged "important", and Ben's `(44, "urgent")` is
    taken out, naming the foreign key. Lean: `example_8_5_key`.
  - [§8.5](../coven.md#85-keys-and-uniqueness): notes are unique by folder and title. Ana adds note 1, "Plan" in
    Work, at 10:00; Ben renames note 2 "Plan" at 11:00; Carol moves note 2 to
    Work at 12:00. Note 2's claim dates from 12:00, so note 1 keeps the
    value. Lean: `example_8_5_stamp`.
  - [§8.6](../coven.md#86-check-constraints): Ana sets start 10 while Ben sets end 8: the row is taken out,
    naming the CHECK. Ben's later end of 20 brings it back. Lean:
    `example_8_6`.
  - [§14.7](../coven.md#147-deleting-a-circle): Ben's entry deletes Gifts while Ana adds note 9.
    Notes 7, 8 and 9 stay at generation 1, hidden with that entry named in
    their losses. Dropping the entry returns all three. There is no circle
    deletion write. Lean: `example_14_7`.

### B9 Audiences

- A row is one table, key and audience, with its own generations ([§14.2](../coven.md#142-moving-rows)).
  Each row change sits in the part of its row's audience; a device applies
  the parts it can read and counts the rest as applied ([§14.4](../coven.md#144-writes)).
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
- [§14.5](../coven.md#145-references) gives the parents: a row points only at rows every reader of it can
  read.
- [§14.1](../coven.md#141-roots-and-descendants) gives the unique rivals: a unique constraint on a root table
  includes the audience column, and one on a descendant table includes the
  foreign key it takes its audience from, so rivals share an audience;
  constraints that could span audiences are refused. Lean: `rivals_closed`.
- Every member's devices read the store log, so they agree on which circles
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
- **Fingerprints** ([§19.1](../coven.md#191-noticing)): computed as if the rule for keys in two audiences
  didn't exist, removal agrees on every row of every audience both devices
  read, so the fingerprint is the same on every device that applied the same
  writes. Lean: `rivalBefore_forFingerprint`, `fingerprint_local`.

### B10 The spec's examples, checked

- [§8](../coven.md#8-merge): todo 7 names both rules. Lean: `example_8`.
- [§8.1](../coven.md#81-example): both orders end with "Shopping" and one lost value. Lean:
  `example_8_1`.
- [§8.2](../coven.md#82-concurrent-writes-to-one-row): "Groceries" is lost on Carol's tablet until Ben's write 9 arrives.
  Lean: `example_8_2`.
- [§8.3](../coven.md#83-deletes): Ben's edit is replaced by Ana's write 7 in every order. Lean:
  `example_8_3`.
- [§8.4](../coven.md#84-foreign-keys): attachment 9 is taken out, then back when Ben's move arrives;
  link 6 holds null, then ends on note 44 once Dan's write arrives. Lean:
  `example_8_4`, `example_8_4_later`.
- [§8.5](../coven.md#85-keys-and-uniqueness): a key change leaves Ben's `(44, "urgent")` taken out; note 2's claim
  dates from 12:00; a loser whose winner leaves in step 3 stays out. Lean:
  `example_8_5_key`, `example_8_5_stamp`, `example_8_5_subnote`,
  `example_8_5_step3`.
- [§8.6](../coven.md#86-check-constraints): the row is taken out, then back when Ben sets end 20. Lean:
  `example_8_6`.
- [§14.2](../coven.md#142-moving-rows): the store's note 1 wins over the circle's. Lean: `Moved.agree`,
  `Moved.store_wins`.
- [§14.7](../coven.md#147-deleting-a-circle): existing and late circle rows keep their
  generations and setters, are hidden by the named entry, and return when
  it drops. Lean: `example_14_7`.

### B11 What Lean checks, and what is outside the model

**§3 coverage:** this model proves merge convergence, causal winners and value
accounting for valid causal inputs and supplied boundary verdicts (B6, B8,
B12–B14). B17 derives causal application from receive steps and filters
changes by their actual data sources. Retirement never regains a write
because another write read it; dependent changes become recorded losses.
Applying those results to stored objects assumes correct authentication
and sealing; it proves neither confidentiality nor signature or cipher security.
Atomic SQLite installation and §2's provider behavior are assumed, not proved.
Delivery and the other §3 guarantees belong to the [separate models and their
limits](../coven.md#3-guarantees), not this convergence theorem.

Lean checks the theorems named in this appendix. `CovenMerge/Axioms.lean`
prints their axioms; only Lean's `propext`, `Classical.choice` and
`Quot.sound` are allowed. `scripts/check.sh` rebuilds the package, audits
these axioms, and runs the Boolean, entry-aware and schema differentials and the
fixed reset and migration counterexamples in B16. Strict comparisons that find
those remaining discrepancies stay out of the check script.

Values are identified by their setters and cells. A column can be its old
name, so a frozen record does not require that column in the new schema.
`freeze_preserves_values` also proves preservation under any fixed mapping
from these identities to scalar values. The model does not execute SQL,
compare schemas, rename columns, convert write payloads, or encode snapshots.
It has no live parent links in loss records; clearing serialized parent maps
is outside it. Frozen records are held separately from the merge state and
are never supplied to current removal rules.

The rules take references, CHECK results and claims as inputs. CHECK expression
normalization and rule names are outside Lean's Boolean CHECK result; the
`unnamed_check_sqlite_differential` compares the database evaluator with SQLite
constraint enforcement, including failures that SQLite abbreviates to one token.
A multi-column reference is one input: capture sets all its components together, so the
ordinary cell rule selects the same setter for each. The database differential
`composite_reference_differential` compares captured whole-tuple setters and
losses with `MergeRunner` in both arrival and timestamp orders, including NULL
components. SQL capture and generated-input expansion remain outside Lean.
Computing those inputs from SQL, reference null substitution, and local insert-to-update
normalization remain outside the general proofs. The examples compute the
inputs they use. Same-key uniqueness assumes the ordered audience claims
specified by §14.2; the proof does not establish those claims from SQL.

Store-log replay supplies the kept entry, its snapshot, and which deletions
are final. Selecting these is outside this package. Schema eligibility and
conversion verdicts are also inputs:
the original `boundaryDevice` applies supplied whole-write verdicts for the
existing Rust differential. `Data` in `Boundary.lean` and B17 additionally apply the
shared per-change data rule. The queue package derives schema admission and
conversion decisions; recorded reads are not data dependencies.

As in B3, convergence assumes valid writes. At a reset it additionally
requires a correct selected snapshot and `ResetGenerations`: a retained
change's generation was reached by a retained input. This states the
requirement that authoring uses the reloaded state, rather than discarded
local generations. `reset_valid` and `reset_causal` then derive the ordinary
merge's validity and causal-order conditions. The reset example checks its
write validity, retained generation witnesses and snapshot correctness.

Files, encryption, network transfer, atomic SQLite installation, retention
until finality, loss dismissal, fingerprint hashing and trigger execution are outside
this model. The functions describe a completed atomic reload; they do not
model a partially installed database. Rust's merge functions retain the kept
circle-deletion entry in each cause. The Boolean and entry-aware differentials
pass, including replacement and removal of those causes (B16). The reset differential
calls the database snapshot reload and exposes a separate eligibility
discrepancy there. The schema-boundary differential passes on its generated
histories; version/conversion verdicts are supplied as described below.
The migration differential exposes retirement caused solely by a non-final
circle deletion; its comparison stops at row selection.
The Boolean inputs and existing merge functions also remain available to
the separate store-log/data package.

B17 models a retained receive queue and checks Rust's causal prerequisite
decision separately. It derives causal order rather than assuming it at the
receive boundary. Retirement can remove a generation's source. B17 records
each dependent change as a loss and proves validity of the surviving merge without assuming
that retirement preserves generation witnesses.

### B12 Circle entries, migration deletes and frozen losses

For [§14.7](../coven.md#147-deleting-a-circle), `EntryInputs` holds the
responsible entry, and `entryView` records `deletedCircleEntry entry`.
`deleted_rule_names_entry` proves the row is hidden and its loss names that
entry. `circle_entries_preserve_merge` proves observation changes no merge
record. `row_returns` proves a present row returns when no reason remains.
`entry_device_converges` includes the named losses; `entry_device_rule_order`
allows every order within §8's three passes.

For [§17.1](../coven.md#171-host-application), `independentInputs` removes
non-final circle causes and reruns all three passes. This also removes their
effect on children. `nonfinal_only_untouched` proves that a row hidden only
by those causes is left out of the entire migration write, preserving all
its merge records and creating no frozen loss. `independent_rule_order` proves
that decision independent of rule order; `nonfinal_does_not_decide` proves that
stripping those causes again cannot change it. The checked
`nonfinal_parent_and_child` history keeps
a parent and child hidden only by the entry, but deletes a row with an
independent CHECK failure. Final circle deletions can cause migration deletes.

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
§17.1 instead gets the frozen schema loss proved by `excluded_values_recorded`.

`migration_converges` covers every valid causal order of the authored
migration and concurrent edits. The migration is authored from fixed inputs;
receivers do not recompute its delete generations from their arrival prefix.
`migration_capture_converges` proves equal author input sets also produce
equal frozen records. `migration_retains_reversible_rows` checks that dropping
a non-final deletion returns retained rows, while an independently invalid
row stays deleted with its frozen loss.

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
its cells and cell losses. A row hidden only by reversible causes stays
hidden, so it keeps a live removed-row record. `hidden_row_after_drop`
proves that record is the old one without the dropped columns' values, and
`dropped_column_frozen_whole` that a record naming a dropped column is
frozen whole, that value included, while the live record no longer holds
it.

### B13 Reset contents on every device

For [§19.3](../coven.md#193-resetting-a-store), `ResetBoundary` distinguishes
positions consumed by the snapshot from writes that contributed to its merge
state. Ignored and excluded writes can have consumed positions without
contributing values, as [format D7](../format.md#d7-snapshots) requires.
`consumed_is_not_input` checks that distinction through a later reload.

`reloadReset` starts with the snapshot and applies only uncovered writes
whose `store_log_read` includes the kept reset. `reset_exact` gives that
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

For [§3](../coven.md#3-guarantees), `value_accounted` quantifies over every
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
cannot both be shown. `same_key_after_return` checks putting a circle row
into the store with the same key: when the deletion drops, the store row
still shows and the circle row is hidden by the other-audience rule.

### B15 Checked counterexamples and readings

The retirement histories in B17 use each write's own landing window. Their
dependent row changes become frozen losses; the dropped creation stays
dropped. The receive results agree in both legal orders.
`UniqueOnce.unique_with_others` remains a checked counterexample to running
uniqueness in an unrestricted loop: a deleted folder with two notes sharing
a title can leave one note or neither, depending on rule order. The spec's
three passes avoid that result; this is not a counterexample to §8.

The readings used are:

- “Wins over” compares changes to the same cell and incarnation, or a delete
  of that incarnation. It does not overwrite unrelated cells or promise that
  no third write wins. Re-insertion advances the generation; old setters and
  the old delete cannot affect its cells. This concerns retained merge inputs;
  resets and schema exclusions use the separate boundary rules below.
- “Any arrival order” permits transport arrival in any order; application
  still obeys causality. “Any rule order” stays within §8's three passes.
- Migration judges independent causes by rerunning all three passes without
  non-final circle deletions, including unique and other-audience claims.
  Finality and the responsible kept entry are supplied by store-log replay.
- A late edit that is eligible after schema handling follows the generation
  delete path. An excluded old-schema edit follows the frozen schema-loss path.
- Snapshot coverage means consumed positions; contributing inputs are
  separate. A kept reset filters uncovered writes before schema exclusion;
  with no reset, only schema eligibility restricts them.
- “The same key shows once” means at most once: another removal rule may
  hide both rows. The store copy wins when it survives the other rules.
- Setter-and-cell identity stands for the written scalar. Freezing captures
  that identity and the old column name; it never substitutes a current
  foreign-key value into a loss.

### B16 Rust differentials and checked discrepancies

`MergeRunner.lean` accepts Boolean removal inputs or `EntryInputs`. Both
modes run the same core merge; the entry-aware mode returns each named
circle-deletion cause without discarding its entry. The Rust adapter calls
`apply` and `removals` and reports the rule Rust actually returns. It does
not supply a missing identity from the test input.

`entry_differential_histories` generates causal writes, random application
orders and prefixes, and changes to kept circle-deletion entries. A mismatch
stops the comparison. The reducer removes writes, changes, values and entries
while retaining a history accepted by Rust's `History::new`.

The entry-aware comparison runs 128 generated histories, eight changes to
the selected deletion entries per history, and three causal application orders
at each sampled prefix (3,072 comparisons). Entry identities vary in both
device and log number. The adapter translates the identity returned by Rust's
`Rule::DeletedCircle(entry)`; it does not reconstruct a cause from the row's
audience.

`entry_differential_agreement` also checks one key-only circle row and one
kept deletion. Both implementations retain generation 1 and its insertion
identity, hide the row, and name the deleting entry. Removing either the insert
or the deletion also agrees. This exercises the entry identity required by
[§14.7](../coven.md#147-deleting-a-circle), not merely Boolean visibility.

`scripts/check.sh` runs the Boolean differential, the strict entry histories,
and the fixed agreement case. SQLite persistence, atomic rollback and
restoration after reopening are checked separately by Rust database tests.

From the repository root, after building `mergeRunner`:

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-merge --all-features entry_differential -- --ignored --nocapture
```

The reset mode calls `reloadReset` on the selected snapshot inputs and incoming
writes. `reset_differential_histories` calls the real database
`load_snapshots`, which installs the snapshot and replays writes through
`snapshot_writes::apply`, `interpretation::accepted` and `WriteBoundary::excludes`.
It compares generations, generation setters, cell setters, cell losses and
frozen losses, and checks that every supplied write position is consumed.
Store-log selection is supplied to both sides; this is not a test of replay
selection, encryption or partially installed state.

The deterministic generator samples causal histories, arrival orders and
prefixes, with inserts, updates and deletes. Authors obtain their row state
from Rust's merge, retaining the selected snapshot inputs after observing the
reset. The reducer removes writes, changes, non-key values, reset reads and
snapshot coverage while preserving format validity, causal closure and the
merge's generation witnesses.

Rust's reset predicate is §19.3's: an uncovered part of the reset audience is
ignored when its write's `store_log_read` lacks the kept reset entry, even if
it read every write in the snapshot. All 128 seeds pass, each in three causal
arrival orders at both a full history and a random prefix (768 comparisons).
`reset_differential_agreement` fixes the former seed-0 history: an empty reset
snapshot and one key-only insert that did not read the reset. Both sides leave
generation 0 and no cells or loss; removing the reset or making the write read
it applies the insert on both sides, and removing the write or covering it
also agrees. Lean's `ResetAgreement` checks the specified results and write
validity.

`scripts/check.sh` runs the strict reset comparison and the fixed agreement
case. Run them separately with:

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-database reset_differential -- --ignored --nocapture
```

The generated authors here retain only snapshot inputs and post-reset data, so
these histories never build on an ignored change. The queue comparison
[`queue_reset_differential_histories`](queue.md) covers authors that read the
reset before reloading: each change using ignored data becomes a dependency
loss there, while its write's other changes apply.

The schema mode calls `boundaryDevice`, including `receivedLoss` and the
selected snapshot's frozen records. `schema_differential_histories` also
calls `load_snapshots`, with a schema-raise boundary and no reset. It compares
the same merge records and frozen whole-row records: row, generation, column
and setter identities, breaking version and excluded-write identity. Values
have the model's setter-and-cell meaning; scalar conversion is not tested.

All 128 seeds pass, each in three causal arrival orders at both a full history
and a random prefix (768 comparisons). The histories include covered ordinary
writes, covered frozen losses, uncovered old-version writes, admitted
new-version writes, deletes without old values, and migration log records
with no row changes. A covered frozen loss comes from an old-version write
the snapshot's author excluded under its own raise; no write carries a lost
disposition (format D5). Frozen rows also carry an old column absent from
the current SQL schema. Covered excluded positions do not become snapshot
merge inputs. The reducer preserves these distinctions while removing
writes, changes, values, coverage and the schema raise; it can also return
an author-excluded write to the snapshot's merge inputs.

The reading is [§17.1](../coven.md#171-host-application): “S already determines
the effects of writes it covers” and “An old-version write outside S is
excluded: keep its values as frozen losses, and pass its position without
applying its changes.” The adapter supplies the eligibility verdict from the
write's version, and tests the boundary's use of it.
Conversion callbacks, per-change data dependencies, and competing
store-log selection remain outside this comparison. Schema tests use no reset.

`scripts/check.sh` runs the strict schema comparison. Run it separately with:

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-database schema_differential_histories -- --ignored --nocapture
```


The migration mode calls `migrationRemoves`, which reruns all three removal
passes through `independentInputs` and `entryView`. The Rust differential
applies generated writes and the circle deletion to a real database, then
opens it with a breaking index migration. This invokes `migration_run::run`
and `migration_state::carry`; it does not reconstruct retirement in the test.
References, CHECK results and unique claims come from `DatabaseRemovalView`
and its SQL evaluators. The generator includes competing insertions, store
and circle audiences, references, CHECK failures and unique rivals, sampled
in causal orders and prefixes. Replay supplies the deleting entry and its
finality; selecting or proving that finality remains outside this family.

The comparison observes which rows the migration deletes, by their changed
generations: Rust's migration write deletes each selected row at its
generation, as `withHiddenDeletes` does, so the selection and the generation
change are the same observation. Finality comes from the store-log horizon:
a final deletion is stored before it. The reducer removes writes, optional
values, references and the deletion, retaining valid causal histories and
schema-required insert cells.

Rust follows [§17.1](../coven.md#171-host-application): with every deletion
final it deletes the rows removal rules hide; with a non-final deletion it
reruns the three passes on the migrated state with and without that
deletion, leaves out each hidden row the deletion decides, and includes each
row that loses only once it is absent. All 128 seeds pass, each in three
causal application orders at both a full history and a random prefix (768
comparisons). Earlier histories stopped at seed 0 (one insert in a circle,
its non-final deletion, and the breaking migration), where Rust retired the
row; `migration_differential_agreement` now checks that both sides leave it
out, and agree with the insert removed, the deletion removed, or the
deletion final (then both delete it). `MigrationCounterexample` keeps the
corresponding Lean results. Database tests check the migration write's
deletes, frozen whole-row losses with original setters and old names, a
returning parent and child after the deletion drops, and late edits losing
to the retained delete on the migrating device and on a device that loads
its snapshot. For `freezeDropped`, they check that dropping a table or a
column freezes each affected pending cell or removed-row loss with its old
names, setters and cause, through name reuse, renames, snapshot reload with
equal fingerprints, and dismissal.

`freeze_differential_histories`
([`migration_hidden_tests.rs`](../../crates/coven-database/src/migration_hidden_tests.rs))
compares the real migration with `freezeDropped` over generated stores. Two
devices edit the same store-row and tag cells concurrently, so the subject
holds cell losses; circle rows, some setting the droppable columns, are
hidden by a non-final circle deletion, so it holds removed-row losses. One
or two later migrations then drop column `c`, rename `d` to `e`, drop `d`
or `e`, and drop table `tags`, in random combinations. Before each step
the runner's `freeze` mode receives every stored loss, as rows, columns and
setters numbered from the old names, with the tables and columns that step
drops. It applies `freezeDropped` with `droppedBy` and returns the frozen
records, the cell losses still active, and each kept hidden row's live
record under `dropSchema` (`lossRecord`). After the step Rust's stored
losses must equal them, frozen ones under their old names and active ones
under the new. Values are compared by setter, as the model identifies them.
All 48 seeds agree over 69 steps, comparing 57 frozen cell losses, 33
frozen whole rows and 69 live removed-row records; the test requires each
kind to occur. With `droppedBy` ignoring whole-row values it fails at one
hidden row and a dropped `d`. `freeze_differential_agreement` checks one migration with each
kind of record. Rows the migration deletes (`captureHidden`), and losses
under other migration changes, are outside this comparison.

`scripts/check.sh` runs the strict comparisons and the agreement cases:

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-database migration_ -- --ignored --nocapture
```

<a id="b17-receive-order-progress-and-the-retirement-gap"></a>

### B17 Receive order, progress, and dropping by data

This models the data rule in [§8](../coven.md#8-merge): building on a dropped
change does not make that change count. Rust's remaining implementation
obligations are listed below and in [queue.md](queue.md).

**One data definition.** `DataDependency.dependsOnDropped` asks whether the
change's recorded generation source or any cell setter it replaced has a
non-retained change at that row. A row includes table, key and audience. The
unit of loss is the D5 row change, not its entire write or audience part; a
multi-column row change is one unit. Deletes retain their change identity
even without scalar values. A source whose change was itself lost cannot
support another change, even if that source's write kept other changes.

`DataDependency.State` is retained merge evidence: source identities, original
change presence, their timestamp order and their own admission verdicts.
`retained` follows only those sources and terminates by strictly decreasing
authored timestamps. `dependencyDecision` is an executable decision of the
source predicate; `depends_iff` gives its exact meaning. There is no read
frontier, future-reader set, device-local observation or loss-dismissal state
in this decision. `state_alone` proves equal evidence gives equal decisions.
Retirement and schema exclusion supply their own base verdicts to this same
function. A theft policy can supply its verdicts there too; this package
does not define theft admission.

This is an extension of retained merge state, not a claim that the existing
compact `St` or D5 records already contain this evidence. `Authored` requires
an actual generation source and the original setters the change replaced.
It checks source reads, timestamp descent, generation advancement, setter
incarnation and cell identity. Capturing the *actual* author-state choice,
including all replaced cells, is an authoring/validation obligation: a numeric
generation, old scalar equality or a plausible timestamp cannot establish it.
The current canonical generation setter may differ from the author's source.
Malformed or unavailable evidence fails validation or waits; it is not treated
as an independent change. Snapshots and conversions must preserve this evidence
separately from dismissible losses.

**No rescue, and no missing generation premise.** `effective` leaves recorded
causal metadata intact and removes only non-retained changes.
`dropped_stays_dropped`, `dependent_not_retained` and `independent_retained`
prove the three decisions. `effective_valid` derives `Valid` for this merge:
if a retained change needs a generation source, that source's change also
survived. No retained-witness hypothesis is assumed for retirement, schema
exclusion or reset. Sources are required to be valid in the original authored
history, before any exclusion.

`Data.loss` captures dependent changes as `FrozenLoss`, preserving
their generation, written values and original write identity. Its dependency
cause lists the failed source identities in authored role order; a write may
occur twice when it supplied both generation and old cell. Direct schema
exclusions instead keep their schema cause. A later ordinary replacement never
acknowledges these frozen values away. `dependent_recorded` covers every
change, including an empty-valued delete; `dependent_loss_preserves` keeps the
written values. Base retirement drops and reset-ignored writes create no
schema loss. Their otherwise admitted dependent changes do create data losses.

`Data.ValueAccounted` extends the ordinary four outcomes with a
frozen loss of the original change. `Data.value_accounted` and
`Receive.receive_value_accounted` quantify over original setters, including
ones removed before merge. Direct schema-excluded changes are covered by
`receive_schema_loss`. `Data.converges` and `loss_converges` prove agreement
of merge fields and recorded losses. `receive_converges` and
`receive_result_converges` derive causal order from actual receive runs and
include the app view, rule losses and snapshot-frozen records. The remaining
premises are valid authored evidence, the same admission facts and consumed
sets, and a correct snapshot with causally closed **consumed** coverage.
An empty snapshot needs no retained-generation or safe-reader premise.

**The four retirement histories.** `ReceiveExamples` uses the same creation,
active edit, concurrent delete, first publication times and legal receive
runs as the original failures. Retirement was temporarily dropped when the
creation landed and the other devices used it; it then returns before
finality. The creation is one second outside its window and remains dropped.

- `retirement_generation_accounted` proves the filtered merge is valid and
  omits the dependent edit, despite the original generation being unavailable.
- `retirement_value_recorded` proves that edit's original value and generation
  are held in a dependency loss naming the creation;
  `retirement_value_accounted` checks the original setter's accounting predicate.
- `retirement_loss_agrees` proves edit-then-delete and delete-then-edit record
  the same edit loss; the dependent delete gets its own record too. Neither
  dependent change alters generations, cells or ordinary merge losses.
- `receive_runs_agree` proves exact equality of both complete receive results,
  for any common rule-input function. It applies the general theorem without
  a retained-witness hypothesis.

`replay_restores_by_own_window` separately checks that dropping the retirement
restores the creation and edit through an atomic rebuild. Receiving or losing
a later reader cannot change the creation's own verdict.

**Finality and the send gate.** `window_exact` checks every kept retirement,
including replacement and member/device removal, using first complete provider
publication times. Exactly thirty days counts. `retired_no_effects` applies
also to snapshot contributions rebuilt under a changed retirement decision.
The window compares storage times only: `window_ignores_reads` shows that
what the write read plays no part. Exclusion by reading belongs to the
receive gate: `Policy.viewRetired` says whether the replay of the write's own
`store_log_read` retires its device, and `passed_before_removal` proves that
no such write passes when the gate refuses them. A write past a window is
consumed without changes. Which retirements a write's own replay keeps is
proved per object in [storage](storage.md#each-object-judged-in-its-own-view).

`retirement_verdict_final` uses the final kept retiring entry and the stable
preceding store-log prefix supplied by §9, including storage-time ties.
Retirements after that prefix may appear or disappear. Each is stored after
the final entry, so those changes cannot reverse either window verdict: an
admitted write already meets the final entry's earlier deadline. The proof
needs no assumption about what the write read. No premise concerns future
readers, their admission, or draining their logs. This proves retirement
admission finality; another independent schema/reset policy can still change
its own verdict.

`send_gate_publication_bound` proves that a catch-up started after finality
blocks an old-id send, and an earlier catch-up plus the strict five-minute
freshness gate and one-day provider request bound cannot publish at or after
finality plus one day and five minutes. This supplies a discovery drain bound,
not an extra admission exception. The store-log finality theorem, completeness
of catch-up, honest sends and the provider-duration bound remain inputs.

**Receive order and progress.** `Receive.lean` separates downloaded bytes from
consumed positions. Each device log downloads a contiguous prefix. Attempts
require retained bytes, other prerequisite checks and every recorded position
to be consumed; a failed attempt changes nothing. `passed_causal` derives the
order for every run. `positionsReady_correct` proves frontier readiness,
including implicit own predecessors, equivalent to checking the expanded past.
Passing an excluded position can release an unrelated change without applying
the excluded data.

`complete_past_eventually_passed`, `arriving_past_eventually_passed` and
`receive_eventually_applied` retain their progress results: required inputs
must eventually arrive or be covered, scheduling must retry retained work,
other checks must eventually stay open, and admission facts must stabilize.
Here `applied` identifies base-admitted writes; each one's changes are separately
filtered and accounted. Arrival alone does not imply progress; `starved` checks
a downloaded write never attempted. Every changed replay decision requires an
atomic rebuild of rows, provenance verdicts and losses; these functions model
the completed transaction, not recovery by a later pass.

**Normative rule and encoding.** §8 specifies per-change data dependence and
original-change accounting. The Lean evidence is not an assertion that Rust's
current write and snapshot encodings already retain those sources. Consumed
frontiers remain separate from retained data; [queue.md](queue.md) describes
the encoding and conversion obligations.

**Rust obligations for the queue work.** Thread provider publication times and
all kept retirements into admission. Capture and validate data provenance at
authoring; store and snapshot it independently of losses, preserving original
row identity and column names through conversions. Classify each change with
the shared rule, record its loss before advancing the consumed position, and
install replay rebuilds atomically. Never infer a source from a frontier or
from the current canonical generation setter. Check both retirement receive
orders, independent changes in the same write, source-change chains, snapshot
reload, dismissal and finality. Concrete conversion obligations are in
[queue.md](queue.md).

**Rust comparison.** `ReceiveRunner.lean` still calls the proved frontier
readiness function. `lean_differential_receive` calls the real database
prerequisite function across three logs and consumed prefixes. Its `window`
check runs `withinWindow` for `mg_differential_retirement_window`, which
compares coven-database's retirement window over every subset of kept
retirements, reads of them, which both sides ignore, and publication times at
and around thirty days. For each case with a kept retirement, read or not, it
also keeps the final prefix through
the earliest one, adds a later retirement, and checks both runs agree, as
`retirement_verdict_final` states. Its `dependency` check runs
`DataDependency.retained` and `droppedSources` for
`mg_differential_data_dependency`, which compares coven-merge's shared
classifier over generated rows with dropped bases, chains and repeated
sources; the database receives every change through that classifier. The Boolean, reset and schema runners
retain their existing paths and comparison scope.
The database's per-change data losses under retirement replay are compared
with the queue model's `Retired.lean` composition of `withinWindow` and the
shared classifier ([queue.md](queue.md), `queue_retirement_differential_histories`):
the real receiver keeps and drops retiring entries between arrivals and must
end, after its replay rebuilds, with the model's rows, verdicts and dependency
losses for the final kept retirements in both arrival orders. Database tests
check the four histories above after a retirement returns to replay, the
setter-only variant, schema precedence, dismissal, release after finality,
rollback of a failed rebuild and a snapshot loaded under another verdict.

### B18 Owner tables and row authenticity

Owner tables are a write permission. They do not promise that another member
cannot cause an owned row to disappear. A foreign key, CHECK, unique contest
or audience rule can remove it under the ordinary schema and merge rules.
There is no owner-specific delete trial, removal-set veto or schema predicate.

**Identity and direct permission.** `Policy` opts tables in and derives each
row's owner from its original creating write's authenticated member.
`selected_creation` uses the smallest timestamp among generation-zero
creations, before filtering owners; `creation_selection_converges` proves
agreement. `later_change_preserves_creation` and `owner_fixed` retain that
identity across reinsertions and copies. First receipt cannot choose the
owner: `immutable_first_arrivals_incompatible` checks why that would conflict
with convergence for concurrent shared-key creations. A newly found earlier
creation can change the selected original creation, after which the affected
changes are checked again. It does not change any creation's author.

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
includes the incarnation, operation, columns and actual scalar values.
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

Three checked histories bound the rule; each was also run on SQLite.
`TriggerFirst`: Ben deletes his track; the cascade deletes Ana's first review,
and a shared AFTER DELETE trigger fired by that deletion deletes her second
review before the cascade reaches it. `trigger_classified_as_action` shows
the trigger's step is classified as the track deletion's action. Its payload
is exactly the action's, so no rule reading only steps can tell them apart; a
receiver accepts nothing the schema would not have done. The author's own
SQLite still knows a trigger made it and refuses the write locally. So
"foreign-key action" on receipt means "the schema's exact consequence", and a
shared trigger's step stays direct whenever it differs from every
consequence. `FollowBack`: Ben changes his track's referenced code and back,
and Ana's review follows both times. `start_view_refuses_cascade` shows that
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
primary keys and turns a key change into a delete and an insert (§8.5), so the
action names the inserted row, and a reference to the old key names an
incarnation that ended; that refuses `Retarget`'s edit even before parents are
compared. `FollowBack` holds either way: the second cascade's cause is a step
on the inserted row, which the view at the write's start never names.

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
one member for good and every key binding is kept (§9, §10). D7 section 7
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

**Rust differentials.** `MergeRunner`'s `creation` mode reports each row's
`genWrite r 1`, the creation `selected_creation` characterizes.
`creation_differential_histories` receives generated concurrent creations,
updates, deletes and reinserts of owner-table rows in several causal orders
through the real database and compares each row's retained creation with it.
The `effects` mode evaluates `resolvedSchema` derivations with `evaluate` and
`verifies`. `captured_actions_are_the_models_consequences` runs SQL deletes,
referenced-key updates and compound and transitive actions on SQLite,
rebuilds each captured foreign-key action's `Reference` from the schema and
its cause's own captured step, and requires the model's exact consequence.
That checks the author's capture against B18's effects.
The `receive` mode decides `Admission.check` and `Admission.guard` for a
received write from the receiver's reconstructed execution: the references
behind each action the receiver found, each row's direct effect, each derived
row's paths combined with `together`, its proposed net change, and the
generation, setter and execution sources of every change with those that do
not survive. `judge_view::tests::receipt_matches_the_model` receives direct,
derived, transitive, coalesced, trigger and unauthorized-root writes, a forged
action payload and an action whose cause's net change differs from its steps,
and requires each recorded verdict to be the model's cause, with the roots of
each derived change (`applied_derived_recorded`) and the failed sources of each
dependent one (`dependencies`, `dropped_contributing_root_no_effect`). The
receiver's classification itself, which supplies those references, is not yet
compared with `Classification.cause`.
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
  ownership::tests::creation_differential_histories \
  execution_capture::tests::lean::captured_actions_are_the_models_consequences \
  owner_permission::tests::local_admission_matches_the_model \
  judge_view::tests::receipt_matches_the_model
```

### B19 Foreign keys on generated columns

A generated column is computed by SQL from the stored columns it reads, and
coven merges stored cells one at a time. `GeneratedReference.lean` checks
what that means for a foreign key on such a column
([§8.4](../coven.md#84-foreign-keys)).

**A single generated key column is unsafe.** `Unwritten` is the table
`c(a, b, ref AS (a || b) REFERENCES p(id))`. One write inserts `a = x`,
`b = y`. Ana then sets `a = q` while Ben, concurrently, sets `b = z`.
`unwritten_reference` runs both arrival orders through the proven step: each
shows `ref = qz`. The insert recorded `xy`, Ana `qy` and Ben `xz`; no write
recorded `qz`, so no author checked that parent, and the foreign-key check
fails on every device. `valid` and `orders_causal` check that the history is
a legal one, so this is the merge result every device reaches.

**Capturing the inputs with the key is safe.** `grouped_cells_share_winner`
proves that when every change sets either all columns of a group or none,
every cell of the group holds the same write in the merge result.
`accepted_reference_written` applies it to the schema check: a key the check
accepts has no generated column, or spans several columns, and then its capture
group holds the key's columns and every generated column's inputs. Every
reference a device shows is the tuple one write set (or, for one stored
column, that cell's own value). `device_reference_written` states this for
any causal order of valid writes. `grouped_reference_recorded` reruns the
counterexample with the inputs captured together: both orders show Ben's
whole tuple `xz`, which he recorded.

**Outside the model.** Which stored columns a generated expression reads,
and capturing a group's components together, are SQL facts supplied as the
`Columns` record and the `Grouped` hypothesis, as for the composite-reference
differential in B11. A key whose generated column reads no column at all is
refused too; the model does not need that case.

### B20 Local keys into values coven updates

Coven applies a change to a synced non-key column with an `UPDATE`, and
SQLite then runs every local foreign key's `ON UPDATE` action. Local rows
exist on one device only, so no write accounts for them.
`LocalUpdateAction.lean` checks the rule of [§8.4](../coven.md#84-foreign-keys).

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
Primary-key changes are deletes and inserts (§8.5) and fall under the delete
rule, which this file does not model.
