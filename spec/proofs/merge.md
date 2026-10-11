## Appendix B. Proof of convergence

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
- *Had read*: write `w` had read write `a` when `w`'s device had applied `a`
  before making `w`; a device has always read its own earlier writes ([§7.1](../coven.md#71-causality)).
- *Causal order*: an order of applying writes in which each write comes after
  every write it had read. B17 derives this order from receive steps, with
  consumed positions distinguished from retained effects at a boundary.
- *Closed set*: a set of writes that holds every write any of them had read.
  The ordinary applied set is closed. After exclusions, closure refers to
  retained inputs; the retirement counterexample in B17 limits that claim.
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
    write that set any of the constraint's columns in it ([§8.5](../coven.md#85-keys-and-uniqueness));
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
B12–B14). B17 derives causal application from receive steps; its checked
retirement counterexamples leave the rule for dependent writes undecided.
Applying those results to stored objects assumes correct authentication
and sealing; it proves neither confidentiality nor signature or cipher security.
Atomic SQLite installation and §2's provider behavior are assumed, not proved.
Delivery and the other §3 guarantees belong to the [separate models and their
limits](../coven.md#3-guarantees), not this convergence theorem.

Lean checks the theorems named in this appendix. `CovenMerge/Axioms.lean`
prints their axioms; only Lean's `propext`, `Classical.choice` and
`Quot.sound` are allowed. `scripts/check.sh` rebuilds the package, audits
these axioms, and runs the Boolean and schema differentials and the fixed
Rust/Lean counterexamples in B16. Strict comparisons that find a discrepancy stay
out of the check script.

Values are identified by their setters and cells. A column can be its old
name, so a frozen record does not require that column in the new schema.
`freeze_preserves_values` also proves preservation under any fixed mapping
from these identities to scalar values. The model does not execute SQL,
compare schemas, rename columns, convert write payloads, or encode snapshots.
It has no live parent links in loss records; clearing serialized parent maps
is outside it. Frozen records are held separately from the merge state and
are never supplied to current removal rules.

The rules take references, CHECK results and claims as inputs. Computing
those inputs from SQL, reference null substitution, and local insert-to-update
normalization remain outside the general proofs. The examples compute the
inputs they use. Same-key uniqueness assumes the ordered audience claims
specified by §14.2; the proof does not establish those claims from SQL.

Store-log replay supplies the kept entry, its snapshot, and which deletions
are final. Selecting these is outside this package. Schema eligibility and
conversion verdicts are also inputs:
`Boundary.lean` applies them, but does not prove the version/conversion
algorithm or exclusion of a write that read an excluded input.

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
model a partially installed database. Rust's merge functions use Boolean
circle causes. The Boolean differential passes; the entry-aware differential
exposes the missing deletion-entry identity in B16. The reset differential
calls the database snapshot reload and exposes a separate eligibility
discrepancy there. The schema-boundary differential passes on its generated
histories; version/conversion verdicts are supplied as described below.
The migration differential exposes retirement caused solely by a non-final
circle deletion; its comparison stops at row selection.
The Boolean inputs and existing merge functions also remain available to
the separate store-log/data package.

B17 models a retained receive queue and checks Rust's causal prerequisite
decision separately. It derives causal order rather than assuming it at the
receive boundary. Retirement can remove the generation witnesses needed by
merge; that case has checked counterexamples, not an unconditional theorem.

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

The retirement histories in B17 expose a missing rule for writes that depend
on a write excluded by its author's retirement deadline.
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

Seed 0 fails. Its history reduces to one insert of a key-only row in a
circle, followed by one kept entry deleting that circle. The row stays at
generation 1, its insertion record remains, and both implementations hide it.
Rust returns `Rule::DeletedCircle`, with no entry. Lean returns
`Rule.deletedCircleEntry 2`. There are no foreign keys, CHECK failures,
competing rows or non-key values in the reduced history.

`entry_differential_counterexample` checks the same discrepancy with entry 1
and verifies that removing either the insert or the deletion removes the
difference. Lean's `EntryCounterexample.entry_identity_gap` checks both model
outputs; `writes_valid`, `no_row_no_gap` and `no_entry_no_gap` check the
history's assumptions and those two reductions. This is a discrepancy with
Rust's result type, not a counterexample to the current spec or the convergence
proof. [§14.7](../coven.md#147-deleting-a-circle) requires the loss record to
name the responsible entry; its identity is part of the cause.

The Boolean differential passes. `scripts/check.sh` runs it and checks the
fixed counterexample. The strict entry-aware comparison fails on this history
and is not in `scripts/check.sh`; it joins the check when Rust names the
deleting entry in its loss rule, and the fixed counterexample then fails.

From the repository root, after building `mergeRunner`:

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-merge --all-features entry_differential -- --ignored --nocapture
```

The reset mode calls `reloadReset` on the selected snapshot inputs and incoming
writes. `reset_differential_histories` calls the real database
`load_snapshots`, which installs the snapshot and replays writes through
`snapshot_writes::apply`, `download::accepted` and `WriteBoundary::excludes`.
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

Seed 0 reduces to an empty reset snapshot and one key-only insert that did not
read the reset. Rust applies it at generation 1; Lean leaves generation 0 and
no cells. Neither produces a loss. `reset_differential_counterexample` fixes
this history and checks that removing the write or reset, covering the write,
or making it read the reset removes the difference. Lean's
`ResetCounterexample` checks the specified results and write validity.

Lean follows [§19.3](../coven.md#193-resetting-a-store): “A write outside the
snapshot that had not read that entry is ignored for the reset audience, even
if it read every write in the snapshot.” Rust's reset predicate instead asks
whether the write read the snapshot's write frontier, and whether it read a
write outside that frontier. An empty frontier passes that test. No sentence
in the current spec licenses that substitution for `store_log_read`.

`scripts/check.sh` checks the fixed reset counterexample; the strict reset
comparison remains ignored and outside the script. Run it explicitly with:

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-database reset_differential_histories -- --ignored --nocapture
```

The schema mode calls `boundaryDevice`, including `receivedLoss` and the
selected snapshot's frozen records. `schema_differential_histories` also
calls `load_snapshots`, with a schema-raise boundary and no reset. It compares
the same merge records and frozen whole-row records: row, generation, column
and setter identities, breaking version and excluded-write identity. Values
have the model's setter-and-cell meaning; scalar conversion is not tested.

All 128 seeds pass, each in three causal arrival orders at both a full history
and a random prefix (768 comparisons). The histories include covered ordinary
writes, covered frozen losses, uncovered old-version writes, admitted
new-version writes, explicitly lost writes, deletes without old values, and
migration log records with no row changes. Frozen rows also carry an old
column absent from the current SQL schema. Covered excluded positions do not
become snapshot merge inputs. The reducer preserves these distinctions while
removing writes, changes, values, coverage and the schema raise; it can also
remove an explicit lost mark.

The reading is [§17.1](../coven.md#171-host-application): “S already determines
the effects of writes it covers” and “An old-version write outside S is
excluded: keep its values as frozen losses, and pass its position without
applying its changes.” The adapter supplies the eligibility verdict from the
write's version or explicit disposition, and tests the boundary's use of it.
Conversion callbacks, deriving exclusions from excluded inputs, and competing
store-log selection remain outside this comparison. Schema tests use no reset,
so the known reset discrepancy cannot stop this family first.

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

The comparison observes which rows the migration retires, by their changed
generations. It does not equate Rust's erased generation records with the
model's ordinary generation deletes or compare migration loss encoding.
Those are different outputs from the row-selection function tested here.
The reducer removes writes, optional values, references and the deletion,
retaining valid causal histories and schema-required insert cells.

Seed 0 reduces to one insert in a circle, its non-final deletion, and the
breaking migration. Only the key, audience and null reference cell remain;
there are no independent CHECK failures, references or competing claims.
Rust retires the row; Lean leaves it out of the migration. The fixed
`migration_differential_counterexample` checks those results and verifies
agreement with the insert removed, the deletion removed, or the deletion
made final. `MigrationCounterexample` checks the corresponding Lean results.

Lean follows [§17.1](../coven.md#171-host-application): “A non-final circle
deletion cannot decide that a migration loses a row” and “Judge independent
causes with non-final deleted-circle causes absent; a child hidden solely
through such a parent is left out too.” Rust selects every active rule loss,
matching the hidden-row selection in “A breaking migration deletes rows
already hidden by removal rules through the ordinary generation path,” but
without that section's non-final exception. Its selection has no finality
input and does not rerun the three passes. No sentence in the current spec
permits dropping this exception.

`scripts/check.sh` checks the fixed migration counterexample. The strict
comparison stops at this first discrepancy and remains outside the script:

```sh
COVEN_MERGE_LEAN="$PWD/spec/proofs/merge/.lake/build/bin/mergeRunner" \
  cargo test -p coven-database migration_differential_histories -- --ignored --nocapture
```

### B17 Receive order, progress, and the retirement gap

`Receive.lean` models separate downloaded and consumed lists. A download
commits only when every smaller positive number in its device's log has a
retained or snapshot-covered write. Different logs interleave arbitrarily;
prefetched reads become visible to this queue when their contiguous prefix
commits. An application attempt requires the write's retained bytes, an
unconsumed position, the other prerequisite checks, and every write in its
finite recorded past to be consumed. A failed attempt changes nothing.
Misses, read failures, and unfinished reads cannot advance download progress.

`passed_causal` derives `CausalFrom` for every receive prefix.
`received_causalOrder` gives `CausalOrder` when no snapshot covers positions.
`ordinary_receive_converges` feeds these results directly to `merge_converges`:
its receive side has no causal-order hypothesis. The authored metadata must
still satisfy B3's `Valid`, and both devices must have consumed the same set.
`positionsReady_correct` in `ReceivePositions.lean` proves
that testing each recorded frontier, including the preceding own write, is
equivalent to testing every positive position that those frontiers cover.

The queue distinguishes passing a position from applying its values.
`ReceiveMerge.lean` uses the existing snapshot, reset, and schema definitions.
It also checks every supplied kept retirement against the write's author,
recorded entry reads, and first complete publication time. Exactly thirty
storage days is allowed; one second later is excluded (`window_exact`). The
policy requires snapshot inputs to satisfy this window too (`snapshotWithinWindow`);
`retired_no_effects` proves an excluded write supplies neither changes nor input
edges. The other-checks gate must refuse a write made after reading its own
removal, rather than consume it merely because it fails the retirement test.

`applied_causal` filters consumed positions into a causal sequence of retained
effects; `applied_causalOrder` gives the form with no initial snapshot inputs.
`passed_not_applied`, `receive_reset_no_loss`, and `receive_schema_loss` prove
that ignored positions can release later writes without adding effects, that
reset-ignored writes create no loss, and that schema exclusions keep the
written values in frozen losses. The schema verdict is supplied, including
its inherited exclusions; the queue does not implement migration conversion.

`receive_isSpec`, `receive_converges`, `receive_result_converges`, and
`receive_value_accounted` connect these receive sequences to B6 and B14.
They cover generations, cells, merge losses, the app view, removed-row losses,
snapshot-frozen losses, and received schema losses. They require valid authored
writes, equal effective sets (equal consumed sets for the complete loss view),
the same kept boundary and retirement facts, a correct closed snapshot, and
`SnapshotGenerations`: every retained change's generation has a retained
witness. No receive-side `CausalOrder` or `CausalFrom` is assumed.

For progress, `complete_past_eventually_passed` proves that a retained write
whose finite past is eventually consumed will itself be consumed.
`arriving_past_eventually_passed` derives consumption from arrival by induction
on authored timestamps. `receive_eventually_applied` concludes that every
eligible uncovered write in that history eventually appears in the applied
sequence. A consumed reset, schema, or retirement exclusion is not claimed
to be applied. These results require:

- every dependency in the chosen causally closed history eventually arrives
  or is covered by the installed snapshot;
- the scheduler eventually retries each retained, unconsumed write;
- other prerequisites eventually stay satisfied, including store-log reads,
  keys, supported schema, authorization, and successful validation;
- the boundary and retirement decisions remain fixed during the run.

Arrival alone cannot imply progress. `ReceiveExamples.lean` checks a run
that downloads one write with empty past and then idles forever. It also
checks holding an effect that arrives before its cause, reset consumption,
schema consumption, and the exact retirement deadline. Replay changing a
decision must rebuild atomically and begin another run; this development
does not prove termination of an indefinitely changing sequence of boundaries.

**Checked retirement counterexamples.** Two writes suffice to invalidate the
retained-generation assumption. Device A creates a row at generation zero.
Its removal was stored at time zero. The creation was authored before that
entry, but first lands at thirty days plus one second while replay drops the
retirement. Active device B reads the creation in that view and authors an
update at generation one. Replay then keeps the retirement again, excluding
A's creation. §10 explicitly permits dropped retirements to return before
finality; sending freshness therefore does not rule out this data history.
The store-log histories producing those verdicts are inputs to this model.
Both writes have valid authored metadata, but §10 gives no inherited verdict
for B's update, whose generation now has no retained witness.
`retirement_removes_generation_witness` checks
this history; the adjacent `example` shows the effect's consumed cause is
absent from the applied sequence.

The same two writes already violate value retention under the pass-and-merge
reading. `retirement_value_unaccounted` proves that B's admitted update is
neither displayed nor recorded as lost, and no replacement had read it.
This holds for every choice of app removal rules. Its adjacent `example`
refutes `ValueAccounted` directly; no concurrent delete is needed.

Add an active device C's concurrent delete at generation one. All devices
read the creation before authoring; neither update nor delete reads the
other. Two legal receive runs consume all three writes. If the receiver
passes the excluded creation and runs the ordinary merge on the other two,
update-then-delete has no loss for B's value; delete-then-update records it
against C's delete. `retirement_loss_disagrees` and `receive_runs_disagree`
check the different loss records and the legal download/attempt sequences.
This is a counterexample to unconditional convergence and value retention
under that reading, not a counterexample to the conditional merge theorem.

Requiring the discarded creation's effects to be applied instead leaves B's
update waiting even after all objects arrive; another `example` checks its
unsatisfied readiness test. Refusing the dependent write would also require
an explicit verdict and would fall outside the progress theorem's successful
validation hypothesis. Reset explicitly discards old dependencies, and schema
exclusion explicitly propagates through old inputs; §10 specifies neither
rule for this retirement case. No choice between them is asserted as a proof.

**Rust comparison.** `ReceiveRunner.lean` calls the proved frontier readiness
function. The ignored `lean_differential_receive` test calls the actual
`coven-database::download::prerequisite` across three logs, varying the write's
number, other-device frontiers, and consumed prefixes. Store-log and schema
gates are satisfied, and actual retained metadata is checked by Rust. This
compares the hold-until-ready decision, including implicit own-write order;
it does not claim Rust implements the modeled reset, schema, retirement,
discovery, or fair scheduling rules. `scripts/check.sh` builds the runner and
runs this comparison alongside the existing differentials.
