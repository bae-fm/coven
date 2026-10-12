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
Conversion callbacks, per-change data dependencies, and competing
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

<a id="b17-receive-order-progress-and-the-retirement-gap"></a>

### B17 Receive order, progress, and dropping by data

This is a proposal implementing the user principle: building on a dropped
write does not make that write count. The normative spec files are unchanged;
the edits required to align them are listed below and in [queue.md](queue.md).
The reader-rescue model has been removed.

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
`post_read_refused` and `passed_before_removal` retain the hard gate on a write
made after reading its own retirement. Such a write is refused before its
position passes; a late pre-retirement write is consumed without changes.

`retirement_verdict_final` uses the final kept retiring entry and the stable
preceding store-log prefix supplied by §9, including storage-time ties.
Retirements after that prefix may appear or disappear. With the post-read send
gate in both views, those changes cannot reverse either window verdict: an
admitted write already meets the final entry's earlier deadline. No premise concerns future
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

**Spec edit checklist.** The normative files have not been edited.

- §10: retain the inclusive per-write retirement window and hard post-read
  refusal. Specify a consumed base drop, per-change dependency losses and
  atomic recomputation when a non-final retiring entry changes. State that
  the retirement verdict is final with its deciding entry; no reader rescues it.
- §3: replace the undecided retirement-dependent-value wording with original
  change accounting, distinguishing intentionally dropped bases from dependent
  values kept as losses.
- §17.1, E13 and D5: replace inherited read exclusion and whole queued-tail
  loss with the shared data rule; the queue write-up gives the conversion cases.
- D5/D7 and the loss API: retain authentic generation-source and old-setter
  identities per row change, preserve original changes across conversion and
  snapshots, and name dependency losses even for empty deletes. Decide the
  concrete encoding; the Lean evidence is not an assertion that current bytes
  already provide it. Keep consumed frontiers separate from applied data.

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
prerequisite function across three logs and consumed prefixes. The Boolean,
reset and schema runners retain their existing paths and comparison scope.
No Rust implementation or differential for the data-loss proposal is added.

### B18 Owner tables and row authenticity

This is a proposal, not a change to the normative spec or Rust. `Ownership`,
`OwnerProtection` and `OwnerEvaluation` check the owner filter, state-based
protection tests and an executable, terminating rejection procedure.
`OwnerDeleteRule` implements the member-context reference rule;
`OwnerDeleteRetention` proves that trial withholding preserves retained setters
and current incarnations. `OwnerDeleteCost` bounds traversal, context selection
and rejection rounds.
`OwnerDeleteContexts` retains the exhaustive rule as a comparison model.
`OwnerExamples`, `OwnerFilterExamples`, `OwnerDeleteExamples`,
`OwnerReinsertExamples`, `OwnerDeleteRuleExamples`,
`OwnerDeleteGenerationExample` and `OwnerDeleteDependents` check races,
reference updates, audience transfers and cascades. The signed-root
argument is in
[the storage model](storage.md#signed-roots-and-row-authorship).
SQL extraction and store-log authority remain outside the model.

**Final owner protection still fails through set-null and uniqueness.** The
checked three-row, four-write history below has no foreign delete to withhold,
so preserving dependent reinsertions cannot change its result. Ben's deletion
is accepted and Ana's unrelated row loses uniqueness. The general final
visibility theorem is therefore false for this rule. No different deletion
rule is installed. The two-row reinsertion history is protected; convergence,
B6, B14, termination and the polynomial graph/rejection bounds remain proved.

**Owner changes and value accounting are proved.** `Policy` makes ownership
an opt-in table declaration and derives a row's owner from its creating
write's member. The creator is retained evidence, independent of generation,
current setters, removed rows, loss dismissal and audience copies. `owner_fixed`
covers copies sharing that creation identity; `creator_allowed`, `opt_out`
and `non_owner_denied` check the policy. Selecting and authenticating the
original creation is an admission requirement, not something inferred from
an arbitrary snapshot creator field. `accepted_owner_matches_filter` connects
the storage proof of a signed creation to the owner this filter enforces,
when both use the authenticated write authors. The reading for competing
original creations is B4's smallest timestamp at generation one, selected
before owner filtering. `selected_creation` proves this record identifies an
actual generation-zero insert; `creation_selection_converges` proves agreement
in every causal order. `later_change_preserves_creation` proves that later
generation updates, deletes and reinsertions cannot change it. Audience
transfers carry that original identity rather than selecting a new creator.
A newly discovered competing original creation can change which creation is
shown for a shared key; that is rejection of the losing creation, not changing
its author. The policy receives the selected authenticated identity; missing
creation evidence is a validation failure, never an arbitrary default.
`first_arrival_owner_diverges` checks two members independently creating the
same shared key: preserving each device's first owner gives different owners.
`immutable_first_arrivals_incompatible` proves that accepting both solo owners,
never revising either during conflict resolution, and convergence cannot all
hold. One creation agrees (`one_creation_agrees`). This rejects first receipt
as the identity rule, not owner tables with an authenticated creation identity.

`base` rejects each unauthorized change separately. It uses the exact
`DataDependency.retained` and `dependsOnDropped` definitions of B17, with the
original generation and replaced-setter evidence. It does not use a new
notion of read dependence. `non_owner_no_effect` proves that change absent
from the effective merge. `non_owner_recorded`, `guard_recorded`,
`dependent_recorded` and `excluded_recorded` preserve its original identity,
generation and values, including empty deletes. `Loss` extends the existing
`LossRecord` envelope with a typed owner/protection/dependency cause and the
excluded write; these records are held separately from ordinary active losses.
A later replacement cannot acknowledge them away.

`converges`, `loss_converges` and `result_converges` prove equality of the core
merge, displayed rows, ordinary rule/cell losses and owner/dependency losses
for every causal order of the same writes and verdicts. `value_accounted`
extends B14 to every original setter, including directly rejected setters:
it has an ordinary B14 outcome or its original value in a change loss.
Validity after filtering is derived by `DataDependency.effective_valid`,
not assumed. The examples check two causal orders: Ben edits Ana's owned row
and an unowned row in one write; Ana then builds on Ben's owned-row setter
while independently editing the unowned row. The unauthorized change and its
dependent change are recorded separately, and the unowned changes survive.

**Moves include merged descendants and replayed membership.** `carried` takes
the root's transitive closure through the merged declared audience-parent
links. `carried_root` and `carried_child` prove inclusion at arbitrary depth.
`carried_order_independent` and `move_order_independent` allow any enumeration
of the same merged row set, not only a canonical row order.
`moveDenied` examines every present owned row in that closure, even if an
ordinary removal rule hides it. The destination's read permission comes from
the replayed membership state, not the mover's authored view.
`unread_owner_blocks_move` and `move_safe` prove the exact veto and its
converse; `move_reason_complete` supplies its protected row and owner, and
`move_recorded` puts that cause in the per-change loss. `concurrent_child_blocks`
checks an unseen child insertion;
`concurrent_membership_blocks` checks a concurrent removal of its owner's
access. A parent move and a root audience edit use the same closure.

These results require the complete relevant merged graph and the same
membership result. They do not prove that a reader restricted to one audience
can discover unseen descendants elsewhere. Extracting links, pairing the old
and new halves of a move, and enumerating generated descendant copies are
outside the model. Carrying an authenticated descendant is read as transport
of its original creator/setters, not permission for the parent's author to
forge new child values. The model does not silently exempt an ordinary
non-owner child edit from the owner filter.

**The member-context delete rule is executable and deterministic.** For a
delete by M, `incarnationContext` starts with the full retained round's row
generations. It keeps all non-deletes, M's own deletes and every foreign
delete M already read. Among unread foreign deletes, it withholds a removal
only when that row has no later retained incarnation: a delete at generation
g remains as generation evidence if the full row generation exceeds g + 1.
That earlier delete cannot remove the later row. The rebuilt context therefore
keeps the later insertion and its updates while withholding current removals.
`incarnation_context_exact` gives the exact selection. Recorded reads use
`M.past` directly; different deleting writes can have different read bases.

This is trial withholding, not an admission rejection. An actually rejected
delete still excludes its data dependents in the final merge. Trials never
resurrect a change already lost to ownership or an earlier rejection round.
`incarnation_context_no_new_change` proves that restriction. The policy and
all reference generations remain authenticated retained evidence.

`OwnerDeleteRetention` proves the distinction for every valid authored history
and causal received set. `delete_source_is_generation` proves a delete can
be a source only for the next insertion's generation; replaced-cell sources
are always inserts or updates. `incarnation_context_retains` follows arbitrary
provenance chains and preserves every retained insert/update and each earlier
delete supporting a later incarnation. `incarnation_context_setters` gives
exactly the same setters at every incarnation. `incarnation_context_current`
proves every present row keeps its generation and every winning cell, including
reference changes. `incarnation_context_generations` proves withholding can
lower a generation by at most one: even after repeated reinsertions and a
last delete, it cannot restore an obsolete earlier incarnation.

`deleteTrial` uses that one rebuilt context and extracts a `ReferenceGraph`.
It rejects exactly when a present row of another owner reaches the deleted
incarnation through retained direct, cascade or audience-parent references.
The test precedes ordinary removals: CHECK, unique and other-audience hiding
do not erase its blockers. Ownership comes from `Policy`.
`references_least` proves the traversal computes the least closed set;
`reference_seed`, `reference_parent` and `reference_path` include paths of any
depth, including unowned intermediate rows. `references_order_independent`
and `reference_veto_order_independent` prove graph enumeration changes neither
reachability nor the veto. `reference_blocker_exact` proves the stated iff;
`passing_has_no_foreign_reference` excludes blockers in that member's context.

Nodes identify row **incarnations**, not logical keys alone. Graph extraction
supplies generation-aware edges, including audience-parent links and any
audience-derived targets, before ordinary removal. Missing evidence cannot
mean an empty graph. SQL extraction and store-log operation grouping remain
outside the model. The retention theorems concern original current setters;
they do not prove that arbitrary SQL-derived claims are unchanged.

The atomic order is:

1. Filter non-owner changes and their data dependents; rebuild from originals.
2. Judge active moves against merged descendants and replayed membership.
   Judge active deletes in their deleting members' contexts, preserving
   generation evidence for later incarnations, before removals.
3. Reject all failing candidates together, retaining each first rejecting
   cause. Rebuild without those changes and their data dependents; repeat
   until no candidate fails.
4. Record the rejected changes and install the final rebuilt core, all three
   ordinary removal passes and all losses together.

`roundState`, `roundTrial` and `protectedResult` execute this order on the
actual merge. `unretained_not_tried` excludes already absent changes from
trials. `settled_admission` and `settled_merge` prove recorded reasons give
exactly the settled merge. `settled_safe` means every surviving retained
candidate passes its **reference test**. The set-null/unique counterexample
below prevents inferring final owner visibility from that result.

**Termination and no remove/restore cycle are proved.** `rounds_bounded`
gives at most D strict rounds for D candidates. `settles_subset` never
re-enables a rejected candidate during one evaluation. `rejection_no_cycle`
proves a strict round cannot have a path back; `settle_idempotent` proves the
settled result stays fixed. `one_row_no_cycle` checks the previous one-row,
one-circle-deletion history: the rejection names the foreign owner and remains
rejected. `rejection_record_exact` and `rejection_reason_witness` retain an
actual first rejecting cause for every rejected candidate.

A changed received set starts a fresh atomic evaluation, as do changed
membership and admission facts. Verdicts can change with those inputs; no
arrival-dependent permanent rejection is stored. Candidates use a canonical
order for stable first-blocker causes. `settle_permutation` additionally
allows permutations when the trial depends only on the active set.

**The graph work is polynomial; end-to-end replay time is outside the bound.**
`closureWork` traces the actual `pick`/`kill` choices of the existing `closeN`
traversal. It charges every scanned row, every parent lookup and the chain
of row comparisons used by membership. With N graph rows and at most E
parents per row, `reference_work_polynomial` proves a bound of
`N × (N × (1 + (1 + E) × (N + 1)) + 1)` charged operations. This is
`O(N³(E + 1))`, or `O(N⁴)` when E is at most N. The final blocker scan adds
`O(N²)` under the same constant-time graph lookup and row-comparison model.
This is a completed traversal, not a timeout with a partial result.

`trial_calls_polynomial` proves at most 2D² charged trial calls for one complete
settling traversal, including its unchanged last round and both occurrences
of `keepRound` in a strict round, without assuming compiler sharing. Each delete
trial uses one context instead of enumerating subsets. It reads the full
round's generations, selects the context and rebuilds that context; there is
no search over histories. `contextChecks` charges a full active-list scan for
every trial call, and `context_checks_polynomial` proves at most 2D³ candidate
checks across the same traversal. Generation and metadata lookups are supplied
operations; replay cost remains outside the bound. Looking up every
candidate's recorded reason separately can repeat that traversal D times;
this is still polynomial in D. The model does not equate D or the number of
retained writes with N: fixed rows can have arbitrarily many historical
changes. `no_row_only_trial_bound` proves that distinct candidates at one row
already exceed any bound depending only on row count: every candidate must
be tested in the first round. Rebuilding history, the recursive data-dependency
filter and SQL extraction are not constant-time graph lookups, and no polynomial-in-rows
bound for the entire `protectedResult` executor is proved. The checked
polynomial claim concerns reference protection on supplied merged graphs.

**Concurrent owner deletion requires a retry.**
`concurrent_owner_delete_requires_retry` has Ben delete his parent while Ana
deletes her child. Ana's deletion is withheld in Ben's test, so Ben records
a loss even though the child is deleted in the final state. Once Ben reads
Ana's deletion, a fresh delete keeps it in its trial context and succeeds,
even if both operations remain candidates. The reading therefore distinguishes
foreign deletes that the deleting write had read from concurrent ones. This
is the conservative concurrency cost; retry is a new authored operation,
not re-enabling the old candidate midway through its evaluation.

`OwnerDeleteExamples` now executes this rule on original writes: two Ben
devices delete the same parent while Ana inserts its child. Both deletions
name Ana's child in their losses and the parent returns
(`rows_restored_and_losses_recorded`); `orders_agree` compares child-first and
child-last arrival. `ReinsertExamples.generations_preserved` checks that
an old delete supporting a reinsertion is permitted and Ana's child at the
new incarnation remains visible. Logical-key-only traversal fails that test
(`row_key_guard_rejects_valid_history`).

**The two-row reinsertion history is protected.**
`DeleteGenerationExample.foreign_child_protected` checks the full five-write
history, with valid writes, authored provenance and causal order:

- Ben creates P; Ana creates C with no parent reference.
- Ana deletes C and reinserts it at generation three, referring to P.
- Ben concurrently deletes P, having read neither Ana's delete nor reinsertion.

`trial_keeps_reinsertion` proves Ben's context keeps C at generation three
with its new setter. `parent_delete_blocked` finds Ana's C; `guard_reason`
records Ben's deletion while permitting Ana's earlier deletion. The final
`protectedResult` restores P at generation one, keeps C's generation-three
setter and shows both rows after all three removal passes. Ben's loss retains
his write, row, generation and `.delete C Ana` cause. Ana's deletion and
reinsertion have no rejected-change loss, and C has no removed-row loss.
`rejecting_delete_excludes_reinsertion` separately checks that actual admission
rejection still drops the dependent insertion; trial withholding no longer
uses that operation for a superseded removal.

**Reference updates, audience transfers and a third-row cascade are checked.**
`DeleteDependents.protected_final` executes each of these histories through
the same trial, rejection rounds, merge and loss recorder:

- Ana changes C's reference to P with an ordinary update, without a
  delete/reinsert. Ben's concurrent deletion loses and C keeps that update.
- Ana reinserts C pointing through an unowned bridge B to P. The bridge does
  not itself veto Ben's deletion; the traversal reaches Ana's C and records
  Ben's deletion. P, B and C survive.
- Ana moves C from the store into a circle, then sets its reference to the
  store's P. Ben's trial withholds C's old-audience removal while keeping its
  destination insertion and later update. The restored store copy would hide
  the circle copy under the ordinary other-audience rule, but the reference
  test still sees the circle copy (`move_trial_before_audience_choice`). Ben's
  delete loses; the final source remains deleted and the destination is shown
  (`move_source_stays_deleted`). Both copies carry the same original owner.

The general retention theorem covers the setters in these cases and arbitrary
longer source chains. `reference_path` covers longer reference and audience
paths on the supplied graph. The move example supplies the two authored halves
with Ana allowed in the destination; it does not establish discovery of an
unreadable audience, generated move pairing, or store-log membership replay.
The existing `move_safe` theorem still requires the complete merged descendant
graph and replayed membership for destination access.

**Final owner protection fails through uniqueness.**
`DeleteRuleExamples.foreign_row_hidden` executes three creations and one
delete through `protectedResult`, with checked `Valid`, authored dependencies
and causal order:

- Ben creates parent P, then creates child C referring to P under set null.
- Ana independently creates row A with no reference to P or C. C and A have
  unique expression `coalesce(parent_id, -1)`, with C's earlier claim winning
  if their expression values agree.
- Ben deletes P concurrently with C's insertion. C's reference becomes null;
  its expression now agrees with A's. C stays visible and A loses uniqueness.

A does not reference P in Ben's context. The graph retains C's authored link
even when C displays null; C belongs to Ben. `trial_permits` and `guards_empty`
prove the revised guard accepts the deletion. The final result hides A and
records its original value in a `.unique` loss. A was visible with the delete
withheld. With no other member's delete in this history, withholding all of
them cannot change the result. The literal Lean `example` checks the hidden
foreign row. `three_roles_needed` checks the absent conflict/no-deletion
reductions and that a foreign child directly referencing P would block.
Three rows and one deletion suffice for this set-null/unique failure.

`context_unchanged` proves that the context keeps every candidate regardless
of the supplied generations. `final_owner_protection_refuted` negates the final
protection assertion using this valid history. The exhaustive
comparison model rejects the unique history (`null_unique_delete_blocked`)
and proves `all_deletes_preserve` through reference and full-removal projections.
Those theorems belong to that comparison model; they do not transfer to the
member-context reference rule and do not replace it.
An unrelated ordinary CHECK can also keep a restored row hidden, so “R
returns” means its delete is excluded in the merge, subject to ordinary rules.

**B6 and B14 hold when withholding preserves incarnations.**
`delete_result_converges` proves equal
complete results for every causal order of the same writes: core, final
view, ordinary losses and recorded ownership/dependency causes.
`delete_value_accounted` applies B14 to `settledGuard`: every original
setter has an ordinary accounting outcome or its original value in a rejected
change record. The remaining counterexample's unique loss is an ordinary accounting
outcome; accounting is weaker than preventing that loss.

`originalCell` in `OwnerCellAccounting.lean` builds the cell leaf from the
original write's row, generation, operation kind, column and scalar.
`setter_has_cell_leaf` covers every original setter.
`authenticated_value_accounted` combines an accepted opening of exactly that
leaf with B14, proving its actual signing author and its value disposition.
It applies to the member-context settled guard as well as other guards.
`accepted_owner_matches_filter` still connects the separate signed creation
marker to the owner filter. The storage proofs establish completeness, hash
minimality and absence of unrequested old scalar openings for these leaves.
Merkle leaves are per cell; owner/dependency rejection remains per row change.
Derived SQL null values and schema conversions are not claimed to be original
signed scalar values. Cryptographic verification, admitted bodies, snapshot
freshness/completeness and SQL correspondence remain the stated boundaries.

**Spec, format and API edit checklist.** No normative files were edited.

- E2: an explicit opt-in owner declaration, with its schema compatibility
  rules. Specify creator identity for shared keys, concurrent creation,
  deletion/reinsertion and cross-audience copies; keep it permanently in
  authenticated retained state.
- E1/E3/E5/E7/E12: commit errors identifying `NotOwner`, an unreadable owner on a
  move, and an owner protected from a delete. Apply the checks atomically to
  direct changes, triggers, cascades and audience operations; name the
  conflicting row/member and roll back the whole write on refusal. Circle
  deletion needs its corresponding typed `SyncError::Rejected` reason.
- E4: read APIs for the creator and each cell's last writer, returning stable
  member and write identities. Distinguish original signed values from
  reference-null substitution and schema conversion. Loss reads need the
  original excluded operation and its owner/move/delete/dependency cause.
  Dismissing a removed owned row obeys the same delete protection; dismissing
  a loss cannot erase the original creation's authentication evidence.
- §§3, 5, 8, 14 and 15: per-change owner rejection, the shared data-dependency
  rule, merged descendant/membership evaluation, atomic recomputation, the
  precise protection/removal order, generation-aware member contexts and
  qualified restoration. Trial withholding keeps a later incarnation and its
  original setters; an actual rejection still drops data dependents. The
  reference-only proposal does not establish final owner visibility through
  set-null and uniqueness; that policy remains unresolved. Specify each
  deleting write's recorded read base and circle-entry feedback.
- D5/D9/D11 and §11.1: a signature binding the write identity and header,
  including one domain-separated Merkle root per audience part, keyed by that
  part's audience key (32 bytes per part). Specify canonical cell/creation/delete
  leaf order, tree shape, empty writes and operation/column encoding. Readers
  verify the header without opening unreadable parts and verify each readable part against its
  own root; envelope authentication must remain intact.
- D7/§15: contributing-write signatures, original creation and setter openings,
  positions/tree shape and maximal absent-subtree hashes. Preserve provenance,
  historical schemas and proof bytes across retention and conversion. A signed
  cell leaf can be opened without overwritten sibling values; separate
  creation markers retain ownership evidence. Verify all required openings
  before installing the snapshot. The storage write-up states per-cell and
  shared-multiproof overhead, including the metadata excluded from byte counts.
- D11: include authenticated creator identity and the added loss causes in
  agreement, with a defined treatment of retained proof/provenance records.
- §2: members cannot forge another member's authenticated creation or cell
  values in owner tables under the stated signature/hash assumptions and
  acceptance checks. This does not prevent storage deletion, resolve stolen
  member keys, authenticate snapshot completeness, or prove that a publisher
  included the latest state. Those limits must remain explicit.
