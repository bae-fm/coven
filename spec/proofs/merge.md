## Appendix B. Proof of convergence

- This appendix proves the convergence guarantee of [§3](../coven.md#3-guarantees) for the merge of [§8](../coven.md#8-merge)
  and the audiences of [§14](../coven.md#14-audiences).
- The proof is checked by machine: a Lean 4 development in
  `spec/proofs/merge/`, which has no unproven step and uses no axiom beyond
  Lean's own.
- Each claim names the Lean theorem that checks it; [B11](#b11-what-lean-checks-and-what-is-prose) lists what is argued
  only here.

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
  every write it had read. Every device applies writes in a causal order.
- *Closed set*: a set of writes that holds every write any of them had read.
  The writes a device has applied form a closed set.
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
  - whether the row is in a circle the store log has deleted ([§14.7](../coven.md#147-deleting-a-circle));
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
- In Lean: `Inputs`, `fires`, `rivalBefore`, `removal`, `view`, in
  `Removal.lean`.

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
  - [§14.7](../coven.md#147-deleting-a-circle): Ben deletes the circle "Gifts" and its notes 7 and 8, while Ana
    adds note 9 to it. Notes 7 and 8 are deleted, and note 9 is taken out
    with its title in `_coven_lost`, naming the deleted circle. Lean:
    `example_14_7`.

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
- [§14.7](../coven.md#147-deleting-a-circle): note 9, added to a deleted circle, is taken out. Lean:
  `example_14_7`.

### B11 What Lean checks, and what is prose

- Lean checks every theorem named above, with no unproven step. The axioms
  they use are only Lean's own: `propext`, `Classical.choice`, `Quot.sound`.
  `CovenMerge/Axioms.lean` prints them.
- What Lean models abstractly:
  - a write's values: a cell's value is the write that set it;
  - the removal rules' inputs: any function of the merged state and the
    store log, in the shape of [B7](#b7-the-removal-rules). The examples compute them from the
    merged state Lean builds.
- Argued here only:
  - that coven computes the rules' inputs from the merged state as [B7](#b7-the-removal-rules) says:
    - presence from generations;
    - stale references by comparing generations;
    - null for a set null reference whose parent's generation was deleted;
    - CHECK on merged values;
    - claim stamps from the cells' writes;
  - that two devices compute the same inputs for a row when the merged
    states of the rows those inputs read agree, which [B9](#b9-audiences)'s locality theorem
    then uses;
  - local triggers: they converge when they compute a function of the
    current rows, since every change coven makes is ordinary SQL ([§8.7](../coven.md#87-triggers));
  - files: where a file is ([§16.1](../coven.md#161-kinds-and-where-files-are)) is an ordinary cell, which the merged state
    covers; uploading and caching aren't modelled;
  - schema changes ([§17](../coven.md#17-schema-changes)) and resets ([§19.3](../coven.md#193-resetting-a-store)), which this model doesn't
    include. The separate [store-log/data coupling](storelog-data.md) supplies
    replay-dependent removal inputs and models snapshot replacement, including
    a counterexample for losing migration values.
