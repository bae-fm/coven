# Merge

Two devices edit the same store while apart; eventually both changesets
apply everywhere, and every device must land on the same rows. This page is
the semantics of that landing: the clock that orders edits, the column-level
merge, and what wins when edits truly collide.

How changesets travel between devices is the [Sync](/docs/sync-model) page;
this one starts where a changeset is already in hand. Examples: Alice and Bob
share the todos store.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

## The clock

Wall clocks cannot order edits: on a laptop a few minutes behind a phone, a
correction would sort as older than the edit it corrects, and lose to it. The
ordering has to survive clocks that drift, sit offline for weeks, or lie, and
it must preserve one guarantee above all: if you pull my edit and then change
it, your change wins. A hybrid logical clock provides exactly that.

`_updated_at` is a hybrid logical clock stamp, not wall-clock time. The host must
treat it as opaque: bind the string coven hands it into the row and never parse
or compare it as a date. Its format, internal to coven, is
`{millis:013}-{counter:04}-{device_id}`, for example
`1735689600000-0000-alice`. The three parts make the string sort
lexicographically in causal order: a fixed-width millisecond field, then a
counter that breaks same-millisecond ties on one device, then the device id that
breaks ties across devices.

The clock is an [`Hlc`](rustdoc:struct:coven::sync::hlc::Hlc).
[`Hlc::now`](rustdoc:method:coven::sync::hlc::Hlc::now) mints the next stamp: if
wall-clock millis moved forward it adopts them and resets the counter, otherwise
it bumps the counter, so each stamp is strictly greater than the last. The host
never calls this directly. It calls `sql.stamp()` inside `handle.sql` or
`handle.write`, binding the result into every synced-row write. The SQL context
and the sync layer share one `Arc<Hlc>`.

The handle open path seeds that clock before it returns, so every stamp minted
through the handle is already past every value on disk. The floor is
`max(persisted high-water mark, max(_updated_at) scanned across every synced
table)`, so a restart cannot mint a stamp behind a value already written. The
on-disk scan is the authoritative source: the high-water mark is flushed only at
cycle end and lags any local row stamp minted between cycles.

### Advancing past pulled rows

As each changeset applies, the cycle takes the greatest `_updated_at` among its
applied rows and calls
[`advance_past`](rustdoc:method:coven::sync::hlc::Hlc::advance_past), so an
edit made between two applies already sorts after the rows the first apply
landed. The next local stamp then sorts strictly after everything pulled so
far: pull, then edit, and the edit wins.

The advance is bounded the same way arbitration is (below): a stamp the
arbiter refused as grossly future never ratchets the clock either, because
only applied rows feed the advance.

Concretely: Alice creates a todo at her 12:00:00, stamped `...-alice`. Bob
pulls it; his clock advances past Alice's stamp. Bob edits the same todo five
seconds later. Even if Bob's wall clock were behind Alice's, his stamp is
seeded past hers, so it is lexicographically greater. His changeset reaches
Alice, her pull applies it, and his edit wins. Both devices converge on Bob's
version: pull-then-edit wins, whatever the wall clocks say.

## The merge

Two devices edit while apart; both changesets eventually apply everywhere.
Merge runs in two stages inside apply.

**Stage one: column-level three-way premerge.** An UPDATE changeset carries,
per column it changed, the value it moved *from* (the base) and the value it
moved *to*. When an incoming update loses row arbitration, the premerge
rescues its column edits: any column the update moved away from a base value
the local row still holds is folded into the local row. The local device never
touched that column, so the incoming edit to it survives. When the incoming
update *wins*, it only writes the columns it changed in the first place.
Either way, concurrent edits to different columns of one row both land.

<svg class="flow" viewBox="0 0 660 190" role="img" aria-label="Base row; phone edits title, laptop edits body; the merged row holds both edits">
<text class="sub" x="330" y="20" text-anchor="middle">base row</text>
<rect class="chip" x="205" y="28" width="125" height="28" rx="7"/>
<text class="lbl s11" x="267" y="46" text-anchor="middle">title: “Milk”</text>
<rect class="chip" x="330" y="28" width="125" height="28" rx="7"/>
<text class="lbl s11" x="392" y="46" text-anchor="middle">body: “2%”</text>
<line class="arrd" x1="240" y1="62" x2="140" y2="92" marker-end="url(#fam)"/>
<line class="arrd" x1="420" y1="62" x2="520" y2="92" marker-end="url(#fam)"/>
<text class="sub" x="120" y="84" text-anchor="middle">phone edits title</text>
<text class="sub" x="540" y="84" text-anchor="middle">laptop edits body</text>
<rect class="chipa" x="35" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="97" y="116" text-anchor="middle">title: “Milk run”</text>
<rect class="chip" x="160" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="222" y="116" text-anchor="middle">body: “2%”</text>
<rect class="chip" x="375" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="437" y="116" text-anchor="middle">title: “Milk”</text>
<rect class="chipa" x="500" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="562" y="116" text-anchor="middle">body: “oat, 2%”</text>
<line class="arr" x1="160" y1="132" x2="270" y2="158" marker-end="url(#fa)"/>
<line class="arr" x1="500" y1="132" x2="390" y2="158" marker-end="url(#fa)"/>
<text class="sub" x="330" y="148" text-anchor="middle">merge</text>
<rect class="chipa" x="205" y="160" width="125" height="28" rx="7"/>
<text class="lbl s11" x="267" y="178" text-anchor="middle">title: “Milk run”</text>
<rect class="chipa" x="330" y="160" width="125" height="28" rx="7"/>
<text class="lbl s11" x="392" y="178" text-anchor="middle">body: “oat, 2%”</text>
</svg>

**Stage two: row arbitration.** For every collision the premerge did not fold
in, [`arbitrate_row_conflict`](rustdoc:fn:coven::sync::conflict::arbitrate_row_conflict)
compares the two `_updated_at` stamps and the later writer wins. Concurrent
edits to the *same* column therefore resolve to the later stamp. The
`_updated_at` column index is read from `PRAGMA table_info` at apply time, so
adding columns to the end of a table stays safe.

Two special cases:

- **Deletes are remove-wins.** A hard delete carries only the row's pre-delete
  stamp and cannot be reconstructed from a later partial update, so an
  incoming delete always wins, and an incoming update targeting a locally
  deleted row is dropped. The row stays gone.
- **Grossly-future stamps are refused.** A member is trusted, so arbitration is
  robustness, not a security boundary; still, a buggy client or broken clock
  could stamp a row far in the future and win every conflict forever. The
  receiver bounds an incoming stamp to its own wall clock plus an offline
  allowance
  ([`MAX_FUTURE_SKEW_MS`](rustdoc:const:coven::sync::hlc::MAX_FUTURE_SKEW_MS),
  30 days) and refuses to let a grossly-future stamp win or ratchet its clock.

### Constraints and foreign keys

A child row can arrive in a changeset whose parent is in a different device's
changeset, not yet applied. The child's insert violates a foreign key and is
dropped on the first pass. Pull collects every such changeset and retries each
once after the first pass over all devices completes, by which point the parent
rows exist. If a changeset still violates a foreign key after the retry, it is
logged and skipped.

Non-foreign-key constraint conflicts (a uniqueness violation, a CHECK failure)
are different: retrying cannot make them valid, so the conflicting rows are
omitted, the affected tables are surfaced in
`ApplyResult::constraint_conflict_tables`, and the changeset is not retried.

