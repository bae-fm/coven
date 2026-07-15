# Local data

A coven store is one SQLite database on the device, plus the files its rows
carry. The host opens it through one handle, declares which tables participate
in sync, and runs ordinary SQL. This page is about that declaration: what a
synced table looks like, what stays local, and how a *gate* keeps chosen rows
on one device even inside a synced table.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

## What a synced table looks like

For a row to travel, every device needs two things it can rely on: a stable
way to say *which* row this is, and a value that orders concurrent edits to
it. Both ride on typed columns, so a third convention pins the types
themselves. These are the conventions a synced table carries, checked at open:

- declared `STRICT`: SQLite refuses an insert or update whose value doesn't
  match a column's declared type, so a synced table can never hold a value off
  the type every peer's apply and coven's own conflict-resolution code expect
- a `TEXT` primary key named `id`, at column position 0: together with the table
  name, this is the logical row identity on every device
- an `_updated_at TEXT NOT NULL` column, stamped through
  [`SqlContext::stamp`](rustdoc:method:coven::SqlContext::stamp): the register
  concurrent edits are ordered by

Everything else about the schema is yours. Tables you do *not* pass to
[`synced_tables`](rustdoc:method:coven::CovenBuilder::synced_tables) never
leave the device: that is also the mechanism for per-device state (window
positions, per-device pin bookkeeping, local paths). Put it in a table you
don't declare.

### Row identity

Every `SyncedTable` declares what its ids mean. Use
`RowIdentity::IndependentUuid` for rows created independently on offline
devices; every id must be a canonical lowercase hyphenated UUIDv4 or UUIDv7.
Use `RowIdentity::SharedKey` only when an application key intentionally names
the same logical row everywhere, such as `settings(id = 'preferences')`. Equal
shared keys merge as one row under the normal `_updated_at` policy.

Changing a primary key removes the old identity and inserts the new identity in
one transaction; SQLite records that the same way as an explicit delete plus
insert. The introduced id must satisfy the table's mode. Even an equal, valid
UUID means one logical row: identity mode prevents predictable key reuse, but
equal `(table, id)` values always denote the same row.

<svg class="flow" viewBox="0 0 660 168" role="img" aria-label="Declared tables sync to the cloud; undeclared tables never leave the device">
<text class="hdr" x="155" y="22" text-anchor="middle">THIS DEVICE</text>
<text class="hdr" x="560" y="22" text-anchor="middle">CLOUD</text>
<rect class="lane" x="10" y="32" width="290" height="124" rx="10"/>
<rect class="lanec" x="470" y="32" width="180" height="124" rx="10"/>
<rect class="chipo" x="30" y="44" width="250" height="26" rx="7"/>
<text class="lbl s11" x="155" y="61" text-anchor="middle">todos · synced</text>
<rect class="chipo" x="30" y="78" width="250" height="26" rx="7"/>
<text class="lbl s11" x="155" y="95" text-anchor="middle">lists · synced</text>
<rect class="chipd" x="30" y="112" width="250" height="26" rx="7"/>
<text class="lbl s11" x="155" y="129" text-anchor="middle">ui_state · not declared</text>
<line class="arr" x1="305" y1="70" x2="462" y2="70" marker-end="url(#fa)"/>
<text class="sub" x="384" y="60" text-anchor="middle">declared tables sync</text>
<line class="arrd" x1="305" y1="125" x2="380" y2="125"/>
<line class="arrd" x1="392" y1="118" x2="380" y2="132"/>
<text class="sub" x="384" y="148" text-anchor="middle">never leaves</text>
<text class="sub" x="560" y="98" text-anchor="middle">changesets · snapshot</text>
</svg>

## Declaring the set

By default, every row of a synced table reaches every device in the store,
and some rows are not meant to: a draft, a private list. If keeping them back
were application logic, every query and every sync path would have to
remember it, and the first one that forgot would leak the row. Instead the
host *gates* the table: privacy becomes a property of the schema, and coven
enforces it everywhere a row can travel. The host declares the gate per table on
the [`SyncedTable`](rustdoc:struct:coven::sync::session::SyncedTable) values
it passes to `Coven::builder(config).write_policy(...).synced_tables(...)`, and coven enforces
it on both paths a row can take to another device: the per-cycle changeset
and the bootstrap snapshot.

Examples: a `workspace` holds `lists`, a `list` holds `todos`, and a list has
a boolean `shared` column. A private list, and the todos under it, should stay
on the device that made it.

[`SyncedTable`](rustdoc:struct:coven::sync::session::SyncedTable) has four
gate forms:

```rust
SyncedTable::new("todos", RowIdentity::IndependentUuid) // no gate of its own
SyncedTable::new("attachments", RowIdentity::IndependentUuid).remote_root()
SyncedTable::new("lists", RowIdentity::IndependentUuid).gated_by("shared")
SyncedTable::new("workspaces", RowIdentity::IndependentUuid).gated_by_descendants()
```

- `new(name, row_identity)` declares the table synced with no gate of its own. With a foreign
  key into a gated root it inherits that gate; without one it syncs
  unconditionally.
- `remote_root()` keeps whole-table row sync and makes blobs on those rows and
  their descendants Remote by construction.
- `gated_by(column)` makes the table a *gated root*: a row syncs only while its
  boolean `column` is true.
- `gated_by_descendants()` marks an *ancestor* that should sync only while a
  gated descendant of it survives.

A table is one of the four, never two at once. Two further properties are
orthogonal to the gate and covered in [Blobs](/docs/blobs): a table may *carry
a blob* (`carries_blob`), and it may be an *asset*, a decoration like a cover
image that rides its subject's gate but never keeps that subject alive.

## One tree, both directions

The gate flows *down* foreign keys; the keep flows *up* them.

<svg class="flow" viewBox="0 0 660 212" role="img" aria-label="A workspace keeps syncing because one shared list survives; a private list and its todos stay local">
<rect class="chip" x="16" y="88" width="150" height="26" rx="7"/>
<text class="lbl s11" x="91" y="105" text-anchor="middle">workspace · Acme</text>
<text class="sub" x="91" y="130" text-anchor="middle">gated_by_descendants</text>
<path class="tree" d="M166 101h24v-48h34M166 101h24v62h34"/>
<rect class="chipa" x="224" y="40" width="186" height="26" rx="7"/>
<text class="lbl s11" x="317" y="57" text-anchor="middle">list · Groceries — shared ✓</text>
<rect class="chipd" x="224" y="150" width="186" height="26" rx="7"/>
<text class="lbl s11" x="317" y="167" text-anchor="middle">list · Journal — shared ✗</text>
<path class="tree" d="M410 53h24v-25h34M410 53h24v25h34"/>
<path class="tree" d="M410 163h24v-25h34M410 163h24v25h34"/>
<rect class="chip" x="468" y="16" width="150" height="24" rx="6"/>
<text class="lbl s11" x="543" y="32" text-anchor="middle">todo · milk</text>
<rect class="chip" x="468" y="66" width="150" height="24" rx="6"/>
<text class="lbl s11" x="543" y="82" text-anchor="middle">todo · bread</text>
<rect class="chipd ghost" x="468" y="126" width="150" height="24" rx="6"/>
<text class="lbl s11 ghost" x="543" y="142" text-anchor="middle">todo · therapy</text>
<rect class="chipd ghost" x="468" y="176" width="150" height="24" rx="6"/>
<text class="lbl s11 ghost" x="543" y="192" text-anchor="middle">todo · gym</text>
<text class="sub" x="317" y="22" text-anchor="middle">keep flows up: one shared list keeps the workspace</text>
<text class="sub" x="317" y="205" text-anchor="middle">gate flows down: a private list takes its todos with it</text>
</svg>

## Gated roots

A gated root carries a boolean column. A row whose column is false stays on the
device that wrote it, and its foreign-key descendants stay with it. With
`lists.gated_by("shared")`, a private list and its todos never leave the device.

The gate flows down foreign keys: a child row syncs iff the row at the top of
its foreign-key chain, the gated root, syncs. `todos` reference `lists`, so a
todo is shared exactly while its list is. The host declares the gate once, on
the root; coven follows the schema's foreign keys to every descendant, so the
children need no declaration of their own.

## Remote roots

A remote root has no gate column. Every row syncs, like
`new(name, row_identity)`, but the row
also anchors blob locality: blobs carried by that row or by descendants are
Remote. Host-provided blobs upload before the row changeset is pushed, and a peer
reads them from the cache or cloud. `make_remote`, `make_local`, and
`cancel_make_remote` reject a remote root because there is no Local state to
enter or leave.

A plain table that carries blobs but is not under a gated root or remote root is
not a blob locality root. Rows still sync, but blob reads fail because coven has
no authoritative Local/Remote answer for the bytes.

## Ancestors

A list belongs to a workspace. A workspace sits above the gate, not below it, so
the root gate never reaches it: a workspace whose every list is private would
still sync its own row and arrive on a peer as an empty workspace, a row
pointing at nothing.

`gated_by_descendants()` removes that orphan. The ancestor syncs only while a
surviving descendant references it. coven infers which descendants count from
the foreign-key graph: every synced table with a foreign key into the ancestor,
except a child that already inherits the ancestor's own gate (a many-to-many
join row, which would otherwise keep its parent alive in a circle). The rule
composes up the chain, so a workspace nested in a parent of its own would sync
only while a surviving workspace, and through it a shared list, kept it alive.

A many-to-many is the case the exception covers. Say todos carry `labels`
through a `todo_labels` join. Mark `labels` with `gated_by_descendants()`: a
label syncs while a shared todo wears it. The `todo_labels` join row inherits the
list's gate downward (it is a descendant of `todos`), so it does not count as a
keep-child of `todos`; it does count for `labels`.

## The keep rule

A private row must not leak through *any* channel, and there are two: the
per-cycle changeset and the bootstrap snapshot. If each path had its own idea
of "private", they would eventually disagree, and the disagreement would be a
leak. So both share one definition of "kept", built as a SQL predicate. For a
gated root it is the column test:

```sql
lists.shared IS NOT NULL AND CAST(lists.shared AS INTEGER) <> 0
```

For a descendant it is an `EXISTS` up the foreign key into its parent's rule:

```sql
EXISTS (SELECT 1 FROM lists
        WHERE lists.id = todos.list_id AND (<lists kept>))
```

For an ancestor it is an `EXISTS` down each inferred child into that child's
rule:

```sql
EXISTS (SELECT 1 FROM lists
        WHERE lists.workspace_id = workspaces.id AND (<lists kept>))
```

Every form bottoms out at some root's column test, so "kept" is one expression
that the changeset filter and the snapshot cleanup evaluate the same way.

## Flipping the gate

The gate is a live switch, not a create-time choice, and both directions
must move complete subtrees: sharing delivers the whole subtree, and
unsharing removes it from peers without destroying the owner's copy.

Setting a root's gate from false to true makes a previously-local subtree
public. Peers never held it, so coven re-emits the whole now-visible connected
component (the root row, its descendants, and the ancestors the new row keeps
alive) as full inserts in that cycle. The subtree lands complete, not as an
update to rows the peer is missing.

The reverse, true to false, is a retract: coven emits deletes for the rows that
leave the shared set so peers remove them, the mirror of the false-to-true
re-emit. The candidate rows are the structural connected component of the roots
that flipped this cycle, minus any row still kept by another root (a sibling that
shares an ancestor is spared; a now-childless ancestor is deleted too). The
flipping device keeps its own rows (now gated-false, local-only): retract writes
only to the outgoing changeset, never deletes locally, and fires once on the flip
cycle. A root that was never shared has nothing on peers to retract, so it emits
nothing.

<svg class="flow" viewBox="0 0 660 240" role="img" aria-label="While shared is on, peers hold the subtree; the flag flips off; one retract removes it from peers while the owner keeps it">
<text class="hdr" x="120" y="22" text-anchor="middle">FLIPPING DEVICE</text>
<text class="hdr" x="540" y="22" text-anchor="middle">PEERS</text>
<rect class="lane" x="10" y="32" width="220" height="176" rx="10"/>
<rect class="lane" x="430" y="32" width="220" height="176" rx="10"/>
<circle class="numc" cx="24" cy="59" r="8"/>
<text class="num" x="24" y="62.5" text-anchor="middle">1</text>
<rect class="chipa" x="40" y="46" width="170" height="26" rx="7"/>
<text class="lbl s11" x="125" y="63" text-anchor="middle">Journal — shared ✓</text>
<rect class="chip" x="450" y="46" width="180" height="26" rx="7"/>
<text class="lbl s11" x="540" y="63" text-anchor="middle">Journal + its todos</text>
<text class="sub" x="330" y="63" text-anchor="middle">in sync</text>
<line class="arrd" x1="20" y1="100" x2="640" y2="100"/>
<circle class="numc" cx="330" cy="100" r="8"/>
<text class="num" x="330" y="103.5" text-anchor="middle">2</text>
<text class="sub" x="330" y="88" text-anchor="middle">shared flips off</text>
<rect class="chip" x="40" y="118" width="170" height="26" rx="7"/>
<text class="lbl s11" x="125" y="135" text-anchor="middle">Journal — shared ✗</text>
<text class="sub" x="125" y="192" text-anchor="middle">rows stay, local-only</text>
<circle class="numc" cx="330" cy="132" r="8"/>
<text class="num" x="330" y="135.5" text-anchor="middle">3</text>
<line class="arr" x1="238" y1="152" x2="422" y2="152" marker-end="url(#fa)"/>
<text class="sub" x="330" y="168" text-anchor="middle">retract: deletes in the changeset</text>
<rect class="chipd ghost" x="450" y="118" width="180" height="26" rx="7"/>
<text class="sub" x="540" y="192" text-anchor="middle">subtree removed</text>
<text class="sub" x="330" y="230" text-anchor="middle">1 peers hold the shared subtree · 2 the flag flips off · 3 one retract removes it there; the owner keeps it</text>
</svg>

## Where it runs

The two paths a row can cross on both apply the same keep rule:

- The changeset filter cuts gated-false rows from each outgoing changeset before
  it is signed and uploaded.
- The snapshot cleanup deletes gated-false rows from the bootstrap copy before
  it is encrypted and uploaded.

So a device that bootstraps from a snapshot and a device that applies live
changesets receive the same set of rows.
