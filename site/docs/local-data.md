# Local data

By default, every row of a synced table reaches every device that shares the
library. To keep some rows on one device, never synced, the host *gates* the
table: a row syncs only while a condition holds. The host declares the gate per
table in
[`set_synced_tables`](rustdoc:fn:coven::sync::session::set_synced_tables), and
coven enforces it on both paths a row can take to another device: the per-cycle
changeset and the bootstrap snapshot.

The examples use a todos app: a `workspace` holds `lists`, and a `list` holds
`todos`. A list has a boolean `shared` column. A private list, and the todos
under it, should stay on the device that made it.

## Declaring a gate

[`SyncedTable`](rustdoc:enum:coven::sync::session::SyncedTable) has three forms:

```rust
SyncedTable::new("todos")                              // no gate of its own
SyncedTable::new("lists").gated_by("shared")           // gated root
SyncedTable::new("workspaces").gated_by_descendants()  // ancestor
```

- `new(name)` declares the table synced with no gate of its own. With a foreign
  key into a gated root it inherits that gate; without one it syncs
  unconditionally.
- `gated_by(column)` makes the table a *gated root*: a row syncs only while its
  boolean `column` is true.
- `gated_by_descendants()` marks an *ancestor* that should sync only while a
  gated descendant of it survives.

A table is one of the three, never two at once.

## Gated roots

A gated root carries a boolean column. A row whose column is false stays on the
device that wrote it, and its foreign-key descendants stay with it. With
`lists.gated_by("shared")`, a private list and its todos never leave the device.

The gate flows down foreign keys: a child row syncs iff the row at the top of
its foreign-key chain, the gated root, syncs. `todos` reference `lists`, so a
todo is shared exactly while its list is. The host declares the gate once, on
the root; coven follows the schema's foreign keys to every descendant, so the
children need no declaration of their own.

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

Both paths share one definition of "kept", built as a SQL predicate. For a
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

Setting a root's gate from false to true makes a previously-local subtree
public. Peers never held it, so coven re-emits the whole now-visible connected
component (the root row, its descendants, and the ancestors the new row keeps
alive) as full inserts in that cycle. The subtree lands complete, not as an
update to rows the peer is missing.

The reverse, true to false, is a freeze: coven stops emitting the row but does
not retract what peers already hold. It never sends a delete to take a shared
row back.

## Where it runs

The two paths a row can cross on both apply the same keep rule:

- The changeset filter cuts gated-false rows from each outgoing changeset before
  it is signed and uploaded.
- The snapshot cleanup deletes gated-false rows from the bootstrap copy before
  it is encrypted and uploaded.

So a device that bootstraps from a snapshot and a device that applies live
changesets receive the same set of rows.
