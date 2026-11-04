# Procedures in SQLite, from a view with no rows

*Draft. The SQL is from bae's schema, `bae-db/migrations/001_initial.sql` at bae 2be627433. The checker
is coven's `owner-construction-check` at coven 1b2f45f18. The SQLite behavior described was checked in the
`sqlite3` shell, version 3.50.6.*

Other titles to consider: "SQLite has no CREATE PROCEDURE", "Calling a block of SQL from a trigger",
"A view as an argument list".

## The problem

bae is a music library app that keeps its library in SQLite and syncs it between devices with coven.
Its composer list shows, for each composer, three counts: the works they are credited on, the releases
with a track that performs one of those works, and their composer credits that no work links.

Each of those three is a view that joins several library tables: tracks, the links from tracks to works,
the works' credited artists, and which artist each artist is shown as. bae keeps each view's rows in a
table of its own, and the list's counts follow those tables. `composer_release` holds the rows of the
view `release_work_composers`; `composer_unlinked_credit` holds those of `unlinked_composer_credits`.

So whenever something under a release changes, that release's rows in both tables have to be
rewritten. A track can be added, moved to another release or removed. A track can gain or lose its link
to a work. A work's credited artists can change. An artist can arrive, or two artists can be merged.
Each of those is a trigger on a different table, and each needs the same step: the same four statements,
for a different release.

SQLite has no `CREATE PROCEDURE`. A trigger's body can hold only `INSERT`, `UPDATE`, `DELETE` and
`SELECT` statements, and none of them runs a named block of SQL. The direct way is to copy the four
statements into every trigger that needs them. In bae's schema the step is needed in thirteen places,
some of them twice in one trigger (once for the old release, once for the new), and every later fix to
the step would have to land in all thirteen.

## The technique

A view can stand in for the procedure:

```sql
CREATE VIEW IF NOT EXISTS composer_release_refresh AS SELECT NULL AS release_id WHERE 0;

CREATE TRIGGER IF NOT EXISTS composer_release_refresh INSTEAD OF INSERT ON composer_release_refresh
BEGIN
    DELETE FROM composer_release WHERE release_id = NEW.release_id
        AND artist_id NOT IN (SELECT artist_id FROM release_work_composers WHERE release_id = NEW.release_id);
    INSERT OR IGNORE INTO composer_release (artist_id, release_id)
    SELECT artist_id, release_id FROM release_work_composers WHERE release_id = NEW.release_id;
    DELETE FROM composer_unlinked_credit WHERE release_id = NEW.release_id
        AND (credit_id, artist_id) NOT IN (
            SELECT credit_id, artist_id FROM unlinked_composer_credits WHERE release_id = NEW.release_id);
    INSERT OR IGNORE INTO composer_unlinked_credit (credit_id, artist_id, release_id)
    SELECT credit_id, artist_id, release_id FROM unlinked_composer_credits WHERE release_id = NEW.release_id;
END;
```

The view's `SELECT` ends in `WHERE 0`, so it never has a row. What it does have is a column,
`release_id`. That column is the procedure's argument list, and the view's name is the procedure's
name.

SQLite lets a trigger stand in for a write to a view. An `INSTEAD OF INSERT` trigger on a view runs in
place of the insert, once for each row the insert would have added, and sees that row's values as
`NEW.<column>`. Nothing is stored: a view has no table of its own to store into. So the trigger's body
is the procedure's body, and an insert into the view is a call:

```sql
CREATE TRIGGER IF NOT EXISTS track_moved AFTER UPDATE OF release_id ON tracks
BEGIN
    INSERT INTO composer_release_refresh (release_id) VALUES (OLD.release_id);
    INSERT INTO composer_release_refresh (release_id) VALUES (NEW.release_id);
END;
```

A procedure with two arguments is a view with two columns:

```sql
CREATE VIEW IF NOT EXISTS artist_merge_refresh AS SELECT NULL AS artist_id, NULL AS into_artist_id WHERE 0;
```

`INSERT … SELECT` makes a loop. The trigger runs once for each row the `SELECT` returns, and not at all
when it returns none. When a work's credited artists change, every release with a track performing
that work is rewritten:

```sql
CREATE TRIGGER IF NOT EXISTS work_artist_added AFTER INSERT ON work_artists
BEGIN
    INSERT INTO composer_work_refresh (work_id) VALUES (NEW.work_id);
    INSERT INTO composer_release_refresh (release_id) SELECT release_id FROM work_releases WHERE work_id = NEW.work_id;
END;
```

A procedure can call another one. Merging two artists changes which artist many others are shown as.
`artist_merge_refresh` updates those, then calls `composer_work_refresh` and `composer_release_refresh`
for every work and release those artists are credited on. The three triggers on `artist_merges` (row
added, changed, removed) each call it.

A procedure's writes fire the triggers on the tables it writes, as any write does. A row that
`composer_work_refresh` adds to `composer_work` fires `composer_detail_work_credited`, which calls a
third procedure, `composer_detail_work_refresh`, to rewrite that work's rows on the composer's own page.

The body above is written as a difference. It deletes the rows the view no longer has, and inserts the
rows it lacks with `INSERT OR IGNORE`, so a row that is already right is left alone. That matters to the
next layer: the triggers that keep the composer list's counts are `AFTER INSERT` and `AFTER DELETE`
triggers on `composer_release`, and a row skipped by `OR IGNORE` fires no `AFTER INSERT` trigger. The
counts move only for rows that really changed. It also makes a second call for the same release
harmless, which happens when, say, a track's link moves between two tracks on one release: the second
call finds nothing to change.

Two more properties of the view help. Reading it returns nothing. And inserting into a view with no `INSTEAD OF INSERT` trigger is an error ("cannot modify
composer_release_refresh because it is a view"), so a call to a procedure whose body is missing fails
instead of doing nothing.

bae's schema has eight of these views. Thirteen inserts call `composer_release_refresh`, in ten
triggers on `artists`, `work_artists`, `track_works` and `tracks` and in the body of
`artist_merge_refresh`.

## Why triggers at all

The tables could be kept by app code instead: after each write to `tracks`, call a function that
rewrites the release. Then every code path that writes `tracks`, now and later, has to remember to make
that call, and one that doesn't leaves the composer list wrong.

A trigger runs as part of the statement that fired it. Whatever writes the table, the derived rows
change in that same statement, and if the procedure fails, the statement fails and its own write is
undone with it. In a test database, a procedure body that raised an error for one release id made the
`INSERT INTO tracks` that called it fail, and the track was not stored. There is no window where the
library holds a track the composer list doesn't account for.

## What it does to a checker

coven has a structural checker, `owner-construction-check`, that checks a Rust workspace against rules
read from a policy file, `owner-construction-check.toml`. bae runs it on its own workspace too. One of
its rules is about the database.

The rule groups the schema's tables into domains. A domain names its tables and its homes: path
prefixes, such as `crates/app-core/src/listing/`, where SQL may create and write those tables. SQL that
writes a domain's table anywhere else is a finding. The checker reads SQL as text, where it is written:
the string literals in the Rust code, and the `.sql` files. It reads each statement for the tables it
writes, and a trigger's body counts as the trigger's writes.

A trigger that writes no domain's table, say one that only raises an error, is part of the schema of
the table it fires on, and lives in that table's domain's homes.

The procedure views defeat that. The body of `track_moved` above is two inserts into
`composer_release_refresh`. A view is none of a domain's tables, so the checker saw a trigger on
`tracks` that writes nothing, and wanted it with `tracks`. What it really writes is `composer_release`
and `composer_unlinked_credit`.

The fix, in coven fe868a8cc, counts a write to a view as the writes of the view's `INSTEAD OF` triggers
for that operation. Where one of those writes a view in turn, that view's triggers count, and so on down:

```rust
fn expand(
    &self,
    table: &str,
    operation: Operation,
    visited: &mut BTreeSet<(String, Operation)>,
    tables: &mut BTreeSet<String>,
) {
    if !self.views.contains(table) {
        tables.insert(table.to_string());
        return;
    }
    if !visited.insert((table.to_string(), operation)) {
        return;
    }
    for writes in self.triggers.get(&(table.to_string(), operation)).into_iter().flatten() {
        for (written, operation) in writes {
            self.expand(written, *operation, visited, tables);
        }
    }
}
```

A table ends the walk: a write to `composer_release` is a write to `composer_release`. The triggers on
that table are statements of their own, and the checker reads them where they are written. The
`visited` set holds each view with the operation it was written by, and each pair is expanded once, so
two views whose triggers write each other don't send the walk around forever.

The walk follows only the triggers for the operation written. An `INSERT` into a view follows its
`INSTEAD OF INSERT` triggers; a view written by an operation no trigger stands in for writes nothing,
matching SQLite, which refuses that write.

With this, `track_moved` writes the composer tables and belongs in their domain's homes. Written
anywhere else, it gets a finding that names the view it went through. From the checker's tests:

> SQL writes note_listing through the view listing_refresh, which only the listing domain writes, in
> crates/app-core/src/listing/

### Who keeps a view

A view needs a home too. A domain keeps its views as it keeps its tables: each view's `CREATE VIEW` sits
in the domain's homes.

The checker assigns most views by itself. A view whose `INSTEAD OF` triggers, followed all the way
down, write only one domain's tables is that domain's. `composer_detail_work_refresh` writes only
`composer_detail_row`, so it goes wherever `composer_detail_row` goes.

Two kinds of view it can't assign: one whose triggers write no domain's table, and one whose triggers
write the tables of several domains. `artist_merge_refresh` would be the second kind if bae put
`artist_shown_as`, which its body updates, in a different domain from the composer tables. A view
whose triggers write several domains, if no domain claims it, is a finding:

> SQL creates view note_moves, whose INSTEAD OF triggers write the tables of domains listing, records

A domain claims such views with the policy's `views` key. From the checker's test policy:

```toml
[[database.domains]]
name = "listing"
homes = ["crates/app-core/src/listing/"]
tables = ["note_listing"]
views = ["listing_moves"]
owner = "Listing"
```

There, `listing_moves`'s only trigger raises an error and writes nothing, so the key is how it gets a
domain. The checker holds the key to the SQL: each view named must be one the production SQL creates,
not a table, and in one domain only.

> database: domain listing names view fixture, which the SQL never creates

### And the crate order

The next commit, coven 1b2f45f18, puts the domains in order. A domain sits in the crate that holds its
homes, and a crate's SQL fires triggers on, and reads, only the tables and views of its own crate's
domains and of the crates before it in the policy's `crate_order`.

That is where the view expansion shows up again. Once `track_moved` counts as the composer domain's
write, it lives with the composer tables but fires on `tracks`. So the crate holding the composer
domain has to come at or after the crate holding `tracks`. A procedure call, followed through its
views, is placed by the tables it really writes, and the crate order then checks that it reaches only
tables of its own crate's domains or of earlier crates.

### What the checker can't see

The checker reads SQL where its text is written. SQL the program builds while it runs, with `format!` or
by joining pieces, names its table nowhere in the source, so none of this applies to it.

## Open questions for the post

- bae's pin of the checker predates fe868a8cc, and bae's policy doesn't list database domains yet, so
  neither commit has run on bae's schema. The findings quoted here are from the checker's own tests.
  Whether to wait until bae's domains are in and quote its real findings.
- bae's import list uses a second form, a view that takes its columns from a table
  (`SELECT path AS key FROM scan_candidate WHERE 0`). Whether to cover it, and why it takes that form.
- Whether to say what SQLite does when procedure views call each other in a cycle. The checker's walk
  stops; the post doesn't say what SQLite does at run time.
