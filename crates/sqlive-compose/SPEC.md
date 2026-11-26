# sqlive-compose

sqlive-compose composes an app's reads from SQL fragments while the app
compiles. The parts of the app, its domains, write the SELECTs they offer to
others as *fragments*. A screen's *read* names the fragments it needs, and the
compiler puts them together into one SQL statement, checks that statement
against the app's schema, and generates the Rust types its rows decode into. The
statement is a constant in the app's code, and the app runs it as a live query
of [sqlive](../sqlive/SPEC.md): a query that runs again when a write changes
what it read. [relay](https://relay.dev) composes GraphQL the same way: each
component writes a fragment, and relay's compiler builds one operation from the
fragments a page names.

The examples in this document come from an issue tracker. Its domains are
issues, people, labels and comments, each in a crate of its own, and its screens
are in the crate `tracker`. Its schema holds these tables:

```sql
CREATE TABLE people (
    id TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    photo BLOB NOT NULL
) STRICT;
CREATE TABLE projects (
    id TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL
) STRICT;
CREATE TABLE issues (
    id TEXT NOT NULL PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects (id),
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    state TEXT NOT NULL,
    author_id TEXT NOT NULL REFERENCES people (id),
    assignee_id TEXT REFERENCES people (id),
    opened_at INTEGER NOT NULL
) STRICT;
CREATE TABLE labels (
    id TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    hidden INTEGER NOT NULL
) STRICT;
CREATE TABLE issue_labels (
    issue_id TEXT NOT NULL REFERENCES issues (id),
    label_id TEXT NOT NULL REFERENCES labels (id),
    PRIMARY KEY (issue_id, label_id)
) STRICT;
CREATE TABLE comments (
    id TEXT NOT NULL PRIMARY KEY,
    issue_id TEXT NOT NULL REFERENCES issues (id),
    author_id TEXT NOT NULL REFERENCES people (id),
    body TEXT NOT NULL,
    written_at INTEGER NOT NULL
) STRICT;
CREATE TABLE comment_reads (
    issue_id TEXT NOT NULL,
    person_id TEXT NOT NULL,
    read_at INTEGER NOT NULL,
    PRIMARY KEY (issue_id, person_id)
) STRICT;
```

The people crate offers a person as a fragment. The `fragment!` macro takes a
header, which Rust parses, and the SQL in a raw string. The header names the
fragment and, in braces, gives the Rust type of each column whose type the
schema alone doesn't give:

```rust
// people/src/lib.rs
sqlive_compose::fragment!(Person { person_id: PersonId }, r#"
    SELECT p.id AS person_id, p.name, p.photo
    FROM people AS p
"#);
```

The issues crate offers `Issue` and `Project` the same way:

```rust
// issues/src/lib.rs
sqlive_compose::fragment!(
    Issue {
        issue_id: IssueId,
        project_id: ProjectId,
        state: IssueState,
        author_id: PersonId,
        assignee_id: Option<PersonId>,
        opened_at: Timestamp,
    },
    r#"
    SELECT i.id AS issue_id, i.project_id, i.number, i.title, i.state,
           i.author_id, i.assignee_id, i.opened_at
    FROM issues AS i
"#);

sqlive_compose::fragment!(Project { project_id: ProjectId }, r#"
    SELECT j.id AS project_id, j.name
    FROM projects AS j
"#);
```

The issue page draws an issue with its author and its project. Its `read!` macro
names the request's fields in the header and, in the SQL, names each fragment
after an `@`, with the fragment's Rust path. Naming a fragment in SQL this way
is a *spread*:

```rust
// tracker/src/issue_page.rs
sqlive_compose::read!(IssuePage(issue: Option<IssueId>), r#"
    SELECT issue.*, author.*, project.*
    FROM @issues::Issue AS issue
    JOIN @people::Person AS author ON author.person_id = issue.author_id
    JOIN @issues::Project AS project ON project.project_id = issue.project_id
    WHERE issue.issue_id = :issue
"#);
```

The compiler replaces each spread with the fragment's SQL as a subquery:

```sql
SELECT issue.*, author.*, project.*
FROM (SELECT i.id AS issue_id, i.project_id, i.number, i.title, i.state,
             i.author_id, i.assignee_id, i.opened_at
      FROM issues AS i) AS issue
JOIN (SELECT p.id AS person_id, p.name, p.photo
      FROM people AS p) AS author ON author.person_id = issue.author_id
JOIN (SELECT j.id AS project_id, j.name
      FROM projects AS j) AS project ON project.project_id = issue.project_id
WHERE issue.issue_id = :issue
```

The compiler also generates the row type, which holds one field for each spread,
typed as that fragment's type:

```rust
pub struct IssuePageRow {
    pub issue: issues::Issue,
    pub author: people::Person,
    pub project: issues::Project,
}
```

The app's build script runs the compiler once for each crate that writes
fragments or reads. The macros themselves only include the Rust that the
compiler wrote.

## 1. Why a read is one statement

A live query can also be a Rust closure that runs several statements in one
snapshot of the database. Written that way, the issue page's query reads the
issue, then reads the author that the issue names, and reads the assignee only
when the issue has one:

```rust
let issue = sql.query_row("SELECT … FROM issues WHERE id = ?1", [id], issue_of)?;
let person = "SELECT … FROM people WHERE id = ?1";
let author = sql.query_row(person, [&issue.author_id], person_of)?;
let assignee = match &issue.assignee_id {
    Some(id) => Some(sql.query_row(person, [id], person_of)?),
    None => None,
};
```

The second statement binds a value that the first returned, and Rust decides
whether the third runs. No build step can read that query as a whole: the SQL is
in pieces between lines of Rust, and what the code reads depends on the data. A
column that a later migration renames fails when the statement runs.

A read composed into one statement can hold neither form. One `SELECT` has no
statement before it to take a value from, and Rust runs only after the row is
read. So the whole read is one constant, and the build can prepare it against
the schema, give each of its columns a type, and ask sqlive what it depends on
(section 8).

Composing leaves two things as they are. The closure above already reads one
snapshot, so its statements agree with each other. The closure also runs again
as a whole when a write changes anything one of its statements read, as a
composed statement does.

## 2. Fragments

A fragment is one `SELECT`, with a name and typed columns, that a domain writes
beside the Rust code that uses its rows. The `SELECT` may declare CTEs of its
own. The fragment's name becomes a Rust type, and other crates spread it by that
type's path.

A fragment gets values from outside in three ways, and its header declares each
one:

- A *parameter* is a value that the request binds, such as the person who is
  looking at the screen.
- A *parent row* is a row of the statement that spreads the fragment, such as
  the issue whose labels the fragment reads.
- A *relation* is a set of rows that the statement hands to the fragment, such
  as the issues whose labels the fragment counts.

A parameter goes in parentheses after the name, with its Rust type, and the SQL
writes the parameter as `:name`. The fragment below reads the projects that hold
an issue in a given state:

```rust
// issues/src/lib.rs
sqlive_compose::fragment!(
    ProjectsWith(state: IssueState) { project_id: ProjectId },
    r#"
    SELECT j.id AS project_id, j.name
    FROM projects AS j
    WHERE EXISTS (SELECT 1 FROM issues AS i
                  WHERE i.project_id = j.id AND i.state = :state)
"#);
```

A parent row follows `on`, and a relation follows `with`. Section 3 shows both.

After those, the braces hold *declarations*, each of which gives a column its
Rust type. A column that the SQL takes unchanged from a table, such as `p.name`
in `Person`, needs none: the compiler takes its type from the table's STRICT
type, and whether it can be NULL from the table's `NOT NULL`. A column that the
SQL computes, such as `count(*) AS unread`, always needs one, since SQLite gives
a computed column no type. A declaration on a table's column replaces the type
the schema gives, as `person_id: PersonId` does in `Person`. Section 9 gives the
rules in full.

## 3. Three ways to spread a fragment

### As a table

A fragment with no parent row stands wherever SQL allows a subquery in `FROM`.
The issue page spreads three that way. The spread takes an alias, and the
statement selects the fragment's columns with `alias.*`.

An issue need not have an assignee, so a read that shows one uses a `LEFT JOIN`.
When no row joins, the row type's field is `None`:

```rust
sqlive_compose::read!(IssueAssignee(issue: IssueId), r#"
    SELECT issue.*, assignee.*
    FROM @issues::Issue AS issue
    LEFT JOIN @people::Person AS assignee
           ON assignee.person_id = issue.assignee_id
    WHERE issue.issue_id = :issue
"#);
```

```rust
pub struct IssueAssigneeRow {
    pub issue: issues::Issue,
    pub assignee: Option<people::Person>,
}
```

The generated reader tells a missing row from a row by the fragment's first `NOT
NULL` column: the field is `None` when that column is NULL. A fragment with no
`NOT NULL` column gives the reader nothing to test, so a `LEFT JOIN` spread of
one fails the build.

### As a value from a parent row

The issue list draws each issue with its labels. A join would repeat the issue
once for each label, and a `FROM` subquery can't read the labels of the issue
beside it: SQLite gives a subquery in `FROM` no access to the other rows of that
`FROM`. A subquery in a result column can read them, and returns one value. So a
fragment that depends on a row of the statement around it spreads as a value.

The header names the parent row's table after `on`, and the SQL names the row's
columns with `@parent`:

```rust
// labels/src/lib.rs
sqlive_compose::fragment!(
    IssueLabels on issues {
        struct Label { name: String, hidden: bool }
        labels: json<Vec<Label>>
    },
    r#"
    SELECT json_group_array(json_object(
               'name', l.name,
               'hidden', json(iif(l.hidden, 'true', 'false')))
               ORDER BY l.name) AS labels
    FROM issue_labels AS il
    JOIN labels AS l ON l.id = il.label_id
    WHERE il.issue_id = @parent.id
"#);
```

A list of labels becomes one value as a JSON array. The header declares the
array's shape field by field, the compiler compares that shape with the SQL's
`json_object` keys (section 8), and the generated reader decodes the array into
`Vec<Label>`.

The statement that spreads the fragment says which of its `FROM` items gives the
parent row, with `parent:` and the item's alias. The item must read the table
that the header names:

```sql
SELECT i.title, @labels::IssueLabels(parent: i) AS labels
FROM issues AS i
WHERE i.project_id = :project
```

The compiler writes the spread as a subquery in parentheses, with the alias in
place of `@parent`:

```sql
SELECT i.title,
       (SELECT json_group_array(json_object(
                   'name', l.name,
                   'hidden', json(iif(l.hidden, 'true', 'false')))
                   ORDER BY l.name) AS labels
        FROM issue_labels AS il
        JOIN labels AS l ON l.id = il.label_id
        WHERE il.issue_id = i.id) AS labels
FROM issues AS i
WHERE i.project_id = :project
```

A value that is one row works the same way. A comment shows its author, and the
people crate offers the author of a comment as one JSON object:

```rust
// people/src/lib.rs
sqlive_compose::fragment!(
    CommentAuthor on comments {
        struct Author { id: PersonId, name: String, photo: hex }
        author: json<Author>
    },
    r#"
    SELECT json_object('id', p.id, 'name', p.name, 'photo', hex(p.photo)) AS author
    FROM people AS p
    WHERE p.id = @parent.author_id
"#);
```

```rust
// comments/src/lib.rs
sqlive_compose::read!(
    IssueComments(issue: IssueId) { id: CommentId, written_at: Timestamp },
    r#"
    SELECT c.id, c.body, c.written_at, @people::CommentAuthor(parent: c) AS author
    FROM comments AS c
    WHERE c.issue_id = :issue
    ORDER BY c.written_at, c.id
"#);
```

JSON has no type for bytes, so the SQL writes the photo as hexadecimal text, and
the shape's `hex` field makes the generated reader turn the text back into
bytes.

A fragment with a parent row returns exactly one column, and a spread of it in
`FROM` fails the build.

A value for each row belongs in a parent-row spread, never in a fragment that
groups its rows by the parent's key and joins as a table. A fragment written
the second way returns the same labels:

```rust
// labels/src/lib.rs
sqlive_compose::fragment!(
    LabelLists { issue_id: IssueId, labels: json<Vec<String>> },
    r#"
    SELECT il.issue_id, json_group_array(l.name) AS labels
    FROM issue_labels AS il
    JOIN labels AS l ON l.id = il.label_id
    GROUP BY il.issue_id
"#);
```

```sql
SELECT i.title, lists.labels
FROM issues AS i
LEFT JOIN @labels::LabelLists AS lists ON lists.issue_id = i.id
WHERE i.project_id = :project
```

sqlive narrows this read as it narrows `IssueLabels`: a condition on a column
that the `GROUP BY` lists passes into the group, so a write to another project's
labels doesn't run it again. SQLite runs the read differently: it computes
`LabelLists` for every issue in the tracker, then picks the project's issues
from the result, on each run, even when the statement fixes one issue by its id.
A parent-row spread becomes a subquery that SQLite runs once for each issue the
read returns. sqlive reports the grouped join as narrowed, so the whole-table
warning (section 8) never names it.

### With rows from the caller

Beside the issue list, the tracker shows how many of the listed issues carry
each label. The labels crate can count labels over any set of issues, but only
the caller knows which issues the list holds. So the fragment declares the
caller's rows as a relation, after `with`, and reads the relation as a table:

```rust
// labels/src/lib.rs
sqlive_compose::fragment!(
    LabelUse with chosen(issue_id TEXT) { label_id: LabelId, uses: i64 },
    r#"
    SELECT l.id AS label_id, l.name, count(*) AS uses
    FROM chosen
    JOIN issue_labels AS il ON il.issue_id = chosen.issue_id
    JOIN labels AS l ON l.id = il.label_id
    GROUP BY l.id
"#);
```

A relation's columns have SQLite's STRICT type names, such as `TEXT`, since the
rows stay inside SQL and never reach Rust.

The spread gives the relation as another spread, or as a subquery in
parentheses, that returns the columns the header lists, in the same order. The
issues crate offers the open issues of a project, and the issue list's screen
hands them to `LabelUse`:

```rust
// issues/src/lib.rs
sqlive_compose::fragment!(
    OpenIssueIds(project: ProjectId) { issue_id: IssueId },
    r#"
    SELECT i.id AS issue_id
    FROM issues AS i
    WHERE i.project_id = :project AND i.state = 'open'
"#);
```

```rust
// tracker/src/issue_list.rs
sqlive_compose::read!(OpenLabelUse(project: ProjectId), r#"
    SELECT used.*
    FROM @labels::LabelUse(chosen: @issues::OpenIssueIds(project: :project)) AS used
    ORDER BY used.uses DESC, used.label_id
"#);
```

The compiler gives the fragment the relation as a CTE, whose name holds the
crate, the fragment and the relation:

```sql
SELECT used.*
FROM (WITH labels__LabelUse__chosen (issue_id) AS (
          SELECT i.id AS issue_id
          FROM issues AS i
          WHERE i.project_id = :project AND i.state = 'open')
      SELECT l.id AS label_id, l.name, count(*) AS uses
      FROM labels__LabelUse__chosen AS chosen
      JOIN issue_labels AS il ON il.issue_id = chosen.issue_id
      JOIN labels AS l ON l.id = il.label_id
      GROUP BY l.id) AS used
ORDER BY used.uses DESC, used.label_id
```

## 4. Fragments inside fragments

A fragment spreads other fragments as a read does. A row of the issue list shows
the issue, its assignee, its labels, and how many comments the person looking at
the list hasn't read. The comments crate offers that last count, for a parent
issue and a person:

```rust
// comments/src/lib.rs
sqlive_compose::fragment!(
    UnreadCount(viewer: PersonId) on issues { unread: i64 },
    r#"
    SELECT count(*) AS unread
    FROM comments AS c
    WHERE c.issue_id = @parent.id
      AND c.written_at > COALESCE((SELECT r.read_at FROM comment_reads AS r
                                   WHERE r.issue_id = c.issue_id
                                     AND r.person_id = :viewer), 0)
"#);
```

The issues crate composes the row from fragments of the other three domains:

```rust
// issues/src/lib.rs
sqlive_compose::fragment!(
    IssueListItem(viewer: PersonId) {
        issue_id: IssueId,
        project_id: ProjectId,
        state: IssueState,
    },
    r#"
    SELECT i.id AS issue_id, i.project_id, i.number, i.title, i.state,
           assignee.*,
           @labels::IssueLabels(parent: i) AS labels,
           @comments::UnreadCount(parent: i, viewer: :viewer) AS unread
    FROM issues AS i
    LEFT JOIN @people::Person AS assignee ON assignee.person_id = i.assignee_id
"#);
```

A spread inside a fragment becomes a field of the fragment's type, by the rules
that a spread inside a read follows:

```rust
pub struct IssueListItem {
    pub(crate) issue_id: IssueId,
    pub(crate) project_id: ProjectId,
    pub(crate) number: i64,
    pub(crate) title: String,
    pub(crate) state: IssueState,
    pub(crate) assignee: Option<people::Person>,
    pub(crate) labels: labels::IssueLabels,
    pub(crate) unread: comments::UnreadCount,
}
```

The issue list's read spreads `IssueListItem` alone, and names neither the
people, the labels nor the comments that each row shows:

```rust
// tracker/src/issue_list.rs
sqlive_compose::read!(
    IssueList(project: ProjectId, state: IssueState, me: PersonId),
    r#"
    SELECT item.*
    FROM @issues::IssueListItem(viewer: :me) AS item
    WHERE item.project_id = :project AND item.state = :state
    ORDER BY item.number DESC
"#);
```

The compiler expands the innermost spreads first. When it compiles the issues
crate, it puts the SQL of `Person`, `IssueLabels` and `UnreadCount` into
`IssueListItem`. When it compiles the tracker crate, it puts the expanded
`IssueListItem` into `IssueList`:

```sql
SELECT item.*
FROM (SELECT i.id AS issue_id, i.project_id, i.number, i.title, i.state,
             assignee.*,
             (SELECT json_group_array(json_object(
                         'name', l.name,
                         'hidden', json(iif(l.hidden, 'true', 'false')))
                         ORDER BY l.name) AS labels
              FROM issue_labels AS il
              JOIN labels AS l ON l.id = il.label_id
              WHERE il.issue_id = i.id) AS labels,
             (SELECT count(*) AS unread
              FROM comments AS c
              WHERE c.issue_id = i.id
                AND c.written_at > COALESCE((SELECT r.read_at FROM comment_reads AS r
                                             WHERE r.issue_id = c.issue_id
                                               AND r.person_id = :me), 0)) AS unread
      FROM issues AS i
      LEFT JOIN (SELECT p.id AS person_id, p.name, p.photo
                 FROM people AS p) AS assignee
             ON assignee.person_id = i.assignee_id) AS item
WHERE item.project_id = :project AND item.state = :state
ORDER BY item.number DESC
```

Every spread binds each of the fragment's parameters to a `:parameter` of the
statement around it, and the compiler rewrites the fragment's parameter to that
name. `UnreadCount` calls the person `:viewer`, and so does `IssueListItem`,
which passes its own `:viewer` on. `IssueList` calls the person `:me`, so the
composed statement reads `:me`. However many fragments need the person, the
statement has one `:me`, and the request binds that value once.

The compiler also renames each CTE that a fragment declares to
`<crate>__<fragment>__<cte>`, as it names a relation, so that no two fragments'
CTEs share a name. The long name also keeps a CTE apart from the names inside a
view that sqlive reads whole. sqlive can't tell the reads through two equal
names apart, and then narrows neither ([sqlive's spec, section
8](../sqlive/SPEC.md#subqueries-in-from-ctes-and-views)).

A crate spreads the fragments of the crates it depends on directly. Within one
crate, fragments may spread each other in any order, but not in a circle: when
`A` spreads `B` and `B` spreads `A`, the build fails and names both.

## 5. Reads and their request

A read is the statement an app runs. Its header lists the fields of the request,
each with its Rust type, and its SQL names each field as a parameter. The
generated parameters struct has one field for each, so Rust code that leaves one
out or misspells one doesn't compile:

```rust
let params = IssueListParams { project, state: IssueState::Open, me };
```

The issue page has an issue to show only after the person picks one. Its
request's field is an `Option`, and `None` binds NULL. `issue.issue_id = NULL`
is true of no row, so the read at the top of this document then returns no row.
Nothing in Rust has to skip the query.

Other screens need a row even when the request names nothing, or names an issue
that a write has since deleted. Such a read starts from the request itself, as
its *root*: a subquery that returns the request's key as one row. The screen's
fragments join the root with `LEFT JOIN`, and each is `None` when nothing joins.
The issue page, written from a root and with its assignee, is:

```rust
// tracker/src/issue_page.rs
sqlive_compose::read!(IssuePage(issue: Option<IssueId>), r#"
    SELECT issue.*, author.*, project.*, assignee.*
    FROM (SELECT :issue AS issue) AS page
    LEFT JOIN @issues::Issue AS issue ON issue.issue_id = :issue
    LEFT JOIN @people::Person AS author ON author.person_id = issue.author_id
    LEFT JOIN @issues::Project AS project ON project.project_id = issue.project_id
    LEFT JOIN @people::Person AS assignee ON assignee.person_id = issue.assignee_id
"#);
```

```rust
pub struct IssuePageRow {
    pub issue: Option<issues::Issue>,
    pub author: Option<people::Person>,
    pub project: Option<issues::Project>,
    pub assignee: Option<people::Person>,
}
```

The join to the root compares with `:issue`, the parameter, although the root
holds the same value in `page.issue`. sqlive reads the conditions of a live
query's statement to find which rows it depends on, so that a write to any other
row doesn't run the query again. A comparison with a parameter tells sqlive
which rows those are. A comparison with a column of the root tells sqlive less,
in either order. sqlive reports what the read depends on in `issues` for each
form:

| The join's condition | What sqlive reports for `issues` |
|---|---|
| `issue.issue_id = :issue` | `keyed by id` |
| `issue.issue_id = page.issue` or `page.issue = issue.issue_id` | `looked up` |

*Keyed* means that sqlive files the query under the issue's id, so a write to
another issue doesn't have to judge the query at all. *Looked up* means that
sqlive narrows the rows but files the query under no one value, so each write to
`issues` judges the query. For a join, the write asks with a small SELECT
whether the row it changed is one the query depends on. The later sections also
meet a third answer, *whole*: the query depends on every row, so a write to any
issue can run it again. A read therefore compares with the request's parameter
and uses the root only to return a row.

## 6. Joining spreads

A statement may name a spread's columns in its conditions and its `ORDER BY`, as
`issue.author_id` in the issue page's join and `item.number` in the issue list.
A result column that names one column of a spread fails the build, because the
columns belong to the fragment's type (section 9).

**The join rule.** A fragment joins its parent on columns it passes through
unchanged, never through a computed CTE column.

`Person` passes `people.id` through as `person_id`, so when the issue page joins
on `author.person_id`, sqlive reads the condition as one on `people.id` and
finds which people the read depends on. A value that a CTE computes is in no
table's column, so a condition on it says nothing to sqlive about the table's
rows.

The two fragments below give an issue's title by its id. `Comment`, a fragment
of the comments crate, gives a comment's id as `comment_id`, its issue's id and
its body. A read that shows a comment beside its issue's title joins `Comment`
with either of the two:

```rust
sqlive_compose::fragment!(IssueTitle { issue_id: IssueId }, r#"
    WITH named AS (SELECT i.id, i.title FROM issues AS i)
    SELECT named.id AS issue_id, named.title FROM named
"#);

sqlive_compose::fragment!(FoldedIssueTitle { issue_id: String }, r#"
    WITH named AS (SELECT lower(i.id) AS issue_id, i.title FROM issues AS i)
    SELECT named.issue_id, named.title FROM named
"#);
```

```sql
SELECT comment.*, title.*
FROM @comments::Comment AS comment
JOIN @issues::IssueTitle AS title ON title.issue_id = comment.issue_id
WHERE comment.comment_id = :comment
```

sqlive reports `issues` as `looked up` for the read that joins `IssueTitle`. For
the same read with `FoldedIssueTitle`, sqlive reports ``whole: nothing narrows
`i` ``, and a write to any issue can run it again.

The rule holds for the other side of the join too. When `comment.issue_id` is
the computed column, sqlive reports `issues` as `looked up` when the condition
names `title.issue_id` first, and as `whole` when it names `comment.issue_id`
first. Even the lookup is worse than one on a table's column: for each issue
that a write changes, it runs the CTE, up to the number of steps that sqlive
allows a lookup. A fragment that needs a computed value returns it beside the
column the join uses.

[sqlive's spec](../sqlive/SPEC.md#5-narrowing-one-table), sections 5 to 8,
states in full which conditions and joins sqlive narrows by.

## 7. One read per concern

A screen subscribes one read for each of its *concerns*. A concern is a part of
the screen that shows its own rows for its own request. The tracker's main
screen has three: the list of a project's issues, the counts above the list, and
the issue that the person selected. Each is a read:

| Concern | Read | What sqlive reports for `issues` |
|---|---|---|
| The list | `IssueList` | `keyed by project_id` |
| The counts | `ProjectCounts` | `keyed by project_id` |
| The selected issue | `IssuePage` | `keyed by id` |

```rust
// issues/src/lib.rs
sqlive_compose::read!(
    ProjectCounts(project: ProjectId) { open: i64, closed: i64 },
    r#"
    SELECT count(*) FILTER (WHERE i.state = 'open') AS open,
           count(*) FILTER (WHERE i.state = 'closed') AS closed
    FROM issues AS i
    WHERE i.project_id = :project
"#);
```

Within a concern, everything goes in one read: the issue page's author, project
and assignee are one read with its issue, because they change with the issue and
the page draws them together. A read may hold as much as it can without reading
a table whole, and the author judges where one concern ends. The compiler checks
only for whole-table reads.

Across concerns, one statement reruns more than separate ones. For a read of the
list and the selected issue together, sqlive reports `issues` as `looked up`,
because sqlive keeps one scope for each table of a statement: one part fixes
`project_id` and the other fixes `id`, and no single column is fixed in both.
Selecting another issue would also run the list again.

A count of every issue in the tracker, added to the issue page, makes it depend
on every issue:

```sql
SELECT (SELECT count(*) FROM issues AS every) AS total, issue.*
FROM @issues::Issue AS issue
WHERE issue.issue_id = :issue
```

sqlive reports ``issues: whole: nothing narrows `every` ``, so a write to any
issue runs the issue page again. The compiler warns on such a read (section 8),
and the author moves the count into a read of its own.

**A list inside a row.** SQLite returns a flat set of rows, so a row with
several lists needs a form for them.

- A *JSON value for each list* keeps the lists apart, as `IssueLabels` does in
  `IssueListItem`. A fragment with a parent row gives each one.
- A *join* repeats the row for each item of the list. Two lists joined to one
  row give the product: an issue with 3 labels and 200 comments gives 600 rows.
  So a join suits at most one list.
- A *`UNION ALL`* of rows of different kinds, padded to one width with a column
  that says which kind each row is, suits one ordered stream of mixed rows. The
  Rust code then groups the rows by that column.

**A request that changes the SQL.** SQL fixes its sort direction and the columns
it compares, so a finite set of choices becomes one read for each, and Rust
picks one of them by the request. A read that tries to hold both in one text
loses sqlive's narrowing and SQLite's indexes:

```sql
-- Reads every issue, whatever :project is.
SELECT issue.* FROM @issues::Issue AS issue
WHERE :project IS NULL OR issue.project_id = :project
```

sqlive reports this one as ``issues: whole: nothing narrows `i` ``. As two
reads, `ProjectIssues` is keyed by `project_id`, and only `AllIssues` reads
every issue.

A list whose length the request decides, such as the issues the person selected,
goes in one parameter: a JSON array of their ids, read with `json_each`. SQL
that reads `json_each` qualifies every column it names. sqlive doesn't know the
columns of a table function, so beside one it can't tell which table a bare name
belongs to, and a condition on a bare name narrows nothing.

## 8. What the build checks

The compiler runs the checks below in this order, when the app compiles. A
mistake stops the build with an error, and each error says which macro call it
concerns: its file, line and column. The messages here are as the tracker's
build script prints them.

**The schema.** The compiler opens an in-memory database and runs the app's
setup step on it (section 10), so every check below runs against the schema that
the app's migrations build.

**Spreads.** A spread must name a fragment of the crate or of a direct
dependency. The spread must bind each of the fragment's parameters to a
`:parameter` of the statement, the parent row to a `FROM` item that reads the
parent table, and each relation to a subquery or a spread. SQLite takes `@name`
as a parameter of its own, so the compiler reserves `@` for its markers and
fails on an `@` parameter.

```sql
SELECT item.* FROM @issues::IssueListItem AS item
```

```text
tracker/src/issue_list.rs:8:1: the spread of issues::IssueListItem binds no value to its parameter viewer
```

A fragment with a parent row can't stand in `FROM`:

```sql
SELECT i.title, labels.* FROM issues AS i, @labels::IssueLabels(parent: i) AS labels
```

```text
tracker/src/issue_list.rs:8:1: labels::IssueLabels reads a parent row, so its spread is a value, not a table in FROM
```

A circle of fragments fails and names them:

```text
issues/src/lib.rs:40:1: issues::A spreads itself: issues::A, issues::B, issues::A
```

**Prepare.** SQLite prepares each fragment on its own and each composed read.
The compiler prepares a fragment with a parent row with each `@parent.column`
written as a subquery that reads that column of the parent table, so the
fragment's own columns keep their tables and types. It prepares a fragment with
a relation over an empty set of rows of the declared types. SQLite reports what
it can't find, and the error holds SQLite's message:

```sql
SELECT i.id AS issue_id, i.titel FROM issues AS i
```

```text
issues/src/lib.rs:12:1: no such column: i.titel
```

SQLite refuses an unknown table, function and collation the same way. SQLite
also refuses a statement that opens more cursors than its limit, which a read
that spreads a fragment over views many times can reach.

**Parameters.** A `:parameter` that the header doesn't declare fails, and so
does a header field that the SQL doesn't name:

```text
tracker/src/issue_page.rs:6:1: IssuePage reads :isue, which its header does not declare
tracker/src/issue_page.rs:6:1: IssuePage declares issue, which its SQL does not read
```

**Column types.** A computed column with no declaration fails:

```rust
sqlive_compose::fragment!(Titles(project: ProjectId), r#"
    SELECT i.id AS issue_id, upper(i.title) AS title
    FROM issues AS i WHERE i.project_id = :project
"#);
```

```text
issues/src/lib.rs:20:1: Titles computes the column title, and its header gives title no type
```

A column that can be NULL needs an `Option`. A table's column can be NULL when
the table doesn't declare it `NOT NULL`, when the SQL places the table on the
right of a `LEFT JOIN`, and when the column comes from a scalar subquery. A
declaration that gives such a column a type other than an `Option` fails:

```rust
sqlive_compose::fragment!(Assignee { assignee_id: PersonId }, r#"
    SELECT i.id AS issue_id, i.assignee_id FROM issues AS i
"#);
```

```text
issues/src/lib.rs:30:1: assignee_id can be NULL, and the header types it PersonId, which holds no NULL
```

A bare column name that the compiler can't place on one `FROM` item needs a
declaration.

**JSON keys.** The compiler reads the `json_object` and `json_group_array` calls
of a `json<T>` column and compares their keys, and how they nest, with `T`'s
fields. The compiler looks inside a `COALESCE` and inside a scalar subquery that
returns the document, so it checks the array in this column:

```sql
SELECT COALESCE((SELECT json_group_array(json_object('name', l.name, 'color', l.hidden))
                 FROM issue_labels AS il JOIN labels AS l ON l.id = il.label_id
                 WHERE il.issue_id = @parent.id), '[]') AS labels
```

```text
labels/src/lib.rs:8:1: labels: the document has the key color, which Label lacks, and lacks Label's key hidden
```

A JSON boolean reads `true` or `false`. SQLite writes an INTEGER column as a
number, so SQL for a `bool` field makes the JSON text, as `json(iif(l.hidden,
'true', 'false'))` does in `IssueLabels`.

`json<T>` with `T` the path of a type that the header doesn't declare is an
*opaque* document. The compiler skips the key check, and the crate's own tests
decode real rows.

**Shapes sqlive can't analyze.** The compiler asks sqlive what each composed
read depends on, with the same report that section 5 quotes. When sqlive can't
analyze one part of a statement, the read depends on every row of every table
the statement reads. So the build fails and names the reason:

```sql
SELECT issue.* FROM @issues::Issue AS issue
RIGHT JOIN @people::Person AS author ON author.person_id = issue.author_id
```

```text
tracker/src/issue_page.rs:6:1: sqlive can't analyze IssuePage: a NATURAL or RIGHT join
```

Section 11 lists the shapes. The compiler asks sqlive before it types the
columns, so a shape that sqlive can't analyze is the error a call reports, not
the column error that the shape also causes.

**Whole-table reads.** When sqlive analyzes a statement and reports a table as
read whole, the compiler warns and names the table and the reason. The build
goes on, because some reads need every row, as `AllIssues` does:

```text
warning: tracker/src/all_issues.rs:5:1: AllIssues reads every row of issues: nothing narrows `i`
```

The warning names what sqlive reruns the read for, not what SQLite reads on
each run. A fragment that groups its rows and joins as a table can read every
row of its tables while sqlive narrows it (section 3).

## 9. Generated types

The compiler writes the Rust for each macro call. A fragment becomes a struct
with one field for each column and spread:

```rust
/// The columns of `Person`.
pub struct Person {
    pub(crate) person_id: PersonId,
    pub(crate) name: String,
    pub(crate) photo: Vec<u8>,
}

impl Person {
    /// Reads the fragment's columns from `row`, the first at `offset`.
    pub fn read(row: &Row<'_>, offset: usize) -> rusqlite::Result<Self> { /* … */ }
}
```

The fields are `pub(crate)`, so only the crate that writes the fragment reads
its columns. The domain decides what other crates see, with methods it writes:

```rust
// people/src/lib.rs
impl Person {
    /// The name this person shows.
    pub fn name(&self) -> &str { &self.name }
}
```

The tracker crate draws the issue's author with `row.author.name()`, and can't
read `row.author.photo`. A domain can rename a column or compute one differently
without changing the crates that spread its fragment. relay gives a component
only the fields of its own fragment for the same reason.

A read becomes three items:

```rust
/// The `IssuePage` read.
pub struct IssuePage;

impl IssuePage {
    /// The composed statement.
    pub const SQL: &str = "SELECT issue.*, author.*, …";
}

/// What the read's request binds.
pub struct IssuePageParams {
    pub issue: Option<IssueId>,
}

impl IssuePageParams {
    /// The parameters by their names in the statement.
    pub fn params(&self) -> Vec<(&'static str, &dyn ToSql)> { /* … */ }
}

/// A row of the read.
pub struct IssuePageRow {
    pub issue: Option<issues::Issue>,
    pub author: Option<people::Person>,
    pub project: Option<issues::Project>,
    pub assignee: Option<people::Person>,
}

impl IssuePageRow {
    /// Reads a row of the statement.
    pub fn read(row: &Row<'_>) -> rusqlite::Result<Self> { /* … */ }
}
```

The row struct has one field for each spread, named by the spread's alias, and
one for each column that the read itself returns, as `IssueComments` returns
`id`, `body` and `written_at`.

**Columns.** A column that comes unchanged from a table takes the Rust type of
its STRICT type, such as `i64` for `INTEGER` and `String` for `TEXT` (section
11). A declaration replaces that type with any type that implements rusqlite's
`FromSql`, as `IssueState` does over `state`, and gives a computed column its
type. A declared `bool` reads an INTEGER 0 or 1. The field is an `Option` where
the column can be NULL.

**Spreads.** Whether a spread's field is an `Option` depends on the spread and
on the fragment:

| Spread | Field |
|---|---|
| In `FROM`, or the right side of an inner join | `Fragment` |
| On the right of a `LEFT JOIN` | `Option<Fragment>` |
| As a value, when the fragment always returns one row | `Fragment` |
| As a value otherwise | `Option<Fragment>` |

A fragment always returns one row when its `SELECT` calls an aggregate and has
neither `GROUP BY` nor `HAVING`. `UnreadCount` counts, so each issue has a
count, 0 when it has no comments. `IssueLabels` gathers labels into an array,
`[]` when there are none. With a `HAVING count(*) > 0`, the same `SELECT` would
return no row for an issue without labels, and the field would be an `Option`.
`CommentAuthor` selects a person by key with no aggregate, so its field is an
`Option`, even though a foreign key says that the person exists.

**JSON documents.** A shape that the header declares becomes a struct with
`Deserialize`, and the column's field holds it. `labels: json<Vec<Label>>` in
`IssueLabels` gives a `labels: Vec<Label>` field. A `hex` field holds bytes: the
generated reader decodes the hexadecimal text that `hex(p.photo)` wrote into
`Vec<u8>`. A field whose value is a bare column is an `Option` where that column
can be NULL.

## 10. How an app's build runs the compiler

The macros and the compiler are in two crates. `sqlive-compose-macros` holds
`fragment!` and `read!`, because Rust puts procedural macros in a crate of their
own. `sqlive-compose` re-exports them, so an app writes
`sqlive_compose::fragment!` and `sqlive_compose::read!`. With its `compiler`
feature, `sqlive-compose` also holds the compiler, which an app's build script
takes as a build dependency. A crate that writes fragments lists both:

```toml
[package]
name = "issues"
links = "issues_sql"

[dependencies]
people = { path = "../people" }
labels = { path = "../labels" }
comments = { path = "../comments" }
sqlive-compose = { workspace = true }

[build-dependencies]
sqlive-compose = { workspace = true, features = ["compiler"] }
```

The app's own binary compiles the macros, and only the build script compiles the
compiler.

Each macro expands to one line that includes the file that the compiler wrote
for it:

```rust
include!(concat!(env!("OUT_DIR"), "/sqlive_compose/IssuePage.rs"));
```

Each fragment and read has a name that no other fragment or read of its crate
has, because the name is the file's name.

**The compiler takes and returns values.** The compiler reads no file and no
environment variable, and opens its in-memory database through sqlive, so it
uses none of the capabilities in [coven's spec,
F.2](../../spec/coven.md#f2-capabilities), itself. Its one entry point is
`compile_crate`, which takes:

- the crate's name;
- a *source reader*, a function that the build script hands in, which takes a
  module file's path and returns the file's text;
- the fragment manifest of each crate the crate depends on directly, as text;
- the *setup step*, the schema as data.

`compile_crate` finds the crate's macro calls by walking its modules from
`src/lib.rs`, with the source reader. When everything passes, it returns:

- each generated file's name and text;
- the crate's own fragment manifest, as text;
- every path the source reader gave a file for, so that Cargo runs the build
  script again when one changes;
- its warnings;
- each call it compiled, in the order the walk finds them: which macro it is,
  its name, its location, its SQL as its author wrote it, with the spreads in
  place, and the tables and views its own SQL reads. The reads are the names
  after FROM and JOIN at any depth, lowercase, with a schema's prefix dropped.
  They leave out the call's own CTEs and relations, table-valued functions and
  what the fragments it spreads read, so a tool that checks which tables a
  crate's calls read takes the compiler's own list and finds the calls, reads
  the SQL and names the tables by no rule of its own. A module marked
  `#[cfg(test)]` is not walked, and its calls are not reported.

When something fails, it returns its errors instead. Each error is a typed
variant that holds its location, what it concerns and its cause, such as
SQLite's own error in the example of section 8. A warning holds its location,
the table and the reason. The build script turns both into text.

**The setup step** holds the schema's SQL texts, in the order to run, and the
name, argument count and determinism of each SQL function, and the name of each
collation, that the schema uses. SQLite fails to prepare a statement that names
an unknown function or collation, so the compiler registers a stand-in under
each of those names. The tracker's setup step runs its migrations in order:

```rust
// tracker-schema/src/lib.rs
pub fn setup() -> Setup {
    Setup {
        sql: vec![
            include_str!("../migrations/0001_tables.sql").into(),
            include_str!("../migrations/0002_comment_reads.sql").into(),
        ],
        functions: vec![Function { name: "fold_case".into(), arguments: 1, deterministic: true }],
        collations: vec![],
    }
}
```

**A crate hands its fragments to the crates that spread them** through a
*manifest*, a JSON text that holds each fragment's Rust path, parameters, parent
table and relations, its expanded SQL, how many columns it returns, the position
of its first column that can't be NULL, and whether it always returns one row.
Because the SQL is expanded already, a crate needs only the manifests of the
crates it depends on directly: the manifest of `issues` holds `IssueListItem`
with the SQL of `UnreadCount` inside it, and the tracker crate composes
`IssueList` without the manifest of `comments`.

Cargo passes on the manifest's path. A crate that sets `links`, as `issues` does
above, can print metadata from its build script, and Cargo runs that script
before the scripts of the crates that depend on it directly. The path reaches
them in the environment variable `DEP_<LINKS>_<KEY>`. So the build script of
`tracker` reads `DEP_ISSUES_SQL_SQLIVE_FRAGMENTS`:

```rust
// tracker/build.rs
use sqlive_compose::compiler::{compile_crate, Input};
use std::{env, fs, path::PathBuf};

fn main() {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let generated = out.join("sqlive_compose");
    fs::create_dir_all(&generated).unwrap();

    // One manifest for each direct dependency that writes fragments.
    let manifests = ["PEOPLE", "LABELS", "ISSUES"]
        .iter()
        .map(|links| {
            let path = env::var(format!("DEP_{links}_SQL_SQLIVE_FRAGMENTS")).unwrap();
            println!("cargo::rerun-if-changed={path}");
            fs::read_to_string(path).unwrap()
        })
        .collect();

    let output = match compile_crate(Input {
        crate_name: "tracker".into(),
        source_reader: &|path| fs::read_to_string(root.join(path)),
        manifests,
        setup: tracker_schema::setup(),
    }) {
        Ok(output) => output,
        Err(errors) => {
            // The script's own function gives each error its text.
            errors.iter().for_each(|error| println!("cargo::error={}", text_of_error(error)));
            std::process::exit(1);
        }
    };
    for (name, text) in &output.files {
        fs::write(generated.join(name), text).unwrap();
    }
    for path in &output.read {
        println!("cargo::rerun-if-changed={}", root.join(path).display());
    }
    for warning in &output.warnings {
        println!("cargo::warning={}", text_of_warning(warning));
    }
    // Offer this crate's own fragments to the crates that depend on it.
    let manifest = out.join("fragments.json");
    fs::write(&manifest, &output.manifest).unwrap();
    println!("cargo::metadata=sqlive_fragments={}", manifest.display());
}
```

Here the crate `tracker-schema` embeds the schema's SQL, and each build script
takes it as a build dependency, so Cargo runs the script again when a migration
changes.

## 11. Reference

### Headers

```text
fragment!(Name <parameters> <parent> <relations> <declarations>, r#"SQL"#);
read!(Name <parameters> <declarations>, r#"SQL"#);

parameters    (name: RustType, …)
parent        on table
relations     with relation(column STRICT_TYPE, …), …
declarations  { column: RustType, …  struct Shape { field: RustType, … } }
```

Every part after the name is optional. A `struct` in the declarations is the
shape of a JSON document, for a column that the same header declares as
`json<T>`. Its fields may have the type `hex`.

### Markers

| Marker | Where | Meaning |
|---|---|---|
| `@path::Fragment` | in `FROM` or as a result column | Spreads the fragment. The path is the fragment type's Rust path: `crate::…` for this crate, `<dependency>::…` for another. |
| `(name: :parameter)` | after a fragment's path | Binds the fragment's parameter `name` to the statement's `:parameter`. |
| `(parent: alias)` | after a fragment's path | Takes the parent row from the `FROM` item `alias`. |
| `(relation: (SELECT …))`, `(relation: @path::Fragment)` | after a fragment's path | Gives the relation its rows. |
| `@parent.column` | in a fragment with a parent row | Names a column of the parent row. |

Arguments in one pair of parentheses are separated by commas.

### Types

The compiler gives a column that comes unchanged from a table this Rust type:

| STRICT type | Rust type |
|---|---|
| `INTEGER` | `i64` |
| `TEXT` | `String` |
| `BLOB` | `Vec<u8>` |
| `REAL` | `f64` |
| `ANY` | `SqlValue` |

### Shapes sqlive can't analyze

A read that holds one of these in a place sqlive reads fails the build:

- a recursive CTE;
- a `NATURAL` or `RIGHT` join;
- a parenthesized join that has an alias, starts with another parenthesized
  join, or joins by `USING`, and one whose rows aren't its tables joined in turn;
- `USING` inside a parenthesized join;
- `x IN table`;
- a hidden column of an FTS5 table, such as `rank`;
- a table or table function outside the main schema, and a name that is no CTE,
  ordinary table, view or FTS5 table that keeps its own content;
- a CTE whose column list has another length than its `SELECT`'s columns;
- SQL that sqlive's parser can't read, or that is more than one `SELECT`.

In an app's view, such a shape makes sqlive read that view whole and analyze the
rest of the statement, so the compiler warns and the build goes on.

### Generated names

| Macro call | Generated |
|---|---|
| `fragment!(Person …)` | `Person`, with `read`, and one struct for each JSON shape |
| `read!(IssuePage …)` | `IssuePage::SQL`, `IssuePageParams`, `IssuePageRow` |
| A CTE or a relation of a fragment | `<crate>__<fragment>__<name>` |
| The file of either macro | `OUT_DIR/sqlive_compose/<Name>.rs` |
