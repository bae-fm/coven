# coven

Sync normally means a backend: a server you run and pay for, and a database
that holds every user's data in the clear.

coven syncs without the server. Devices exchange end-to-end-encrypted changes
through storage the user already has (Dropbox, OneDrive, iCloud, or any
S3-compatible endpoint), and merge them locally.

The data is SQLite. You keep your schema; coven owns the connections and runs
your queries through them, so it can capture each change with SQLite's
pre-update hook, encrypt and sign it, move it through the user's storage, and
apply other devices' changes back.

## Queries

An app writes its queries as compiled statements. Its build script checks each
one against the schema as the app compiles, and the app runs it by its type:

```rust
coven::read!(writer NoteTitles, r#"
    SELECT n.title FROM notes AS n ORDER BY n.title
"#);

coven::write!(AddNote(id: coven::UuidText, title: String), r#"
    INSERT INTO notes (id, title) VALUES (:id, :title)
"#);

handle.write(move |sql| Ok::<_, CovenError>(sql.run(&AddNoteParams { id, title })?)).await?;
let titles: LiveQuery<_, CovenError> =
    handle.subscribe(|sql| Ok(sql.fetch(&NoteTitlesParams {})?));
```

SQL strings work too, with `sql.query` and `sql.execute`, for statements an app
builds as it runs. [`examples/notes`](examples/notes) is an app crate with its
build script, and [the spec, §4.2](spec/coven.md#42-compiled-statements) states
what the build step needs.

This Cargo workspace is the implementation. Its design is
[`spec/coven.md`](spec/coven.md), and every byte it
writes to storage is in [`spec/format.md`](spec/format.md).

CloudKit is built into the Apple-platform storage adapter. The app supplies its
container identifier and iCloud entitlement. See the
[signed macOS conformance host](crates/coven-storage/cloudkit-live.md) for live testing.

Docs: <https://coven.bae.fm>
