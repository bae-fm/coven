# Example

This page builds a small notes app to show how coven fits into an app. The app
owns its schema, its UI and its product policy. coven owns the SQLite
connections, recording and merging changes, encrypted sync, members, and files.
They meet at one builder and a handful of methods after that.

The household: Ana creates a store called Household on her phone, connects
Dropbox, invites Ben, and they keep a Gifts list Carol can't read.

## The schema

Notes can be in the store or in a circle; attachments and tags follow their
note. Attachments carry the user's own files.

```rust
use coven::{
    Audience, Coven, DropboxAccess, DropboxLocation, FileDecl, InviteAccess,
    InviteDelivery, InviteTarget, MemberRole, Migration, OAuthAccess, Provenance,
    RowIdentity, StorageConfig, StoreLayout, SyncStatus, SyncedTable, UuidText,
};
use uuid::Uuid;

const SCHEMA: &str = "
CREATE TABLE notes (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    audience TEXT NOT NULL,
    UNIQUE (audience, title)
);
CREATE TABLE attachments (
    id TEXT PRIMARY KEY NOT NULL,
    note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    hash BLOB,
    location TEXT
);
CREATE TABLE tags (id TEXT PRIMARY KEY NOT NULL);
CREATE TABLE note_tags (
    note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
    tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (note_id, tag_id)
);
CREATE TABLE ui_state (key TEXT PRIMARY KEY, value TEXT);  -- local: not declared
";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("attachments", 2 << 30, Provenance::UserProvided)),
        SyncedTable::new("tags", RowIdentity::SharedKey),
        SyncedTable::new("note_tags", RowIdentity::SharedKey)
            .key_columns(["note_id", "tag_id"])
            .audience_from("note_id"),
    ]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::addition(1, "initial", SCHEMA)]
}
```

Notes and attachments get random UUID keys, so two devices never make the same
one. Tags use shared keys: Ana and Ben each adding the tag "urgent" offline make
one row. `ui_state` isn't declared, so it never leaves the device.
`UNIQUE (audience, title)` includes the audience column, as a unique constraint
on an audience root must ([Circles](/docs/circles#unique-values-don-t-cross-audiences)).
The `hash` and `location` columns belong to coven, which fills them in when a
file is attached ([Files](/docs/files#declaring-files)).

## Create the store

```rust
coven::set_keyring_service("com.example.notes")?;

let layout = StoreLayout::new(app_dir);
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .create_store("Household")
    .await?;
```

`create_store` makes Ana's member keys and puts them in the OS keychain, then
creates the store on the phone. It needs no network. On later launches the app
opens the same store:

```rust
let store = layout.stores().await?[0].id;
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .open(store)
    .await?;
```

## Write and read

```rust
let note = UuidText(Uuid::now_v7());
handle.write(move |sql| {
    sql.execute(
        "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, ?3)",
        (note, "Groceries", Audience::Store),
    )?;
    sql.execute("INSERT INTO tags (id) VALUES ('urgent') ON CONFLICT DO NOTHING", [])?;
    sql.execute("INSERT INTO note_tags (note_id, tag_id) VALUES (?1, 'urgent')", (note,))?;
    Ok(())
}).await?;
```

The write commits on the phone at once, rows and its record for upload in one
transaction. A live query reruns whenever a write changes rows it read, whether
the write was Ana's or arrived from another device:

```rust
let mut notes = handle.subscribe(|sql| {
    Ok(sql.query("SELECT id, title FROM notes ORDER BY title", [], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?)
});
while let Some(Ok(rows)) = notes.next().await {
    show_notes(rows);
}
```

## Connect storage

Ana chooses a shared Dropbox folder. The app registers its own Dropbox app keys
and a sign-in presenter on the builder; coven runs the sign-in and keeps the
tokens.

```rust
handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await?;
let setup = handle
    .setup_oauth_storage(
        StorageConfig::Dropbox { location: DropboxLocation::SharedFolder { namespace_id } },
        "Ana's phone",
    )
    .await?;
show_blurred_qr_code(setup.restore_code.as_str());
```

Setup checks Dropbox, creates the store there, returns Ana's restore code, and
starts syncing. Her note goes out in the next pass. On later launches,
`handle.start_sync()` starts syncing again.

## Show what sync is doing

```rust
let mut status = handle.subscribe_sync_status();
loop {
    match &*status.borrow_and_update() {
        SyncStatus::Synced { finished_at } => show_last_synced(*finished_at),
        SyncStatus::Offline { .. } => show_offline(),
        SyncStatus::Blocked { reason, .. } => show_blocked(reason),
        SyncStatus::Failed { error } => show_sync_error(error),
        SyncStatus::Disconnected | SyncStatus::Stopped | SyncStatus::Syncing => {}
    }
    if status.changed().await.is_err() {
        break;
    }
}
```

`Blocked` means a slot can't apply yet, most often because the app needs an
update. Anything else that can't proceed, such as a file that can't upload or a
provider share waiting for the folder's owner, is a record in
`handle.subscribe_pending()` ([Sync](/docs/sync-model#status-and-pending-work)).

## Offer back what was lost

Ben retitles a note while Ana, offline, retitles it too. One title wins on every
device; the other is a lost value the app can show and offer back:

```rust
let mut lost = handle.subscribe_lost_values();
while let Some(Ok(values)) = lost.next().await {
    for value in &values {
        offer_to_restore(value); // e.g. "Ben's title was replaced"
    }
}
```

Restoring a value is an ordinary write. `dismiss_lost_values` drops the ones the
person has dealt with, on every device ([Merge](/docs/merge#lost-values)).

## Attach a file

```rust
let attachment = UuidText(Uuid::now_v7());
let size = std::fs::metadata(&path)?.len() as i64;
let prepared = coven::prepare_user_file(&path, |read| show_progress(read)).await?;
handle.write(move |sql| {
    sql.execute(
        "INSERT INTO attachments (id, note_id, name, size) VALUES (?1, ?2, ?3, ?4)",
        (attachment, note, "receipt.pdf", size),
    )?;
    sql.register_user_file("attachments", attachment, prepared)?;
    Ok(())
}).await?;
```

The file stays where the user keeps it; coven never moves or changes it. It
uploads in the background. On Ben's laptop the row arrives first, and the file
downloads when he opens it:

```rust
let file = handle.file_ref("attachments", attachment).await?;
let bytes = handle.read_file(&file).await?;
```

## Invite Ben

```rust
let invite = handle
    .create_invite(
        InviteTarget::NewMember { role: MemberRole::Member, name: "Ben".into() },
        InviteAccess::ProviderAccount { email: "ben@example.com".into() },
        InviteDelivery::InPerson,
    )
    .await?;
show_qr_code(&invite.code);
```

Ben is beside Ana, so she shows the invite in person. Ben's phone scans the
code, signs in to his own Dropbox, and calls `join_with_invite`, which writes a
join request and waits. Ana's phone reads and checks the request in its next
sync pass and approves it on its own. A sent invite waits for Ana to compare a
confirmation number instead. The whole flow, and restoring on a new device of
one's own, is on the [Sharing](/docs/sharing#adding-a-person-invites) page.

## Keep the Gifts list from Carol

```rust
let ben = invite.member; // the member id Ana's invite fixed for Ben

let circles = handle.circles();
let gifts = circles.create("Gifts").await?;
circles.add_member(gifts, &ben).await?;

let list = UuidText(Uuid::now_v7());
handle.write(move |sql| {
    sql.execute(
        "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, ?3)",
        (list, "Carol's birthday", gifts),
    )?;
    Ok(())
}).await?;
```

The note, its attachments and its tags are in Gifts: encrypted with the Gifts
key, readable on Ana's and Ben's devices and not on Carol's. Moving a note
between the store and a circle is an `UPDATE` of its `audience` column
([Circles](/docs/circles#moving-rows)).
