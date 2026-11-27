//! A notes app's queries, written as an app that uses coven writes them: each
//! statement is a `coven::read!` or `coven::write!` that the build script
//! checks and compiles, and this crate depends on `coven` alone to run them.

use coven::{CovenError, CovenHandle, CovenResult, LiveQuery, SqlRead, SqlWrite, UuidText};

/// The app's schema, which its one migration runs and its build compiles the
/// statements against.
pub const SCHEMA: &str = include_str!("../schema.sql");

coven::fragment!(
    Note,
    r#"
    SELECT n.id, n.title FROM notes AS n
"#
);

impl Note {
    /// The note's id, as the database holds it.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The note's title.
    pub fn title(&self) -> &str {
        &self.title
    }
}

// `writer` marks a read that `CovenHandle` runs. A read without it runs on a
// `CovenReadHandle`.
coven::read!(
    writer NoteList,
    r#"
    SELECT note.* FROM @crate::Note AS note ORDER BY note.title, note.id
"#
);

// The state of sync, a view of coven's that only the store's writer reads.
coven::read!(
    writer SyncLine { state: String },
    r#"
    SELECT s.state FROM coven_sync_state AS s
"#
);

coven::write!(
    AddNote(id: coven::UuidText, title: String),
    r#"
    INSERT INTO notes (id, title) VALUES (:id, :title)
"#
);

coven::write!(
    Retitle(id: coven::UuidText, title: String),
    r#"
    UPDATE notes SET title = :title WHERE id = :id RETURNING title
"#
);

/// Adds a note, and returns how many rows that added.
pub async fn add_note(handle: &CovenHandle, id: UuidText, title: String) -> CovenResult<usize> {
    handle.write(move |sql| Ok::<_, CovenError>(sql.run(&AddNoteParams { id, title })?)).await
}

/// Retitles the note `id`, and returns the title each row it changed now has.
pub async fn retitle(
    handle: &CovenHandle,
    id: UuidText,
    title: String,
) -> CovenResult<Vec<String>> {
    handle
        .write(move |sql| {
            let rows = sql.run_returning(&RetitleParams { id, title })?;
            Ok::<_, CovenError>(rows.into_iter().map(|row| row.title).collect())
        })
        .await
}

/// The notes by title, as a live query.
pub fn notes(handle: &CovenHandle) -> LiveQuery<Vec<NoteListRow>, CovenError> {
    handle.subscribe(|sql| Ok(sql.fetch(&NoteListParams {})?))
}

/// The state of sync now.
pub async fn sync_state(handle: &CovenHandle) -> CovenResult<String> {
    handle.read(|sql| Ok::<_, CovenError>(sql.fetch_row(&SyncLineParams {})?.state)).await
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
