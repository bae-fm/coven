//! The files each move to the cloud lists (§17.3, api.md E4), kept as a
//! table so the app's local triggers can follow them (§9.4), as they follow
//! `coven_pins`.
//!
//! `_coven_move_files` holds the files each moving or failed move lists, and
//! `_coven_moves` each move's state. Coven's triggers on them, kept with them
//! in `file_moves_schema.rs`, insert a file's row with its move's state as the
//! move lists it, change the state of each of the move's rows as the move
//! fails, and delete a file's row as the list drops it, in the same
//! statement. A switched or cancelled move lists nothing, so a row's state is
//! moving or failed, and a file is in at most one move.

/// The table of the files moving and failed moves list, which the app's
/// triggers may follow.
pub(crate) const MOVES: &str = "coven_moves";

pub(super) const SCHEMA: &str = "CREATE TABLE coven_moves (
        move INTEGER NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('moving','failed')),
        file TEXT PRIMARY KEY NOT NULL
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX _coven_moves_move ON coven_moves(move);";
