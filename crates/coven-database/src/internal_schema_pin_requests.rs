//! The pins coven has not yet made, kept as a table so the app's local
//! triggers can follow them (§9.4), as they follow `coven_pins`.
//!
//! `_coven_pin_requests` holds each file a switch to the cloud asked this
//! device to keep whole, until coven makes the pin or the file is gone
//! (§17.8). Coven's triggers on it, kept with it in `file_pins_schema.rs`,
//! insert a file's entry as its request is recorded and delete it as the
//! request ends, in the same statement. The table holds only the file: the
//! copy the request keeps and its wait are coven's.

/// The table of files with a pin request, which the app's triggers may
/// follow.
pub(crate) const PIN_REQUESTS: &str = "coven_pin_requests";

pub(super) const SCHEMA: &str = "CREATE TABLE coven_pin_requests (
        file TEXT PRIMARY KEY NOT NULL
    ) STRICT, WITHOUT ROWID;";
