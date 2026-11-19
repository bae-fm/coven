// A crate between the helpers (`helpers.rs`) and the read code that calls
// them (`helped.rs`): no read code of this crate calls its function, so its
// record holds what the helpers' record holds for the helper it calls.

use reads_helpers::{Album, newest_first};

pub fn newest(albums: Vec<Album>) -> Vec<Album> {
    newest_first(albums)
}
