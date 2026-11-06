//! Where a test hears that file work reached a path: about to open it, or
//! finding its lock held, by another holder or by an instance of this
//! process that is closing, and about to wait for it. A stalled disk, such as a
//! FIFO in a file's place, then holds the work there, and the test knows it
//! is held rather than giving it time to get there.

use crate::locks::lock_unpoisoned;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

type Watch = (PathBuf, tokio::sync::oneshot::Sender<()>);

static WATCHED: Mutex<Vec<Watch>> = Mutex::new(Vec::new());

/// Told once file work reaches `path`: about to open it, or finding its
/// lock held and about to wait for it.
pub fn when_reached(path: &Path) -> tokio::sync::oneshot::Receiver<()> {
    let (told, telling) = tokio::sync::oneshot::channel();
    lock_unpoisoned(&WATCHED).push((resolved(path), told));
    telling
}

/// File work reached `path`: tell whoever waits for that.
pub(super) fn reached(path: &Path) {
    let path = resolved(path);
    let mut watched = lock_unpoisoned(&WATCHED);
    let (found, rest): (Vec<Watch>, Vec<Watch>) =
        std::mem::take(&mut *watched).into_iter().partition(|(watched, _)| *watched == path);
    *watched = rest;
    for (_, told) in found {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a test that stopped listening has no use for it"
        )]
        let _ = told.send(());
    }
}

/// `path` under its parent's canonical path, as the work and the test may
/// spell one folder differently.
fn resolved(path: &Path) -> PathBuf {
    match (path.parent().and_then(|parent| std::fs::canonicalize(parent).ok()), path.file_name()) {
        (Some(parent), Some(name)) => parent.join(name),
        _ => path.to_owned(),
    }
}
