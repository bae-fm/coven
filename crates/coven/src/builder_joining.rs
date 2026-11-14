//! The app's joining write on the builder (E1).

use super::*;

/// The app's joining write, as the builder keeps it.
pub(super) type JoiningHook =
    dyn Fn(&JoiningWrite<'_>, &Joining) -> Result<(), DbError> + Send + Sync;

impl CovenBuilder {
    /// The app's write for a member's device as its registration applies
    /// (E1): it runs in the transaction that first applies the registration
    /// of one of this person's devices, on a join's admission, a snapshot
    /// that covers it, a restore, a pairing or a reset's installation, as a
    /// local write whose `read_through` is the slot that transaction applies
    /// through. It reads the store as that transaction leaves it, with the
    /// facts of its member's latest admission (`Joining`), and runs on every
    /// device of the member once each, so the app writes it to change
    /// nothing a second time. An error fails the transaction: the slot
    /// doesn't apply, the pass reports it, and the next pass tries again.
    /// Without one, nothing runs.
    pub fn joining_write<F>(mut self, write: F) -> Self
    where
        F: Fn(&JoiningWrite<'_>, &Joining) -> Result<(), DbError> + Send + Sync + 'static,
    {
        self.joining_write = Some(Arc::new(write));
        self
    }
}
