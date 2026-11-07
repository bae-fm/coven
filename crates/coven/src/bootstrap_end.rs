//! Ending an installation that finishes without its store (§13.4).

use super::*;

/// Record, before anything is deleted, that a join ends without a store.
pub(super) fn record_ending(
    request: &BootstrapRequest,
    pending: &BootstrapStore,
) -> Result<(), BootstrapError> {
    match request {
        BootstrapRequest::Join { code, .. } => {
            Ok(coven_sync::record_ending(&pending.directory(), code)?)
        }
        BootstrapRequest::Restore { .. } => Ok(()),
    }
}

/// Remove an unfinished first installation that ends without a store: a join
/// declined, expired or cancelled before its request was sent, or a restore
/// cancelled, or ended by the facts once recorded (§13.1), which may have
/// loaded into a join's installation. A first installation is the only one
/// of its store on the device, so every device-only entry of the store is
/// its own, and all of them go: the custody a cut commit wrote, then the
/// provider credentials, which only a join's or a recorded restore's kept
/// sign-in can be before publication, then the saved code or record; then
/// the installation (§13.4). A join records that it ends before any of
/// that, so a failure part way leaves the code and installation, which list
/// as the join it was, and a retry finishes the deletion without connecting.
/// A restore's retry ends again (signing in first once the kept sign-in is
/// gone). A failure after the code leaves the installation alone, which
/// lists as neither. Credentials are never left
/// without their code or record (§13.2).
pub(super) fn remove_unfinished<T>(
    device_entries: &StoreKeychain,
    pending: &BootstrapStore,
    result: Result<T, BootstrapError>,
) -> Result<T, BootstrapError> {
    match device_entries.delete_device_entries() {
        Ok(()) => cleanup(pending, result),
        Err(error) => combine(result, Err(error.into())),
    }
}

fn cleanup<T>(
    pending: &BootstrapStore,
    result: Result<T, BootstrapError>,
) -> Result<T, BootstrapError> {
    combine(result, pending.cancel().map_err(BootstrapError::from))
}
