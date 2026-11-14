//! What a reset's registration does to the old id (§19.3).

use super::*;

/// What the fresh device's registration does to the old id. A reset already
/// recorded keeps its choice. Otherwise the old installation's database says:
/// a copy that recorded finding another installation using its id registers
/// a new device and replaces nothing, and every other reset, a database SQLite
/// reports damaged included, registers in the old id's place (§19.3).
pub(super) async fn registration_of(directory: &StoreDir) -> Result<Registration, BootstrapError> {
    if let Some(record) = directory.unfinished_replacement().map_err(CovenError::from)? {
        return Ok(record.registration);
    }
    match coven_database::read_device_id_in_use(directory.database_path(), directory.bugs()).await {
        Ok(Some(_)) => Ok(Registration::NewDevice),
        Ok(None) | Err(coven_database::DbError::DamagedDatabase) => Ok(Registration::ReplacesOld),
        Err(error) => Err(SyncError::from(error).into()),
    }
}

#[cfg(test)]
#[path = "bootstrap_reset_copy_tests.rs"]
mod tests;
