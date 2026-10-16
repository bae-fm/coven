//! This installation's completed replacement notice and its reserved counters (§10).

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::files::DeviceReset;
use coven_foundation::id_source::DeviceId;

pub(crate) fn read(database: &DatabaseConnection) -> Result<Option<DeviceReset>, DbError> {
    Ok(database
        .query(
            "SELECT old_device,new_device FROM _coven_device_reset WHERE singleton=1",
            [],
            |row| Ok((row.get::<_, [u8; 8]>(0)?, row.get::<_, [u8; 8]>(1)?)),
        )?
        .into_iter()
        .next()
        .map(|(old, new)| DeviceReset {
            old_device: DeviceId(u64::from_be_bytes(old)),
            new_device: DeviceId(u64::from_be_bytes(new)),
        }))
}

pub(crate) fn record(database: &DatabaseConnection, reset: &DeviceReset) -> Result<(), DbError> {
    database.internal_execute(
        "INSERT INTO _coven_device_reset(singleton,old_device,new_device) VALUES(1,?1,?2)
         ON CONFLICT(singleton) DO UPDATE SET old_device=excluded.old_device,
         new_device=excluded.new_device",
        (
            reset.old_device.0.to_be_bytes().as_slice(),
            reset.new_device.0.to_be_bytes().as_slice(),
        ),
    )?;
    Ok(())
}

/// The greatest numbers this database has reserved for its own objects. A stored
/// number beyond one of them proves another copy uses this id; equality does
/// not (§10).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IdentityReservations {
    /// Committed writes, uploaded or still queued.
    pub writes: u64,
    /// Applied own entries and the fixed entry awaiting publication.
    pub entries: u64,
    /// Reserved sealed key copies, confirmed or not.
    pub key_copies: u64,
    /// The greatest snapshot number reserved: every snapshot preparation
    /// takes the next of its own sequence, published, abandoned or not (§10).
    pub snapshots: u64,
}

pub(crate) fn reservations(
    database: &DatabaseConnection,
    device: DeviceId,
) -> Result<IdentityReservations, DbError> {
    let key = device.0.to_be_bytes();
    let greatest = |sql: &str| -> Result<u64, DbError> {
        Ok(database
            .query_row(sql, [key.as_slice()], |row| {
                row.get::<_, Option<[u8; 8]>>(0)
            })?
            .map_or(0, u64::from_be_bytes))
    };
    let writes = greatest("SELECT max(number) FROM _coven_positions WHERE device=?1")?.max(
        greatest("SELECT max(number) FROM _coven_uploads WHERE device=?1")?,
    );
    let entries = greatest("SELECT max(number) FROM _coven_store_log WHERE device=?1")?.max(
        greatest("SELECT max(number) FROM _coven_store_log_uploads WHERE device=?1")?,
    );
    let snapshots = database.query_row(
        "SELECT number FROM _coven_snapshot_reservation WHERE singleton=1",
        [],
        |row| row.get::<_, i64>(0),
    )? as u64;
    Ok(IdentityReservations {
        writes,
        entries,
        key_copies: crate::key_upload::last_reserved(database, device)?,
        snapshots,
    })
}
