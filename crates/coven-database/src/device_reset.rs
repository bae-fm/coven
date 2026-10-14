//! This installation's completed replacement notice and its reserved counters (§10).

use crate::{sqlite::DatabaseConnection, DbError};
use coven_foundation::files::{DeviceReset, DeviceResetReason};
use coven_foundation::id_source::DeviceId;

fn reason_text(reason: DeviceResetReason) -> &'static str {
    match reason {
        DeviceResetReason::IdentityMismatch => "identity-mismatch",
        DeviceResetReason::StorageAhead => "storage-ahead",
        DeviceResetReason::SlotMismatch => "slot-mismatch",
        DeviceResetReason::Replaced => "replaced",
        DeviceResetReason::DatabaseDamage => "database-damage",
    }
}

fn reason(text: &str) -> Result<DeviceResetReason, DbError> {
    Ok(match text {
        "identity-mismatch" => DeviceResetReason::IdentityMismatch,
        "storage-ahead" => DeviceResetReason::StorageAhead,
        "slot-mismatch" => DeviceResetReason::SlotMismatch,
        "replaced" => DeviceResetReason::Replaced,
        "database-damage" => DeviceResetReason::DatabaseDamage,
        _ => return Err(DbError::DamagedDatabase),
    })
}

pub(crate) fn read(database: &DatabaseConnection) -> Result<Option<DeviceReset>, DbError> {
    database
        .query(
            "SELECT old_device,new_device,reason FROM _coven_device_reset WHERE singleton=1",
            [],
            |row| {
                Ok((
                    row.get::<_, [u8; 8]>(0)?,
                    row.get::<_, [u8; 8]>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?
        .into_iter()
        .next()
        .map(|(old, new, text)| {
            Ok(DeviceReset {
                old_device: DeviceId(u64::from_be_bytes(old)),
                new_device: DeviceId(u64::from_be_bytes(new)),
                reason: reason(&text)?,
            })
        })
        .transpose()
}

pub(crate) fn record(database: &DatabaseConnection, reset: &DeviceReset) -> Result<(), DbError> {
    database.internal_execute(
        "INSERT INTO _coven_device_reset(singleton,old_device,new_device,reason) VALUES(1,?1,?2,?3)
         ON CONFLICT(singleton) DO UPDATE SET old_device=excluded.old_device,
         new_device=excluded.new_device,reason=excluded.reason",
        (
            reset.old_device.0.to_be_bytes().as_slice(),
            reset.new_device.0.to_be_bytes().as_slice(),
            reason_text(reset.reason),
        ),
    )?;
    Ok(())
}

/// The greatest numbers this database has reserved for its own objects. A stored
/// number beyond one of them proves a stale copy; equality does not (§10).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IdentityReservations {
    /// Committed writes, uploaded or still queued.
    pub writes: u64,
    /// Applied own entries and the fixed entry awaiting publication.
    pub entries: u64,
    /// Reserved sealed key copies, confirmed or not.
    pub key_copies: u64,
    /// The greatest snapshot number reserved: the id of the last snapshot
    /// operation started, finished or not. Ids of other operations reserve
    /// no snapshot number (§10).
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
