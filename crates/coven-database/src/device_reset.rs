//! This installation's completed replacement notice, and the record that it
//! stopped because another installation uses its device id (§19.3).

use crate::sqlite::StoreSql;
use crate::DbError;
use coven_foundation::files::DeviceReset;
use coven_merge::Slot;

pub(crate) fn read(database: &StoreSql) -> Result<Option<DeviceReset>, DbError> {
    Ok(database
        .host_live_read(
            "SELECT old_device,new_device FROM _coven_device_reset WHERE singleton=1",
            [],
            |row| Ok(DeviceReset { old_device: row.get(0)?, new_device: row.get(1)? }),
        )?
        .into_iter()
        .next())
}

pub(crate) fn record(database: &StoreSql, reset: &DeviceReset) -> Result<(), DbError> {
    database.execute(
        "INSERT INTO _coven_device_reset(singleton,old_device,new_device) VALUES(1,?1,?2)
         ON CONFLICT(singleton) DO UPDATE SET old_device=excluded.old_device,
         new_device=excluded.new_device",
        (reset.old_device, reset.new_device),
    )?;
    Ok(())
}

/// Record the slot that showed another installation using this device's id.
/// The first record stays: a later pass finding the same evidence changes
/// nothing.
pub(crate) fn record_in_use(database: &StoreSql, slot: Slot) -> Result<(), DbError> {
    database.execute(
        "INSERT INTO _coven_device_id_in_use(singleton,slot) VALUES(1,?1)
         ON CONFLICT(singleton) DO NOTHING",
        [slot.get().to_be_bytes().as_slice()],
    )?;
    Ok(())
}

/// The slot this installation recorded when it stopped on another
/// installation's use of its device id, if it did.
pub(crate) fn read_in_use(database: &StoreSql) -> Result<Option<Slot>, DbError> {
    let found = database.query(
        "SELECT slot FROM _coven_device_id_in_use WHERE singleton=1",
        [],
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    found
        .into_iter()
        .next()
        .map(|bytes| {
            let bytes: [u8; 8] = bytes
                .try_into()
                .map_err(|_length| DbError::Invariant { what: "a slot is 8 bytes" })?;
            Slot::new(u64::from_be_bytes(bytes))
                .ok_or(DbError::Invariant { what: "a recorded slot is positive" })
        })
        .transpose()
}

#[cfg(test)]
#[path = "device_reset_tests.rs"]
pub(crate) mod tests;
