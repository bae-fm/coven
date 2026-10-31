//! This installation's completed replacement notice and its reserved counters (§19.3).

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
        (reset.old_device.0.to_be_bytes().as_slice(), reset.new_device.0.to_be_bytes().as_slice()),
    )?;
    Ok(())
}
