//! Coven's own SQL functions, registered on every connection before the
//! app's: what shared triggers, migrations and coven's views call (§4,
//! Appendix C.1).

use coven_foundation::id_source::{DeviceId, FileId};
use rusqlite::functions::FunctionFlags;
use rusqlite::types::{Value, ValueRef};
use rusqlite::Connection;

use crate::authorization::SqlAuthorization;

/// A function whose result depends on its arguments alone, which views and
/// indexes may call.
const PURE: FunctionFlags = FunctionFlags::SQLITE_UTF8
    .union(FunctionFlags::SQLITE_DETERMINISTIC)
    .union(FunctionFlags::SQLITE_INNOCUOUS);

#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn register(
    connection: &Connection,
    authorization: &SqlAuthorization,
) -> rusqlite::Result<()> {
    connection.create_scalar_function(
        "coven_applying",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        authorization.applying_function(),
    )?;
    connection.create_scalar_function(
        "_coven_rowid_changed",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        authorization.rowid_changed_function(),
    )?;
    connection.create_scalar_function(
        "_coven_loss_order",
        2,
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_DIRECTONLY,
        |ctx| crate::loss_record::sort_key(ctx.get(0)?, &ctx.get::<Vec<u8>>(1)?),
    )?;
    // A row's key as coven encodes it (format.md D3), so app SQL can join
    // its rows to coven's views and migrations can compare keys.
    connection.create_scalar_function("coven_key", -1, PURE, |ctx| {
        let values = (0..ctx.len())
            .map(|i| crate::write_encoding::value(ctx.get_raw(i)))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        crate::write_rows::equality_key(&values)
            .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
    })?;
    // A stored time as milliseconds since the Unix epoch, for coven's views.
    connection.create_scalar_function("_coven_unix_ms", 1, PURE, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Null => Value::Null,
            ValueRef::Blob(bytes) => Value::Integer(unix_ms(bytes)?),
            _ => return Err(damaged()),
        })
    })?;
    // The cache's name for a file, from its uploader and id (Appendix C.3).
    connection.create_scalar_function("_coven_cache_id", 2, PURE, |ctx| {
        let device = DeviceId(u64::from_be_bytes(
            ctx.get::<Vec<u8>>(0)?.try_into().map_err(|_| damaged())?,
        ));
        let file = FileId(uuid::Uuid::from_bytes(
            ctx.get::<Vec<u8>>(1)?.try_into().map_err(|_| damaged())?,
        ));
        Ok(crate::database::file_database::file_cache_id(device, file))
    })?;
    Ok(())
}

/// Register `_coven_device()`, this installation's device id as coven
/// stores device ids, on a connection to the store's database. Coven's
/// member and device views call it to say which ones are this device's.
pub(crate) fn register_device(connection: &Connection, device: DeviceId) -> rusqlite::Result<()> {
    connection.create_scalar_function(
        "_coven_device",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        move |_| Ok(device.0.to_be_bytes().to_vec()),
    )
}

#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn unix_ms(bytes: &[u8]) -> rusqlite::Result<i64> {
    let time = crate::user_file::decode_time(bytes).map_err(|_| damaged())?;
    let millis = match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(elapsed) => i64::try_from(elapsed.as_millis()),
        Err(before) => i64::try_from(before.duration().as_millis()).map(|millis| -millis),
    };
    millis.map_err(|_| damaged())
}

fn damaged() -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(crate::DbError::DamagedDatabase))
}
