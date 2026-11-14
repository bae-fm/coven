//! Coven's own SQL functions, registered on every connection before the
//! app's: what shared triggers, migrations and coven's views call (§4,
//! Appendix C.1).

use coven_foundation::id_source::DeviceId;
use rusqlite::functions::FunctionFlags;
use rusqlite::types::{Value, ValueRef};
use rusqlite::Connection;

use crate::authorization::SqlAuthorization;

/// A function whose result depends on its arguments alone, which views and
/// indexes may call.
const PURE: FunctionFlags = FunctionFlags::SQLITE_UTF8
    .union(FunctionFlags::SQLITE_DETERMINISTIC)
    .union(FunctionFlags::SQLITE_INNOCUOUS);

/// A function of its arguments alone that only top-level statements call:
/// no view, trigger or index.
const INTERNAL: FunctionFlags = FunctionFlags::SQLITE_UTF8
    .union(FunctionFlags::SQLITE_DETERMINISTIC)
    .union(FunctionFlags::SQLITE_DIRECTONLY);

/// What `coven_path_text` returns for its argument: NULL for NULL, the text
/// `path_bytes_text` shows for a path's bytes, and an error for any other
/// value. `test_utils::path_text_sql` hands it to an app's test that
/// registers the function itself on a connection outside any store, so the
/// test gets the result a store does.
pub(crate) fn path_text_sql(value: ValueRef<'_>) -> rusqlite::Result<Value> {
    Ok(match value {
        ValueRef::Null => Value::Null,
        ValueRef::Blob(bytes) => Value::Text(coven_foundation::files::path_bytes_text(bytes)),
        _ => {
            return Err(rusqlite::Error::UserFunctionError(
                "coven_path_text takes a path's bytes, as coven_user_files holds them".into(),
            ))
        }
    })
}

pub(crate) fn register(
    connection: &Connection,
    authorization: &SqlAuthorization,
) -> rusqlite::Result<()> {
    add(
        connection,
        authorization,
        "coven_applying",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        authorization.applying_function(),
    )?;
    add(
        connection,
        authorization,
        "_coven_rowid_changed",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        authorization.rowid_changed_function(),
    )?;
    add(
        connection,
        authorization,
        "_coven_loss_order",
        2,
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_DIRECTONLY,
        |ctx| crate::loss_record::sort_key(ctx.get(0)?, &ctx.get::<Vec<u8>>(1)?),
    )?;
    // A row's key as coven encodes it (format.md D3), so app SQL can join
    // its rows to coven's views and migrations can compare keys.
    add(connection, authorization, "coven_key", -1, PURE, coven_key)?;
    // The device a device-only file is on, as `coven_devices.device` holds
    // device ids, or NULL for a file in storage (§17.2).
    add(connection, authorization, "coven_file_device", 1, PURE, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Text(text) => {
                let text = std::str::from_utf8(text).map_err(|_utf8| invalid_coordinates())?;
                match coven_format::coordinates::Coordinates::decode(text)
                    .map_err(|_invalid| invalid_coordinates())?
                {
                    coven_format::coordinates::Coordinates::DeviceOnly(device) => {
                        Value::Blob(device.0.to_be_bytes().to_vec())
                    }
                    coven_format::coordinates::Coordinates::Cloud(_) => Value::Null,
                }
            }
            _ => return Err(invalid_coordinates()),
        })
    })?;
    // The text that shows a path from the bytes `coven_user_files.path`
    // holds, as `path_bytes_text` gives it (§17.3).
    add(connection, authorization, "coven_path_text", 1, PURE, |ctx| {
        path_text_sql(ctx.get_raw(0))
    })?;
    // What a file record and an app's file reference protect (§17.2,
    // §17.10), for coven's own statements that bring protection up to date a
    // table at a time: the file a reference names, canonical UUID text or
    // NULL; the file a record's key names; and where a record's coordinates
    // store its file, as the uploader's device id, the stored object's id
    // and the object's path `<uploader>/<object>`, the cache's name for it,
    // each NULL for a file on one device only.
    add(connection, authorization, "_coven_file_id", 1, INTERNAL, |ctx| {
        let value = crate::write_encoding::value(ctx.get_raw(0))?;
        Ok(crate::file_table::id(&value).map(|file| file.0.to_string()))
    })?;
    add(connection, authorization, "_coven_file_of_key", 1, INTERNAL, |ctx| {
        crate::file_table::file_of_key(&ctx.get::<Vec<u8>>(0)?)
            .map(|file| file.0.to_string())
            .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))
    })?;
    add(connection, authorization, "_coven_stored_device", 1, INTERNAL, |ctx| {
        Ok(stored(ctx)?.map(|location| location.uploader().0.to_be_bytes().to_vec()))
    })?;
    add(connection, authorization, "_coven_stored_object", 1, INTERNAL, |ctx| {
        Ok(stored(ctx)?.map(|location| location.object()))
    })?;
    add(connection, authorization, "_coven_stored_object_path", 1, INTERNAL, |ctx| {
        Ok(stored(ctx)?.map(|location| {
            crate::database::file_database::object_path(location.uploader(), location.object())
        }))
    })?;
    // A stored time as milliseconds since the Unix epoch, for coven's views.
    add(connection, authorization, "_coven_unix_ms", 1, PURE, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Null => Value::Null,
            ValueRef::Blob(bytes) => Value::Integer(unix_ms(bytes)?),
            _ => return Err(damaged()),
        })
    })?;
    // A stored time exactly, as whole seconds since the Unix epoch rounded
    // down and the nanoseconds past that second, as POSIX's timespec splits
    // it, for `coven_user_files`. No one SQL integer holds every
    // modification time in nanoseconds: Windows gives times back to 1601.
    add(connection, authorization, "_coven_unix_seconds", 1, PURE, |ctx| {
        Ok(Value::Integer(unix_time(ctx)?.0))
    })?;
    add(connection, authorization, "_coven_unix_nanos", 1, PURE, |ctx| {
        Ok(Value::Integer(unix_time(ctx)?.1.into()))
    })?;
    // What a record's JSON names, for `coven_pending`: the variant of an
    // enum value and the bytes of a path written as an array of numbers.
    add(connection, authorization, "_coven_variant_kind", 1, PURE, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Null => Value::Null,
            ValueRef::Text(text) => Value::Text(variant_kind(text)?),
            _ => return Err(damaged()),
        })
    })?;
    add(connection, authorization, "_coven_json_bytes", 1, PURE, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Null => Value::Null,
            ValueRef::Text(text) => Value::Blob(json_bytes(text)?),
            _ => return Err(damaged()),
        })
    })?;
    // What a join request's encoded bytes name, for `coven_invites`: the
    // member the invite fixed, as canonical UUID text, and the name a new
    // person typed, NULL for a recovery.
    add(connection, authorization, "_coven_request_member", 1, PURE, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Null => Value::Null,
            ValueRef::Blob(bytes) => Value::Text(join_request(bytes)?.member.to_string()),
            _ => return Err(damaged()),
        })
    })?;
    add(connection, authorization, "_coven_request_joiner", 1, PURE, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Null => Value::Null,
            ValueRef::Blob(bytes) => match join_request(bytes)?.joiner.person() {
                Some(name) => Value::Text(name.to_owned()),
                None => Value::Null,
            },
            _ => return Err(damaged()),
        })
    })?;
    Ok(())
}

/// The join request `bytes` encode: what an invite's `Waiting` and
/// `Approving` states keep of the request they hold.
#[expect(clippy::map_err_ignore, reason = "a stored value that won't decode is DamagedDatabase")]
fn join_request(bytes: &[u8]) -> rusqlite::Result<coven_format::objects::JoinRequest> {
    match coven_format::Object::decode(bytes).map_err(|_| damaged())? {
        coven_format::Object::JoinRequest(request) => Ok(request),
        _ => Err(damaged()),
    }
}

/// The name of the variant in `json`, which serde wrote for an enum value:
/// a unit variant as its name, any other as an object whose one key is its
/// name. The name in snake case: `KeyCopy` is `key_copy`.
#[expect(clippy::map_err_ignore, reason = "a stored value that won't decode is DamagedDatabase")]
fn variant_kind(json: &[u8]) -> rusqlite::Result<String> {
    let value: serde_json::Value = serde_json::from_slice(json).map_err(|_| damaged())?;
    let name = match &value {
        serde_json::Value::String(name) => name,
        serde_json::Value::Object(fields) if fields.len() == 1 => {
            fields.keys().next().ok_or_else(damaged)?
        }
        _ => return Err(damaged()),
    };
    let mut kind = String::with_capacity(name.len() + 2);
    for (place, letter) in name.chars().enumerate() {
        if letter.is_uppercase() && place > 0 {
            kind.push('_');
        }
        kind.extend(letter.to_lowercase());
    }
    Ok(kind)
}

/// The bytes `json` lists as an array of numbers from 0 to 255.
#[expect(clippy::map_err_ignore, reason = "a stored value that won't decode is DamagedDatabase")]
fn json_bytes(json: &[u8]) -> rusqlite::Result<Vec<u8>> {
    serde_json::from_slice(json).map_err(|_| damaged())
}

/// Register `function` as `name` on `connection`, as `authorization` keeps
/// coven's SQL functions.
fn add<T: rusqlite::functions::SqlFnOutput>(
    connection: &Connection,
    authorization: &SqlAuthorization,
    name: &'static str,
    arguments: std::ffi::c_int,
    flags: FunctionFlags,
    function: impl Fn(&rusqlite::functions::Context<'_>) -> rusqlite::Result<T> + Send + 'static,
) -> rusqlite::Result<()> {
    connection.create_scalar_function(
        name,
        arguments,
        flags,
        authorization.function(name, function),
    )
}

/// Where the coordinates in the function's argument store their file, or
/// `None` for a file on one device only. Coordinates that don't decode are
/// damage.
fn stored(ctx: &rusqlite::functions::Context<'_>) -> rusqlite::Result<Option<crate::FileLocation>> {
    let coordinates = crate::write_encoding::value(ctx.get_raw(0))?;
    match crate::file_location::Place::decode(&coordinates) {
        Ok(crate::file_location::Place::Cloud(location)) => Ok(Some(location)),
        Ok(crate::file_location::Place::DeviceOnly(_)) => Ok(None),
        Err(error) => Err(rusqlite::Error::UserFunctionError(Box::new(error))),
    }
}

/// A row's key from its key values; no values name no row, so the call is
/// refused. SQLite may call it while it plans a statement, as well as when
/// the statement runs.
fn coven_key(ctx: &rusqlite::functions::Context<'_>) -> rusqlite::Result<Vec<u8>> {
    if ctx.is_empty() {
        return Err(rusqlite::Error::UserFunctionError(
            "coven_key takes a row's key: one value or more".into(),
        ));
    }
    let values = (0..ctx.len())
        .map(|i| crate::write_encoding::value(ctx.get_raw(i)))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    crate::write_rows::equality_key(&values)
        .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
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

/// The stored time in the function's argument as `unix_seconds_and_nanos`
/// gives it, so the view and an app's own times agree.
#[expect(clippy::map_err_ignore, reason = "a stored value that won't decode is DamagedDatabase")]
fn unix_time(ctx: &rusqlite::functions::Context<'_>) -> rusqlite::Result<(i64, u32)> {
    let ValueRef::Blob(bytes) = ctx.get_raw(0) else {
        return Err(damaged());
    };
    let time = crate::user_file::decode_time(bytes).map_err(|_| damaged())?;
    Ok(crate::user_file::unix_seconds_and_nanos(time))
}

fn invalid_coordinates() -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(
        "coven_file_device takes a file's coordinates, as coven_files holds them".into(),
    )
}

fn damaged() -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(crate::DbError::DamagedDatabase))
}

#[cfg(test)]
#[path = "coven_functions_tests.rs"]
mod tests;
