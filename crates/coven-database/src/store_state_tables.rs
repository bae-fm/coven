//! SQLite representation of committed membership and audience state.
use crate::{
    sqlite::DatabaseConnection,
    store_state::*,
    write_encoding::{audience, counter, decode_failure},
    DbError,
};
use coven_crypto::{MemberPublicKeys, SealingKey, SigningKey};
use coven_format::admin::MemberRole;
use coven_foundation::id_source::{CircleId, DeviceId, MemberId, StoreId};
use coven_merge::{Audience, Slot};
use rusqlite::{params, types::Type, Row};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn slot(row: &Row<'_>, column: usize) -> rusqlite::Result<Slot> {
    Ok(Slot(counter(row.get(column)?)))
}

/// A stored membership fact that doesn't decode: the database is damaged
/// (§19.3).
fn damaged(column: usize, kind: Type) -> rusqlite::Error {
    decode_failure(column, kind, DbError::DamagedDatabase)
}

/// The value at `column`, which the membership tables' constraints keep
/// decodable; a value that isn't is `DamagedDatabase`.
fn stored<T: rusqlite::types::FromSql>(
    row: &Row<'_>,
    column: usize,
    kind: Type,
) -> rusqlite::Result<T> {
    row.get(column).map_err(|error| match error {
        rusqlite::Error::InvalidColumnType(..)
        | rusqlite::Error::FromSqlConversionFailure(..)
        | rusqlite::Error::IntegralValueOutOfRange(..) => damaged(column, kind),
        error => error,
    })
}

/// A member id as the membership tables hold it: its sixteen UUID bytes.
pub(crate) fn member(row: &Row<'_>, column: usize) -> rusqlite::Result<MemberId> {
    Ok(MemberId(uuid::Uuid::from_bytes(stored(row, column, Type::Blob)?)))
}

/// The member a device registered under, once its registration has applied.
pub(crate) fn device_member(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Option<MemberId>, DbError> {
    Ok(db
        .query("SELECT member FROM _coven_devices WHERE device=?1", [device], |r| member(r, 0))?
        .pop())
}

/// A public key pair as the membership tables hold it: the signing key at
/// `signing`, the sealing key in the column after it.
fn member_keys(row: &Row<'_>, signing: usize) -> rusqlite::Result<MemberPublicKeys> {
    Ok(MemberPublicKeys {
        signing: SigningKey::from_bytes(stored(row, signing, Type::Blob)?)
            .map_err(|_not_a_key| damaged(signing, Type::Blob))?,
        sealing: SealingKey::from_bytes(stored(row, signing + 1, Type::Blob)?),
    })
}

/// A `_coven_members` row, read as
/// `SELECT member,signing,sealing,role,removed,access`. A fact that doesn't
/// decode is `DamagedDatabase`.
pub(crate) fn stored_member(row: &Row<'_>) -> rusqlite::Result<(MemberId, StoreMember)> {
    Ok((
        member(row, 0)?,
        StoreMember {
            keys: member_keys(row, 1)?,
            role: match stored::<String>(row, 3, Type::Text)?.as_str() {
                "admin" => MemberRole::Admin,
                "member" => MemberRole::Member,
                _ => return Err(damaged(3, Type::Text)),
            },
            removed: stored(row, 4, Type::Integer)?,
            access: serde_json::from_slice(&stored::<Vec<u8>>(row, 5, Type::Blob)?)
                .map_err(|_not_access| damaged(5, Type::Blob))?,
        },
    ))
}

/// A `_coven_devices.state` value.
pub(crate) fn device_state(row: &Row<'_>, column: usize) -> rusqlite::Result<DeviceState> {
    match stored::<String>(row, column, Type::Text)?.as_str() {
        "active" => Ok(DeviceState::Active),
        "removed" => Ok(DeviceState::Removed),
        "replaced" => Ok(DeviceState::Replaced),
        _ => Err(damaged(column, Type::Text)),
    }
}

pub(crate) fn circle(text: String) -> rusqlite::Result<CircleId> {
    match audience(&text)? {
        Audience::Circle(circle) => Ok(circle),
        Audience::Store => Err(rusqlite::Error::InvalidQuery),
    }
}

pub(crate) fn deleted_circles(
    db: &DatabaseConnection,
) -> Result<BTreeMap<CircleId, Slot>, DbError> {
    Ok(db
        .query("SELECT circle,slot FROM _coven_circle_deletions", [], |r| {
            Ok((circle(r.get(0)?)?, slot(r, 1)?))
        })?
        .into_iter()
        .collect())
}

pub(crate) fn name(database: &DatabaseConnection) -> Result<String, DbError> {
    database
        .query("SELECT name FROM _coven_store WHERE singleton=1", [], |row| row.get(0))?
        .pop()
        .ok_or(DbError::DamagedDatabase)
}

/// The current name and the slot that set it; a local creation's name counts
/// as set by slot 1, which will publish it (§13.1).
pub(crate) fn named(database: &DatabaseConnection) -> Result<(String, Slot), DbError> {
    database
        .query("SELECT name,named_at FROM _coven_store WHERE singleton=1", [], |row| {
            Ok((row.get(0)?, slot(row, 1)?))
        })?
        .pop()
        .ok_or(DbError::DamagedDatabase)
}

/// Whether the applied history has created the store.
pub(crate) fn created(database: &DatabaseConnection) -> Result<bool, DbError> {
    database.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_store WHERE singleton=1 AND created=1)",
        [],
        |r| r.get(0),
    )
}

pub(crate) fn read(database: &DatabaseConnection) -> Result<StoreState, DbError> {
    let mut state = StoreState::default();
    state.store = database
        .query(
            "SELECT store,name,named_at FROM _coven_store WHERE singleton=1 AND created=1",
            [],
            |r| {
                Ok(StoreIdentity {
                    id: StoreId(uuid::Uuid::from_bytes(r.get(0)?)),
                    name: r.get(1)?,
                    named_at: slot(r, 2)?,
                })
            },
        )?
        .pop();
    state.members = database
        .query(
            "SELECT member,signing,sealing,role,removed,access FROM _coven_members",
            [],
            stored_member,
        )?
        .into_iter()
        .collect();
    state.bound_keys = database
        .query("SELECT signing,sealing FROM _coven_bound_keys", [], |row| member_keys(row, 0))?
        .into_iter()
        .collect();
    state.devices = database
        .query("SELECT device,member,name,state FROM _coven_devices", [], |row| {
            Ok((
                row.get::<_, DeviceId>(0)?,
                StoreDevice {
                    member: member(row, 1)?,
                    name: row.get(2)?,
                    state: device_state(row, 3)?,
                },
            ))
        })?
        .into_iter()
        .collect();
    state.circles = database
        .query("SELECT circle,name FROM _coven_circles", [], |row| {
            Ok((circle(row.get(0)?)?, StoreCircle { name: row.get(1)?, members: BTreeSet::new() }))
        })?
        .into_iter()
        .collect();
    for (circle, member) in
        database.query("SELECT circle,member FROM _coven_circle_members", [], |row| {
            Ok((circle(row.get(0)?)?, member(row, 1)?))
        })?
    {
        state.circles.get_mut(&circle).ok_or(DbError::DamagedDatabase)?.members.insert(member);
    }
    state.circle_deletions = deleted_circles(database)?;
    for boundary in crate::Boundary::load_selected(database)? {
        boundary.apply(&mut state);
    }
    Ok(state)
}
pub(crate) fn replace(database: &DatabaseConnection, state: &StoreState) -> Result<(), DbError> {
    if let Some(store) = &state.store {
        database.internal_execute("INSERT INTO _coven_store(singleton,store,name,named_at,created) VALUES(1,?1,?2,?3,1) ON CONFLICT(singleton) DO UPDATE SET store=excluded.store,name=excluded.name,named_at=excluded.named_at,created=1",params![store.id.0.as_bytes().as_slice(),store.name,store.named_at.0.to_be_bytes().as_slice()])?;
    }
    for table in [
        "_coven_circle_deletions",
        "_coven_circle_members",
        "_coven_devices",
        "_coven_bound_keys",
        "_coven_circles",
        "_coven_members",
    ] {
        database.internal_execute(&format!("DELETE FROM {table}"), [])?;
    }
    for (id, member) in &state.members {
        let role = match member.role {
            MemberRole::Admin => "admin",
            MemberRole::Member => "member",
        };
        database.internal_execute(
            "INSERT INTO _coven_members(member,signing,sealing,role,removed,access)
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                id.0.as_bytes().as_slice(),
                member.keys.signing.as_bytes().as_slice(),
                member.keys.sealing.as_bytes().as_slice(),
                role,
                member.removed,
                serde_json::to_vec(&member.access).expect("member access encoding")
            ],
        )?;
    }
    for keys in &state.bound_keys {
        database.internal_execute(
            "INSERT INTO _coven_bound_keys(signing,sealing) VALUES(?1,?2)",
            params![keys.signing.as_bytes().as_slice(), keys.sealing.as_bytes().as_slice()],
        )?;
    }
    for (id, device) in &state.devices {
        database.internal_execute("INSERT INTO _coven_last_claims(device,number,digest,through) VALUES(?1,zeroblob(8),NULL,zeroblob(8)) ON CONFLICT(device) DO NOTHING",[id])?;
        database.internal_execute(
            "INSERT INTO _coven_devices(device,member,name,state) VALUES(?1,?2,?3,?4)",
            params![
                id,
                device.member.0.as_bytes().as_slice(),
                device.name,
                match device.state {
                    DeviceState::Active => "active",
                    DeviceState::Removed => "removed",
                    DeviceState::Replaced => "replaced",
                }
            ],
        )?;
    }
    for (id, circle) in &state.circles {
        database.internal_execute(
            "INSERT INTO _coven_circles(circle,name) VALUES(?1,?2)",
            params![id.to_string(), circle.name],
        )?;
        for member in &circle.members {
            database.internal_execute(
                "INSERT INTO _coven_circle_members(circle,member) VALUES(?1,?2)",
                (id.to_string(), member.0.as_bytes().as_slice()),
            )?;
        }
    }
    for (circle, entry) in &state.circle_deletions {
        database.internal_execute(
            "INSERT INTO _coven_circle_deletions(circle,slot) VALUES(?1,?2)",
            (circle.to_string(), entry.0.to_be_bytes().as_slice()),
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "store_state_tables_tests.rs"]
pub(crate) mod tests;
