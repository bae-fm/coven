//! SQLite representation of committed membership and audience state.
use crate::{
    sqlite::DatabaseConnection,
    store_state::*,
    write_encoding::{damaged, stored},
    DbError,
};
use coven_crypto::{MemberPublicKeys, SealingKey, SigningKey};
use coven_format::admin::MemberRole;
use coven_foundation::id_source::{CircleId, DeviceId, MemberId, StoreId};
use coven_merge::Slot;
use rusqlite::{params, types::Type, Row};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn slot(row: &Row<'_>, column: usize) -> rusqlite::Result<Slot> {
    crate::write_encoding::slot_counter(row.get(column)?)
}

/// A member id as the membership tables hold it: its canonical text.
pub(crate) fn member(row: &Row<'_>, column: usize) -> rusqlite::Result<MemberId> {
    stored(row, column, Type::Text)
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
/// `SELECT member,signing,sealing,role,removed,access,joined_circle,joined_person`.
/// A fact that doesn't
/// decode is `DamagedDatabase`.
pub(crate) fn stored_member(row: &Row<'_>) -> rusqlite::Result<(MemberId, StoreMember)> {
    let joined = match stored::<Option<CircleId>>(row, 6, Type::Text)? {
        Some(circle) => Some(Joined { person: stored(row, 7, Type::Text)?, circle }),
        None => None,
    };
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
            access: stored::<Option<Vec<u8>>>(row, 5, Type::Blob)?
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()
                .map_err(|_not_access| damaged(5, Type::Blob))?,
            joined,
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

/// A circle id as the membership and audience tables hold it: its canonical
/// text.
pub(crate) fn circle(row: &Row<'_>, column: usize) -> rusqlite::Result<CircleId> {
    stored(row, column, Type::Text)
}

pub(crate) fn deleted_circles(
    db: &DatabaseConnection,
) -> Result<BTreeMap<CircleId, Slot>, DbError> {
    Ok(db
        .query("SELECT circle,slot FROM _coven_circle_deletions", [], |r| {
            Ok((circle(r, 0)?, slot(r, 1)?))
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

/// Save the name a store has before its applied history creates it, with
/// the slot that set it: a local store's chosen name, which slot 1 will set
/// (§10.3), or the name a join's invite carried, current at the slot the
/// inviting device had applied, for a store a join published before it
/// loaded (§13.2). Once the applied history has created the store, its name
/// is the one the slots set. `joined` marks a store a join published: its
/// device is registered only by the admission its first load applies, and
/// until then it refuses synced writes (`awaiting_registration`).
pub(crate) fn name_before_creation(
    database: &DatabaseConnection,
    store: StoreId,
    name: &str,
    named_at: Slot,
    joined: bool,
) -> Result<(), DbError> {
    database.internal_execute(
        "INSERT INTO _coven_store(singleton,store,name,named_at,created,joined) VALUES(1,?1,?2,?3,0,?4) ON CONFLICT(singleton) DO UPDATE SET name=excluded.name,named_at=excluded.named_at,joined=excluded.joined WHERE created=0",
        params![store, name, named_at.get().to_be_bytes().as_slice(), joined],
    )?;
    Ok(())
}

/// Whether this store was published by a join and the state it has applied
/// does not yet register `device`. A synced write made now would record the
/// slot it had applied, below the registration, and every other device would
/// refuse it for lack of a device to author it (§7.1, §13.4).
pub(crate) fn awaiting_registration(
    database: &DatabaseConnection,
    device: DeviceId,
) -> Result<bool, DbError> {
    database.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_store WHERE singleton=1 AND joined=1)
            AND NOT EXISTS(SELECT 1 FROM _coven_devices WHERE device=?1)",
        [device],
        |r| r.get(0),
    )
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
            |r| Ok(StoreIdentity { id: r.get(0)?, name: r.get(1)?, named_at: slot(r, 2)? }),
        )?
        .pop();
    state.members = database
        .query(
            "SELECT member,signing,sealing,role,removed,access,joined_circle,joined_person FROM _coven_members",
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
        .query("SELECT circle,name,base FROM _coven_circles", [], |row| {
            Ok((
                circle(row, 0)?,
                StoreCircle { name: row.get(1)?, members: BTreeSet::new(), base: row.get(2)? },
            ))
        })?
        .into_iter()
        .collect();
    for (circle, member) in
        database.query("SELECT circle,member FROM _coven_circle_members", [], |row| {
            Ok((circle(row, 0)?, member(row, 1)?))
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
        database.internal_execute("INSERT INTO _coven_store(singleton,store,name,named_at,created,joined) VALUES(1,?1,?2,?3,1,0) ON CONFLICT(singleton) DO UPDATE SET store=excluded.store,name=excluded.name,named_at=excluded.named_at,created=1",params![store.id,store.name,store.named_at.get().to_be_bytes().as_slice()])?;
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
            "INSERT INTO _coven_members(member,signing,sealing,role,removed,access,joined_circle,joined_person)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                id,
                member.keys.signing.as_bytes().as_slice(),
                member.keys.sealing.as_bytes().as_slice(),
                role,
                member.removed,
                member
                    .access
                    .as_ref()
                    .map(|access| serde_json::to_vec(access).expect("member access encoding")),
                member.joined.as_ref().map(|joined| joined.circle),
                member.joined.as_ref().and_then(|joined| joined.person.as_deref())
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
        crate::sequence::note_device(database, *id)?;
        database.internal_execute(
            "INSERT INTO _coven_devices(device,member,name,state) VALUES(?1,?2,?3,?4)",
            params![
                id,
                device.member,
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
            "INSERT INTO _coven_circles(circle,name,base) VALUES(?1,?2,?3)",
            params![id, circle.name, circle.base],
        )?;
        for member in &circle.members {
            database.internal_execute(
                "INSERT INTO _coven_circle_members(circle,member) VALUES(?1,?2)",
                (id, member),
            )?;
        }
    }
    for (circle, entry) in &state.circle_deletions {
        database.internal_execute(
            "INSERT INTO _coven_circle_deletions(circle,slot) VALUES(?1,?2)",
            (circle, entry.get().to_be_bytes().as_slice()),
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "store_state_tables_tests.rs"]
pub(crate) mod tests;
