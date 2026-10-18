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
pub(crate) fn member(row: &Row<'_>, column: usize) -> rusqlite::Result<MemberId> {
    Ok(MemberId(uuid::Uuid::from_bytes(row.get(column)?)))
}

/// A `_coven_devices.state` value.
pub(crate) fn device_state(text: String) -> rusqlite::Result<DeviceState> {
    match text.as_str() {
        "active" => Ok(DeviceState::Active),
        "removed" => Ok(DeviceState::Removed),
        "replaced" => Ok(DeviceState::Replaced),
        _ => Err(rusqlite::Error::InvalidQuery),
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
        .query(
            "SELECT name FROM _coven_store WHERE singleton=1",
            [],
            |row| row.get(0),
        )?
        .pop()
        .ok_or(DbError::DamagedDatabase)
}

pub(crate) fn read(database: &DatabaseConnection) -> Result<StoreState, DbError> {
    let mut state = StoreState::default();
    state.store = database
        .query(
            "SELECT store,name FROM _coven_store WHERE singleton=1 AND created=1",
            [],
            |r| {
                Ok(StoreIdentity {
                    id: StoreId(uuid::Uuid::from_bytes(r.get(0)?)),
                    name: r.get(1)?,
                })
            },
        )?
        .pop();
    state.members = database
        .query(
            "SELECT member,signing,sealing,role,removed,access FROM _coven_members",
            [],
            |row| {
                let role = match row.get::<_, String>(3)?.as_str() {
                    "admin" => MemberRole::Admin,
                    "member" => MemberRole::Member,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                Ok((
                    member(row, 0)?,
                    StoreMember {
                        keys: MemberPublicKeys {
                            signing: SigningKey::from_bytes(row.get(1)?)
                                .map_err(|error| decode_failure(1, Type::Blob, error))?,
                            sealing: SealingKey::from_bytes(row.get(2)?),
                        },
                        role,
                        removed: row.get(4)?,
                        access: serde_json::from_slice(&row.get::<_, Vec<u8>>(5)?)
                            .map_err(|error| decode_failure(5, Type::Blob, error))?,
                    },
                ))
            },
        )?
        .into_iter()
        .collect();
    state.bound_keys = database
        .query("SELECT signing,sealing FROM _coven_bound_keys", [], |row| {
            Ok(MemberPublicKeys {
                signing: SigningKey::from_bytes(row.get(0)?)
                    .map_err(|error| decode_failure(0, Type::Blob, error))?,
                sealing: SealingKey::from_bytes(row.get(1)?),
            })
        })?
        .into_iter()
        .collect();
    state.devices = database
        .query(
            "SELECT device,member,name,state FROM _coven_devices",
            [],
            |row| {
                Ok((
                    DeviceId(counter(row.get(0)?)),
                    StoreDevice {
                        member: member(row, 1)?,
                        name: row.get(2)?,
                        state: device_state(row.get(3)?)?,
                    },
                ))
            },
        )?
        .into_iter()
        .collect();
    state.circles = database
        .query("SELECT circle,name FROM _coven_circles", [], |row| {
            Ok((
                circle(row.get(0)?)?,
                StoreCircle {
                    name: row.get(1)?,
                    members: BTreeSet::new(),
                },
            ))
        })?
        .into_iter()
        .collect();
    for (circle, member) in database.query(
        "SELECT circle,member FROM _coven_circle_members",
        [],
        |row| Ok((circle(row.get(0)?)?, member(row, 1)?)),
    )? {
        state
            .circles
            .get_mut(&circle)
            .ok_or(DbError::DamagedDatabase)?
            .members
            .insert(member);
    }
    state.circle_deletions = deleted_circles(database)?;
    for boundary in crate::Boundary::load_selected(database)? {
        boundary.apply(&mut state);
    }
    Ok(state)
}
pub(crate) fn replace(database: &DatabaseConnection, state: &StoreState) -> Result<(), DbError> {
    if let Some(store) = &state.store {
        database.internal_execute("INSERT INTO _coven_store(singleton,store,name,created) VALUES(1,?1,?2,1) ON CONFLICT(singleton) DO UPDATE SET store=excluded.store,name=excluded.name,created=1",params![store.id.0.as_bytes().as_slice(),store.name])?;
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
            params![
                keys.signing.as_bytes().as_slice(),
                keys.sealing.as_bytes().as_slice()
            ],
        )?;
    }
    for (id, device) in &state.devices {
        database.internal_execute("INSERT INTO _coven_last_claims(device,number,digest,through) VALUES(?1,zeroblob(8),NULL,zeroblob(8)) ON CONFLICT(device) DO NOTHING",[id.0.to_be_bytes().as_slice()])?;
        database.internal_execute(
            "INSERT INTO _coven_devices(device,member,name,state) VALUES(?1,?2,?3,?4)",
            params![
                id.0.to_be_bytes().as_slice(),
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
