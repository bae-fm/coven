//! Store-log values in their local tables. No replay decisions happen here.

use std::collections::{BTreeMap, BTreeSet};

use coven_crypto::{MemberId, SealingPublicKey};
use coven_format::{
    store_log::{MemberRole, SnapshotId},
    value::EntryId,
    Object,
};
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, StoreId};
use coven_merge::Audience;
use rusqlite::{
    params,
    types::{FromSql, Type},
    Row, ToSql,
};

use crate::{
    sqlite::DatabaseConnection,
    store_log::*,
    write_encoding::{audience, audience_text, counter, decoded},
    DbError, ReplayEntry, StoreLogCheck,
};

pub(crate) fn entry_id(row: &Row<'_>, start: usize) -> rusqlite::Result<EntryId> {
    Ok(EntryId {
        device: DeviceId(counter(row.get(start)?)),
        number: counter(row.get(start + 1)?),
    })
}

fn member(row: &Row<'_>, column: usize) -> rusqlite::Result<MemberId> {
    MemberId::from_bytes(row.get(column)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Blob, Box::new(error))
    })
}

fn circle(text: String) -> rusqlite::Result<CircleId> {
    match audience(&text)? {
        Audience::Circle(circle) => Ok(circle),
        Audience::Store => Err(rusqlite::Error::InvalidQuery),
    }
}

pub(crate) fn outcome(row: &Row<'_>, start: usize) -> rusqlite::Result<EntryOutcome> {
    match row.get::<_, String>(start)?.as_str() {
        "kept" => Ok(EntryOutcome::Kept),
        "beaten" => Ok(EntryOutcome::Dropped(DropReason::BeatenBy(entry_id(
            row,
            start + 1,
        )?))),
        "target" => Ok(EntryOutcome::Dropped(DropReason::TargetGone)),
        "admin" => Ok(EntryOutcome::Dropped(DropReason::NoAdminLeft)),
        "authority" => Ok(EntryOutcome::Dropped(DropReason::NotAllowed)),
        "keys" => Ok(EntryOutcome::Dropped(DropReason::WrongCircleKeys)),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

pub(crate) fn deleted_circles(
    database: &DatabaseConnection,
) -> Result<BTreeSet<CircleId>, DbError> {
    Ok(database
        .query(
            "SELECT circle FROM _coven_circles WHERE deleted=1",
            [],
            |row| circle(row.get(0)?),
        )?
        .into_iter()
        .collect())
}

pub(crate) fn current_store_key(database: &DatabaseConnection) -> Result<Option<KeyId>, DbError> {
    Ok(database
        .query("SELECT key FROM _coven_store", [], |row| {
            Ok(KeyId(uuid::Uuid::from_bytes(row.get(0)?)))
        })?
        .into_iter()
        .next())
}

pub(crate) fn read(database: &DatabaseConnection) -> Result<StoreLog, DbError> {
    let mut log = StoreLog::default();
    for (entry, outcome) in database.query(
        "SELECT record,outcome,beaten_device,beaten_number,author_view FROM _coven_store_log",
        [],
        |row| {
            let Object::StoreLog(entry) = decoded(Object::decode(&row.get::<_, Vec<u8>>(0)?))?
            else {
                return Err(rusqlite::Error::InvalidQuery);
            };
            let outcome = outcome(row, 1)?;
            let check = StoreLogCheck::decode(&row.get::<_, Vec<u8>>(4)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(4, Type::Blob, Box::new(error))
            })?;
            Ok((ReplayEntry { entry, check }, outcome))
        },
    )? {
        log.replay.entries.insert(entry.entry.position, outcome);
        log.entries.push(entry);
    }
    log.entries.sort_by_key(|entry| entry.entry.timestamp);
    let state = &mut log.replay.state;
    state.members = database
        .query(
            "SELECT member,sealing,role,removed,access FROM _coven_members",
            [],
            |row| {
                let role = match row.get::<_, String>(2)?.as_str() {
                    "admin" => MemberRole::Admin,
                    "member" => MemberRole::Member,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                Ok((
                    member(row, 0)?,
                    StoreMember {
                        sealing: SealingPublicKey::from_bytes(row.get(1)?),
                        role,
                        removed: row.get(3)?,
                        access: serde_json::from_slice(&row.get::<_, Vec<u8>>(4)?).map_err(
                            |e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    4,
                                    Type::Blob,
                                    Box::new(e),
                                )
                            },
                        )?,
                    },
                ))
            },
        )?
        .into_iter()
        .collect();
    state.devices = database
        .query(
            "SELECT device,member,name,removed FROM _coven_devices",
            [],
            |row| {
                Ok((
                    DeviceId(counter(row.get(0)?)),
                    StoreDevice {
                        member: member(row, 1)?,
                        name: row.get(2)?,
                        removed: row.get(3)?,
                    },
                ))
            },
        )?
        .into_iter()
        .collect();
    state.circles = database
        .query(
            "SELECT circle,name,key,deleted FROM _coven_circles",
            [],
            |row| {
                Ok((
                    circle(row.get(0)?)?,
                    StoreCircle {
                        name: row.get(1)?,
                        key: KeyId(uuid::Uuid::from_bytes(row.get(2)?)),
                        deleted: row.get(3)?,
                        members: BTreeSet::new(),
                    },
                ))
            },
        )?
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
    state.store = database
        .query("SELECT id,name,key FROM _coven_store", [], |row| {
            let text: String = row.get(0)?;
            let id = uuid::Uuid::parse_str(&text).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })?;
            Ok(StoreIdentity {
                id: StoreId(id),
                name: row.get(1)?,
                key: KeyId(uuid::Uuid::from_bytes(row.get(2)?)),
            })
        })?
        .into_iter()
        .next();
    state.schema = read_versions(database, "schema")?;
    state.format = read_versions(database, "format")?;
    state.resets = database
        .query(
            "SELECT audience,snapshot_device,snapshot_number
             FROM _coven_resets",
            [],
            |row| {
                let audience = audience(&row.get::<_, String>(0)?)?;
                Ok((
                    audience.clone(),
                    SnapshotId {
                        audience,
                        device: DeviceId(counter(row.get(1)?)),
                        number: counter(row.get(2)?),
                    },
                ))
            },
        )?
        .into_iter()
        .collect();
    Ok(log)
}

fn read_versions<N: FromSql>(
    database: &DatabaseConnection,
    kind: &str,
) -> Result<BTreeMap<Audience, StoreVersion<N>>, DbError> {
    Ok(database
        .query(
            "SELECT version,snapshot_device,snapshot_number,entry_device,entry_number,audience
             FROM _coven_versions WHERE kind=?1",
            [kind],
            |row| {
                let audience = audience(&row.get::<_, String>(5)?)?;
                Ok((
                    audience.clone(),
                    StoreVersion {
                        number: row.get(0)?,
                        snapshot: SnapshotId {
                            audience,
                            device: DeviceId(counter(row.get(1)?)),
                            number: counter(row.get(2)?),
                        },
                        entry: entry_id(row, 3)?,
                    },
                ))
            },
        )?
        .into_iter()
        .collect())
}

pub(crate) fn replace(
    database: &DatabaseConnection,
    replay: &StoreLogReplay,
    previous: &BTreeMap<EntryId, EntryOutcome>,
) -> Result<(), DbError> {
    for (entry, outcome) in replay
        .entries
        .iter()
        .filter(|(id, outcome)| previous.get(id) != Some(outcome))
    {
        let (tag, beaten) = match outcome {
            EntryOutcome::Kept => ("kept", None),
            EntryOutcome::Dropped(reason) => match reason {
                DropReason::BeatenBy(entry) => ("beaten", Some(entry)),
                DropReason::TargetGone => ("target", None),
                DropReason::NoAdminLeft => ("admin", None),
                DropReason::NotAllowed => ("authority", None),
                DropReason::WrongCircleKeys => ("keys", None),
            },
        };
        database.internal_execute(
            "UPDATE _coven_store_log SET outcome=?1,beaten_device=?2,beaten_number=?3
             WHERE device=?4 AND number=?5",
            params![
                tag,
                beaten.map(|e| e.device.0.to_be_bytes().to_vec()),
                beaten.map(|e| e.number.to_be_bytes().to_vec()),
                entry.device.0.to_be_bytes().as_slice(),
                entry.number.to_be_bytes().as_slice()
            ],
        )?;
    }
    for table in [
        "_coven_circle_members",
        "_coven_devices",
        "_coven_circles",
        "_coven_members",
        "_coven_store",
        "_coven_versions",
        "_coven_resets",
    ] {
        database.internal_execute(&format!("DELETE FROM {table}"), [])?;
    }
    let state = &replay.state;
    for (id, member) in &state.members {
        let role = match member.role {
            MemberRole::Admin => "admin",
            MemberRole::Member => "member",
        };
        database.internal_execute(
            "INSERT INTO _coven_members(member,sealing,role,removed,access) VALUES(?1,?2,?3,?4,?5)",
            params![
                id.to_bytes().as_slice(),
                member.sealing.as_bytes().as_slice(),
                role,
                member.removed,
                serde_json::to_vec(&member.access).expect("member access encoding")
            ],
        )?;
    }
    for (id, device) in &state.devices {
        database.internal_execute(
            "INSERT INTO _coven_devices(device,member,name,removed) VALUES(?1,?2,?3,?4)",
            params![
                id.0.to_be_bytes().as_slice(),
                device.member.to_bytes().as_slice(),
                device.name,
                device.removed
            ],
        )?;
    }
    for (id, circle) in &state.circles {
        database.internal_execute(
            "INSERT INTO _coven_circles(circle,name,key,deleted) VALUES(?1,?2,?3,?4)",
            params![
                id.to_string(),
                circle.name,
                circle.key.0.as_bytes().as_slice(),
                circle.deleted
            ],
        )?;
        for member in &circle.members {
            database.internal_execute(
                "INSERT INTO _coven_circle_members(circle,member) VALUES(?1,?2)",
                (id.to_string(), member.to_bytes().as_slice()),
            )?;
        }
    }
    if let Some(store) = &state.store {
        database.internal_execute(
            "INSERT INTO _coven_store(id,name,key) VALUES(?1,?2,?3)",
            (
                store.id.to_string(),
                &store.name,
                store.key.0.as_bytes().as_slice(),
            ),
        )?;
    }
    for (audience, version) in &state.schema {
        put_version(database, "schema", audience, version)?;
    }
    for (audience, version) in &state.format {
        put_version(database, "format", audience, version)?;
    }
    for (audience, snapshot) in &state.resets {
        database.internal_execute(
            "INSERT INTO _coven_resets(audience,snapshot_device,snapshot_number)
             VALUES(?1,?2,?3)",
            (
                audience_text(audience),
                snapshot.device.0.to_be_bytes().as_slice(),
                snapshot.number.to_be_bytes().as_slice(),
            ),
        )?;
    }
    Ok(())
}

fn put_version<N: ToSql>(
    database: &DatabaseConnection,
    kind: &str,
    audience: &Audience,
    version: &StoreVersion<N>,
) -> Result<(), DbError> {
    database.internal_execute(
        "INSERT INTO _coven_versions(kind,audience,version,snapshot_device,snapshot_number,
                                    entry_device,entry_number)
         VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            kind,
            audience_text(audience),
            version.number,
            version.snapshot.device.0.to_be_bytes().as_slice(),
            version.snapshot.number.to_be_bytes().as_slice(),
            version.entry.device.0.to_be_bytes().as_slice(),
            version.entry.number.to_be_bytes().as_slice()
        ],
    )?;
    Ok(())
}
