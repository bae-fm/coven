//! Store-log values in the six local tables. No replay decisions happen here.

use std::collections::BTreeSet;

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
    DbError,
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

pub(crate) fn read(database: &DatabaseConnection) -> Result<StoreLog, DbError> {
    let mut log = StoreLog::default();
    for (entry, outcome) in database.query(
        "SELECT record,outcome,beaten_device,beaten_number FROM coven_store_log",
        [],
        |row| {
            let Object::StoreLog(entry) = decoded(Object::decode(&row.get::<_, Vec<u8>>(0)?))?
            else {
                return Err(rusqlite::Error::InvalidQuery);
            };
            let outcome = match row.get::<_, String>(1)?.as_str() {
                "kept" => EntryOutcome::Kept,
                "beaten" => EntryOutcome::Dropped(DropReason::BeatenBy(entry_id(row, 2)?)),
                "target" => EntryOutcome::Dropped(DropReason::TargetGone),
                "admin" => EntryOutcome::Dropped(DropReason::NoAdminLeft),
                "authority" => EntryOutcome::Dropped(DropReason::NotAllowed),
                "keys" => EntryOutcome::Dropped(DropReason::WrongCircleKeys),
                _ => return Err(rusqlite::Error::InvalidQuery),
            };
            Ok((entry, outcome))
        },
    )? {
        log.replay.entries.insert(entry.position, outcome);
        log.entries.push(entry);
    }
    log.entries.sort_by_key(|entry| entry.timestamp);
    let state = &mut log.replay.state;
    state.members = database
        .query(
            "SELECT member,sealing,role,removed FROM coven_members",
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
                    },
                ))
            },
        )?
        .into_iter()
        .collect();
    state.devices = database
        .query(
            "SELECT device,member,name,removed FROM coven_devices",
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
            "SELECT circle,name,key,deleted FROM coven_circles",
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
        "SELECT circle,member FROM coven_circle_members",
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
        .query(
            "SELECT store,name,key FROM coven_store_state WHERE kind='store'",
            [],
            |row| {
                let text: String = row.get(0)?;
                let id = uuid::Uuid::parse_str(&text).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
                })?;
                Ok(StoreIdentity {
                    id: StoreId(id),
                    name: row.get(1)?,
                    key: KeyId(uuid::Uuid::from_bytes(row.get(2)?)),
                })
            },
        )?
        .into_iter()
        .next();
    state.schema = read_version(database, "schema")?;
    state.format = read_version(database, "format")?;
    state.resets = database.query("SELECT audience,snapshot_device,snapshot_number FROM coven_store_state WHERE kind='reset'", [], |row| {
        let audience = audience(&row.get::<_, String>(0)?)?;
        Ok((audience.clone(), SnapshotId { audience, device: DeviceId(counter(row.get(1)?)), number: counter(row.get(2)?) }))
    })?.into_iter().collect();
    Ok(log)
}

fn read_version<N: FromSql>(
    database: &DatabaseConnection,
    kind: &str,
) -> Result<Option<StoreVersion<N>>, DbError> {
    Ok(database.query("SELECT version,snapshot_device,snapshot_number,entry_device,entry_number FROM coven_store_state WHERE kind=?1", [kind], |row| {
        Ok(StoreVersion { number: row.get(0)?, snapshot: SnapshotId { audience: Audience::Store, device: DeviceId(counter(row.get(1)?)), number: counter(row.get(2)?) }, entry: entry_id(row, 3)? })
    })?.into_iter().next())
}

pub(crate) fn replace(
    database: &DatabaseConnection,
    replay: &StoreLogReplay,
) -> Result<(), DbError> {
    for (entry, outcome) in &replay.entries {
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
        database.internal_execute("UPDATE coven_store_log SET outcome=?1,beaten_device=?2,beaten_number=?3 WHERE device=?4 AND number=?5", params![tag, beaten.map(|e| e.device.0.to_be_bytes().to_vec()), beaten.map(|e| e.number.to_be_bytes().to_vec()), entry.device.0.to_be_bytes().as_slice(), entry.number.to_be_bytes().as_slice()])?;
    }
    for table in [
        "coven_circle_members",
        "coven_devices",
        "coven_circles",
        "coven_members",
        "coven_store_state",
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
            "INSERT INTO coven_members(member,sealing,role,removed) VALUES(?1,?2,?3,?4)",
            params![
                id.to_bytes().as_slice(),
                member.sealing.as_bytes().as_slice(),
                role,
                member.removed
            ],
        )?;
    }
    for (id, device) in &state.devices {
        database.internal_execute(
            "INSERT INTO coven_devices(device,member,name,removed) VALUES(?1,?2,?3,?4)",
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
            "INSERT INTO coven_circles(circle,name,key,deleted) VALUES(?1,?2,?3,?4)",
            params![
                id.to_string(),
                circle.name,
                circle.key.0.as_bytes().as_slice(),
                circle.deleted
            ],
        )?;
        for member in &circle.members {
            database.internal_execute(
                "INSERT INTO coven_circle_members(circle,member) VALUES(?1,?2)",
                (id.to_string(), member.to_bytes().as_slice()),
            )?;
        }
    }
    if let Some(store) = &state.store {
        database.internal_execute("INSERT INTO coven_store_state(kind,audience,store,name,key) VALUES('store','store',?1,?2,?3)", (store.id.to_string(), &store.name, store.key.0.as_bytes().as_slice()))?;
    }
    if let Some(version) = &state.schema {
        put_version(database, "schema", version)?;
    }
    if let Some(version) = &state.format {
        put_version(database, "format", version)?;
    }
    for (audience, snapshot) in &state.resets {
        database.internal_execute("INSERT INTO coven_store_state(kind,audience,snapshot_device,snapshot_number) VALUES('reset',?1,?2,?3)", (audience_text(audience), snapshot.device.0.to_be_bytes().as_slice(), snapshot.number.to_be_bytes().as_slice()))?;
    }
    Ok(())
}

fn put_version<N: ToSql>(
    database: &DatabaseConnection,
    kind: &str,
    version: &StoreVersion<N>,
) -> Result<(), DbError> {
    database.internal_execute("INSERT INTO coven_store_state(kind,audience,version,snapshot_device,snapshot_number,entry_device,entry_number) VALUES(?1,'store',?2,?3,?4,?5,?6)", params![kind, version.number, version.snapshot.device.0.to_be_bytes().as_slice(), version.snapshot.number.to_be_bytes().as_slice(), version.entry.device.0.to_be_bytes().as_slice(), version.entry.number.to_be_bytes().as_slice()])?;
    Ok(())
}
