//! SQLite representation of committed membership and audience state.
use crate::sqlite::StoreSql;
use crate::{
    store_state::*,
    write_encoding::{damaged, stored},
    DbError,
};
use coven_crypto::{MemberPublicKeys, SealingKey, SigningKey};
use coven_foundation::id_source::{CircleId, DeviceId, MemberId, StoreId};
use coven_merge::Slot;
use sqlive::rusqlite::{params, types::Type, Row};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn slot(row: &Row<'_>, column: usize) -> sqlive::rusqlite::Result<Slot> {
    crate::write_encoding::slot_counter(row.get(column)?)
}

/// A member id as the membership tables hold it: its canonical text.
pub(crate) fn member(row: &Row<'_>, column: usize) -> sqlive::rusqlite::Result<MemberId> {
    stored(row, column, Type::Text)
}

/// The member a device registered under, once its registration has applied.
pub(crate) fn device_member(db: &StoreSql, device: DeviceId) -> Result<Option<MemberId>, DbError> {
    Ok(db
        .query("SELECT member FROM _coven_devices WHERE device=?1", [device], |r| member(r, 0))?
        .pop())
}

/// Whether `member` is an active admin in the counted state.
pub(crate) fn is_admin(db: &StoreSql, member: MemberId) -> Result<bool, DbError> {
    let role = db
        .query("SELECT role FROM _coven_members WHERE member=?1 AND removed=0", [member], |r| {
            stored::<coven_format::admin::MemberRole>(r, 0, Type::Text)
        })?
        .pop();
    Ok(role == Some(coven_format::admin::MemberRole::Admin))
}

/// A public key pair as the membership tables hold it: the signing key at
/// `signing`, the sealing key in the column after it.
fn member_keys(row: &Row<'_>, signing: usize) -> sqlive::rusqlite::Result<MemberPublicKeys> {
    Ok(MemberPublicKeys {
        signing: SigningKey::from_bytes(stored(row, signing, Type::Blob)?)
            .map_err(|_not_a_key| damaged(signing, Type::Blob))?,
        sealing: SealingKey::from_bytes(stored(row, signing + 1, Type::Blob)?),
    })
}

/// A `_coven_members` row, read as
/// `SELECT member,signing,sealing,role,removed,access,joined_circle,joined_person,keys_by`.
/// A fact that doesn't
/// decode is `DamagedDatabase`.
pub(crate) fn stored_member(row: &Row<'_>) -> sqlive::rusqlite::Result<(MemberId, StoreMember)> {
    stored_member_at(row, 0)
}

/// [`stored_member`], whose nine columns start at column `first`.
fn stored_member_at(
    row: &Row<'_>,
    first: usize,
) -> sqlive::rusqlite::Result<(MemberId, StoreMember)> {
    let joined =
        Joined { person: stored(row, first + 7, Type::Text)?, circle: circle(row, first + 6)? };
    Ok((
        member(row, first)?,
        StoreMember {
            keys: member_keys(row, first + 1)?,
            keys_by: row.get(first + 8)?,
            role: stored(row, first + 3, Type::Text)?,
            removed: stored(row, first + 4, Type::Integer)?,
            access: stored::<Option<Vec<u8>>>(row, first + 5, Type::Blob)?
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()
                .map_err(|_not_access| damaged(first + 5, Type::Blob))?,
            joined,
        },
    ))
}

/// A circle id as the membership and audience tables hold it: its canonical
/// text.
pub(crate) fn circle(row: &Row<'_>, column: usize) -> sqlive::rusqlite::Result<CircleId> {
    stored(row, column, Type::Text)
}

pub(crate) fn deleted_circles(db: &StoreSql) -> Result<BTreeMap<CircleId, Slot>, DbError> {
    Ok(db
        .query("SELECT circle,slot FROM _coven_circle_deletions", [], |r| {
            Ok((circle(r, 0)?, slot(r, 1)?))
        })?
        .into_iter()
        .collect())
}

pub(crate) fn name(database: &StoreSql) -> Result<String, DbError> {
    crate::live_reads::once(&name_read(), database)
}

/// The store's name.
pub(crate) fn name_read() -> crate::live_reads::CovenRead<String, String> {
    let sql = "SELECT name FROM _coven_store WHERE singleton=1";
    crate::live_reads::CovenRead::new(sql, crate::live_reads::unbound, |row| row.get(0))
        .then(|mut names| names.pop().ok_or(DbError::DamagedDatabase))
}

/// The current name and the slot that set it; a local creation's name counts
/// as set by slot 1, which will publish it (§13.1).
pub(crate) fn named(database: &StoreSql) -> Result<(String, Slot), DbError> {
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
    database: &StoreSql,
    store: StoreId,
    name: &str,
    named_at: Slot,
    joined: bool,
) -> Result<(), DbError> {
    database.execute(
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
    database: &StoreSql,
    device: DeviceId,
) -> Result<bool, DbError> {
    Ok(database.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_store WHERE singleton=1 AND joined=1)
            AND NOT EXISTS(SELECT 1 FROM _coven_devices WHERE device=?1)",
        [device],
        |r| r.get(0),
    )?)
}

/// Whether the applied history has created the store.
pub(crate) fn created(database: &StoreSql) -> Result<bool, DbError> {
    Ok(database.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_store WHERE singleton=1 AND created=1)",
        [],
        |r| r.get(0),
    )?)
}

/// Every fact of the applied state, one row each, as one statement: the
/// first column names the part a row belongs to, and the others hold the part's
/// columns in the order the part's reader decodes them.
const STATE: &str = "SELECT 'store',store,name,named_at,NULL,NULL,NULL,NULL,NULL,NULL
     FROM _coven_store WHERE singleton=1 AND created=1
   UNION ALL
     SELECT 'member',member,signing,sealing,role,removed,access,joined_circle,joined_person,keys_by
     FROM _coven_members
   UNION ALL
     SELECT 'bound_key',signing,sealing,NULL,NULL,NULL,NULL,NULL,NULL,NULL FROM _coven_bound_keys
   UNION ALL
     SELECT 'device',device,member,name,state,NULL,NULL,NULL,NULL,NULL FROM _coven_devices
   UNION ALL
     SELECT 'circle',circle,name,base,NULL,NULL,NULL,NULL,NULL,NULL FROM _coven_circles
   UNION ALL
     SELECT 'circle_member',circle,member,NULL,NULL,NULL,NULL,NULL,NULL,NULL
     FROM _coven_circle_members
   UNION ALL
     SELECT 'circle_deletion',circle,slot,NULL,NULL,NULL,NULL,NULL,NULL,NULL
     FROM _coven_circle_deletions
   UNION ALL
     SELECT 'boundary',kind,version,audience,snapshot_device,snapshot_number,slot,included,applied,NULL
     FROM _coven_boundaries";

/// One row of [`STATE`], decoded.
enum Fact {
    Store(StoreIdentity),
    Member(MemberId, StoreMember),
    BoundKey(MemberPublicKeys),
    Device(DeviceId, StoreDevice),
    Circle(CircleId, StoreCircle),
    CircleMember(CircleId, MemberId),
    CircleDeletion(CircleId, Slot),
    Boundary(crate::write_boundary::RetainedBoundary),
}

fn fact(row: &Row<'_>) -> sqlive::rusqlite::Result<Fact> {
    Ok(match row.get::<_, String>(0)?.as_str() {
        "store" => Fact::Store(StoreIdentity {
            id: row.get(1)?,
            name: row.get(2)?,
            named_at: slot(row, 3)?,
        }),
        "member" => {
            let (id, member) = stored_member_at(row, 1)?;
            Fact::Member(id, member)
        }
        "bound_key" => Fact::BoundKey(member_keys(row, 1)?),
        "device" => Fact::Device(
            row.get(1)?,
            StoreDevice {
                member: member(row, 2)?,
                name: row.get(3)?,
                state: stored(row, 4, Type::Text)?,
            },
        ),
        "circle" => Fact::Circle(
            circle(row, 1)?,
            StoreCircle { name: row.get(2)?, members: BTreeSet::new(), base: row.get(3)? },
        ),
        "circle_member" => Fact::CircleMember(circle(row, 1)?, member(row, 2)?),
        "circle_deletion" => Fact::CircleDeletion(circle(row, 1)?, slot(row, 2)?),
        "boundary" => Fact::Boundary(crate::write_boundary::retained_boundary(row, 1)?),
        _ => return Err(damaged(0, Type::Text)),
    })
}

/// The applied state, from one statement over the membership and audience
/// tables and the retained boundaries.
pub(crate) fn read(database: &StoreSql) -> Result<StoreState, DbError> {
    crate::live_reads::once(&state_read(), database)
}

/// The applied state, from one statement over the membership and audience
/// tables and the retained boundaries.
pub(crate) fn state_read() -> crate::live_reads::CovenRead<impl Send + 'static, StoreState> {
    crate::live_reads::CovenRead::new(STATE, crate::live_reads::unbound, fact).then(state_of)
}

/// The applied state `facts`, the rows of `STATE`, give.
fn state_of(facts: Vec<Fact>) -> Result<StoreState, DbError> {
    let mut state = StoreState::default();
    let mut circle_members = Vec::new();
    let mut boundaries = Vec::new();
    for fact in facts {
        match fact {
            Fact::Store(store) => state.store = Some(store),
            Fact::Member(id, member) => {
                state.members.insert(id, member);
            }
            Fact::BoundKey(keys) => {
                state.bound_keys.insert(keys);
            }
            Fact::Device(id, device) => {
                state.devices.insert(id, device);
            }
            Fact::Circle(id, circle) => {
                state.circles.insert(id, circle);
            }
            Fact::CircleMember(circle, member) => circle_members.push((circle, member)),
            Fact::CircleDeletion(circle, slot) => {
                state.circle_deletions.insert(circle, slot);
            }
            Fact::Boundary(boundary) => boundaries.push(boundary),
        }
    }
    for (circle, member) in circle_members {
        state.circles.get_mut(&circle).ok_or(DbError::DamagedDatabase)?.members.insert(member);
    }
    // Later boundaries of an audience replace earlier ones.
    boundaries.sort_by_key(|row| row.boundary.entry());
    for row in boundaries {
        row.boundary.apply(&mut state);
    }
    Ok(state)
}
/// The statement that stores one member, with the values [`member_values`] binds.
pub(crate) const INSERT_MEMBER: &str =
    "INSERT INTO _coven_members(member,signing,sealing,role,removed,access,joined_circle,joined_person,keys_by)
     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)";

/// `member`'s values for [`INSERT_MEMBER`], in the order its placeholders take them.
pub(crate) fn member_values<'a>(
    id: &'a MemberId,
    member: &'a StoreMember,
) -> impl sqlive::rusqlite::Params + 'a {
    (
        id,
        member.keys.signing.as_bytes().as_slice(),
        member.keys.sealing.as_bytes().as_slice(),
        member.role,
        member.removed,
        member
            .access
            .as_ref()
            .map(|access| serde_json::to_vec(access).expect("member access encoding")),
        member.joined.circle,
        member.joined.person.as_deref(),
        member.keys_by,
    )
}

pub(crate) fn replace(database: &StoreSql, state: &StoreState) -> Result<(), DbError> {
    if let Some(store) = &state.store {
        database.execute("INSERT INTO _coven_store(singleton,store,name,named_at,created,joined) VALUES(1,?1,?2,?3,1,0) ON CONFLICT(singleton) DO UPDATE SET store=excluded.store,name=excluded.name,named_at=excluded.named_at,created=1",params![store.id,store.name,store.named_at.get().to_be_bytes().as_slice()])?;
    }
    for table in [
        "_coven_circle_deletions",
        "_coven_circle_members",
        "_coven_devices",
        "_coven_bound_keys",
        "_coven_circles",
        "_coven_members",
    ] {
        database.execute(&format!("DELETE FROM {table}"), [])?;
    }
    for (id, member) in &state.members {
        database.execute(INSERT_MEMBER, member_values(id, member))?;
    }
    for keys in &state.bound_keys {
        database.execute(
            "INSERT INTO _coven_bound_keys(signing,sealing) VALUES(?1,?2)",
            params![keys.signing.as_bytes().as_slice(), keys.sealing.as_bytes().as_slice()],
        )?;
    }
    for (id, device) in &state.devices {
        crate::sequence::note_device(database, *id)?;
        database.execute(
            "INSERT INTO _coven_devices(device,member,name,state) VALUES(?1,?2,?3,?4)",
            params![id, device.member, device.name, device.state],
        )?;
    }
    for (id, circle) in &state.circles {
        database.execute(
            "INSERT INTO _coven_circles(circle,name,base) VALUES(?1,?2,?3)",
            params![id, circle.name, circle.base],
        )?;
        for member in &circle.members {
            database.execute(
                "INSERT INTO _coven_circle_members(circle,member) VALUES(?1,?2)",
                (id, member),
            )?;
        }
    }
    for (circle, entry) in &state.circle_deletions {
        database.execute(
            "INSERT INTO _coven_circle_deletions(circle,slot) VALUES(?1,?2)",
            (circle, entry.get().to_be_bytes().as_slice()),
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "store_state_tables_tests.rs"]
pub(crate) mod tests;
