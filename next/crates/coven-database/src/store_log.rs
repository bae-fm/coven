//! The replay's values and the atomic boundary between sync and SQLite (§9).

use std::collections::{BTreeMap, BTreeSet};

use coven_crypto::{MemberId, SealingPublicKey};
use coven_format::store_log::{MemberRole, SnapshotId, StoreLogEntry};
use coven_format::{value::EntryId, Object};
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, StoreId};
use coven_merge::Audience;

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};

/// Applied entries and their replay, read from one committed state (§9).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreLog {
    /// Every applied, checked entry, in timestamp order, including dropped entries.
    pub entries: Vec<StoreLogEntry>,
    /// The result computed by sync for exactly these entries.
    pub replay: StoreLogReplay,
}

/// The whole result of replaying the applied entries; the database never replays them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreLogReplay {
    /// Members, devices, circles, keys, versions and resets selected by replay.
    pub state: StoreLogState,
    /// One kept or dropped mark for every applied entry, including a drop's reason.
    pub entries: BTreeMap<EntryId, EntryOutcome>,
}

/// Store-log state, including removed identities needed to check older writes (§10).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreLogState {
    /// Absent before the first entry creates the store.
    pub store: Option<StoreIdentity>,
    /// Every member added by a kept entry, including removed members.
    pub members: BTreeMap<MemberId, StoreMember>,
    /// Every device added by a kept entry, including removed devices.
    pub devices: BTreeMap<DeviceId, StoreDevice>,
    /// Every circle made by a kept entry, including deleted circles.
    pub circles: BTreeMap<CircleId, StoreCircle>,
    /// The selected schema raise per audience; creation itself carries no raise.
    pub schema: BTreeMap<Audience, StoreVersion<u32>>,
    /// The selected format raise per audience; creation itself carries no raise.
    pub format: BTreeMap<Audience, StoreVersion<u16>>,
    /// The last kept reset of each audience (§19.3).
    pub resets: BTreeMap<Audience, SnapshotId>,
}

/// The created store and the current key selected in replay order (§11).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    /// The store's id.
    pub id: StoreId,
    /// Its name.
    pub name: String,
    /// The latest kept entry's store key.
    pub key: KeyId,
}

/// A member's signing key is the key in [`StoreLogState::members`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreMember {
    /// The public key used to seal store and circle keys to this member.
    pub sealing: SealingPublicKey,
    /// The member's role, retained when removed.
    pub role: MemberRole,
    /// Whether a kept removal removed this member.
    pub removed: bool,
}

/// One install, retained after removal so its older writes remain attributable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreDevice {
    /// The member who added the device.
    pub member: MemberId,
    /// The device's name from its addition.
    pub name: String,
    /// Whether the device or its member has been removed.
    pub removed: bool,
}

/// A circle and its current membership; deleted circles retain their name and key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreCircle {
    /// The last kept name.
    pub name: String,
    /// The current key selected in replay order (§11).
    pub key: KeyId,
    /// Whether a kept deletion or removal deleted the circle.
    pub deleted: bool,
    /// Current members; empty after deletion.
    pub members: BTreeSet<MemberId>,
}

/// A selected version raise and the snapshot it chose (§17).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreVersion<N> {
    /// The raised schema or format version.
    pub number: N,
    /// The snapshot in this version.
    pub snapshot: SnapshotId,
    /// The kept raise that selected it.
    pub entry: EntryId,
}

/// An applied entry's disposition in the latest replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryOutcome {
    /// Applied, including a change already in place.
    Kept,
    /// Ignored for this replay and reported to its author.
    Dropped(DropReason),
}

/// Why replay dropped an entry (§9, §20.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// A conflicting concurrent entry beat it, even if that entry later dropped.
    BeatenBy(EntryId),
    /// A member, device or circle needed at its place in replay did not exist.
    TargetGone,
    /// Its effect would leave no admin.
    NoAdminLeft,
    /// Its author's view did not authorize the change, including an unseen device.
    NotAllowed,
    /// A removal did not replace exactly the shared circle keys in its author's view.
    WrongCircleKeys,
}

pub(crate) fn apply(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    entry: StoreLogEntry,
    replay: StoreLogReplay,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    let bytes = Object::StoreLog(entry.clone()).encode().map_err(|error| {
        DbError::InvalidStoreLogEntry {
            entry: entry.position,
            error,
        }
    })?;
    database.transaction(|database| {
        let mut applied = BTreeSet::new();
        for (position, record) in database.query(
            "SELECT device,number,record FROM coven_store_log",
            [],
            |row| Ok((crate::store_log_tables::entry_id(row, 0)?, row.get::<_, Vec<u8>>(2)?)),
        )? {
            if position == entry.position && record != bytes {
                return Err(DbError::StoreLogEntryChanged(position));
            }
            applied.insert(position);
        }
        applied.insert(entry.position);
        if !applied.iter().eq(replay.entries.keys()) {
            return Err(DbError::StoreLogEntriesChanged);
        }
        database.internal_execute(
            "INSERT INTO coven_store_log(device,number,record,outcome) VALUES(?1,?2,?3,'kept') ON CONFLICT(device,number) DO NOTHING",
            (entry.position.device.0.to_be_bytes().as_slice(), entry.position.number.to_be_bytes().as_slice(), &bytes),
        )?;
        let previous = crate::store_log_tables::deleted_circles(database)?;
        crate::store_log_tables::replace(database, &replay)?;
        let deleted = crate::store_log_tables::deleted_circles(database)?;
        let mut touched = BTreeSet::new();
        for circle in previous.symmetric_difference(&deleted) {
            touched.extend(database.query(
                "SELECT DISTINCT table_name,key,audience FROM coven_rows WHERE audience=?1",
                [circle.to_string()], crate::row_queries::read_identity,
            )?);
        }
        let visible = crate::write_rows::AppView::after(database, schema);
        let store = crate::merge_store::MergeStore::new(database, &visible);
        let affected = crate::write_apply::WriteApply::new(database, schema, &store, &visible, &visible, &deleted)
            .apply(None, touched)?;
        files.retain_rows(affected, &deleted)?;
        files.before_commit()?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "store_log_tests.rs"]
pub(crate) mod tests;
