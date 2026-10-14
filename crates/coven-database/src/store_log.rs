//! The replay's values and the atomic boundary between sync and SQLite (§9).

use std::collections::{BTreeMap, BTreeSet};

use coven_crypto::MemberPublicKeys;
use coven_format::store_log::{MemberRole, SnapshotId};
use coven_format::{value::EntryId, Object};
use coven_foundation::id_source::{CircleId, DeviceId, MemberId, StoreId};
use coven_merge::Audience;

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError, ReplayEntry};

pub(crate) fn positions(
    database: &DatabaseConnection,
) -> Result<coven_format::value::EntryPositions, DbError> {
    Ok(coven_format::value::EntryPositions(database.query(
        "SELECT device,max(number) FROM _coven_store_log GROUP BY device ORDER BY device",
        [],
        |row| crate::store_log_tables::entry_id(row, 0),
    )?))
}

/// Applied entries and their replay, read from one committed state (§9).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreLog {
    /// Every applied entry, including dropped entries, with its first publication
    /// time and immutable checks, in timestamp order.
    pub entries: Vec<ReplayEntry>,
    /// The result computed by sync for exactly these entries.
    pub replay: StoreLogReplay,
    /// Greatest certified cutoff since the Unix epoch. Absence differs from a
    /// certified empty prefix. Duration preserves the provider tick's precision
    /// even on platforms whose SystemTime has coarser precision.
    pub horizon: Option<std::time::Duration>,
}

impl StoreLog {
    /// Read identity from the first kept creation, using the persisted marks.
    /// The id and name have no separately persisted replay copy.
    pub(crate) fn created_store(&self) -> Option<StoreIdentity> {
        use coven_format::store_log::StoreChange;
        self.entries.iter().find_map(|applied| {
            let StoreChange::CreateStore { store, name, .. } = &applied.entry.change else {
                return None;
            };
            (self.replay.entries.get(&applied.entry.position) == Some(&EntryOutcome::Kept)).then(
                || StoreIdentity {
                    id: *store,
                    name: name.clone(),
                },
            )
        })
    }

    /// Finality is a strict comparison with the one persisted horizon (§9).
    pub fn is_final(&self, entry: EntryId) -> bool {
        self.horizon.is_some_and(|horizon| {
            self.entries.iter().any(|applied| {
                applied.entry.position == entry
                    && applied
                        .stored_at
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("nonnegative provider time")
                        < horizon
            })
        })
    }

    /// Circles `next` makes readable to `member` that this applied log already
    /// created while `member` could not read them (§14.4). Their parts may have
    /// been skipped under positions that have since passed, so each one reloads
    /// from storage: after leaving, after deletion, and after a dropped creation.
    /// A write with a part in a circle reads a creation of that circle that its
    /// author's view kept. A circle whose creation first arrives with `next`
    /// has no skipped part, and neither does a creation that landed too late,
    /// which no view ever keeps (§9).
    pub fn returning_circles(&self, next: &StoreLogState, member: &MemberId) -> BTreeSet<CircleId> {
        use coven_format::store_log::StoreChange;
        self.entries
            .iter()
            .filter(|applied| !applied.check.landed_too_late())
            .filter_map(|applied| match &applied.entry.change {
                StoreChange::CreateCircle { circle, .. } => Some(*circle),
                _ => None,
            })
            .filter(|circle| {
                next.reads_circle(*circle, member)
                    && !self.replay.state.reads_circle(*circle, member)
            })
            .collect()
    }

    /// Effective raise/reset entries in replay order, excluding kept no-ops.
    pub fn boundaries(&self) -> impl Iterator<Item = crate::Boundary> + '_ {
        crate::Boundary::select(self.entries.iter().filter_map(|entry| {
            (self.replay.entries[&entry.entry.position] == EntryOutcome::Kept)
                .then(|| crate::Boundary::from_entry(&entry.entry))
                .flatten()
        }))
    }

    /// Applied positions represented by these entries, including dropped entries.
    pub fn positions(&self) -> coven_format::value::EntryPositions {
        let mut positions = BTreeMap::new();
        for applied in &self.entries {
            let id = applied.entry.position;
            positions
                .entry(id.device)
                .and_modify(|number: &mut u64| *number = (*number).max(id.number))
                .or_insert(id.number);
        }
        coven_format::value::EntryPositions(
            positions
                .into_iter()
                .map(|(device, number)| EntryId { device, number })
                .collect(),
        )
    }
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
///
/// Schema versions belong to each audience: the store's version is
/// under [`Audience::Store`], and each circle has its own. A version is absent
/// until a kept raise selects it, as in Appendix C. These are independent of the
/// local app migration version returned by [`crate::Database::schema_version`].
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
    /// Deleted-circle causes selected after replay, including circles whose
    /// creation dropped. Every cause is a kept entry; absence means no deletion.
    pub circle_deletions: BTreeMap<CircleId, EntryId>,
    /// The selected schema raise per audience; creation itself carries no raise.
    pub schema: BTreeMap<Audience, StoreVersion>,
    /// The last kept reset of each audience (§19.3).
    pub resets: BTreeMap<Audience, SnapshotId>,
}

impl StoreLogState {
    /// Whether `member` remains in the store and in `circle`, which exists and
    /// is not deleted (§14.3).
    pub fn reads_circle(&self, circle: CircleId, member: &MemberId) -> bool {
        self.members.get(member).is_some_and(|m| !m.removed)
            && !self.circle_deletions.contains_key(&circle)
            && self
                .circles
                .get(&circle)
                .is_some_and(|c| c.members.contains(member))
    }

    /// Every circle `member` reads under [`Self::reads_circle`]: the circles
    /// whose parts its devices open, snapshot and reload (§14.4, §15).
    pub fn readable_circles(&self, member: &MemberId) -> BTreeSet<CircleId> {
        self.circles
            .keys()
            .copied()
            .filter(|circle| self.reads_circle(*circle, member))
            .collect()
    }
}

/// The created store, as its kept creation entry names it (§9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    /// The store's id.
    pub id: StoreId,
    /// Its name.
    pub name: String,
}

impl StoreIdentity {
    /// Opening requires directory settings to match checked creation (§9).
    pub fn check_settings(
        &self,
        settings: &coven_foundation::files::StoreSettings,
    ) -> Result<(), DbError> {
        check_creation(self.id, &self.name, settings.id, &settings.name)
    }
}

/// A creation's id and name must be the ones the directory settings name (§9).
pub(crate) fn check_creation(
    store: StoreId,
    name: &str,
    settings_store: StoreId,
    settings_name: &str,
) -> Result<(), DbError> {
    if store != settings_store || name != settings_name {
        return Err(DbError::StoreSettingsMismatch {
            store,
            name: name.into(),
        });
    }
    Ok(())
}

/// A member as replay has them, keyed by stable id in [`StoreLogState::members`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreMember {
    /// The current public pair: the signing key that verifies this member's
    /// objects in this view and the sealing key copies go to (§9, §11).
    pub keys: MemberPublicKeys,
    /// Current access selected by replay. Revocation also reads dropped and
    /// superseded access from [`StoreLog::entries`] (§13).
    pub access: coven_format::MemberAccess,
    /// The member's role, retained when removed.
    pub role: MemberRole,
    /// Whether a kept removal removed this member.
    pub removed: bool,
}

/// One install, retained after removal or replacement so its older objects
/// remain attributable (§10).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreDevice {
    /// The member who added the device.
    pub member: MemberId,
    /// The device's name from its addition.
    pub name: String,
    /// Whether kept entries removed or replaced it.
    pub state: DeviceState,
}

impl StoreDevice {
    /// Neither removed nor replaced: it may still author objects (§10).
    pub fn active(&self) -> bool {
        self.state == DeviceState::Active
    }

    /// Removed, by its own removal or its member's: it stops for good.
    pub fn removed(&self) -> bool {
        self.state == DeviceState::Removed
    }
}

/// What kept entries did to a device id (E9). Replacement carries no log
/// ends: older objects are judged by their recorded reads (§10, D6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceState {
    /// Registered and neither removed nor replaced.
    Active,
    /// A kept removal of the device or of its member.
    Removed,
    /// A kept add-device entry names this id as the one it replaces.
    Replaced,
}

/// A circle and its current membership; deleted circles retain their name.
/// Its keys come from received introductions, not replay state (§11).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreCircle {
    /// The last kept name.
    pub name: String,
    /// Current members; empty after deletion.
    pub members: BTreeSet<MemberId>,
}

/// A selected version raise and the snapshot it chose (§17).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreVersion {
    /// The raised schema version.
    pub number: u32,
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
    /// Ignored by replay and reported to its author.
    Dropped(DropReason),
}

/// The member, device or circle named by a rejected store-log change (§9, E5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryTarget {
    /// A store member or a member of a circle.
    Member(MemberId),
    /// A device identity.
    Device(DeviceId),
    /// A circle identity.
    Circle(CircleId),
}

/// Why replay dropped an entry (§9, E5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// First publication was more than thirty days after an unread stored entry.
    /// This entry is consumed and excluded permanently, including from author views.
    LandedTooLate,
    /// A conflicting concurrent entry beat it, even if that entry later dropped.
    BeatenBy(EntryId),
    /// A member, device or circle needed at its place in replay did not exist.
    TargetGone(EntryTarget),
    /// Its effect would leave no admin.
    NoAdminLeft,
    /// Its author's view did not authorize the change, including an unseen device.
    NotAllowed,
}

pub(crate) fn apply<P: crate::PendingValue>(
    database: &DatabaseConnection,
    before: &DatabaseConnection,
    schema: &WriteSchema,
    incoming: &[ReplayEntry],
    replay: &StoreLogReplay,
    circles: &crate::entry_effects::CircleReads,
    files: &crate::file_write::FileWrite<'_>,
    operations: &[crate::NewOperation],
    updates: &[crate::OperationUpdate],
    access_keys: &[crate::AccessKeyDeletion],
    provider_access: &[crate::ProviderAccessChange],
) -> Result<(), DbError> {
    let encoded = incoming
        .iter()
        .map(|incoming| {
            let bytes = Object::StoreLog(incoming.entry.clone())
                .encode()
                .map_err(|error| DbError::InvalidStoreLogEntry {
                    entry: incoming.entry.position,
                    error,
                })?;
            Ok((
                incoming,
                bytes,
                incoming.check.encode(),
                crate::user_file::encode_time(incoming.stored_at),
                crate::write_encoding::encoded(coven_format::merge_fields::encode_timestamp(
                    &incoming.entry.timestamp,
                ))?,
            ))
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    database.transaction(|database| {
        let previous_boundaries = crate::Boundary::load_selected(database)?;
        for (incoming, bytes, author_view, stored_at, _) in &encoded {
            let id = incoming.entry.position;
            for (record, view, time) in database.query(
                "SELECT record,author_view,stored_at FROM _coven_store_log WHERE device=?1 AND number=?2",
                (id.device.0.to_be_bytes().as_slice(), id.number.to_be_bytes().as_slice()),
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, Vec<u8>>(2)?)),
            )? {
                if &record != bytes || &view != author_view || &time != stored_at {
                    return Err(DbError::StoreLogEntryChanged(id));
                }
            }
        }
        let outcomes: BTreeMap<_, _> = database
            .query(
                "SELECT device,number,outcome,beaten_device,beaten_number,target FROM _coven_store_log",
                [],
                |row| {
                    Ok((
                        crate::store_log_tables::entry_id(row, 0)?,
                        crate::store_log_tables::outcome(row, 2)?,
                    ))
                },
            )?
            .into_iter()
            .collect();
        let mut applied: BTreeSet<_> = outcomes.keys().copied().collect();
        applied.extend(incoming.iter().map(|entry| entry.entry.position));
        if !applied.iter().eq(replay.entries.keys()) {
            return Err(DbError::StoreLogEntriesChanged);
        }
        for (incoming, bytes, author_view, stored_at, timestamp) in &encoded {
            let id = incoming.entry.position;
            let (retires_device, retires_member) = crate::write_landing::retires(&incoming.entry);
            database.internal_execute(
                "INSERT INTO _coven_store_log(device,number,record,author_view,stored_at,outcome,timestamp,retires_device,retires_member)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(device,number) DO NOTHING",
                rusqlite::params![id.device.0.to_be_bytes().as_slice(), id.number.to_be_bytes().as_slice(),
                 bytes, author_view, stored_at,
                 if incoming.check.landed_too_late() { "late" } else { "kept" }, timestamp,
                 retires_device, retires_member],
            )?;
            if let Some(boundary) = crate::Boundary::from_entry(&incoming.entry) {
                boundary.record(database)?;
            }
            crate::store_log_upload::retire(database, id, bytes)?;
        }
        let previous = crate::store_log_tables::deleted_circles(database)?;
        crate::store_log_tables::replace(database, replay, &outcomes)?;
        let deleted = crate::store_log_tables::deleted_circles(database)?;
        // Retired writes whose window verdict this replay changed (§10).
        let rejudged = crate::write_landing::rejudge::<P>(database)?;
        let mut affected = crate::entry_effects::replay::<P>(
            database,
            before,
            schema,
            &previous_boundaries,
            &deleted,
            circles,
            &rejudged,
        )?;
        let mut touched = BTreeSet::new();
        let changed: BTreeSet<_> = previous.keys().chain(deleted.keys()).copied()
            .filter(|circle| previous.get(circle) != deleted.get(circle)).collect();
        for circle in changed {
            touched.extend(database.query(
                "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE audience=?1",
                [circle.to_string()],
                crate::row_queries::read_identity,
            )?);
        }
        let visible = crate::write_rows::AppView::after(database, schema);
        let store = crate::merge_store::MergeStore::new(database, &visible);
        affected.extend(crate::write_apply::WriteApply::new(
            database, schema, &store, &visible, &visible, &deleted,
        )
        .apply(None, touched)?);
        files.retain_rows(affected, &deleted)?;
        files.before_commit()?;
        for update in updates {
            crate::operation::advance(database, update)?;
        }
        for operation in operations {
            crate::operation::insert(database, operation)?;
        }
        let own = database.pending_device().ok_or(DbError::DamagedDatabase)?;
        crate::operation::record_access_keys::<P>(database, own, access_keys)?;
        crate::provider_access::adopt::<P>(database, own, provider_access)?;
        for (id, outcome) in &replay.entries {
            // A reset can clear a report without changing the entry's fate.
            // Only a changed replay outcome creates or removes that report.
            if id.device != own || outcomes.get(id) == Some(outcome) {
                continue;
            }
            match outcome {
                EntryOutcome::Kept => {
                    crate::pending::clear_log(database, crate::LogObject::Entry(*id))?;
                }
                EntryOutcome::Dropped(reason) => {
                    crate::pending::save(database, &P::dropped_entry(*id, reason.clone()))?;
                }
            }
        }
        for entry in incoming {
            if entry.entry.position.device != own {
                crate::pending::clear_log(database, crate::LogObject::Entry(entry.entry.position))?;
            }
        }
        crate::entry_inputs::release(database)?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "store_log_tests.rs"]
pub(crate) mod tests;
