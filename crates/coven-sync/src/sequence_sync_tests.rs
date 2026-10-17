use crate::*;
use coven_crypto::{
    custody::{InMemoryCustody, KeySession},
    MemberIdentity, StoreKey, StoreKeyring,
};
use coven_database::{Database, DatabaseBuilder, Migration, RowIdentity, SyncedTable};
use coven_format::admin::{MemberRole, StoreChange};
use coven_foundation::{
    files::StoreLayout,
    id_source::{IdSource, KeyId, MemberId, SequentialIds, StoreId},
};
use coven_storage::{test_utils::MemoryStorage, ObjectPath, Storage};
use std::sync::Arc;

#[cfg(test)]
pub(crate) struct Device {
    pub(crate) _temp: tempfile::TempDir,
    pub(crate) sync: Arc<SequenceSync>,
    pub(crate) storage: MemoryStorage,
    pub(crate) identity: MemberIdentity,
}
impl Device {
    pub(crate) fn database(&self) -> &Database {
        &self.sync.database
    }
}
#[cfg(test)]
pub(crate) struct Group {
    pub(crate) ids: Arc<SequentialIds>,
    pub(crate) store: StoreId,
    pub(crate) ring: StoreKeyring,
    pub(crate) storage: MemoryStorage,
    pub(crate) clock: Arc<coven_foundation::clock::FixedClock>,
    /// The app schema version installations open at.
    pub(crate) version: std::sync::atomic::AtomicU32,
    /// Whether a breaking migration journals the raises passes publish.
    pub(crate) raises: std::sync::atomic::AtomicBool,
}
impl Group {
    pub(crate) fn new() -> Self {
        let ids = Arc::new(SequentialIds::new());
        let store = StoreId(ids.new_id());
        let ring = StoreKeyring::new(StoreKey::from_bytes(KeyId(ids.new_id()), [7; 32]));
        let clock = Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::UNIX_EPOCH,
        ));
        Self {
            ids,
            store,
            ring,
            storage: MemoryStorage::builder()
                .clock(clock.clone())
                .build()
                .unwrap(),
            clock,
            version: 1.into(),
            raises: false.into(),
        }
    }
    pub(crate) async fn device(&self, seed: u8) -> Device {
        let temp = tempfile::tempdir().unwrap();
        StoreLayout::new(temp.path().into())
            .create_store_dir(self.store, "sequence tests", self.ids.as_ref())
            .unwrap();
        let identity = MemberIdentity::new(
            MemberId(self.ids.new_id()),
            coven_format::test_utils::keys_from_seed(seed),
        );
        self.open(temp, identity).await
    }
    /// Close `device` and open it again beside a copy of its installation,
    /// device id included, as restoring a backup with intact custody does.
    pub(crate) async fn copy(&self, device: Device) -> (Device, Device) {
        device.sync.database.close().await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        copy_tree(device._temp.path(), temp.path());
        let identity = device.identity.clone();
        let original = self.open(device._temp, identity.clone()).await;
        (original, self.open(temp, identity).await)
    }
    /// Close `device` and open its installation again at the group's
    /// current schema version.
    pub(crate) async fn reopen(&self, device: Device) -> Device {
        device.sync.database.close().await.unwrap();
        self.open(device._temp, device.identity).await
    }
    pub(crate) async fn open(&self, temp: tempfile::TempDir, identity: MemberIdentity) -> Device {
        self.open_with(
            temp,
            identity,
            Some(self.ring.clone()),
            self.storage.for_device(),
        )
        .await
    }
    /// A new installation of `identity` whose store custody holds no key yet,
    /// reaching storage through `storage` (§12).
    pub(crate) async fn newcomer(
        &self,
        identity: MemberIdentity,
        storage: MemoryStorage,
    ) -> Device {
        let (temp, _) = self.directory();
        self.open_with(temp, identity, None, storage).await
    }
    /// A new installation's unpublished directory.
    pub(crate) fn directory(&self) -> (tempfile::TempDir, coven_foundation::files::StoreDir) {
        let temp = tempfile::tempdir().unwrap();
        let directory = StoreLayout::new(temp.path().into())
            .create_store_dir(self.store, "sequence tests", self.ids.as_ref())
            .unwrap();
        (temp, directory)
    }
    /// A group on a provider that shares the store with accounts.
    pub(crate) fn shared() -> Self {
        let mut group = Self::new();
        group.storage = MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::Dropbox)
            .clock(group.clock.clone())
            .build()
            .unwrap();
        group
    }
    pub(crate) async fn open_with(
        &self,
        temp: tempfile::TempDir,
        identity: MemberIdentity,
        ring: Option<StoreKeyring>,
        storage: MemoryStorage,
    ) -> Device {
        let directory = StoreLayout::new(temp.path().into()).store_dir(&self.store);
        let mut database = DatabaseBuilder::new(directory.clone());
        if self.raises.load(std::sync::atomic::Ordering::SeqCst) {
            database = database.migration_operation(SequenceSync::migration_operation);
        }
        let database = database
            .synced_tables(vec![
                SyncedTable::new("notes", RowIdentity::SharedKey),
                SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience"),
            ])
            .migrations(
                [
                    Migration::sql(
                        1,
                        "notes",
                        "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL);
                         CREATE TABLE pins(id TEXT PRIMARY KEY NOT NULL,audience TEXT NOT NULL,
                           title TEXT NOT NULL)",
                    ),
                    Migration::sql(2, "titles", "CREATE INDEX titles ON notes(title)"),
                ]
                .into_iter()
                .take(self.version.load(std::sync::atomic::Ordering::SeqCst) as usize)
                .collect(),
            )
            .open()
            .await
            .unwrap();
        let device = directory.settings().unwrap().device_id;
        let state = Arc::new(SyncState::new());
        let clock: coven_foundation::clock::ClockRef = self.clock.clone();
        let waits = Arc::new(
            RetryWaits::open(database.clone(), clock.clone())
                .await
                .unwrap(),
        );
        let transfers = Arc::new(coven_storage::TransferBudget::new(TransferLimits::default()));
        let storage = storage
            .with_transfers(transfers.clone())
            .with_publication_gate(state.clone())
            .with_cooldown(waits.clone());
        let ids: coven_foundation::id_source::IdSourceRef = self.ids.clone();
        let files = Files::new(
            coven_database::FileDatabase::new(database.clone()),
            directory.clone(),
            Some(Arc::new(storage.clone())),
            state.clone(),
            waits.clone(),
            ids,
            transfers,
        );
        let sync = Arc::new(
            SequenceSync::open(
                database,
                Arc::new(storage.clone()),
                directory,
                device,
                Arc::new(
                    KeySession::store(Arc::new(match ring {
                        Some(ring) => InMemoryCustody::new(ring),
                        None => InMemoryCustody::empty(),
                    }))
                    .unwrap(),
                ),
                Arc::new(
                    KeySession::member(Arc::new(InMemoryCustody::new(identity.clone()))).unwrap(),
                ),
                state,
                clock,
                self.ids.clone(),
                waits,
                files,
            )
            .await
            .unwrap(),
        );
        Device {
            _temp: temp,
            sync,
            storage,
            identity,
        }
    }
    pub(crate) async fn create(&self, ana: &Device) {
        self.create_with_access(ana, access("ana")).await;
    }
    pub(crate) async fn create_with_access(
        &self,
        ana: &Device,
        access: coven_format::MemberAccess,
    ) {
        self.create_as(ana, access).await.unwrap();
    }
    pub(crate) async fn create_result(&self, ana: &Device) -> Result<(), SyncError> {
        self.create_as(ana, access("ana")).await
    }
    async fn create_as(
        &self,
        ana: &Device,
        access: coven_format::MemberAccess,
    ) -> Result<(), SyncError> {
        let key = self.ring.store_key_ids().next().unwrap();
        ana.sync
            .administer(StoreChange::CreateStore {
                store: self.store,
                name: "sequence tests".into(),
                admin: ana.identity.keys().public_keys(),
                access,
                device_name: "Ana".into(),
                key,
                key_hash: self.ring.store_key(key).unwrap().hash(),
            })
            .await
    }
    pub(crate) async fn admit(&self, ana: &Device, ben: &Device) {
        ana.sync
            .administer(StoreChange::AddMember {
                member: ben.identity.member(),
                keys: ben.identity.keys().public_keys(),
                role: MemberRole::Admin,
                access: access("ben"),
            })
            .await
            .unwrap();
        ben.sync
            .administer(StoreChange::AddDevice {
                name: "Ben".into(),
                replaces: None,
            })
            .await
            .unwrap();
        ana.sync.pass().await.unwrap();
    }
}
pub(crate) fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
pub(crate) fn access(name: &str) -> coven_format::MemberAccess {
    coven_format::MemberAccess::S3AccessKey {
        access_key_id: name.into(),
        principal: coven_format::S3Principal::Console,
    }
}
pub(crate) async fn insert(db: &Database, id: &'static str, title: &'static str) {
    db.write(move |sql| {
        sql.execute("INSERT INTO notes VALUES(?1,?2)", (id, title))?;
        Ok(())
    })
    .await
    .unwrap();
}
pub(crate) async fn pin(
    db: &Database,
    id: &'static str,
    circle: coven_foundation::id_source::CircleId,
    title: &'static str,
) {
    db.write(move |sql| {
        sql.execute(
            "INSERT INTO pins VALUES(?1,?2,?3)",
            (id, circle.to_string(), title),
        )?;
        Ok(())
    })
    .await
    .unwrap();
}
pub(crate) async fn pins(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        Ok(
            sql.query("SELECT id,title FROM pins ORDER BY id", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?,
        )
    })
    .await
    .unwrap()
}
pub(crate) async fn rows(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        Ok(
            sql.query("SELECT id,title FROM notes ORDER BY id", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?,
        )
    })
    .await
    .unwrap()
}

/// §14.4: applied membership controls reading even when a retained key opens
/// a retry authored before departure; rejoining reloads the skipped history.
#[tokio::test]
async fn a_departed_reader_consumes_old_key_parts_unopened_until_rejoining() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember {
            circle,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(
        ben.sync
            .keys
            .read()
            .unwrap()
            .unwrap()
            .circle_key_ids(circle)
            .count(),
        1
    );
    pin(
        ana.database(),
        "00000000-0000-4000-8000-000000000001",
        circle,
        "before departure",
    )
    .await;
    insert(ana.database(), "store", "readable").await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let leave = async {
        observed.await.unwrap();
        ben.sync
            .administer(StoreChange::RemoveCircleMember {
                circle,
                member: ben.identity.member(),
            })
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (sent, ()) = tokio::join!(ana.sync.pass(), leave);
    sent.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(
        rows(ben.database()).await,
        [("store".into(), "readable".into())]
    );
    assert!(
        pins(ben.database()).await.is_empty(),
        "retaining a key does not retain reading permission"
    );
    assert!(ben
        .database()
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .is_empty());
    ana.sync
        .administer(StoreChange::AddCircleMember {
            circle,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(pins(ben.database()).await.len(), 1);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn idle_pass_reads_only_the_next_slot_and_floor() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let count = ana.storage.request_count();
    assert!(matches!(
        ana.sync.pass().await.unwrap(),
        SequenceStatus::Synced { through: 2 }
    ));
    assert_eq!(ana.storage.request_count() - count, 2);
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn one_slot_carries_the_captured_queue_and_ben_applies_it_in_order() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let before = ana.sync.database.applied_slot().await.unwrap();
    insert(&ana.sync.database, "a", "one").await;
    insert(&ana.sync.database, "b", "two").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), before + 1);
    ben.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    assert_eq!(
        rows(&ben.sync.database).await,
        [("a".into(), "one".into()), ("b".into(), "two".into())]
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn a_blocked_slot_stops_reads_and_sends_but_preserves_app_writes() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ben.sync.database, "bad", "first").await;
    ben.sync.pass().await.unwrap();
    let blocked = ben.sync.database.applied_slot().await.unwrap();
    let path = ObjectPath::slot(group.store, blocked.try_into().unwrap());
    group.storage.corrupt_byte(&path, 0).await.unwrap();
    insert(&ben.sync.database, "later", "second").await;
    ben.sync.pass().await.unwrap();
    assert!(ana.sync.pass().await.is_err());
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), blocked - 1);
    assert!(matches!(
        &*ana.sync.state.subscribe().borrow(),
        SequenceStatus::Blocked(_)
    ));
    insert(&ana.sync.database, "local", "retained").await;
    let count = ana.storage.request_count();
    assert!(ana.sync.pass().await.is_err());
    assert_eq!(ana.storage.request_count(), count);
    assert_eq!(
        rows(&ana.sync.database).await,
        [("local".into(), "retained".into())]
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn a_stalled_slot_body_leaves_the_application_writer_available() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let title = "x".repeat(150_000);
    ana.sync
        .database
        .write(move |sql| {
            sql.execute("INSERT INTO notes VALUES('remote',?1)", [title])?;
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let number = ana.sync.database.applied_slot().await.unwrap();
    let path = ObjectPath::slot(group.store, number.try_into().unwrap());
    let (stalled, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_body_at(path, 65_537, stalled, held)
        .await;
    let app = async {
        observed.await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            insert(&ben.sync.database, "local", "during download"),
        )
        .await
        .expect("network input held the application writer");
        resume.send(()).unwrap();
    };
    let (synced, ()) = tokio::join!(ben.sync.pass(), app);
    synced.unwrap();
    assert_eq!(rows(&ben.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn committed_slots_release_their_received_and_local_claim_bytes() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "applied").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    for device in [&ana, &ben] {
        let budget = device
            .sync
            .database
            .set_input_budget(0)
            .await
            .expect("applied inputs are eligible for eviction");
        assert_eq!(budget.retained, 0);
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn writes_claimed_after_their_device_removal_are_consumed_and_refused() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ben.sync.database, "kept", "before removal").await;
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ana.sync
        .administer(StoreChange::RemoveDevice {
            device: ben.sync.device,
        })
        .await
        .unwrap();
    let removal = ana.sync.database.applied_slot().await.unwrap();
    insert(&ben.sync.database, "late", "after removal").await;
    {
        // A device that ignores its removal claims the slot after it.
        let mut session = ben.sync.session.lock().await;
        session.through = removal;
        let member = ben.sync.member.read().unwrap().unwrap();
        let mut ring = ben.sync.keys.read().unwrap().unwrap();
        let captured = ben
            .sync
            .database
            .waiting_snapshot_headers()
            .await
            .unwrap()
            .last()
            .unwrap()
            .header
            .position
            .number;
        let _ = ben
            .sync
            .send_writes(&mut session, &mut ring, &member, captured)
            .await;
    }
    assert!(group
        .storage
        .status(&ObjectPath::slot(
            group.store,
            (removal + 1).try_into().unwrap()
        ))
        .await
        .unwrap()
        .is_some());
    assert!(
        matches!(ana.sync.pass().await.unwrap(), SequenceStatus::Synced {through} if through == removal + 1)
    );
    assert_eq!(
        rows(&ana.sync.database).await,
        [("kept".into(), "before removal".into())]
    );
    let refused: Vec<_> = ana.sync.database.pending::<PendingRecord>().await.unwrap().into_iter()
        .filter(|record| matches!(record.reason, PendingReason::WritesRefused {cutoff, ..} if cutoff.0 == removal)).collect();
    assert_eq!(refused.len(), 1);
    assert_eq!(
        refused[0].subject,
        PendingSubject::Slot(coven_merge::Slot(removal + 1))
    );
    insert(&ana.sync.database, "after", "refusal").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        removal + 2,
        "a refused slot does not stop later slots"
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_slot_this_copy_never_attempted_shows_another_copy_of_its_device_id() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let (ana, copy) = group.copy(ana).await;
    let applied = copy.sync.database.applied_slot().await.unwrap();
    insert(&ana.sync.database, "a", "from the original").await;
    ana.sync.pass().await.unwrap();
    insert(&copy.sync.database, "b", "never sent").await;
    let result = copy.sync.pass().await;
    assert!(
        matches!(result, Err(SyncError::DeviceIdInUse(DeviceIdEvidence::UnattemptedSlot {slot})) if slot.0 == applied + 1),
        "{result:?}"
    );
    assert!(matches!(
        &*copy.sync.state.subscribe().borrow(),
        SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(_))
    ));
    assert_eq!(copy.sync.database.applied_slot().await.unwrap(), applied);
    let count = copy.storage.request_count();
    assert!(copy.sync.pass().await.is_err());
    assert_eq!(
        copy.storage.request_count(),
        count,
        "a stopped copy sends nothing"
    );
    for device in [&ana, &copy] {
        device.sync.database.close().await.unwrap();
    }
}

mod differential {
    use crate::sequence_sync::tests::*;
    use crate::{SequenceSync, SyncError};
    use coven_merge::Audience;
    use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};
    use serde_json::json;

    async fn compare(count: usize, already: usize, publish: bool) {
        let group = Group::new();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group.create(&ana).await;
        group.admit(&ana, &ben).await;
        for i in 0..count {
            ana.sync
                .database
                .write(move |sql| {
                    sql.execute(
                        "INSERT INTO notes VALUES(?1,?2)",
                        (i.to_string(), format!("note {i}")),
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            ana.sync.pass().await.unwrap();
            if i < already {
                ben.sync.pass().await.unwrap();
            }
        }
        let covers = ana.sync.database.applied_slot().await.unwrap();
        let announcement = ana.sync.snapshot(Audience::Store).await.unwrap();
        let last = ana.sync.database.applied_slot().await.unwrap();
        if already > count {
            ben.sync.pass().await.unwrap();
        }
        let applied = ben.sync.database.applied_slot().await.unwrap();
        let snapshot = ana.sync.session.lock().await.history.announcements[&announcement]
            .snapshot
            .clone();
        let snapshot_path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
        let fetched: Vec<_> = (applied + 1..=last).collect();
        let input = json!({"kind":"read","applied":applied,"fetched":fetched,
            "first_kept":if publish {covers+1} else {1}});
        let expected = crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input);
        // Hold the last input while another device publishes a floor. Losing the
        // floor reply leaves every old slot available, so existence alone cannot
        // tell the reader that its fetched prefix must be discarded.
        let retention = async {
            if publish {
                group.clock.set(
                    std::time::UNIX_EPOCH
                        + crate::sequence_sync::sequence_retention::GRACE
                        + std::time::Duration::from_secs(1),
                );
                ana.storage
                    .set_faults(Faults {
                        lose_completion_reply: true,
                        ..Faults::none()
                    })
                    .await;
                let mut session = ana.sync.session.lock().await;
                ana.sync
                    .retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity)
                    .await
                    .unwrap();
                drop(session);
                assert!(group
                    .storage
                    .status(&ObjectPath::floor(group.store))
                    .await
                    .unwrap()
                    .is_some());
                for n in &fetched {
                    assert!(group
                        .storage
                        .status(&ObjectPath::slot(group.store, (*n).try_into().unwrap()))
                        .await
                        .unwrap()
                        .is_some());
                }
                // A missing required snapshot exposes the reload branch before
                // it can install a replacement prefix. Ordinary forward reading
                // never opens an announcement's body.
                group.storage.delete(&snapshot_path).await.unwrap();
            }
        };
        let read = async {
            let mut session = ben.sync.session.lock().await;
            let mut ring = group.ring.clone();
            SequenceSync::read_forward(&ben.sync, &mut session, &mut ring, &ben.identity).await
        };
        let result = if fetched.is_empty() {
            retention.await;
            read.await
        } else {
            let (stalled, observed) = tokio::sync::oneshot::channel();
            let (resume, held) = tokio::sync::oneshot::channel();
            group
                .storage
                .hold_body_at(
                    ObjectPath::slot(group.store, last.try_into().unwrap()),
                    1,
                    stalled,
                    held,
                )
                .await;
            let publish_after_read = async {
                observed.await.unwrap();
                retention.await;
                resume.send(()).unwrap();
            };
            let (result, ()) = tokio::join!(read, publish_after_read);
            result
        };
        let reload = match result {
            Ok(()) => false,
            Err(SyncError::Storage(error)) if error.failure() == StorageFailure::NotFound => true,
            Err(error) => panic!("unexpected read error: {error:?}; {input}"),
        };
        let through = ben.sync.database.applied_slot().await.unwrap();
        assert_eq!(
            json!({"reload":reload,"apply":(applied+1..=through).collect::<Vec<_>>()}),
            expected,
            "{input}"
        );
        for device in [&ana, &ben] {
            device.sync.database.close().await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
    async fn sequence_floor_matches_lean() {
        let mut exhaustive = 0;
        for count in 0..=4 {
            for already in 0..=count + 1 {
                for publish in [false, true] {
                    compare(count, already, publish).await;
                    exhaustive += 1;
                }
            }
        }
        let mut rng = 0x51eade_u64;
        for _ in 0..24 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let count = (rng % 12) as usize;
            compare(
                count,
                ((rng >> 8) as usize) % (count + 2),
                (rng >> 16) & 1 == 1,
            )
            .await;
        }
        eprintln!("sequence read: {exhaustive} exhaustive + 24 seeded storage passes");
    }
}

#[tokio::test]
async fn a_replacement_connection_keeps_progress_when_the_original_continues() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let replacement = ana.sync.reconnect(Arc::new(ana.storage.clone()));
    insert(
        ana.database(),
        "candidate",
        "applied before custody failure",
    )
    .await;
    replacement.pass().await.unwrap();
    let applied = ana.database().applied_slot().await.unwrap();
    drop(replacement);
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.database().applied_slot().await.unwrap(), applied);
    insert(ana.database(), "original", "after custody failure").await;
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.database().applied_slot().await.unwrap(), applied + 1);
    assert_eq!(rows(ana.database()).await.len(), 2);
    ana.database().close().await.unwrap();
}
