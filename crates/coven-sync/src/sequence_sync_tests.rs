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
#[cfg(test)]
pub(crate) struct Group {
    pub(crate) ids: Arc<SequentialIds>,
    pub(crate) store: StoreId,
    pub(crate) ring: StoreKeyring,
    pub(crate) storage: MemoryStorage,
    pub(crate) clock: Arc<coven_foundation::clock::FixedClock>,
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
        let database = DatabaseBuilder::new(directory.clone())
            .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
            .migrations(vec![Migration::sql(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)",
            )])
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
        let storage = storage
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
            TransferLimits::default(),
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
            .unwrap();
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

#[tokio::test]
async fn idle_pass_reads_only_the_next_slot_and_floor() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let count = ana.storage.request_count();
    assert!(matches!(
        ana.sync.pass().await.unwrap(),
        SyncStatus::Synced { through: 2 }
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
        SyncStatus::Blocked(_)
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
        matches!(ana.sync.pass().await.unwrap(), SyncStatus::Synced {through} if through == removal + 1)
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
        SyncStatus::Stopped(SyncFailure::DeviceIdInUse(_))
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
