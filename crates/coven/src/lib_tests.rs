use crate::*;
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

/// Wait for committed writes to land. A pass that captured its queue before
/// the write can finish afterward, so its finish time cannot confirm upload.
pub(crate) async fn wait_for_sent(handle: &CovenHandle) {
    let mut status = handle.subscribe_sync_status();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            status.borrow_and_update();
            if handle.test_queued_writes().await.unwrap().is_empty() {
                return;
            }
            status.changed().await.expect("open loop status");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("uploads did not finish: {:?}", &*status.borrow()));
}

#[test]
fn file_name_error_fields_are_available_through_the_facade() {
    let name: FileName = FileName::new("attachment.data").unwrap();
    let error = DbError::FileNameReused { name };
    let DbError::FileNameReused { name } = error else {
        unreachable!()
    };
    assert_eq!(name.as_str(), "attachment.data");
    let invalid: FileNameError = FileName::new("../outside").unwrap_err();
    assert_eq!(invalid, FileNameError::Character);
}

#[tokio::test]
async fn storage_metadata_and_snapshot_subjects_are_available_through_the_facade() {
    let storage = MemoryStorage::builder().build().unwrap();
    let store = coven_storage::test_utils::STORE_ID;
    let device = DeviceId(7);
    let path = ObjectPath::snapshot(store, Audience::Store, device, 3.try_into().unwrap());
    storage.create(&path, b"snapshot").await.unwrap();
    let object: StoredObject = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(object.path, path);
    assert_eq!(object.size, 8);
    let checksum: Option<ChecksumValue> = object.checksum.map(|checksum| match checksum {
        ObjectChecksum::FullObject(value) | ObjectChecksum::Composite(value) => value,
    });
    assert_eq!(checksum, None);

    let snapshot: SnapshotId = path.snapshot_id().unwrap();
    assert_eq!(snapshot.device, device);
    assert_eq!(snapshot.number, 3);
    assert_eq!(snapshot.audience, Audience::Store);
    let record = PendingRecord::new(
        PendingSubject::Snapshot(snapshot),
        PendingReason::Missing { path },
    );
    assert_eq!(record.retry(), Retry::Automatic);
}

#[cfg(test)]
pub(crate) struct Device {
    pub(crate) app: TestCoven,
    pub(crate) layout: StoreLayout,
    pub(crate) store: StoreId,
    pub(crate) handle: CovenHandle,
    pub(crate) storage: Arc<MemoryStorage>,
}
#[cfg(test)]
pub(crate) struct Network {
    root: tempfile::TempDir,
    pub(crate) clock: Arc<FixedClock>,
    /// Every device's clock: the fixed clock, with waits shortened.
    pub(crate) polling: Arc<PollingClock>,
    ids: IdSourceRef,
    /// The synced tables and migrations every device opens with.
    schema: (Vec<SyncedTable>, Vec<Migration>),
    pub(crate) devices: Vec<Device>,
    _sign_in: Option<crate::authentication::SignIn>,
}
fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("parents", RowIdentity::SharedKey),
        SyncedTable::new("children", RowIdentity::SharedKey),
        SyncedTable::new("circle_notes", RowIdentity::IndependentUuid).audience_column("audience"),
    ]
}
fn migrations() -> Vec<Migration> {
    vec![Migration::addition(1, "records", "
        CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL UNIQUE, value INTEGER NOT NULL);
        CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY, parent TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE, value INTEGER NOT NULL);
        CREATE TABLE circle_notes(id TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL, audience TEXT NOT NULL);
    ")]
}
fn builder(
    app: &TestCoven,
    layout: StoreLayout,
    clock: Arc<PollingClock>,
    ids: IdSourceRef,
    storage: Arc<MemoryStorage>,
) -> CovenBuilder {
    app.builder(layout)
        .synced_tables(tables())
        .migrations(migrations())
        .clock(clock)
        .id_source(ids)
        .storage_connector(storage)
}
#[path = "../tests/fixtures/polling_clock.rs"]
mod polling_clock;
pub(crate) use polling_clock::PollingClock;
#[path = "../tests/fixtures/file_network.rs"]
mod file_network;
pub(crate) use file_network::{attach, file_network, file_stored, wait_for_empty_queue};

impl Network {
    pub(crate) async fn new(count: usize) -> Self {
        let mut network = Self::with_schema(CloudProvider::S3, tables(), migrations()).await;
        for device in 1..count {
            network.join(device).await;
        }
        network.quiet().await;
        network
    }

    pub(crate) async fn with_schema(
        provider: CloudProvider,
        tables: Vec<SyncedTable>,
        migrations: Vec<Migration>,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let app_dir = root.path().join("0");
        std::fs::create_dir(&app_dir).unwrap();
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let polling = Arc::new(PollingClock::new(clock.clone()));
        let ids: IdSourceRef = Arc::new(SequentialIds::new());
        let app = TestCoven::new();
        let directory = app
            .create_closed_store(&StoreLayout::new(app_dir.clone()), "Household", ids.clone())
            .await
            .unwrap();
        let storage = Arc::new(
            MemoryStorage::builder()
                .provider(provider)
                .clock(clock.clone())
                .transfer_limits(1024 * 1024, 65536)
                .build()
                .unwrap(),
        );
        let builder = builder(
            &app,
            StoreLayout::new(app_dir.clone()),
            polling.clone(),
            ids.clone(),
            storage.clone(),
        )
        .synced_tables(tables.clone())
        .migrations(migrations.clone());
        let (builder, sign_in) =
            if matches!(provider, CloudProvider::Dropbox | CloudProvider::OneDrive) {
                let sign_in = crate::authentication::SignIn::new(clock.clone()).await;
                let mut builder = sign_in.configure(builder);
                builder
                    .authenticate(storage.config().oauth_access().unwrap())
                    .await
                    .unwrap();
                (builder, Some(sign_in))
            } else {
                (builder, None)
            };
        let handle = builder.open(directory.id()).await.unwrap();

        match provider {
            CloudProvider::S3 => handle
                .setup_s3_storage(
                    storage.config(),
                    "Device 0",
                    "owner-key".into(),
                    SecretText::new("secret".into()),
                )
                .await
                .unwrap(),
            CloudProvider::CloudKit => handle
                .setup_cloudkit_storage("container", "Device 0")
                .await
                .unwrap(),
            _ => handle
                .setup_oauth_storage(storage.config(), "Device 0")
                .await
                .unwrap(),
        };
        let initial_layout = StoreLayout::new(app_dir);
        let network = Self {
            _sign_in: sign_in,
            root,
            clock,
            polling,
            ids,
            schema: (tables, migrations),
            devices: vec![Device {
                app,
                layout: initial_layout,
                store: directory.id(),
                handle,
                storage,
            }],
        };
        network.sync(0).await;
        network
    }
    async fn join(&mut self, index: usize) {
        self.join_as(
            index,
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
        )
        .await;
    }
    /// Join device `index` through an invite for `target`, approved by device 0.
    pub(crate) async fn join_as(&mut self, index: usize, target: InviteTarget) {
        let owner = &self.devices[0].handle;
        let invite = owner
            .create_invite(
                target,
                InviteAccess::S3AccessKey {
                    access_key_id: format!("member-{index}"),
                    secret_access_key: SecretText::new("secret".into()),
                },
                crate::InviteDelivery::Sent {
                    lifetime: crate::InviteLifetime::Day,
                },
            )
            .await
            .unwrap();
        let app = TestCoven::new();
        let app_dir = self.root.path().join(index.to_string());
        std::fs::create_dir(&app_dir).unwrap();
        let layout = StoreLayout::new(app_dir);
        let storage = Arc::new(self.devices[0].storage.for_device());
        let control = JoinControl::new();
        let name = format!("Device {index}");
        let joining = join_with_invite(
            builder(
                &app,
                layout.clone(),
                self.polling.clone(),
                self.ids.clone(),
                storage.clone(),
            )
            .synced_tables(self.schema.0.clone())
            .migrations(self.schema.1.clone()),
            &invite.code,
            &name,
            |_| {},
            &control,
        );
        let (handle, _) = join_and_approve(owner, joining, |_| async {}).await;
        let handle = handle.unwrap().unwrap();
        let store = decode_code_info(&invite.code).unwrap().metadata().store_id;
        handle.start_sync().await.unwrap();
        self.devices.push(Device {
            app,
            layout,
            store,
            handle,
            storage,
        });
        self.sync(index).await;
    }
    pub(crate) async fn sync(&self, index: usize) {
        let handle = &self.devices[index].handle;
        // A pass already running may have read state before this request;
        // the pass after it serves the request.
        let running = matches!(
            &*handle.subscribe_sync_status().borrow(),
            SyncStatus::Syncing
        );
        self.pass(index).await;
        if running {
            self.pass(index).await;
        }
        let pending = handle.pending().await.unwrap();
        assert!(
            pending
                .iter()
                .all(|record| !matches!(record.subject, PendingSubject::Operation { .. })),
            "{pending:?}"
        );
    }

    /// Request a pass and wait for one finishing after the request.
    async fn pass(&self, index: usize) {
        let handle = &self.devices[index].handle;
        self.clock.set(self.clock.now() + Duration::from_secs(1));
        let after = self.clock.now();
        let mut status = handle.subscribe_sync_status();
        status.borrow_and_update();
        handle.sync_now();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                status.changed().await.unwrap();
                match &*status.borrow_and_update() {
                    SyncStatus::Synced { finished_at } if *finished_at >= after => {
                        break;
                    }
                    SyncStatus::Failed { error } => panic!("device {index}: {error:?}"),
                    _ => {}
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "device {index} did not finish sync: {:?}",
                &*status.borrow()
            )
        });
    }

    async fn quiet(&self) {
        for _ in 0..3 {
            for i in 0..self.devices.len() {
                self.sync(i).await;
            }
        }
    }
    async fn rows(
        &self,
        index: usize,
    ) -> (
        Vec<(String, String, i64)>,
        Vec<(String, String, i64)>,
        Vec<(String, i64, String)>,
    ) {
        self.devices[index]
            .handle
            .read(|sql| {
                Ok((
                    sql.query("SELECT id,label,value FROM parents ORDER BY id", [], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?,
                    sql.query(
                        "SELECT id,parent,value FROM children ORDER BY id",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )?,
                    sql.query(
                        "SELECT id,value,audience FROM circle_notes ORDER BY id",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )?,
                ))
            })
            .await
            .unwrap()
    }
    async fn raise_schema(&mut self) {
        let mut changed = migrations();
        changed.push(Migration::breaking(
            2,
            "extra",
            "ALTER TABLE parents ADD COLUMN extra INTEGER NOT NULL DEFAULT 0",
            crate::OldWrites::Drop,
        ));
        for device in &mut self.devices {
            device.handle.close().await.unwrap();
            device.handle = builder(
                &device.app,
                device.layout.clone(),
                self.polling.clone(),
                self.ids.clone(),
                device.storage.clone(),
            )
            .migrations(changed.clone())
            .open(device.store)
            .await
            .unwrap();
            device.handle.start_sync().await.unwrap();
        }
        self.quiet().await;
    }
    async fn assert_converged(&self) {
        let expected = self.rows(0).await;
        let applied = self.devices[0].handle.applied_slot().await;
        let schema = self.devices[0].handle.store_state().await.schema;
        for (i, device) in self.devices.iter().enumerate() {
            assert_eq!(self.rows(i).await, expected, "device {i} rows");
            assert_eq!(
                device.handle.applied_slot().await,
                applied,
                "device {i} slot"
            );
            assert_eq!(
                device.handle.store_state().await.schema,
                schema,
                "device {i} schema"
            );
            let status = device.handle.subscribe_sync_status();
            let value = status.borrow();
            let SyncStatus::Synced { .. } = &*value else {
                panic!("{value:?}")
            };
        }
    }
    /// A builder for a device's directory, as the app would reopen it.
    pub(crate) fn reopen(&self, device: &Device) -> CovenBuilder {
        builder(
            &device.app,
            device.layout.clone(),
            self.polling.clone(),
            self.ids.clone(),
            device.storage.clone(),
        )
        .synced_tables(self.schema.0.clone())
        .migrations(self.schema.1.clone())
    }
    pub(crate) async fn close(self) {
        for device in self.devices {
            device.handle.close().await.unwrap();
        }
    }
}

/// Carol, the member on device 1, after Ana replaced her keys with a pair
/// Carol prepared elsewhere. Her laptop applied the replacement and closed.
#[cfg(test)]
pub(crate) struct KeysReplaced {
    pub(crate) carol: Device,
    pub(crate) member: MemberId,
    /// Carol's restore code with the prepared pair, current in history.
    pub(crate) code: String,
    /// The code her laptop showed before, which history now refuses.
    pub(crate) stale: String,
}

impl Network {
    /// Replace device 1's member keys from device 0, the admin, and remove
    /// device 1 from the network once it has stopped on the replacement.
    pub(crate) async fn replace_keys_elsewhere(&mut self) -> KeysReplaced {
        let carol = self.devices.remove(1);
        let stale = carol.handle.restore_code().await.unwrap();
        let old = coven_sync::read_restore_code(&stale).unwrap();
        let prepared = MemberKeys::generate().unwrap();
        self.devices[0]
            .handle
            .replace_member_keys_for(&old.member, prepared.public_keys())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut status = carol.handle.subscribe_sync_status();
            while !matches!(
                carol.handle.restore_code().await,
                Err(SyncError::MemberKeysReplaced { .. })
            ) {
                carol.handle.sync_now();
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        carol.handle.close().await.unwrap();
        let member = old.member;
        let code = coven_format::codes::RestoreCode {
            member_keys: prepared,
            ..old
        }
        .to_text()
        .unwrap()
        .to_string();
        KeysReplaced {
            carol,
            member,
            code,
            stale,
        }
    }
}

/// What this installation's keychain holds for a store.
#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) struct Held {
    pub(crate) device: Option<DeviceId>,
    pub(crate) keys: MemberPublicKeys,
    pub(crate) code: Option<Vec<u8>>,
    pub(crate) credentials: Option<Vec<u8>>,
}

pub(crate) fn held(app: &TestCoven, store: StoreId) -> Held {
    use coven_crypto::custody::KeyringCustody;
    let keychain = Arc::new(app.store_keychain(store));
    let identity = MemberKeyCustody::unlock(&KeyringCustody::new(keychain.clone()))
        .unwrap()
        .unwrap();
    let bytes = |value: Option<SecretBytes>| value.map(|value| value.as_bytes().to_vec());
    Held {
        device: keychain.device_id().unwrap(),
        keys: identity.keys().public_keys(),
        code: bytes(app.synced_keychain(store).restore_code().unwrap()),
        credentials: bytes(keychain.storage_credentials().unwrap()),
    }
}

/// The first checked request among the owner's open invites.
pub(crate) async fn next_join_request(owner: &CovenHandle) -> JoinRequest {
    let mut invites = owner.subscribe_invites();
    loop {
        if let Some(request) = invites
            .borrow_and_update()
            .iter()
            .find_map(|invite| invite.request.clone())
        {
            return request;
        }
        invites.changed().await.unwrap();
    }
}

pub(crate) async fn join_with_response<T, U>(
    joining: impl std::future::Future<Output = T>,
    response: impl std::future::Future<Output = U>,
) -> (T, U) {
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, response)
    })
    .await
    .unwrap()
}

pub(crate) async fn join_and_approve<T, F: std::future::Future<Output = ()>>(
    owner: &CovenHandle,
    joining: impl std::future::Future<Output = T>,
    before: impl FnOnce(JoinRequest) -> F,
) -> (T, MemberId) {
    join_with_response(joining, async {
        let request = next_join_request(owner).await;
        before(request.clone()).await;
        owner.approve_join_request(&request).await.unwrap();
        request.member
    })
    .await
}

/// The objects in storage that `keep` selects, among everything the store holds.
pub(crate) async fn stored(
    storage: &MemoryStorage,
    store: StoreId,
    keep: impl Fn(&ObjectPath) -> bool,
) -> Vec<StoredObject> {
    storage
        .list(&ObjectPrefix::all(store))
        .await
        .unwrap()
        .into_iter()
        .filter(|object| keep(&object.path))
        .collect()
}

/// The snapshot bodies in storage.
pub(crate) async fn snapshots(storage: &MemoryStorage, store: StoreId) -> Vec<StoredObject> {
    stored(storage, store, |path| path.snapshot_id().is_some()).await
}

struct Generator(u64);
impl Generator {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9e3779b97f4a7c15)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

async fn random_write(handle: &CovenHandle, random: &mut Generator, audience: String) {
    let action = random.next() % 8;
    let circle_row =
        uuid::Uuid::from_u128(0x00000000000070008000000000000000 | (random.next() % 16) as u128)
            .to_string();
    let parent = format!("p{}", random.next() % 16);
    let child = format!("c{}", random.next() % 24);
    let label = format!("label{}", random.next() % 10);
    let value = (random.next() % 1000) as i64;
    handle.write(move |sql| {
        match action {
            0 => { sql.execute("INSERT OR IGNORE INTO parents(id,label,value) VALUES(?1,?2,?3)", (&parent,&label,value))?; }
            1 => { sql.execute("UPDATE parents SET value=?1 WHERE id=?2", (value,&parent))?; }
            2 => { sql.execute("UPDATE OR IGNORE parents SET label=?1 WHERE id=?2", (&label,&parent))?; }
            3 => { sql.execute("DELETE FROM parents WHERE id=?1", [&parent])?; }
            4 => { sql.execute("INSERT OR IGNORE INTO children SELECT ?1,id,?2 FROM parents WHERE id=?3", (&child,value,&parent))?; }
            5 => { sql.execute("UPDATE children SET value=?1 WHERE id=?2", (value,&child))?; }
            6 => { sql.execute("DELETE FROM children WHERE id=?1", [&child])?; }
            7 => { sql.execute("INSERT INTO circle_notes(id,value,audience) VALUES(?1,?2,?3) ON CONFLICT(id) DO UPDATE SET value=excluded.value", (&circle_row,value,&audience))?; }
            _ => unreachable!(),
        }
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn seeded_whole_system_convergence() {
    for seed in [0, 1, 2, 7, 42, 31337] {
        let task = tokio::spawn(async move {
            let mut random = Generator::new(seed);
            let mut network = Network::new(2 + (seed % 3) as usize).await;
            let circle = network.devices[0]
                .handle
                .circles()
                .create("Friends")
                .await
                .unwrap();
            for device in &network.devices[1..] {
                let member = device
                    .handle
                    .members()
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|m| m.is_self)
                    .unwrap()
                    .id;
                network.devices[0]
                    .handle
                    .circles()
                    .add_member(circle, &member)
                    .await
                    .unwrap();
            }
            network.quiet().await;
            let mut online = vec![true; network.devices.len()];
            for step in 0..80 {
                let index = (random.next() as usize) % network.devices.len();
                if random.next().is_multiple_of(4) {
                    online[index] = random.next().is_multiple_of(2);
                    network.devices[index].storage.set_online(online[index]);
                }
                let audience = if random.next().is_multiple_of(3) {
                    circle.to_string()
                } else {
                    "store".into()
                };
                random_write(&network.devices[index].handle, &mut random, audience).await;
                let syncing = (random.next() as usize) % network.devices.len();
                if online[syncing] {
                    network.sync(syncing).await;
                }
                if [19, 39, 59].contains(&step) {
                    for (i, device) in network.devices.iter().enumerate() {
                        device.storage.set_online(true);
                        online[i] = true;
                    }
                    network.quiet().await;
                    match step {
                        19 => {
                            let member = network.devices[1]
                                .handle
                                .members()
                                .await
                                .unwrap()
                                .into_iter()
                                .find(|m| m.is_self)
                                .unwrap()
                                .id;
                            network.devices[0]
                                .handle
                                .circles()
                                .remove_member(circle, &member)
                                .await
                                .unwrap();
                            network.quiet().await;
                            let audience = circle.to_string();
                            network.devices[0].handle.write(move |sql| { sql.execute("INSERT INTO circle_notes(id,value,audience) VALUES('00000000-0000-7000-8000-000000000100',19,?1)", [audience])?; Ok(()) }).await.unwrap();
                            network.quiet().await;
                            network.devices[0]
                                .handle
                                .circles()
                                .add_member(circle, &member)
                                .await
                                .unwrap();
                            network.quiet().await;
                            network.assert_converged().await;
                        }
                        39 => {
                            network.devices[0].handle.reset_store().await.unwrap();
                            network.quiet().await;
                        }
                        59 => network.raise_schema().await,
                        _ => unreachable!(),
                    }
                }
            }
            for device in &network.devices {
                device.storage.set_online(true);
            }
            network.quiet().await;
            if network.devices.len() > 2 {
                let retired = network.devices.pop().unwrap();
                let member = retired
                    .handle
                    .members()
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|m| m.is_self)
                    .unwrap()
                    .id;
                network.devices[0]
                    .handle
                    .remove_member(&member)
                    .await
                    .unwrap();
                retired.handle.close().await.unwrap();
                network.quiet().await;
            }
            network.assert_converged().await;
            network.close().await;
        });
        match tokio::time::timeout(Duration::from_secs(90), task).await {
            Ok(Ok(())) => {}
            result => panic!("convergence seed {seed}: {result:?}"),
        }
    }
}

#[tokio::test]
async fn removing_a_member_after_a_replacement_key_leaves_no_key_work() {
    let network = Network::new(2).await;
    let member = network.devices[1]
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.is_self)
        .unwrap()
        .id;
    network.devices[1]
        .handle
        .setup_s3_storage(
            network.devices[1].storage.config(),
            "Device 1",
            "replacement-key".into(),
            SecretText::new("replacement-secret".into()),
        )
        .await
        .unwrap();
    network.quiet().await;
    network.devices[0]
        .handle
        .remove_member(&member)
        .await
        .unwrap();
    let owner = &network.devices[0].handle;
    owner.stop_sync();
    owner
        .subscribe_sync_status()
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
    // The person deletes both keys in the console; coven records nothing (§14.3).
    assert!(owner.pending().await.unwrap().is_empty());
    network.close().await;
}

#[tokio::test]
async fn a_returning_device_loads_snapshots_covering_deleted_writes_and_keeps_its_queue() {
    let network = Network::new(2).await;
    network.devices[1].handle.stop_sync();
    let mut status = network.devices[1].handle.subscribe_sync_status();
    status
        .wait_for(|state| matches!(state, SyncStatus::Stopped))
        .await
        .unwrap();
    network.devices[1]
        .handle
        .write(|sql| {
            sql.execute("INSERT INTO parents VALUES('waiting','local',7)", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let label = "x".repeat(1024 * 1024 + 1);
    network.devices[0]
        .handle
        .write(move |sql| {
            sql.execute("INSERT INTO parents VALUES('remote',?1,9)", [label])?;
            Ok(())
        })
        .await
        .unwrap();
    network.sync(0).await;
    network
        .clock
        .set(network.clock.now() + Duration::from_secs(31 * 24 * 3600));
    network.sync(0).await;
    network.sync(0).await;
    // Retention moved the floor past slot 1 without waiting for device 1.
    let first = ObjectPath::slot(network.devices[0].store, 1.try_into().unwrap());
    assert!(network.devices[0]
        .storage
        .status(&first)
        .await
        .unwrap()
        .is_none());
    network.devices[1].handle.start_sync().await.unwrap();
    network.quiet().await;
    assert_eq!(network.rows(1).await.0.len(), 2);
    network.assert_converged().await;
    network.close().await;
}

/// §6.2 and E5: a complete reset slot waits for its required reload body.
#[tokio::test]
async fn a_missing_reset_body_blocks_its_slot_and_applies_once_it_arrives() {
    let network = Network::new(2).await;
    let reader = &network.devices[1].handle;
    reader.stop_sync();
    reader
        .subscribe_sync_status()
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
    let before = reader.applied_slot().await;
    network.devices[0].handle.reset_store().await.unwrap();
    network.sync(0).await;
    let storage = &network.devices[0].storage;
    let snapshots = snapshots(storage, network.devices[0].store).await;
    assert_eq!(snapshots.len(), 1);
    let path = &snapshots[0].path;
    let bytes = storage.read(path).await.unwrap();
    storage.delete(path).await.unwrap();
    reader.start_sync().await.unwrap();
    let mut status = reader.subscribe_sync_status();
    let blocked = tokio::time::timeout(
        Duration::from_secs(15),
        status.wait_for(|s| matches!(s, SyncStatus::Blocked { .. })),
    )
    .await
    .unwrap()
    .unwrap()
    .clone();
    let SyncStatus::Blocked { slot, reason, .. } = blocked else {
        unreachable!()
    };
    assert!(matches!(
        reason,
        PendingReason::Storage {
            failure: coven_storage::StorageFailure::NotFound,
            ..
        }
    ));
    assert!(reader
        .pending()
        .await
        .unwrap()
        .iter()
        .any(|record| record.subject == PendingSubject::Slot(slot)));
    // Nothing after the blocked slot applies.
    assert!(reader.applied_slot().await < slot.0);
    assert!(reader.applied_slot().await >= before);
    storage.create(path, &bytes).await.unwrap();
    network.sync(1).await;
    assert_eq!(
        reader.applied_slot().await,
        network.devices[0].handle.applied_slot().await
    );
    network.assert_converged().await;
    network.close().await;
}

mod pending_records {
    use super::*;

    #[tokio::test]
    async fn a_damaged_slot_blocks_the_reader_once_without_reading_it_again() {
        let network = Network::new(2).await;
        let reader = &network.devices[1].handle;
        reader.stop_sync();
        reader
            .subscribe_sync_status()
            .wait_for(|s| matches!(s, SyncStatus::Stopped))
            .await
            .unwrap();
        let mut received = reader.subscribe_pending();
        assert!(received.next().await.unwrap().is_empty());
        let owner = &network.devices[0].handle;
        owner
            .write(|sql| {
                sql.execute("INSERT INTO parents VALUES('one','label',1)", [])?;
                Ok(())
            })
            .await
            .unwrap();
        network.sync(0).await;
        let storage = &network.devices[0].storage;
        let slot = owner.applied_slot().await;
        let path = ObjectPath::slot(network.devices[0].store, slot.try_into().unwrap());
        let size = storage.status(&path).await.unwrap().unwrap().size;
        storage
            .corrupt_byte(&path, size as usize - 1)
            .await
            .unwrap();
        reader.start_sync().await.unwrap();
        reader.sync_now();
        let mut status = reader.subscribe_sync_status();
        tokio::time::timeout(
            Duration::from_secs(15),
            status.wait_for(|s| matches!(s, SyncStatus::Blocked { .. })),
        )
        .await
        .unwrap()
        .unwrap();
        let record = PendingRecord::new(
            PendingSubject::Slot(Slot(slot)),
            PendingReason::Refused(Refusal::Signature { cause: None }),
        );
        assert_eq!(received.next().await.unwrap(), vec![record.clone()]);
        let reads = || async {
            network.devices[1]
                .storage
                .reads()
                .await
                .into_iter()
                .filter(|(read, _, _)| read == &path)
                .count()
        };
        let before = reads().await;
        reader.sync_now();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(reads().await, before);
        assert!(network.rows(1).await.0.is_empty());
        // The owner's own pass is unaffected.
        network.sync(0).await;
        assert!(owner.pending().await.unwrap().is_empty());
        network.close().await;
    }
}

/// A restore code as coven saves it in iCloud Keychain, with the name its
/// fixture store was created with (§13.1).
pub(crate) fn keychain_entry(code: &coven_format::codes::RestoreCode) -> SecretBytes {
    SecretBytes::new(
        coven_format::codes::KeychainRestoreCode::encode("Household", 1, code)
            .unwrap()
            .to_vec(),
    )
}

/// The saved entry iCloud Keychain holds for `store`, decoded.
pub(crate) fn saved_entry(
    app: &TestCoven,
    store: StoreId,
) -> Option<coven_format::codes::KeychainRestoreCode> {
    app.synced_keychain(store)
        .restore_code()
        .unwrap()
        .map(|bytes| {
            coven_sync::read_saved_code(store, bytes.as_bytes())
                .unwrap()
                .0
        })
}

/// The readable entries of a listing, for fixtures that save only codes coven
/// reads; an unreadable entry fails the test.
pub(crate) fn readable(entries: Vec<SavedRestoreEntry>) -> Vec<SavedRestoreCode> {
    entries
        .into_iter()
        .map(|entry| match entry {
            SavedRestoreEntry::Readable(saved) => saved,
            unreadable => panic!("expected a readable saved entry, got {unreadable:?}"),
        })
        .collect()
}
