use crate::*;
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

#[cfg(test)]
pub(crate) struct Device {
    app: TestCoven,
    pub(crate) layout: StoreLayout,
    pub(crate) store: StoreId,
    pub(crate) handle: CovenHandle,
    pub(crate) storage: Arc<MemoryStorage>,
}
#[cfg(test)]
pub(crate) struct Network {
    root: tempfile::TempDir,
    pub(crate) clock: Arc<FixedClock>,
    ids: IdSourceRef,
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
    vec![Migration::sql(1, "records", "
        CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL UNIQUE, value INTEGER NOT NULL);
        CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY, parent TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE, value INTEGER NOT NULL);
        CREATE TABLE circle_notes(id TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL, audience TEXT NOT NULL);
    ")]
}
fn builder(
    app: &TestCoven,
    layout: StoreLayout,
    clock: ClockRef,
    ids: IdSourceRef,
    storage: Arc<MemoryStorage>,
) -> CovenBuilder {
    let clock = Arc::new(PollingClock(clock));
    app.builder(layout)
        .synced_tables(tables())
        .migrations(migrations())
        .clock(clock)
        .id_source(ids)
        .storage_connector(storage)
}
/// Preserve the fixture's timestamps while polling without production delays.
#[cfg(test)]
pub(crate) struct PollingClock(pub(crate) ClockRef);
impl Clock for PollingClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }
    fn sleep(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(if duration == Duration::from_secs(1) {
            Duration::from_millis(1)
        } else {
            duration
        }))
    }
}

impl Network {
    async fn new(count: usize) -> Self {
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
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let ids: IdSourceRef = Arc::new(SequentialIds::new());
        let app = TestCoven::new();
        let directory = app
            .create_store(
                &StoreLayout::new(root.path().join("0")),
                "Household",
                ids.clone(),
            )
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
            StoreLayout::new(root.path().join("0")),
            clock.clone(),
            ids.clone(),
            storage.clone(),
        )
        .synced_tables(tables)
        .migrations(migrations);
        let (builder, sign_in) = if matches!(
            provider,
            CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
        ) {
            let sign_in = crate::authentication::SignIn::new(clock.clone()).await;
            let mut builder = sign_in.configure(builder);
            builder.authenticate(provider).await.unwrap();
            (builder, Some(sign_in))
        } else {
            (builder, None)
        };
        let handle = builder.open(directory.id()).await.unwrap();
        handle.initialize_identity().unwrap();
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
                .setup_cloudkit_storage(storage.config(), "Device 0")
                .await
                .unwrap(),
            _ => handle
                .setup_oauth_storage(storage.config(), "Device 0")
                .await
                .unwrap(),
        };
        let initial_layout = StoreLayout::new(root.path().join("0"));
        let network = Self {
            _sign_in: sign_in,
            root,
            clock,
            ids,
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
        let owner = &self.devices[0].handle;
        let invite = owner
            .create_invite(
                MemberRole::Member,
                InviteAccess::S3AccessKey {
                    access_key_id: format!("member-{index}"),
                    secret_access_key: SecretText::new("secret".into()),
                },
            )
            .await
            .unwrap();
        let app = TestCoven::new();
        let layout = StoreLayout::new(self.root.path().join(index.to_string()));
        let storage = Arc::new(self.devices[0].storage.for_device());
        let (_, cancel) = tokio::sync::watch::channel(false);
        let name = format!("Device {index}");
        let joining = join_with_invite(
            builder(
                &app,
                layout.clone(),
                self.clock.clone(),
                self.ids.clone(),
                storage.clone(),
            ),
            &invite.code,
            &name,
            |_| {},
            &cancel,
        );
        let (handle, _) = join_and_approve(owner, joining, |_| async {}).await;
        let handle = handle.unwrap().unwrap();
        let store = decode_code_info(&invite.code).unwrap().store_id;
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
        assert!(handle.pending_operations().await.unwrap().is_empty());
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
        changed.push(Migration::sql(
            2,
            "extra",
            "ALTER TABLE parents ADD COLUMN extra INTEGER NOT NULL DEFAULT 0",
        ));
        for device in &mut self.devices {
            device.handle.close().await.unwrap();
            device.handle = builder(
                &device.app,
                device.layout.clone(),
                self.clock.clone(),
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
        let state = posted(&self.devices[0]).await;
        for (i, device) in self.devices.iter().enumerate() {
            assert_eq!(self.rows(i).await, expected, "device {i} rows");
            let other = posted(device).await;
            assert_eq!(other.writes, state.writes, "device {i} write positions");
            assert_eq!(
                other.store_log, state.store_log,
                "device {i} store-log positions"
            );
            assert_eq!(
                other.schema_version, state.schema_version,
                "device {i} schema"
            );
            assert_eq!(
                other.fingerprints, state.fingerprints,
                "device {i} fingerprints"
            );
            let status = device.handle.subscribe_sync_status();
            let value = status.borrow();
            let SyncStatus::Synced { .. } = &*value else {
                panic!("{value:?}")
            };
        }
    }
    pub(crate) async fn close(self) {
        for device in self.devices {
            device.handle.close().await.unwrap();
        }
    }
}

pub(crate) async fn next_join_request(owner: &CovenHandle) -> JoinRequest {
    let mut requests = owner.subscribe_join_requests();
    loop {
        if let Some(request) = requests.borrow_and_update().first().cloned() {
            return request;
        }
        requests.changed().await.unwrap();
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

pub(crate) async fn posted(device: &Device) -> coven_format::objects::PostedPositions {
    use coven_format::{codes::RestoreCode, sealed_single::SingleChunkObject, Object};
    let code = RestoreCode::from_text(&device.handle.restore_code().await.unwrap()).unwrap();
    let member = device
        .handle
        .get_members()
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.is_self)
        .unwrap();
    let id = device
        .layout
        .store_dir(&device.store)
        .settings()
        .unwrap()
        .device_id;
    let path = ObjectPath::positions(id);
    let bytes = device.storage.read(&path).await.unwrap();
    let object = SingleChunkObject::decode(&bytes).unwrap();
    let SingleChunkObject::PostedPositions {
        key,
        chunk,
        signature,
    } = &object
    else {
        panic!("positions object")
    };
    let key_path = ObjectPath::store_key(*key, &member.id);
    let key = code
        .member_keys
        .open_store_key(
            key_path.as_str(),
            &device.storage.read(&key_path).await.unwrap(),
        )
        .unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    member
        .id
        .verify_object(path.as_str(), &hash.finish(), signature)
        .unwrap();
    let plain = key
        .derive()
        .open_object_chunk(
            path.as_str(),
            &object.prefix().encode().unwrap(),
            0,
            0,
            chunk,
        )
        .unwrap();
    let Object::PostedPositions(positions) = Object::decode(&plain).unwrap() else {
        panic!("positions frame")
    };
    assert_eq!(positions.device, id);
    positions
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
                    .get_members()
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
                                .get_members()
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
                    .get_members()
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
async fn reconnecting_with_a_replacement_key_records_the_access_that_removal_revokes() {
    let network = Network::new(2).await;
    let member = network.devices[1]
        .handle
        .get_members()
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
    assert_eq!(
        network.devices[0]
            .handle
            .remove_member(&member)
            .await
            .unwrap(),
        MemberRemoval::DeleteAccessKey {
            access_key_id: "replacement-key".into()
        }
    );
    let owner = &network.devices[0].handle;
    owner.stop_sync();
    owner
        .subscribe_sync_status()
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
    assert_eq!(
        owner.access_keys_to_delete().await.unwrap(),
        ["member-1", "replacement-key"].map(|key| AccessKeyToDelete {
            access_key_id: key.into(),
            member: Some(member.clone()),
        })
    );
    owner.confirm_access_key_deleted("member-1").await.unwrap();
    assert_eq!(
        owner.access_keys_to_delete().await.unwrap(),
        vec![AccessKeyToDelete {
            access_key_id: "replacement-key".into(),
            member: Some(member),
        }]
    );
    assert!(owner.pending_operations().await.unwrap().is_empty());
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
    assert!(network.devices[0]
        .storage
        .list(&ObjectPrefix::device_logs())
        .await
        .unwrap()
        .is_empty());
    network.devices[1].handle.start_sync().await.unwrap();
    network.quiet().await;
    assert_eq!(network.rows(1).await.0.len(), 2);
    network.assert_converged().await;
    network.close().await;
}

#[tokio::test]
async fn an_internal_reload_failure_reports_sync_status_and_retries_without_app_action() {
    let network = Network::new(2).await;
    let reader = &network.devices[1].handle;
    reader.stop_sync();
    reader
        .subscribe_sync_status()
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
    network.devices[0].handle.reset_store().await.unwrap();
    network.sync(0).await;
    let storage = &network.devices[0].storage;
    let snapshots = storage.list(&ObjectPrefix::snapshots()).await.unwrap();
    assert_eq!(snapshots.len(), 1);
    let path = &snapshots[0].path;
    let bytes = storage.read(path).await.unwrap();
    storage.delete(path).await.unwrap();
    let device = reader
        .get_members()
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.is_self)
        .unwrap()
        .devices[0];
    let positions = ObjectPath::positions(device);
    let before = storage.read(&positions).await.unwrap();
    reader.start_sync().await.unwrap();
    let mut status = reader.subscribe_sync_status();
    tokio::time::timeout(
        Duration::from_secs(15),
        status.wait_for(|s| matches!(s, SyncStatus::Failed { .. })),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reader.pending_operations().await.unwrap().is_empty());
    assert_eq!(storage.read(&positions).await.unwrap(), before);
    storage.create(path, &bytes).await.unwrap();
    network.sync(1).await;
    assert_ne!(storage.read(&positions).await.unwrap(), before);
    network.close().await;
}

mod stuck_logs {
    use super::*;

    #[tokio::test]
    async fn stuck_logs_are_live_while_sync_succeeds_and_reset_withdraws_the_report() {
        let network = Network::new(2).await;
        let reader = &network.devices[1].handle;
        reader.stop_sync();
        reader
            .subscribe_sync_status()
            .wait_for(|s| matches!(s, SyncStatus::Stopped))
            .await
            .unwrap();
        let mut received = reader.subscribe_stuck_logs();
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
        let object = storage
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .pop()
            .unwrap();
        storage
            .corrupt_byte(&object.path, object.size as usize - 1)
            .await
            .unwrap();
        reader.start_sync().await.unwrap();
        network.sync(1).await;
        let record = LogRefusal {
            object: LogObject::Write(object.path.write_id().unwrap()),
            failure: RefusalCode::Signature,
        };
        assert_eq!(
            received.next().await.unwrap(),
            vec![StuckLog {
                record,
                reported_by: None
            }]
        );
        let before = network.devices[1]
            .storage
            .reads()
            .await
            .into_iter()
            .filter(|(path, _, _)| path == &object.path)
            .count();
        network.sync(1).await;
        assert_eq!(
            network.devices[1]
                .storage
                .reads()
                .await
                .into_iter()
                .filter(|(path, _, _)| path == &object.path)
                .count(),
            before
        );
        network.sync(0).await;
        let report = owner.subscribe_stuck_logs().next().await.unwrap();
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].record, record);
        assert!(report[0].reported_by.is_some());
        assert!(network.rows(1).await.0.is_empty());
        owner.reset_store().await.unwrap();
        network.sync(1).await;
        network.sync(0).await;
        assert!(received.next().await.unwrap().is_empty());
        assert!(owner
            .subscribe_stuck_logs()
            .next()
            .await
            .unwrap()
            .is_empty());
        network.assert_converged().await;
        network.close().await;
    }
}
