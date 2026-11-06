//! A network of devices on one in-memory store and one held clock, as
//! multi-device tests run them.
use super::*;
use coven_storage::test_utils::MemoryStorage;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

#[cfg(test)]
pub(crate) struct Device {
    pub(crate) app: Coven,
    pub(crate) store: StoreId,
    pub(crate) handle: CovenHandle,
    pub(crate) storage: Arc<MemoryStorage>,
    /// The device's connector over `storage`, across reopening.
    pub(crate) connector: Arc<Connector>,
}

impl Device {
    /// Stop sync, which returns once the active pass and this connection's
    /// transfers have finished and the provider's client is released (E5).
    /// Then no pass or upload runs until sync starts again.
    pub(crate) async fn stop_sync(&self) {
        self.connector.stop(&self.handle).await;
    }
}

/// Stop `handle`'s sync in a task of its own, for a test that lets the pass
/// or transfer stopping waits for go on meanwhile.
pub(crate) fn stopping(handle: &CovenHandle) -> tokio::task::JoinHandle<Result<(), SyncError>> {
    let handle = handle.clone();
    tokio::spawn(async move { handle.stop_sync().await })
}

#[cfg(test)]
pub(crate) struct Network {
    root: tempfile::TempDir,
    pub(crate) clock: Arc<FixedClock>,
    /// Every device's clock: the fixed clock, its waits held from the
    /// start, so a backoff or cooldown ends only as the test moves the
    /// clock or asks for a pass.
    pub(crate) polling: Arc<PollingClock>,
    /// Where the test's own moves have put the clock: a second for each
    /// pass it asked for, and the waits it ended.
    moved_to: std::sync::Mutex<std::time::SystemTime>,
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
pub(crate) fn migrations() -> Vec<Migration> {
    vec![Migration::addition(1, "records", "
        CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL UNIQUE, value INTEGER NOT NULL);
        CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY, parent TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE, value INTEGER NOT NULL);
        CREATE TABLE circle_notes(id TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL, audience TEXT NOT NULL);
    ")]
}
fn builder(
    app: &Coven,
    clock: Arc<PollingClock>,
    ids: IdSourceRef,
    connector: Arc<Connector>,
) -> CovenBuilder {
    app.builder()
        .synced_tables(tables())
        .namespaces(file_network::namespaces())
        .migrations(migrations())
        .clock(clock)
        .id_source(ids)
        .storage_connector(connector)
}

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
        polling.hold_waits();
        let ids: IdSourceRef = Arc::new(SequentialIds::new());
        let app = Coven::in_memory(app_dir);
        let directory = app.create_closed_store("Household", ids.clone()).await.unwrap();
        let storage = Arc::new(
            MemoryStorage::builder()
                .provider(provider)
                .clock(clock.clone())
                .transfer_limits(1024 * 1024, 65536)
                .build()
                .unwrap(),
        );
        let connector = Connector::new(storage.clone());
        let builder = builder(&app, polling.clone(), ids.clone(), connector.clone())
            .synced_tables(tables.clone())
            .migrations(migrations.clone());
        let (builder, sign_in) = if provider.signs_in_with_oauth() {
            let sign_in = crate::authentication::SignIn::new().await;
            let mut builder = sign_in.configure(builder);
            builder.authenticate(storage.config().oauth_access().unwrap()).await.unwrap();
            (builder, Some(sign_in))
        } else {
            (builder, None)
        };
        let handle = builder.open(directory.id(), |_| ()).await.unwrap();

        match provider {
            CloudProvider::S3 => handle
                .setup_s3_storage(
                    storage.config(),
                    "Device 0",
                    "owner-key".into(),
                    SecretText::new("secret".into()),
                    |_| {},
                )
                .await
                .unwrap(),
            CloudProvider::CloudKit => {
                handle.setup_cloudkit_storage("container", "Device 0", |_| {}).await.unwrap()
            }
            _ => handle.setup_oauth_storage(storage.config(), "Device 0", |_| {}).await.unwrap(),
        };
        let network = Self {
            _sign_in: sign_in,
            root,
            moved_to: std::sync::Mutex::new(clock.now()),
            clock,
            polling,
            ids,
            schema: (tables, migrations),
            devices: vec![Device { app, store: directory.id(), handle, storage, connector }],
        };
        network.sync(0).await;
        network
    }
    async fn join(&mut self, index: usize) {
        self.join_as(
            index,
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
        )
        .await;
    }
    /// Join device `index` through an invite for `target`, approved by device 0.
    pub(crate) async fn join_as(&mut self, index: usize, target: InviteTarget) {
        let joined = self.try_join_as(index, target).await;
        assert!(joined.is_ok(), "{:?}", joined.err());
    }
    /// Join device `index` as `join_as` does, returning what the join returned:
    /// the device joins the network only when it opened.
    pub(crate) async fn try_join_as(
        &mut self,
        index: usize,
        target: InviteTarget,
    ) -> Result<(), BootstrapError> {
        let owner = &self.devices[0].handle;
        // Device 0's invite looks for the join's request and the join checks
        // for its approval, each backing off between looks, on held waits:
        // device 0 reads the request in the pass asked for once the join
        // waits, and Check now runs the check that finds the approval.
        let invite = crate::tests::invite(
            owner,
            target,
            InviteAccess::S3AccessKey {
                access_key_id: format!("member-{index}"),
                secret_access_key: SecretText::new("secret".into()),
            },
            crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Day },
        )
        .await;
        let app_dir = self.root.path().join(index.to_string());
        std::fs::create_dir(&app_dir).unwrap();
        let app = Coven::in_memory(app_dir);
        let storage = Arc::new(self.devices[0].storage.for_device());
        let connector = Connector::new(storage.clone());
        let control = JoinControl::new();
        let name = format!("Device {index}");
        let (waiting, mut waits) = tokio::sync::watch::channel(false);
        let joining = join_with_invite(
            builder(&app, self.polling.clone(), self.ids.clone(), connector.clone())
                .synced_tables(self.schema.0.clone())
                .migrations(self.schema.1.clone()),
            invite.shown_code(),
            &name,
            |status| {
                if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
                    waiting.send_replace(true);
                }
            },
            &control,
        );
        let (handle, ()) = join_with_response(joining, async {
            waits.wait_for(|waiting| *waiting).await.unwrap();
            self.pass(0).await;
            let request = next_join_request(owner).await;
            approve(owner, &request).await;
            control.check_now();
        })
        .await;
        let handle = handle?.expect("an approved join opens");
        let store = decode_code_info(invite.shown_code()).unwrap().metadata().store_id;
        handle.start_sync().await.unwrap();
        self.devices.push(Device { app, store, handle, storage, connector });
        self.sync(index).await;
        Ok(())
    }
    pub(crate) async fn sync(&self, index: usize) {
        let handle = &self.devices[index].handle;
        self.pass(index).await;
        let pending = handle.pending().await.unwrap();
        assert!(
            pending
                .iter()
                .all(|record| !matches!(record.subject, PendingSubject::Operation { .. })),
            "{pending:?}"
        );
    }

    /// Request a pass and wait for the first one that begins after the
    /// request. A pass already running may have read state before it, so its
    /// status can't say whether it served the request; the retry's answer
    /// comes from the pass that took the request.
    pub(crate) async fn pass(&self, index: usize) {
        let handle = &self.devices[index].handle;
        self.move_clock(Duration::from_secs(1));
        match handle.retry_now().await.unwrap().await {
            RetryOutcome::Synced { .. } => {}
            outcome => panic!("device {index}: {outcome:?}"),
        }
    }

    /// Move every device's clock on by `by`, as the test means that time to
    /// pass: the waits that end by then end.
    pub(crate) fn move_clock(&self, by: Duration) {
        *coven_foundation::locks::lock_unpoisoned(&self.moved_to) += by;
        self.clock.set(self.clock.now() + by);
    }

    /// End `wait`, a backoff or cooldown the code armed, by moving every
    /// device's clock to its end, as its passing would.
    pub(crate) fn end_wait(&self, wait: coven_database::RetryWait) {
        let left = wait.not_before.duration_since(self.clock.now()).unwrap_or_default();
        self.move_clock(left);
    }

    /// Every device's clock is where the test's own moves put it: no wait
    /// passed on its own.
    pub(crate) fn assert_moved_only_by_the_test(&self) {
        assert_eq!(self.clock.now(), *coven_foundation::locks::lock_unpoisoned(&self.moved_to));
    }

    /// Three rounds of a pass on every device, so each has read what the
    /// others sent.
    pub(crate) async fn quiet(&self) {
        for _ in 0..3 {
            for i in 0..self.devices.len() {
                self.sync(i).await;
            }
        }
    }
    /// Device `index`'s parents, children and circle notes.
    pub(crate) async fn rows(
        &self,
        index: usize,
    ) -> (Vec<(String, String, i64)>, Vec<(String, String, i64)>, Vec<(String, i64, String)>) {
        self.devices[index]
            .handle
            .read(|sql| {
                Ok::<_, CovenError>((
                    sql.query("SELECT id,label,value FROM parents ORDER BY id", [], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?,
                    sql.query("SELECT id,parent,value FROM children ORDER BY id", [], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?,
                    sql.query("SELECT id,value,audience FROM circle_notes ORDER BY id", [], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })?,
                ))
            })
            .await
            .unwrap()
    }
    /// Reopen every device with a breaking version 2, as an updated app
    /// does, and settle.
    pub(crate) async fn raise_schema(&mut self) {
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
                self.polling.clone(),
                self.ids.clone(),
                device.connector.clone(),
            )
            .migrations(changed.clone())
            .open(device.store, |_| ())
            .await
            .unwrap();
            device.handle.start_sync().await.unwrap();
        }
        self.quiet().await;
    }
    /// Every device holds device 0's rows, applied slot and schema, synced.
    pub(crate) async fn assert_converged(&self) {
        let expected = self.rows(0).await;
        let applied = self.devices[0].handle.applied_slot().await;
        let schema = self.devices[0].handle.store_state().await.schema;
        for (i, device) in self.devices.iter().enumerate() {
            assert_eq!(self.rows(i).await, expected, "device {i} rows");
            assert_eq!(device.handle.applied_slot().await, applied, "device {i} slot");
            assert_eq!(device.handle.store_state().await.schema, schema, "device {i} schema");
            let value = device.handle.test_sync_status().latest().await;
            assert_eq!(value.state, SyncState::Synced, "device {i}: {value:?}");
        }
    }
    /// Open later joins and reopens with `migrations`, as an updated app does.
    pub(crate) fn update_app(&mut self, migrations: Vec<Migration>) {
        self.schema.1 = migrations;
    }

    /// A builder for a device's directory, as the app would reopen it.
    pub(crate) fn reopen(&self, device: &Device) -> CovenBuilder {
        builder(&device.app, self.polling.clone(), self.ids.clone(), device.connector.clone())
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
        let replaced = self.devices[0]
            .handle
            .replace_member_keys_for(&old.member, prepared.public_keys())
            .await
            .unwrap();
        landed(&self.devices[0].handle, replaced).await;
        // The first pass that begins after the replacement landed reads it,
        // and stops Carol's laptop on it.
        self.clock.set(self.clock.now() + Duration::from_secs(1));
        let outcome = carol.handle.retry_now().await.unwrap().await;
        assert!(
            matches!(carol.handle.restore_code().await, Err(SyncError::MemberKeysReplaced { .. })),
            "the pass after the replacement ended {outcome:?}"
        );
        carol.handle.close().await.unwrap();
        let member = old.member;
        let code = coven_format::codes::RestoreCode { member_keys: prepared, ..old }
            .to_text()
            .unwrap()
            .to_string();
        KeysReplaced { carol, member, code, stale }
    }
}
