use crate::*;
use coven_foundation::clock::HANG_GUARD;
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

/// Wait until the last pass, as the app's SQL reads it, is one `expected`
/// accepts.
pub(crate) async fn last_pass(
    handle: &CovenHandle,
    expected: impl FnMut(&Option<LastPass>) -> bool,
) -> Option<LastPass> {
    handle.test_last_pass().wait_for(expected).await
}

/// Wait for a pass to end synced, where the last pass before it was none,
/// a failure, or the first after setup.
pub(crate) async fn synced(handle: &CovenHandle) {
    last_pass(handle, |last| *last == Some(LastPass::Synced)).await;
}

/// How many cache files the device keeps.
pub(crate) fn cached(device: &Device) -> usize {
    let cache =
        device.app.store_dir(&device.store).area_path(coven_foundation::files::FileArea::Cache);
    std::fs::read_dir(cache).map_or(0, Iterator::count)
}

/// The stored path of a file whose record says it is in storage.
pub(crate) fn stored_path(store: StoreId, file: &FileRef) -> coven_storage::ObjectPath {
    match file.place() {
        FilePlace::Cloud { location, .. } => location.path(store),
        FilePlace::DeviceOnly { .. } => panic!("the file is not stored"),
    }
}

/// Stage `bytes` in the app's `files` namespace for the cloud, in row `id`
/// of `table`.
pub(crate) async fn stage(
    device: &Device,
    table: &'static str,
    id: &str,
    bytes: Vec<u8>,
) -> FileId {
    let id = id.to_owned();
    device
        .handle
        .test_staged_write(
            move |batch| Ok::<_, CovenError>(batch.put_file("files", bytes, Place::Cloud)),
            move |sql, file| {
                sql.execute(&format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"), (&id, file))?;
                Ok(file)
            },
        )
        .await
        .unwrap()
}

/// Wait for committed writes to land. A pass that captured its queue before
/// the write can finish afterward, so its finish time cannot confirm upload:
/// the upload queue itself is followed.
pub(crate) async fn wait_for_sent(handle: &CovenHandle) {
    let mut unsent = coven_database::test_utils::Followed::new(handle.subscribe_unsent_work());
    unsent.wait_for(|work| work.writes == 0).await;
}

/// The persisted wait of `file`'s queued upload, once one `ready` accepts
/// is armed.
pub(crate) async fn upload_wait(
    handle: &CovenHandle,
    file: FileId,
    ready: impl Fn(&coven_database::RetryWait) -> bool,
) -> coven_database::RetryWait {
    let position: i64 = handle
        .read(move |sql| {
            Ok::<_, CovenError>(sql.query_row(
                "SELECT position FROM coven_uploads WHERE file=?1",
                [file],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    let mut wait = handle.test_follow_retry_wait(coven_database::RetrySubject::Upload(position));
    coven_foundation::clock::within(wait.wait_for(|wait| wait.as_ref().is_some_and(&ready)))
        .await
        .expect("the upload's wait is armed")
        .expect("the upload's wait")
}

/// The connection's persisted wait, once one `ready` accepts is armed.
pub(crate) async fn connection_wait(
    handle: &CovenHandle,
    ready: impl Fn(&coven_database::RetryWait) -> bool,
) -> coven_database::RetryWait {
    let mut wait = handle.test_follow_retry_wait(coven_database::RetrySubject::Sync);
    coven_foundation::clock::within(wait.wait_for(|wait| wait.as_ref().is_some_and(&ready)))
        .await
        .expect("the connection's wait is armed")
        .expect("the connection's wait")
}

/// Wait for the operation a write journaled to end, as an app waiting in
/// place does, and return its outcome.
pub(crate) async fn outcome(
    handle: &CovenHandle,
    journaled: Option<OperationId>,
) -> OperationOutcome {
    handle.operation_outcome(journaled.expect("the write journaled an operation")).await.unwrap()
}

/// Wait for the operation a write journaled to land, and return its slot.
pub(crate) async fn landed(handle: &CovenHandle, journaled: Option<OperationId>) -> Slot {
    match outcome(handle, journaled).await {
        OperationOutcome::Landed(slot) => slot,
        outcome => panic!("the operation ended {outcome:?}"),
    }
}

/// Make an invite, as an app does in a write of its own, and wait for the
/// pass that grants its access to show its code.
pub(crate) async fn invite(
    owner: &CovenHandle,
    target: InviteTarget,
    access: InviteAccess,
    lifetime: crate::InviteLifetime,
) -> Invite {
    let (id, _, _) = owner.create_invite(target, access, lifetime).await.unwrap();
    owner
        .test_invites()
        .wait_for(|invites| {
            invites
                .iter()
                .any(|invite| invite.id == id && !matches!(invite.stage, InviteStage::Granting))
        })
        .await
        .into_iter()
        .find(|invite| invite.id == id)
        .expect("the invite is listed")
}

/// Approve a join request, as an app does in a write of its own, and wait
/// for its binding to land, which the joining device's admission needs.
pub(crate) async fn approve(owner: &CovenHandle, request: &JoinRequest) -> Slot {
    landed(owner, Some(owner.approve_join_request(request).await.unwrap())).await
}

/// Make a circle, as an app does in a write of its own, and wait for it to
/// land, as the app's rows in it need.
pub(crate) async fn circle(handle: &CovenHandle, name: &str) -> CircleId {
    let (circle, operation) = handle.circles().create(name).await.unwrap();
    landed(handle, Some(operation)).await;
    circle
}

#[test]
fn file_name_error_fields_are_available_through_the_facade() {
    let name: FileName = FileName::new("attachment.data").unwrap();
    let error = DbError::FileNameReused { name };
    let DbError::FileNameReused { name } = error else { unreachable!() };
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
    let record =
        PendingRecord::new(PendingSubject::Snapshot(snapshot), PendingReason::Missing { path });
    assert_eq!(record.retry(), Retry::Automatic);
}

pub(crate) use coven_foundation::clock::PollingClock;
#[path = "../tests/fixtures/file_network.rs"]
mod file_network;
pub(crate) use file_network::*;
#[path = "../tests/fixtures/connector.rs"]
mod connector;
pub(crate) use connector::{Clients, Connector};
#[path = "../tests/fixtures/network.rs"]
mod network;
pub(crate) use network::*;

/// What this installation's keychain holds for a store.
#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) struct Held {
    pub(crate) device: Option<DeviceId>,
    pub(crate) keys: MemberPublicKeys,
    pub(crate) code: Option<Vec<u8>>,
    pub(crate) credentials: Option<Vec<u8>>,
}

pub(crate) fn held(app: &Coven, store: StoreId) -> Held {
    use coven_crypto::custody::KeyringCustody;
    let keychain = Arc::new(app.store_keychain(store));
    let identity =
        Custody::<MemberIdentity>::unlock(&KeyringCustody::new(keychain.clone())).unwrap().unwrap();
    let bytes = |value: Option<SecretBytes>| value.map(|value| value.as_bytes().to_vec());
    Held {
        device: keychain.device_id().unwrap(),
        keys: identity.keys().public_keys(),
        code: bytes(app.person_entries(store).restore_code().unwrap()),
        credentials: bytes(keychain.storage_credentials().unwrap()),
    }
}

/// The first checked request among the owner's open invites.
/// Where `invite`'s join request goes: under the device that made it, as its
/// code names it (§13.2).
pub(crate) fn request_path(invite: &Invite) -> coven_storage::ObjectPath {
    coven_sync::read_invite_code(invite.shown_code()).unwrap().request_path()
}

/// Whether no join request is at `path`: nothing, or the marker an ended
/// invite leaves in the request's place (§13.2).
pub(crate) async fn no_request(
    storage: &coven_storage::test_utils::MemoryStorage,
    path: &coven_storage::ObjectPath,
) -> bool {
    use coven_storage::Storage;
    match storage.read(path).await {
        Ok(found) => coven_format::invite_end::InviteEnd::decode(&found).is_ok(),
        Err(error) if error.failure() == coven_storage::StorageFailure::NotFound => true,
        Err(error) => panic!("reading the request: {error}"),
    }
}

pub(crate) async fn next_join_request(owner: &CovenHandle) -> JoinRequest {
    let invites = owner
        .test_invites()
        .wait_for(|invites| invites.iter().any(|invite| invite.shown_request().cloned().is_some()))
        .await;
    invites.into_iter().find_map(|invite| invite.shown_request().cloned()).unwrap()
}

pub(crate) async fn join_with_response<T, U>(
    joining: impl std::future::Future<Output = T>,
    response: impl std::future::Future<Output = U>,
) -> (T, U) {
    tokio::time::timeout(HANG_GUARD, async { tokio::join!(joining, response) }).await.unwrap()
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
        Ok::<_, CovenError>(())
    }).await.unwrap();
}

/// A network's devices' clock moves only by the passes the test asks for, a
/// second each: one as device 0 sets up, one as it reads the join's request,
/// one as device 1 first syncs, and six as both settle. No wait of device
/// 0's invite, which looks for the request between those passes, passes on
/// its own.
#[tokio::test]
async fn a_join_moves_the_clock_only_by_its_passes() {
    let network = Network::new(2).await;
    assert_eq!(network.clock.now(), UNIX_EPOCH + Duration::from_secs(1009));
    network.close().await;
}

#[tokio::test]
async fn seeded_whole_system_convergence() {
    for seed in [0, 1, 2, 7, 42, 31337] {
        let task = tokio::spawn(async move {
            let mut random = Generator::new(seed);
            let mut network = Network::new(2 + (seed % 3) as usize).await;
            let circle = crate::tests::circle(&network.devices[0].handle, "Friends").await;
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
                network.devices[0].handle.circles().add_member(circle, &member).await.unwrap();
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
                            network.devices[0].handle.write(move |sql| { sql.execute("INSERT INTO circle_notes(id,value,audience) VALUES('00000000-0000-7000-8000-000000000100',19,?1)", [audience])?; Ok::<_, CovenError>(()) }).await.unwrap();
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
                network.devices[0].handle.remove_member(&member).await.unwrap();
                retired.handle.close().await.unwrap();
                network.quiet().await;
            }
            network.assert_converged().await;
            // An offline device's failed passes back off: those waits are
            // held, so only the test's passes moved the clock.
            network.assert_moved_only_by_the_test();
            network.close().await;
        });
        match tokio::time::timeout(HANG_GUARD, task).await {
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
            |_| {},
        )
        .await
        .unwrap();
    network.quiet().await;
    network.devices[0].handle.remove_member(&member).await.unwrap();
    network.devices[0].stop_sync().await;
    let owner = &network.devices[0].handle;
    // The person deletes both keys in the console; coven records nothing (§14.3).
    assert!(owner.pending().await.unwrap().is_empty());
    network.close().await;
}

#[tokio::test]
async fn a_returning_device_loads_snapshots_covering_deleted_writes_and_keeps_its_queue() {
    let network = Network::new(2).await;
    network.devices[1].stop_sync().await;
    network.devices[1]
        .handle
        .write(|sql| {
            sql.execute("INSERT INTO parents VALUES('waiting','local',7)", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    let label = "x".repeat(1024 * 1024 + 1);
    network.devices[0]
        .handle
        .write(move |sql| {
            sql.execute("INSERT INTO parents VALUES('remote',?1,9)", [label])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    network.sync(0).await;
    network.clock.set(network.clock.now() + Duration::from_secs(31 * 24 * 3600));
    network.sync(0).await;
    network.sync(0).await;
    // Retention moved the floor past slot 1 without waiting for device 1.
    let first = ObjectPath::slot(network.devices[0].store, 1.try_into().unwrap());
    assert!(network.devices[0].storage.status(&first).await.unwrap().is_none());
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
    network.devices[1].stop_sync().await;
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
    let mut last = reader.test_last_pass();
    let blocked = last.wait_for(|last| matches!(last, Some(LastPass::Blocked { .. }))).await;
    let Some(LastPass::Blocked { slot, reason, .. }) = blocked else { unreachable!() };
    assert!(matches!(
        reason,
        PendingReason::Storage { failure: coven_storage::StorageFailure::NotFound, .. }
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
    assert_eq!(reader.applied_slot().await, network.devices[0].handle.applied_slot().await);
    network.assert_converged().await;
    network.close().await;
}

mod pending_records {
    use super::*;

    #[tokio::test]
    async fn a_damaged_slot_blocks_the_reader_once_without_reading_it_again() {
        let network = Network::new(2).await;
        let reader = &network.devices[1].handle;
        network.devices[1].stop_sync().await;
        let mut received: crate::LiveQuery<_, crate::CovenError> = reader.subscribe(|sql| {
            Ok(sql.query("SELECT record FROM coven_pending ORDER BY position", [], |r| {
                r.get::<_, PendingRecord>(0)
            })?)
        });
        assert!(received.next().await.expect("the live query answers").unwrap().is_empty());
        let owner = &network.devices[0].handle;
        owner
            .write(|sql| {
                sql.execute("INSERT INTO parents VALUES('one','label',1)", [])?;
                Ok::<_, CovenError>(())
            })
            .await
            .unwrap();
        network.sync(0).await;
        let storage = &network.devices[0].storage;
        let slot = owner.applied_slot().await;
        let path = ObjectPath::slot(network.devices[0].store, slot.try_into().unwrap());
        let size = storage.status(&path).await.unwrap().unwrap().size;
        storage.corrupt_byte(&path, size as usize - 1).await.unwrap();
        reader.start_sync().await.unwrap();
        reader.sync_now();
        let mut last = reader.test_last_pass();
        last.wait_for(|last| matches!(last, Some(LastPass::Blocked { .. }))).await;
        let record = PendingRecord::new(
            PendingSubject::Slot(Slot(slot)),
            PendingReason::Refused(Refusal::Signature { cause: None }),
        );
        assert_eq!(
            received.next().await.expect("the live query answers").unwrap(),
            vec![record.clone()]
        );
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
        reader.test_parked().await;
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
        coven_format::codes::KeychainRestoreCode::encode("Household", 1, code).unwrap().to_vec(),
    )
}

/// The saved entry iCloud Keychain holds for `store`, decoded.
pub(crate) fn saved_entry(
    app: &Coven,
    store: StoreId,
) -> Option<coven_format::codes::KeychainRestoreCode> {
    app.person_entries(store)
        .restore_code()
        .unwrap()
        .map(|bytes| coven_sync::read_saved_code(store, bytes.as_bytes()).unwrap().0)
}

/// `location` as a code carries it: codes leave out the folder label setup
/// records (format.md D13).
pub(crate) fn as_coded(mut location: crate::StorageConfig) -> crate::StorageConfig {
    match &mut location {
        crate::StorageConfig::Dropbox { folder_label, .. }
        | crate::StorageConfig::OneDrive { folder_label, .. } => *folder_label = None,
        crate::StorageConfig::S3 { .. } | crate::StorageConfig::CloudKit { .. } => {}
    }
    location
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
