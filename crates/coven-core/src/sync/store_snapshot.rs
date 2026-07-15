//! Append-only snapshot publication for the Store protocol.

use super::membership::MembershipChain;
use super::publish_blobs::ensure_publishable_blobs;
use super::snapshot::{CreatedSnapshot, SnapshotError};
use super::storage::SyncStorage;
use super::store_commit::{
    snapshot_image_semantic_prefix, snapshot_semantic_prefix, CommitFrontier, ObjectHash,
    SnapshotMeta,
};
use super::store_objects::append_and_verify;
use super::store_objects::{list_snapshot_metas, load_snapshot_image};
use crate::keys::UserKeypair;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn push_store_snapshot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    snapshot: CreatedSnapshot,
    coverage: CommitFrontier,
    schema_version: u32,
    keypair: &UserKeypair,
    created_at: String,
    membership: Option<&MembershipChain>,
    db: &crate::database::Database,
) -> Result<SnapshotMeta, SnapshotError> {
    let _publication = db.lock_snapshot_publication().await;
    if let Some(pending) = db
        .outbound_snapshot_publication()
        .await
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
    {
        return publish_durable_snapshot(storage, db, pending).await;
    }
    let author = hex::encode(keypair.public_key());
    let authorized = match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            membership.is_none_or(|chain| chain.is_owner_now(&author))
        }
        crate::WritePolicy::Serial => db
            .serial_membership_state()
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
            .ok_or_else(|| {
                SnapshotError::PublicationState(
                    "Serial snapshot publication has no membership state".to_string(),
                )
            })?
            .is_owner(&author),
    };
    if !authorized {
        return Err(SnapshotError::UnauthorizedAuthor(author));
    }
    if coverage.policy() != db.write_policy() {
        return Err(SnapshotError::Parse(format!(
            "snapshot coverage uses {:?}, database uses {:?}",
            coverage.policy(),
            db.write_policy()
        )));
    }
    if !snapshot.publish_blobs.is_empty() {
        ensure_publishable_blobs(db, storage, &snapshot.publish_blobs)
            .await
            .map_err(|error| match error {
                super::publish_blobs::PublishBlobError::RemoteCheck {
                    namespace,
                    id,
                    source,
                } => SnapshotError::PublishBlobRemoteCheck {
                    namespace,
                    id,
                    source,
                },
                error => SnapshotError::PublishBlobs(error.to_string()),
            })?;
    }
    let image_hash = ObjectHash::digest(&snapshot.db_image);
    let meta = SnapshotMeta::signed(
        store_root_hash,
        image_hash,
        coverage,
        schema_version,
        created_at,
        keypair,
    )
    .map_err(|error| SnapshotError::Parse(error.to_string()))?;
    let snapshot_hash = meta.snapshot_hash();
    db.stage_snapshot_publication(
        snapshot_hash,
        image_hash,
        snapshot.db_image,
        meta.to_bytes(),
    )
    .await
    .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
    let pending = db
        .outbound_snapshot_publication()
        .await
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
        .ok_or_else(|| {
            SnapshotError::PublicationState("staged snapshot publication row is absent".to_string())
        })?;
    publish_durable_snapshot(storage, db, pending).await
}

pub(crate) async fn drain_outbound_store_snapshot(
    storage: &dyn SyncStorage,
    db: &crate::database::Database,
) -> Result<Option<SnapshotMeta>, SnapshotError> {
    let _publication = db.lock_snapshot_publication().await;
    let Some(pending) = db
        .outbound_snapshot_publication()
        .await
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
    else {
        return Ok(None);
    };
    publish_durable_snapshot(storage, db, pending)
        .await
        .map(Some)
}

async fn publish_durable_snapshot(
    storage: &dyn SyncStorage,
    db: &crate::database::Database,
    pending: crate::database::DurableSnapshotPublication,
) -> Result<SnapshotMeta, SnapshotError> {
    let unverified: SnapshotMeta = serde_json::from_slice(&pending.meta_bytes)
        .map_err(|error| SnapshotError::PublicationState(format!("snapshot metadata: {error}")))?;
    let meta = SnapshotMeta::parse_at(
        &pending.meta_bytes,
        unverified.store_root_hash,
        &unverified.author_pubkey,
        pending.snapshot_hash,
    )
    .map_err(|error| {
        SnapshotError::PublicationState(format!("verify snapshot metadata: {error}"))
    })?;
    if meta.image_hash != pending.image_hash
        || ObjectHash::digest(&pending.image_bytes) != pending.image_hash
    {
        return Err(SnapshotError::PublicationState(
            "snapshot metadata and image bytes do not name the same image".to_string(),
        ));
    }
    append_and_verify(
        storage,
        &snapshot_image_semantic_prefix(&meta.author_pubkey, pending.image_hash),
        ".db",
        &pending.image_bytes,
    )
    .await
    .map_err(SnapshotError::StoreObject)?;
    append_and_verify(
        storage,
        &snapshot_semantic_prefix(&meta.author_pubkey, pending.snapshot_hash),
        ".json",
        &pending.meta_bytes,
    )
    .await
    .map_err(SnapshotError::StoreObject)?;
    db.complete_snapshot_publication(pending.snapshot_hash)
        .await
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
    Ok(meta)
}

pub async fn select_store_snapshot(
    storage: &dyn SyncStorage,
    store_id: &str,
    expected_store_root_hash: ObjectHash,
    expected_founder: &str,
    membership_floor: &crate::join_code::MembershipFloor,
    binary_schema_version: u32,
) -> Result<(ObjectHash, crate::WritePolicy, SnapshotMeta, Vec<u8>), SnapshotError> {
    let store_protocol_root = super::store_objects::load_pinned_store_protocol_root(
        storage,
        expected_store_root_hash,
        store_id,
        expected_founder,
    )
    .await
    .map_err(snapshot_object_error)?
    .ok_or_else(|| {
        SnapshotError::Bucket(super::storage::StorageError::NotFound(
            super::store_commit::store_protocol_root_semantic_prefix(expected_store_root_hash),
        ))
    })?;
    let membership = match (store_protocol_root.value.write_policy, membership_floor) {
        (
            crate::WritePolicy::MergeConcurrent,
            crate::join_code::MembershipFloor::MergeConcurrent(floor),
        ) => {
            let entries = super::membership_ops::list_membership_entries(storage)
                .await
                .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?;
            Some(
                super::membership_ops::load_anchored_chain_at_floor(
                    storage,
                    &entries,
                    &store_protocol_root.value.author_pubkey,
                    floor,
                )
                .await
                .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?,
            )
        }
        (crate::WritePolicy::Serial, crate::join_code::MembershipFloor::Serial(_)) => None,
        (policy, floor) => {
            return Err(SnapshotError::UnauthorizedAuthor(format!(
                "invite membership floor {floor:?} does not match Store write policy {policy:?}"
            )))
        }
    };
    let metas = list_snapshot_metas(storage, store_protocol_root.semantic_hash)
        .await
        .map_err(snapshot_object_error)?;
    let mut authorized = Vec::new();
    for meta in metas.metas {
        if meta.value.coverage.policy() != store_protocol_root.value.write_policy {
            return Err(SnapshotError::Parse(format!(
                "snapshot coverage uses {:?}, Store protocol root uses {:?}",
                meta.value.coverage.policy(),
                store_protocol_root.value.write_policy
            )));
        }
        let author_is_owner = match membership.as_ref() {
            Some(membership) => membership.is_owner_now(&meta.value.author_pubkey),
            None => {
                let position = meta
                    .value
                    .coverage
                    .serial_position()
                    .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?
                    .cloned();
                super::store_pull::load_serial_authorization_at_position(
                    storage,
                    store_protocol_root.semantic_hash,
                    position,
                )
                .await
                .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?
                .membership
                .is_owner(&meta.value.author_pubkey)
            }
        };
        if !author_is_owner {
            continue;
        }
        authorized.push(meta.value);
    }
    if authorized.is_empty() {
        return Err(SnapshotError::Bucket(
            super::storage::StorageError::NotFound("store-v1/snapshots".to_string()),
        ));
    }
    let mut maximal = Vec::new();
    for (index, candidate) in authorized.iter().enumerate() {
        let dominated = authorized.iter().enumerate().any(|(other_index, other)| {
            other_index != index && coverage_dominates(&other.coverage, &candidate.coverage)
        });
        if !dominated {
            maximal.push(candidate.clone());
        }
    }
    maximal.sort_by_key(SnapshotMeta::snapshot_hash);
    let chosen = maximal
        .pop()
        .expect("an authorized snapshot has at least one maximal element");
    if chosen.schema_version > binary_schema_version {
        return Err(SnapshotError::SchemaTooNew {
            snapshot_version: chosen.schema_version,
            supported: binary_schema_version,
        });
    }
    let image = load_snapshot_image(storage, &chosen.author_pubkey, chosen.image_hash)
        .await
        .map_err(snapshot_object_error)?
        .ok_or_else(|| {
            SnapshotError::Bucket(super::storage::StorageError::NotFound(
                snapshot_image_semantic_prefix(&chosen.author_pubkey, chosen.image_hash),
            ))
        })?;
    Ok((
        store_protocol_root.semantic_hash,
        store_protocol_root.value.write_policy,
        chosen,
        image.value,
    ))
}

pub(crate) fn coverage_dominates(left: &CommitFrontier, right: &CommitFrontier) -> bool {
    let (CommitFrontier::MergeConcurrent(left), CommitFrontier::MergeConcurrent(right)) =
        (left, right)
    else {
        return match (left, right) {
            (CommitFrontier::Serial(Some(_)), CommitFrontier::Serial(None)) => true,
            (CommitFrontier::Serial(Some(left)), CommitFrontier::Serial(Some(right))) => {
                left.seq > right.seq
                    || (left.seq == right.seq && left.commit_hash == right.commit_hash)
            }
            _ => false,
        };
    };
    let mut strictly_ahead = false;
    for (device_id, right_position) in right {
        let Some(left_position) = left.get(device_id) else {
            return false;
        };
        if left_position.seq < right_position.seq
            || (left_position.seq == right_position.seq
                && left_position.commit_hash != right_position.commit_hash)
        {
            return false;
        }
        strictly_ahead |= left_position.seq > right_position.seq;
    }
    strictly_ahead || left.len() > right.len()
}

fn snapshot_object_error(error: super::store_objects::StoreObjectError) -> SnapshotError {
    SnapshotError::Bucket(super::storage::StorageError::Storage(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{CloudHome, SequentialCopyIdGenerator};
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::founder_entry;
    use crate::sync::snapshot::CreatedSnapshot;
    use crate::sync::store_commit::{
        store_protocol_root_semantic_prefix, CommitPosition, StoreProtocolRoot,
    };
    use crate::sync::store_objects::{
        append_and_verify, discover_store_protocol_root, StoreObjectError,
    };
    use crate::sync::test_helpers::{
        open_serial_test_db, open_test_db, publish_test_serial_store_protocol_root,
        publish_test_store_protocol_root, test_migrations, test_synced_tables,
    };

    fn storage(
        home: &InMemoryCloudHome,
        signer: &UserKeypair,
        copy_source: &str,
    ) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "snapshot-store-test",
            signer.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(copy_source)))
    }

    async fn initialized_store(
        copy_source: &str,
    ) -> (
        InMemoryCloudHome,
        UserKeypair,
        CloudSyncStorage,
        crate::database::Database,
        ObjectHash,
        crate::join_code::MembershipFloor,
    ) {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let storage = storage(&home, &owner, copy_source);
        let db = open_test_db();
        let store_root_hash = publish_test_store_protocol_root(
            &db,
            &storage,
            "snapshot-store-test",
            "dev-owner",
            &owner,
        )
        .await;
        let membership = crate::sync::test_helpers::publish_test_founder_membership(
            &storage,
            "snapshot-store-test",
            &owner,
        )
        .await;
        (
            home,
            owner,
            storage,
            db,
            store_root_hash,
            crate::join_code::MembershipFloor::MergeConcurrent(membership.author_heads()),
        )
    }

    fn snapshot(bytes: &[u8]) -> CreatedSnapshot {
        CreatedSnapshot {
            db_image: bytes.to_vec(),
            host_blobs: Vec::new(),
            publish_blobs: Vec::new(),
        }
    }

    fn merge_coverage(coverage: BTreeMap<String, CommitPosition>) -> CommitFrontier {
        CommitFrontier::MergeConcurrent(coverage)
    }

    fn count_prefix(home: &InMemoryCloudHome, prefix: &str) -> usize {
        home.appended_keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .count()
    }

    #[tokio::test]
    async fn store_protocol_root_failures_leave_no_false_pin_and_ambiguous_append_coalesces_on_retry(
    ) {
        let home = InMemoryCloudHome::new();
        let founder = UserKeypair::generate();
        let storage = storage(&home, &founder, "store-protocol-root-crash");
        let store_protocol_root = StoreProtocolRoot::signed(
            "snapshot-store-test".to_string(),
            founder_entry(
                "snapshot-store-test",
                &founder,
                "0000000000001-0000-founder",
            ),
            1,
            crate::sync::test_helpers::test_sync_routing_hash(),
            crate::WritePolicy::MergeConcurrent,
            &founder,
        )
        .unwrap();
        let hash = store_protocol_root.object_hash();

        home.fail_append_before_call(1);
        assert!(append_and_verify(
            &storage,
            &store_protocol_root_semantic_prefix(hash),
            ".json",
            &store_protocol_root.to_bytes(),
        )
        .await
        .is_err());
        assert!(matches!(
            discover_store_protocol_root(&storage, "snapshot-store-test", None).await,
            Err(StoreObjectError::Storage(
                super::super::storage::StorageError::NotFound(_)
            ))
        ));

        home.fail_append_after_call(1);
        assert!(append_and_verify(
            &storage,
            &store_protocol_root_semantic_prefix(hash),
            ".json",
            &store_protocol_root.to_bytes(),
        )
        .await
        .is_err());
        let visible = discover_store_protocol_root(&storage, "snapshot-store-test", None)
            .await
            .expect("ambiguous append left an exact valid Store protocol root");
        assert_eq!(visible.semantic_hash, hash);
        assert_eq!(visible.copies.len(), 1);

        append_and_verify(
            &storage,
            &store_protocol_root_semantic_prefix(hash),
            ".json",
            &store_protocol_root.to_bytes(),
        )
        .await
        .expect("retry Store protocol root append");
        let retried = discover_store_protocol_root(&storage, "snapshot-store-test", None)
            .await
            .expect("coalesce exact Store protocol root retries");
        assert_eq!(retried.semantic_hash, hash);
        assert_eq!(retried.copies.len(), 2);
    }

    #[tokio::test]
    async fn store_protocol_root_discovery_ignores_unrelated_objects_and_refuses_multiple_valid_roots(
    ) {
        let home = InMemoryCloudHome::new();
        home.put_object("unrelated/object.json", b"unrelated bytes".to_vec())
            .await
            .unwrap();
        let first = UserKeypair::generate();
        let storage = storage(&home, &first, "store-protocol-root-fork");
        assert!(matches!(
            discover_store_protocol_root(&storage, "snapshot-store-test", None).await,
            Err(StoreObjectError::Storage(
                super::super::storage::StorageError::NotFound(_)
            ))
        ));

        for (index, signer) in [first, UserKeypair::generate()].into_iter().enumerate() {
            let store_protocol_root = StoreProtocolRoot::signed(
                "snapshot-store-test".to_string(),
                founder_entry(
                    "snapshot-store-test",
                    &signer,
                    &format!("000000000000{}-0000-founder", index + 1),
                ),
                1,
                crate::sync::test_helpers::test_sync_routing_hash(),
                crate::WritePolicy::MergeConcurrent,
                &signer,
            )
            .unwrap();
            append_and_verify(
                &storage,
                &store_protocol_root_semantic_prefix(store_protocol_root.object_hash()),
                ".json",
                &store_protocol_root.to_bytes(),
            )
            .await
            .unwrap();
        }
        assert!(matches!(
            discover_store_protocol_root(&storage, "snapshot-store-test", None).await,
            Err(StoreObjectError::SemanticFork { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_image_and_meta_failures_never_activate_an_incomplete_generation() {
        for failed_call in 1..=2 {
            let (home, owner, storage, db, store_root_hash, floor) =
                initialized_store(&format!("snapshot-before-{failed_call}")).await;
            home.fail_append_before_call(failed_call);
            let first = push_store_snapshot(
                &storage,
                store_root_hash,
                snapshot(b"snapshot-image"),
                merge_coverage(BTreeMap::new()),
                1,
                &owner,
                "2026-01-01T00:00:00Z".to_string(),
                None,
                &db,
            )
            .await;
            assert!(first.is_err());
            assert_eq!(
                count_prefix(&home, "store-v1/snapshot-images/"),
                usize::from(failed_call > 1),
            );
            assert_eq!(count_prefix(&home, "store-v1/snapshots/"), 0);

            push_store_snapshot(
                &storage,
                store_root_hash,
                snapshot(b"snapshot-image"),
                merge_coverage(BTreeMap::new()),
                1,
                &owner,
                "2026-01-01T00:00:00Z".to_string(),
                None,
                &db,
            )
            .await
            .expect("retry snapshot publication");
            let (_, _, selected, image) = select_store_snapshot(
                &storage,
                "snapshot-store-test",
                store_root_hash,
                &crate::keys::public_key_hex(&owner),
                &floor,
                1,
            )
            .await
            .expect("select completed snapshot");
            assert_eq!(selected.image_hash, ObjectHash::digest(b"snapshot-image"));
            assert_eq!(image, b"snapshot-image");
        }
    }

    #[tokio::test]
    async fn ambiguous_meta_append_is_already_selectable_and_retry_coalesces() {
        let (home, owner, storage, db, store_root_hash, floor) =
            initialized_store("snapshot-after").await;
        home.fail_append_after_call(2);
        assert!(push_store_snapshot(
            &storage,
            store_root_hash,
            snapshot(b"ambiguous-snapshot"),
            merge_coverage(BTreeMap::new()),
            1,
            &owner,
            "2026-01-01T00:00:00Z".to_string(),
            None,
            &db,
        )
        .await
        .is_err());
        let (_, _, selected, image) = select_store_snapshot(
            &storage,
            "snapshot-store-test",
            store_root_hash,
            &crate::keys::public_key_hex(&owner),
            &floor,
            1,
        )
        .await
        .expect("meta physically visible despite ambiguous response");
        assert_eq!(image, b"ambiguous-snapshot");

        push_store_snapshot(
            &storage,
            store_root_hash,
            snapshot(b"ambiguous-snapshot"),
            selected.coverage.clone(),
            1,
            &owner,
            "2026-01-01T00:00:00Z".to_string(),
            None,
            &db,
        )
        .await
        .expect("retry exact snapshot generation");
        assert_eq!(count_prefix(&home, "store-v1/snapshots/"), 2);
    }

    #[tokio::test]
    async fn snapshot_publication_resumes_exact_bytes_after_restart_and_lost_append_result() {
        let directory = tempfile::tempdir().expect("snapshot outbox directory");
        let path = directory.path().join("store.sqlite3");
        let open = || {
            crate::database::Database::open(
                &path,
                crate::sync::test_helpers::test_synced_tables(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                "snapshot-restart-device".to_string(),
                &crate::sync::test_helpers::test_migrations(),
            )
            .expect("open snapshot outbox database")
            .0
        };
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let storage = storage(&home, &owner, "snapshot-restart");
        let db = open();
        let store_root_hash = publish_test_store_protocol_root(
            &db,
            &storage,
            "snapshot-store-test",
            "snapshot-restart-device",
            &owner,
        )
        .await;
        let membership = crate::sync::test_helpers::publish_test_founder_membership(
            &storage,
            "snapshot-store-test",
            &owner,
        )
        .await;

        home.fail_append_after_call(1);
        let first = push_store_snapshot(
            &storage,
            store_root_hash,
            snapshot(b"restart-exact-snapshot"),
            merge_coverage(BTreeMap::new()),
            1,
            &owner,
            "2026-07-14T00:00:00Z".to_string(),
            Some(&membership),
            &db,
        )
        .await;
        assert!(
            first.is_err(),
            "lost image append result must fail the caller"
        );
        let pending = db
            .outbound_snapshot_publication()
            .await
            .expect("read snapshot outbox")
            .expect("snapshot outbox remains owned");
        drop(db);

        let reopened = open();
        let published = drain_outbound_store_snapshot(&storage, &reopened)
            .await
            .expect("resume exact snapshot publication")
            .expect("pending snapshot was drained");
        assert_eq!(published.snapshot_hash(), pending.snapshot_hash);
        assert_eq!(published.image_hash, pending.image_hash);
        assert!(reopened
            .outbound_snapshot_publication()
            .await
            .expect("read completed snapshot outbox")
            .is_none());
        assert_eq!(
            reopened
                .get_protocol_state(crate::database::LAST_SNAPSHOT_HASH_STATE_KEY)
                .await
                .expect("read completed snapshot hash"),
            Some(pending.snapshot_hash.to_string()),
        );
    }

    #[tokio::test]
    async fn winning_newer_schema_snapshot_fails_without_falling_back_or_opening_its_image() {
        let (home, owner, storage, db, store_root_hash, floor) =
            initialized_store("snapshot-schema").await;
        push_store_snapshot(
            &storage,
            store_root_hash,
            snapshot(b"older"),
            merge_coverage(BTreeMap::new()),
            1,
            &owner,
            "2026-01-01T00:00:00Z".to_string(),
            None,
            &db,
        )
        .await
        .unwrap();
        let mut coverage = BTreeMap::new();
        coverage.insert(
            "dev-a".to_string(),
            CommitPosition {
                seq: 1,
                commit_hash: ObjectHash::digest(b"covered-commit"),
            },
        );
        let newer = push_store_snapshot(
            &storage,
            store_root_hash,
            snapshot(b"newer"),
            merge_coverage(coverage),
            2,
            &owner,
            "2026-01-02T00:00:00Z".to_string(),
            None,
            &db,
        )
        .await
        .unwrap();
        let image_prefix = snapshot_image_semantic_prefix(&newer.author_pubkey, newer.image_hash);
        let image_listing = home.list_appended(&image_prefix).await.unwrap();
        for locator in image_listing.objects {
            home.remove_appended_candidate(&locator);
        }

        assert!(matches!(
            select_store_snapshot(
                &storage,
                "snapshot-store-test",
                store_root_hash,
                &crate::keys::public_key_hex(&owner),
                &floor,
                1,
            )
            .await,
            Err(SnapshotError::SchemaTooNew {
                snapshot_version: 2,
                supported: 1,
            })
        ));
    }

    #[tokio::test]
    async fn empty_serial_snapshot_bootstrap_preserves_the_root_frontier_and_policy() {
        let temp = tempfile::tempdir().expect("Serial snapshot directory");
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let storage = storage(&home, &owner, "serial-snapshot");
        let source = open_serial_test_db();
        let store_root_hash = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "snapshot-store-test",
            "serial-source",
            &owner,
        )
        .await;
        let snapshot_dir = temp.path().to_path_buf();
        let tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                crate::sync::snapshot::create_snapshot(connection, &snapshot_dir, &tables)
                    .map_err(|error| crate::database::DbError(error.to_string()))
            })
            .await
            .expect("create Serial snapshot image");
        assert!(matches!(
            push_store_snapshot(
                &storage,
                store_root_hash,
                snapshot(b"wrong-policy"),
                CommitFrontier::MergeConcurrent(BTreeMap::new()),
                source.schema_version(),
                &owner,
                "2026-07-14T00:00:00Z".to_string(),
                None,
                &source,
            )
            .await,
            Err(SnapshotError::Parse(_))
        ));

        push_store_snapshot(
            &storage,
            store_root_hash,
            CreatedSnapshot {
                db_image: image,
                host_blobs: Vec::new(),
                publish_blobs: Vec::new(),
            },
            CommitFrontier::Serial(None),
            source.schema_version(),
            &owner,
            "2026-07-14T00:00:01Z".to_string(),
            None,
            &source,
        )
        .await
        .expect("publish Serial snapshot");

        let target = temp.path().join("serial-bootstrap.db");
        let bootstrap = crate::sync::snapshot::bootstrap_from_snapshot(
            &storage,
            "snapshot-store-test",
            store_root_hash,
            &crate::keys::public_key_hex(&owner),
            &crate::join_code::MembershipFloor::Serial(None),
            source.schema_version(),
            &target,
        )
        .await
        .expect("select Serial snapshot");
        assert_eq!(bootstrap.write_policy(), crate::WritePolicy::Serial);
        let installed = bootstrap
            .open_database(
                "snapshot-store-test",
                &target,
                test_synced_tables(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                "serial-reader".to_string(),
                &test_migrations(),
            )
            .await
            .expect("install Serial snapshot");
        assert_eq!(installed.write_policy(), crate::WritePolicy::Serial);
        assert_eq!(
            installed.snapshot_coverage_frontier().await.unwrap(),
            BTreeMap::new()
        );
        assert!(!home
            .appended_keys()
            .iter()
            .any(|key| key.starts_with("store-v1/membership/")));
    }
}
