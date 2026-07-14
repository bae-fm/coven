//! Proof-gated deletion of Store package copies covered by a snapshot.

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::cloud::ListingCoverage;

use super::membership::MembershipChain;
use super::storage::SyncStorage;
use super::store_commit::{
    CommitPosition, ObjectHash, SnapshotMeta, StoreAck, StoreDeviceRegistrationState,
};
use super::store_objects::{
    list_latest_ack_chains, list_latest_registration_chains, list_snapshot_metas,
    list_store_packages, load_commit_slot, load_snapshot_image, StoreObjectError,
};

#[derive(Debug, PartialEq, Eq)]
pub struct StoreReclaimResult {
    pub packages_deleted: u64,
    pub physical_copies_deleted: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreReclaimError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("Store package reclamation requires a complete {listing} listing")]
    IncompleteListing { listing: &'static str },
    #[error("no authorized complete Store snapshot is available for reclamation")]
    NoSnapshot,
    #[error("active member {member:?} has no Store device registration history")]
    MissingRegisteredDevice { member: String },
    #[error(
        "active Store device {device_id:?} for member {member:?} has no valid acknowledgement"
    )]
    MissingAcknowledgement { member: String, device_id: String },
    #[error("Store device {device_id:?} registration author {registration_author:?} does not match acknowledgement author {ack_author:?}")]
    AckAuthorMismatch {
        device_id: String,
        registration_author: String,
        ack_author: String,
    },
    #[error("active member {member:?} device {ack_device_id:?} has no acknowledgement covering snapshot position {device_id}/{position:?}")]
    StaleAcknowledgement {
        member: String,
        ack_device_id: String,
        device_id: String,
        position: CommitPosition,
    },
    #[error(
        "Store ancestry for {device_id}/{position:?} is missing commit sequence {missing_seq}"
    )]
    MissingAncestry {
        device_id: String,
        position: CommitPosition,
        missing_seq: u64,
    },
    #[error("Store ancestry for {device_id}/{seq} hashes to {actual}, expected {expected}")]
    AncestryMismatch {
        device_id: String,
        seq: u64,
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("deleting Store package {device_id}/{seq} failed after {deleted_copies} physical copies: {source}")]
    PartialDelete {
        device_id: String,
        seq: u64,
        deleted_copies: u64,
        source: super::storage::StorageError,
    },
}

pub async fn reclaim_store_packages(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    membership: &MembershipChain,
    membership_proof: super::pull::MembershipListingProof,
) -> Result<StoreReclaimResult, StoreReclaimError> {
    if !membership_proof.is_complete() {
        return Err(StoreReclaimError::IncompleteListing {
            listing: "membership",
        });
    }
    let metas = list_snapshot_metas(storage, store_root_hash).await?;
    require_complete(metas.coverage, "snapshot metadata")?;
    if metas
        .metas
        .iter()
        .any(|meta| meta.coverage != ListingCoverage::CompleteAtScan)
    {
        return Err(StoreReclaimError::IncompleteListing {
            listing: "snapshot metadata",
        });
    }
    let snapshot = choose_snapshot(storage, membership, metas.metas).await?;

    let registration_listing = list_latest_registration_chains(storage, store_root_hash).await?;
    require_complete(registration_listing.coverage, "device registration")?;
    if registration_listing
        .latest_by_device
        .values()
        .any(|registration| registration.coverage != ListingCoverage::CompleteAtScan)
    {
        return Err(StoreReclaimError::IncompleteListing {
            listing: "device registration proof",
        });
    }
    let ack_chains = list_latest_ack_chains(storage, store_root_hash).await?;
    require_complete(ack_chains.coverage, "acknowledgement")?;
    require_registered_device_acks(
        storage,
        store_root_hash,
        membership,
        &snapshot,
        &registration_listing.latest_by_device,
        &ack_chains.latest_by_device,
    )
    .await?;

    let package_listing = list_store_packages(storage, store_root_hash).await?;
    require_complete(package_listing.coverage, "package")?;
    if package_listing.packages.iter().any(|package| {
        package.package.coverage != ListingCoverage::CompleteAtScan
            || package.commit_coverage != ListingCoverage::CompleteAtScan
    }) {
        return Err(StoreReclaimError::IncompleteListing {
            listing: "package proof",
        });
    }

    let mut packages_deleted = 0_u64;
    let mut physical_copies_deleted = 0_u64;
    for package in package_listing.packages {
        let Some(snapshot_position) = snapshot.coverage.get(&package.device_id) else {
            continue;
        };
        if !position_covers(
            storage,
            store_root_hash,
            &package.device_id,
            snapshot_position,
            &package.commit_position,
        )
        .await?
        {
            continue;
        }
        let mut deleted_for_package = 0_u64;
        for locator in &package.package.copies {
            if let Err(source) = storage.delete_protocol_object(locator).await {
                return Err(StoreReclaimError::PartialDelete {
                    device_id: package.device_id,
                    seq: package.seq,
                    deleted_copies: deleted_for_package,
                    source,
                });
            }
            deleted_for_package += 1;
        }
        packages_deleted += 1;
        physical_copies_deleted += deleted_for_package;
    }
    Ok(StoreReclaimResult {
        packages_deleted,
        physical_copies_deleted,
    })
}

fn require_complete(
    coverage: ListingCoverage,
    listing: &'static str,
) -> Result<(), StoreReclaimError> {
    if coverage != ListingCoverage::CompleteAtScan {
        return Err(StoreReclaimError::IncompleteListing { listing });
    }
    Ok(())
}

async fn choose_snapshot(
    storage: &dyn SyncStorage,
    membership: &MembershipChain,
    metas: Vec<super::store_objects::VerifiedCopies<SnapshotMeta>>,
) -> Result<SnapshotMeta, StoreReclaimError> {
    let mut authorized = Vec::new();
    for meta in metas {
        if !membership.is_owner_now(&meta.value.author_pubkey) {
            continue;
        }
        let Some(image) =
            load_snapshot_image(storage, &meta.value.author_pubkey, meta.value.image_hash).await?
        else {
            continue;
        };
        require_complete(image.coverage, "snapshot image")?;
        authorized.push(meta.value);
    }
    let mut maximal = Vec::new();
    for (index, candidate) in authorized.iter().enumerate() {
        let dominated = authorized.iter().enumerate().any(|(other_index, other)| {
            other_index != index
                && super::store_snapshot::coverage_dominates(&other.coverage, &candidate.coverage)
        });
        if !dominated {
            maximal.push(candidate.clone());
        }
    }
    maximal.sort_by_key(SnapshotMeta::snapshot_hash);
    maximal.pop().ok_or(StoreReclaimError::NoSnapshot)
}

async fn require_registered_device_acks(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    membership: &MembershipChain,
    snapshot: &SnapshotMeta,
    registrations: &BTreeMap<
        String,
        super::store_objects::VerifiedCopies<super::store_commit::StoreDeviceRegistration>,
    >,
    latest_by_device: &BTreeMap<String, super::store_objects::VerifiedCopies<StoreAck>>,
) -> Result<(), StoreReclaimError> {
    let active: BTreeSet<_> = membership
        .current_members()
        .into_iter()
        .map(|(pubkey, _)| pubkey)
        .collect();
    let mut active_registrations = BTreeMap::new();
    let mut authors_with_active_registration = BTreeSet::new();
    for (device_id, registration) in registrations {
        let author = registration.value.author_pubkey.clone();
        if !active.contains(&author) {
            continue;
        }
        if registration.value.state == StoreDeviceRegistrationState::Active {
            authors_with_active_registration.insert(author);
            active_registrations.insert(device_id.clone(), registration);
        }
    }
    for member in active {
        if !authors_with_active_registration.contains(&member) {
            return Err(StoreReclaimError::MissingRegisteredDevice { member });
        }
    }
    for (ack_device_id, registration) in active_registrations {
        let member = registration.value.author_pubkey.clone();
        let ack = latest_by_device.get(&ack_device_id).ok_or_else(|| {
            StoreReclaimError::MissingAcknowledgement {
                member: member.clone(),
                device_id: ack_device_id.clone(),
            }
        })?;
        if ack.value.author_pubkey != member {
            return Err(StoreReclaimError::AckAuthorMismatch {
                device_id: ack_device_id,
                registration_author: member,
                ack_author: ack.value.author_pubkey.clone(),
            });
        }
        for (device_id, snapshot_position) in &snapshot.coverage {
            let covered = match ack.value.frontier.get(device_id) {
                Some(ack_position) => {
                    position_covers(
                        storage,
                        store_root_hash,
                        device_id,
                        ack_position,
                        snapshot_position,
                    )
                    .await?
                }
                None => false,
            };
            if !covered {
                return Err(StoreReclaimError::StaleAcknowledgement {
                    member: member.clone(),
                    ack_device_id: ack.value.device_id.clone(),
                    device_id: device_id.clone(),
                    position: snapshot_position.clone(),
                });
            }
        }
    }
    Ok(())
}

async fn position_covers(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    device_id: &str,
    covering: &CommitPosition,
    covered: &CommitPosition,
) -> Result<bool, StoreReclaimError> {
    if covering.seq < covered.seq {
        return Ok(false);
    }
    let mut seq = covering.seq;
    let mut expected = covering.commit_hash;
    while seq > covered.seq {
        let commit = load_commit_slot(storage, store_root_hash, device_id, seq)
            .await?
            .ok_or_else(|| StoreReclaimError::MissingAncestry {
                device_id: device_id.to_string(),
                position: covering.clone(),
                missing_seq: seq,
            })?;
        require_complete(commit.coverage, "commit ancestry")?;
        let actual = commit.value.commit_hash();
        if actual != expected {
            return Err(StoreReclaimError::AncestryMismatch {
                device_id: device_id.to_string(),
                seq,
                expected,
                actual,
            });
        }
        expected = commit
            .value
            .previous_commit_hash
            .expect("verified commit above covered sequence has a predecessor");
        seq -= 1;
    }
    Ok(expected == covered.commit_hash)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::keys::UserKeypair;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{CopyId, SequentialCopyIdGenerator};
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::{founder_entry, MemberRole};
    use crate::sync::store_commit::{
        ack_copy_key, ack_semantic_prefix, registration_semantic_prefix, StoreAck,
        StoreDeviceRegistration, StoreDeviceRegistrationState,
    };
    use crate::sync::store_objects::{append_and_verify, load_commit_slot, load_package};
    use crate::sync::store_outbound::{drain_store_writes, prepare_pending_store_write};
    use crate::sync::test_helpers::{
        bootstrap_chain, host_exec, open_test_db, pubkey_hex, publish_test_store_protocol_root,
        temp_store_dir,
    };

    struct ReclaimSetup {
        home: InMemoryCloudHome,
        storage: CloudSyncStorage,
        owner: UserKeypair,
        member: Option<UserKeypair>,
        chain: MembershipChain,
        store_root_hash: ObjectHash,
        coverage: BTreeMap<String, CommitPosition>,
    }

    fn storage(home: &InMemoryCloudHome, signer: &UserKeypair, source: &str) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "reclaim-store-test",
            signer.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(source)))
    }

    async fn setup(with_member: bool) -> ReclaimSetup {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let storage = storage(&home, &owner, "reclaim-owner");
        let db = open_test_db();
        let store_root_hash = publish_test_store_protocol_root(
            &db,
            &storage,
            "reclaim-store-test",
            "dev-owner",
            &owner,
        )
        .await;
        let mut chain = bootstrap_chain(founder_entry(
            "reclaim-store-test",
            &owner,
            "0000000000001-0000-test-store-protocol-root",
        ));
        let member = with_member.then(UserKeypair::generate);
        if let Some(member) = &member {
            let entry = chain
                .signed_set_member(
                    &owner,
                    pubkey_hex(member),
                    None,
                    MemberRole::Member,
                    "0000000000002-0000-owner".to_string(),
                )
                .expect("owner adds member");
            chain.add_entry(entry).unwrap();
        }
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'covered', NULL, 1, '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write(
            &db,
            &storage,
            "dev-owner",
            "2026-01-01T00:00:00Z",
            &owner,
            &store_dir,
            Some(&chain),
            None,
        )
        .await
        .unwrap());
        assert_eq!(drain_store_writes(&db, &storage).await.unwrap(), 1);
        let coverage = db.materialized_frontier().await.unwrap();
        super::super::store_snapshot::push_store_snapshot(
            &storage,
            store_root_hash,
            super::super::snapshot::CreatedSnapshot {
                db_image: b"reclaim snapshot".to_vec(),
                host_blobs: Vec::new(),
                publish_blobs: Vec::new(),
            },
            coverage.clone(),
            1,
            &owner,
            "2026-01-01T00:00:00Z".to_string(),
            Some(&chain),
            &db,
        )
        .await
        .unwrap();
        let setup = ReclaimSetup {
            home,
            storage,
            owner,
            member,
            chain,
            store_root_hash,
            coverage,
        };
        register_device(&setup, "dev-owner", &setup.owner).await;
        setup
    }

    async fn register_device(
        setup: &ReclaimSetup,
        device_id: &str,
        signer: &UserKeypair,
    ) -> StoreDeviceRegistration {
        let registration = StoreDeviceRegistration::signed(
            setup.store_root_hash,
            device_id.to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            signer,
        )
        .unwrap();
        append_and_verify(
            &setup.storage,
            &registration_semantic_prefix(device_id, 1, registration.registration_hash()),
            ".json",
            &registration.to_bytes(),
        )
        .await
        .unwrap();
        registration
    }

    async fn retire_device(
        setup: &ReclaimSetup,
        device_id: &str,
        signer: &UserKeypair,
    ) -> StoreDeviceRegistration {
        let active = StoreDeviceRegistration::signed(
            setup.store_root_hash,
            device_id.to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            signer,
        )
        .unwrap();
        let retired = StoreDeviceRegistration::signed(
            setup.store_root_hash,
            device_id.to_string(),
            2,
            Some(active.registration_hash()),
            StoreDeviceRegistrationState::Retired,
            signer,
        )
        .unwrap();
        append_and_verify(
            &setup.storage,
            &registration_semantic_prefix(device_id, 2, retired.registration_hash()),
            ".json",
            &retired.to_bytes(),
        )
        .await
        .unwrap();
        retired
    }

    async fn publish_ack(
        setup: &ReclaimSetup,
        device_id: &str,
        revision: u64,
        previous: Option<ObjectHash>,
        frontier: BTreeMap<String, CommitPosition>,
        signer: &UserKeypair,
        stamp: &str,
    ) -> StoreAck {
        let ack = StoreAck::signed(
            setup.store_root_hash,
            device_id.to_string(),
            revision,
            previous,
            frontier,
            stamp.to_string(),
            signer,
        )
        .unwrap();
        append_and_verify(
            &setup.storage,
            &ack_semantic_prefix(device_id, revision, ack.ack_hash()),
            ".json",
            &ack.to_bytes(),
        )
        .await
        .unwrap();
        ack
    }

    fn package_count(setup: &ReclaimSetup) -> usize {
        setup
            .home
            .appended_keys()
            .into_iter()
            .filter(|key| key.starts_with("store-v1/packages/dev-owner/1/"))
            .count()
    }

    #[tokio::test]
    async fn every_registered_device_of_a_shared_author_requires_its_own_covering_ack() {
        let setup = setup(false).await;
        register_device(&setup, "dev-owner-sibling", &setup.owner).await;
        publish_ack(
            &setup,
            "dev-owner",
            1,
            None,
            setup.coverage.clone(),
            &setup.owner,
            "2026-01-01T00:00:00Z",
        )
        .await;
        assert!(matches!(
            reclaim_store_packages(&setup.storage, setup.store_root_hash, &setup.chain, super::super::pull::MembershipListingProof::complete_for_test()).await,
            Err(StoreReclaimError::MissingAcknowledgement { device_id, .. })
                if device_id == "dev-owner-sibling"
        ));
        assert_eq!(package_count(&setup), 1);

        publish_ack(
            &setup,
            "dev-owner-sibling",
            1,
            None,
            setup.coverage.clone(),
            &setup.owner,
            "2026-01-01T00:00:01Z",
        )
        .await;
        let result = reclaim_store_packages(
            &setup.storage,
            setup.store_root_hash,
            &setup.chain,
            super::super::pull::MembershipListingProof::complete_for_test(),
        )
        .await
        .unwrap();
        assert_eq!(result.packages_deleted, 1);
        assert_eq!(package_count(&setup), 0);
        assert!(setup
            .home
            .appended_keys()
            .iter()
            .any(|key| key.starts_with("store-v1/commits/dev-owner/1/")));
    }

    #[tokio::test]
    async fn active_member_without_registration_history_refuses_and_removal_drops_the_obligation() {
        let mut setup = setup(true).await;
        publish_ack(
            &setup,
            "dev-owner",
            1,
            None,
            setup.coverage.clone(),
            &setup.owner,
            "2026-01-01T00:00:00Z",
        )
        .await;
        let member_pubkey = hex::encode(setup.member.as_ref().unwrap().public_key());
        assert!(matches!(
            reclaim_store_packages(&setup.storage, setup.store_root_hash, &setup.chain, super::super::pull::MembershipListingProof::complete_for_test()).await,
            Err(StoreReclaimError::MissingRegisteredDevice { member }) if member == member_pubkey
        ));
        assert_eq!(package_count(&setup), 1);

        let removal = setup
            .chain
            .signed_remove_member(
                &setup.owner,
                pubkey_hex(setup.member.as_ref().unwrap()),
                "0000000000003-0000-owner".to_string(),
            )
            .expect("owner removes member");
        setup.chain.add_entry(removal).unwrap();
        assert_eq!(
            reclaim_store_packages(
                &setup.storage,
                setup.store_root_hash,
                &setup.chain,
                super::super::pull::MembershipListingProof::complete_for_test()
            )
            .await
            .unwrap()
            .packages_deleted,
            1
        );
    }

    #[tokio::test]
    async fn incomplete_registration_listing_refuses_before_any_package_delete() {
        let setup = setup(false).await;
        publish_ack(
            &setup,
            "dev-owner",
            1,
            None,
            setup.coverage.clone(),
            &setup.owner,
            "2026-01-01T00:00:00Z",
        )
        .await;
        setup.home.set_listing_coverage(ListingCoverage::BestEffort);
        assert!(matches!(
            reclaim_store_packages(
                &setup.storage,
                setup.store_root_hash,
                &setup.chain,
                super::super::pull::MembershipListingProof::complete_for_test()
            )
            .await,
            Err(StoreReclaimError::IncompleteListing { .. })
        ));
        assert_eq!(package_count(&setup), 1);
    }

    #[tokio::test]
    async fn incomplete_membership_entry_or_head_listing_refuses_before_any_package_delete() {
        for proof in [
            super::super::pull::MembershipListingProof::for_test(
                ListingCoverage::BestEffort,
                ListingCoverage::CompleteAtScan,
            ),
            super::super::pull::MembershipListingProof::for_test(
                ListingCoverage::CompleteAtScan,
                ListingCoverage::BestEffort,
            ),
        ] {
            let setup = setup(false).await;
            let result =
                reclaim_store_packages(&setup.storage, setup.store_root_hash, &setup.chain, proof)
                    .await;
            assert!(matches!(
                result,
                Err(StoreReclaimError::IncompleteListing {
                    listing: "membership"
                })
            ));
            assert_eq!(package_count(&setup), 1);
        }
    }

    #[tokio::test]
    async fn retiring_an_active_members_only_device_refuses_reclamation() {
        let setup = setup(false).await;
        retire_device(&setup, "dev-owner", &setup.owner).await;
        let result = reclaim_store_packages(
            &setup.storage,
            setup.store_root_hash,
            &setup.chain,
            super::super::pull::MembershipListingProof::complete_for_test(),
        )
        .await;
        assert!(matches!(
            result,
            Err(StoreReclaimError::MissingRegisteredDevice { .. })
        ));
        assert_eq!(package_count(&setup), 1);
    }

    #[tokio::test]
    async fn stale_and_hash_mismatched_acknowledgements_refuse_reclamation() {
        for hash_mismatch in [false, true] {
            let setup = setup(false).await;
            let frontier = if hash_mismatch {
                BTreeMap::from([(
                    "dev-owner".to_string(),
                    CommitPosition {
                        seq: 1,
                        commit_hash: ObjectHash::digest(b"wrong commit"),
                    },
                )])
            } else {
                BTreeMap::new()
            };
            publish_ack(
                &setup,
                "dev-owner",
                1,
                None,
                frontier,
                &setup.owner,
                "2026-01-01T00:00:00Z",
            )
            .await;
            assert!(matches!(
                reclaim_store_packages(
                    &setup.storage,
                    setup.store_root_hash,
                    &setup.chain,
                    super::super::pull::MembershipListingProof::complete_for_test()
                )
                .await,
                Err(StoreReclaimError::StaleAcknowledgement { .. })
            ));
            assert_eq!(package_count(&setup), 1);
        }
    }

    #[tokio::test]
    async fn malformed_forked_and_missing_predecessor_ack_chains_refuse() {
        for case in ["malformed", "fork", "missing-predecessor"] {
            let setup = setup(false).await;
            match case {
                "malformed" => {
                    let hash = ObjectHash::digest(b"garbage ack");
                    let copy_id: CopyId = "11".repeat(32).parse().unwrap();
                    setup.home.insert_appended_candidate(
                        &ack_copy_key("dev-owner", 1, hash, copy_id),
                        b"not an ack".to_vec(),
                    );
                }
                "fork" => {
                    publish_ack(
                        &setup,
                        "dev-owner",
                        1,
                        None,
                        setup.coverage.clone(),
                        &setup.owner,
                        "2026-01-01T00:00:00Z",
                    )
                    .await;
                    publish_ack(
                        &setup,
                        "dev-owner",
                        1,
                        None,
                        setup.coverage.clone(),
                        &setup.owner,
                        "2026-01-01T00:00:01Z",
                    )
                    .await;
                }
                "missing-predecessor" => {
                    publish_ack(
                        &setup,
                        "dev-owner",
                        2,
                        Some(ObjectHash::digest(b"missing ack one")),
                        setup.coverage.clone(),
                        &setup.owner,
                        "2026-01-01T00:00:00Z",
                    )
                    .await;
                }
                _ => unreachable!(),
            }
            assert!(reclaim_store_packages(
                &setup.storage,
                setup.store_root_hash,
                &setup.chain,
                super::super::pull::MembershipListingProof::complete_for_test()
            )
            .await
            .is_err());
            assert_eq!(package_count(&setup), 1, "case {case}");
        }
    }

    #[tokio::test]
    async fn conflicting_registration_authors_refuse_reclamation() {
        let setup = setup(false).await;
        let outsider = UserKeypair::generate();
        register_device(&setup, "dev-owner", &outsider).await;
        publish_ack(
            &setup,
            "dev-owner",
            1,
            None,
            setup.coverage.clone(),
            &setup.owner,
            "2026-01-01T00:00:00Z",
        )
        .await;
        assert!(matches!(
            reclaim_store_packages(&setup.storage, setup.store_root_hash, &setup.chain, super::super::pull::MembershipListingProof::complete_for_test()).await,
            Err(StoreReclaimError::Object(StoreObjectError::SemanticFork { slot, .. }))
                if slot == "store-v1/devices/dev-owner/1"
        ));
        assert_eq!(package_count(&setup), 1);
    }

    #[tokio::test]
    async fn partial_physical_copy_delete_reports_no_reclaimed_package() {
        let setup = setup(false).await;
        publish_ack(
            &setup,
            "dev-owner",
            1,
            None,
            setup.coverage.clone(),
            &setup.owner,
            "2026-01-01T00:00:00Z",
        )
        .await;
        let commit = load_commit_slot(&setup.storage, setup.store_root_hash, "dev-owner", 1)
            .await
            .unwrap()
            .unwrap();
        let package = load_package(&setup.storage, &commit.value)
            .await
            .unwrap()
            .unwrap();
        append_and_verify(
            &setup.storage,
            &commit.value.package.object_key,
            ".pkg",
            &package.value,
        )
        .await
        .unwrap();
        assert_eq!(package_count(&setup), 2);
        setup.home.fail_appended_delete_on_call(2);
        assert!(matches!(
            reclaim_store_packages(
                &setup.storage,
                setup.store_root_hash,
                &setup.chain,
                super::super::pull::MembershipListingProof::complete_for_test()
            )
            .await,
            Err(StoreReclaimError::PartialDelete {
                deleted_copies: 1,
                ..
            })
        ));
        assert_eq!(package_count(&setup), 1);
    }
}
