//! Durable append-only Store device registration and retirement.

use crate::database::Database;
use crate::keys::UserKeypair;

use super::membership::{is_exact_self_registration, MembershipChain};
use super::storage::{CoordinationStorage, SyncStorage};
use super::store_commit::{
    commit_semantic_prefix, head_semantic_prefix, registration_semantic_prefix, ObjectHash,
    StoreBatchCommit, StoreCommitOrder, StoreDeviceHead, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreDeviceRegistrationState, StoreSerialHead,
};
use super::store_objects::{append_and_verify, StoreObjectError};

#[derive(Debug, thiserror::Error)]
pub enum StoreRegistrationError {
    #[error("Store device registration database state: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("Store device registration is missing protocol state {key:?}")]
    MissingState { key: &'static str },
    #[error("Store device registration bytes are invalid: {0}")]
    Invalid(String),
    #[error("retired Store device {device_id:?} cannot become active again")]
    RetiredDevice { device_id: String },
    #[error("Store device registration activation: {0}")]
    Outbound(#[from] super::store_outbound::StoreOutboundError),
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn ensure_active_registration(
    db: &Database,
    storage: &dyn SyncStorage,
    signer: &UserKeypair,
) -> Result<(), StoreRegistrationError> {
    ensure_active_registration_with_coordination(
        db,
        storage,
        None,
        signer,
        None,
        "1970-01-01T00:00:00Z",
    )
    .await
}

pub async fn ensure_active_registration_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
    published_at: &str,
) -> Result<(), StoreRegistrationError> {
    drain_registration_outbox(db, storage, coordination, signer, membership, published_at).await?;
    match db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
    {
        Some(registration)
            if registration.state == StoreDeviceRegistrationState::Active
                && registration.published =>
        {
            require_activated_registration(db, &registration).await?;
            return Ok(());
        }
        Some(registration) if registration.state == StoreDeviceRegistrationState::Active => {
            return Err(StoreRegistrationError::Database(format!(
                "Store device registration revision {} remained unpublished after drain",
                registration.revision
            )))
        }
        Some(_) => {
            return Err(StoreRegistrationError::RetiredDevice {
                device_id: protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?,
            })
        }
        None => {}
    }
    let registration = StoreDeviceRegistration::signed(
        required_store_root_hash(db).await?,
        protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?,
        1,
        None,
        StoreDeviceRegistrationState::Active,
        signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    db.stage_store_device_registration(registration)
        .await
        .map_err(database_error)?;
    drain_registration_outbox(db, storage, coordination, signer, membership, published_at)
        .await
        .map(|_| ())
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn retire_registration(
    db: &Database,
    storage: &dyn SyncStorage,
    signer: &UserKeypair,
) -> Result<bool, StoreRegistrationError> {
    retire_registration_with_coordination(db, storage, None, signer, None, "1970-01-01T00:00:00Z")
        .await
}

pub async fn retire_registration_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
    published_at: &str,
) -> Result<bool, StoreRegistrationError> {
    drain_registration_outbox(db, storage, coordination, signer, membership, published_at).await?;
    let Some(latest) = db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
    else {
        return Ok(false);
    };
    if latest.state == StoreDeviceRegistrationState::Retired && latest.published {
        require_activated_registration(db, &latest).await?;
        return Ok(true);
    }
    if latest.state == StoreDeviceRegistrationState::Retired {
        return Err(StoreRegistrationError::Database(format!(
            "Store device retirement revision {} remained unpublished after drain",
            latest.revision
        )));
    }
    let registration = StoreDeviceRegistration::signed(
        required_store_root_hash(db).await?,
        protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?,
        latest.revision + 1,
        Some(latest.registration_hash),
        StoreDeviceRegistrationState::Retired,
        signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    db.stage_store_device_registration(registration)
        .await
        .map_err(database_error)?;
    drain_registration_outbox(db, storage, coordination, signer, membership, published_at).await?;
    Ok(true)
}

pub async fn drain_registration_outbox(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
    published_at: &str,
) -> Result<u64, StoreRegistrationError> {
    let store_root_hash = required_store_root_hash(db).await?;
    let device_id = protocol_value(db, crate::database::LOCAL_DEVICE_ID_STATE_KEY).await?;
    let mut published = 0_u64;
    while let Some(outbound) = db
        .oldest_unpublished_store_device_registration()
        .await
        .map_err(database_error)?
    {
        let registration = StoreDeviceRegistration::parse_at(
            &outbound.registration_bytes,
            store_root_hash,
            &device_id,
            outbound.revision,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if registration.registration_hash() != outbound.registration_hash
            || registration.previous_registration_hash != outbound.previous_registration_hash
            || registration.state != outbound.state
        {
            return Err(StoreRegistrationError::Invalid(
                "durable registration columns differ from its exact signed bytes".to_string(),
            ));
        }
        append_and_verify(
            storage,
            &super::storage::ProtocolObjectContext::store(
                store_root_hash,
                super::storage::ProtocolObjectDomain::StoreDeviceRegistration,
            ),
            &registration_semantic_prefix(
                &device_id,
                outbound.revision,
                outbound.registration_hash,
            ),
            ".json",
            &outbound.registration_bytes,
        )
        .await?;
        if outbound.activation_commit_bytes.is_none() {
            prepare_registration_activation(
                db,
                storage,
                coordination,
                signer,
                membership,
                published_at,
                &registration,
            )
            .await?;
        }
        let prepared = db
            .oldest_unpublished_store_device_registration()
            .await
            .map_err(database_error)?
            .ok_or_else(|| {
                StoreRegistrationError::Database(
                    "prepared Store registration activation disappeared".to_string(),
                )
            })?;
        if prepared.revision != outbound.revision
            || prepared.registration_hash != outbound.registration_hash
        {
            return Err(StoreRegistrationError::Database(
                "prepared Store registration activation changed ownership".to_string(),
            ));
        }
        publish_registration_activation(db, storage, coordination, &registration, &prepared)
            .await?;
        published = published.checked_add(1).ok_or_else(|| {
            StoreRegistrationError::Database("registration publish count exceeded u64".to_string())
        })?;
    }
    Ok(published)
}

#[allow(clippy::too_many_arguments)]
async fn prepare_registration_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
    published_at: &str,
    registration: &StoreDeviceRegistration,
) -> Result<(), StoreRegistrationError> {
    let reference = StoreDeviceRegistrationRef::from_registration(registration);
    match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            let previous = db
                .latest_local_store_position()
                .await
                .map_err(database_error)?;
            let mut dependencies = db.materialized_frontier().await.map_err(database_error)?;
            dependencies.remove(&registration.device_id);
            let author_pubkey = crate::keys::public_key_hex(signer);
            let membership_grant = match membership {
                Some(chain) => match chain.write_grant_coord(&author_pubkey) {
                    Some(grant) => Some(grant),
                    None if chain.contains_member_now(&author_pubkey) => None,
                    None => {
                        return Err(StoreRegistrationError::Invalid(
                            "local identity is not a current Store member".to_string(),
                        ))
                    }
                },
                None => None,
            };
            let requires_self_registration_exception =
                membership_grant.is_none() && membership.is_some();
            let commit = StoreBatchCommit::signed_with_registrations(
                registration.store_root_hash,
                db.new_write_id(),
                registration.device_id.clone(),
                StoreCommitOrder::MergeConcurrent {
                    seq: previous.as_ref().map_or(1, |position| position.seq + 1),
                    previous_commit_hash: previous.map(|position| position.commit_hash),
                    dependencies,
                },
                membership_grant,
                vec![reference],
                signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if requires_self_registration_exception
                && !is_exact_self_registration(&commit, std::slice::from_ref(registration))
            {
                return Err(StoreRegistrationError::Invalid(
                    "Follower registration commit is not an exact control-only self-registration"
                        .to_string(),
                ));
            }
            let head = StoreDeviceHead::signed(
                registration.store_root_hash,
                registration.device_id.clone(),
                Some(commit.position()),
                published_at.to_string(),
                signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            db.stage_merge_store_device_registration_activation(
                registration.revision,
                registration.registration_hash(),
                commit,
                head,
            )
            .await
            .map_err(database_error)
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "Serial registration activation requires coordination".to_string(),
                )
            })?;
            let base =
                super::store_outbound::current_serial_head_position(db, coordination).await?;
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            let commit = StoreBatchCommit::signed_with_registrations(
                registration.store_root_hash,
                db.new_write_id(),
                registration.device_id.clone(),
                StoreCommitOrder::Serial {
                    seq: base.as_ref().map_or(1, |position| position.seq + 1),
                    previous_commit_hash: base.as_ref().map(|position| position.commit_hash),
                },
                None,
                vec![reference],
                signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            authorization
                .authorize_and_apply_with_registrations(&commit, std::slice::from_ref(registration))
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let head = StoreSerialHead::signed(
                registration.store_root_hash,
                Some(commit.position()),
                Some(commit.write_id.clone()),
                signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            db.stage_serial_store_device_registration_activation(
                registration.revision,
                registration.registration_hash(),
                commit,
                head,
            )
            .await
            .map_err(database_error)
        }
    }
}

async fn publish_registration_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    registration: &StoreDeviceRegistration,
    prepared: &crate::database::DurableDeviceRegistration,
) -> Result<(), StoreRegistrationError> {
    let commit_bytes = prepared.activation_commit_bytes.as_ref().ok_or_else(|| {
        StoreRegistrationError::Invalid(
            "Store registration activation has no durable commit bytes".to_string(),
        )
    })?;
    let unverified: StoreBatchCommit = serde_json::from_slice(commit_bytes)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let stream_id = unverified.order.stream_id(&unverified.device_id);
    let commit = StoreBatchCommit::parse_at(
        commit_bytes,
        registration.store_root_hash,
        db.write_policy(),
        stream_id,
        unverified.seq(),
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    if commit != unverified
        || commit.device_registrations.as_slice()
            != [StoreDeviceRegistrationRef::from_registration(registration)]
    {
        return Err(StoreRegistrationError::Invalid(
            "durable Store registration activation differs from its signed bytes".to_string(),
        ));
    }
    let head_bytes = prepared.activation_head_bytes.as_ref().ok_or_else(|| {
        StoreRegistrationError::Invalid(
            "Store registration activation has no durable head bytes".to_string(),
        )
    })?;
    match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            let head = StoreDeviceHead::parse_at(
                head_bytes,
                registration.store_root_hash,
                &registration.device_id,
                commit.seq(),
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if head.position.as_ref() != Some(&commit.position()) {
                return Err(StoreRegistrationError::Invalid(
                    "Store registration head does not activate its commit".to_string(),
                ));
            }
            append_and_verify(
                storage,
                &super::storage::ProtocolObjectContext::store(
                    registration.store_root_hash,
                    super::storage::ProtocolObjectDomain::StoreCommit,
                ),
                &commit_semantic_prefix(
                    &registration.device_id,
                    commit.seq(),
                    commit.commit_hash(),
                ),
                ".json",
                commit_bytes,
            )
            .await?;
            append_and_verify(
                storage,
                &super::storage::ProtocolObjectContext::store(
                    registration.store_root_hash,
                    super::storage::ProtocolObjectDomain::StoreHead,
                ),
                &head_semantic_prefix(&registration.device_id, commit.seq(), head.head_hash()),
                ".json",
                head_bytes,
            )
            .await?;
            db.complete_merge_store_device_registration_activation(
                registration.revision,
                registration.registration_hash(),
                commit,
            )
            .await
            .map_err(database_error)
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                StoreRegistrationError::Invalid(
                    "Serial registration activation requires coordination".to_string(),
                )
            })?;
            let head = StoreSerialHead::parse(head_bytes, registration.store_root_hash)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if head.commit.as_ref() != Some(&commit.position()) {
                return Err(StoreRegistrationError::Invalid(
                    "Serial registration head does not activate its commit".to_string(),
                ));
            }
            let base = commit.previous_commit_hash().map(|commit_hash| {
                super::store_commit::CommitPosition {
                    seq: commit.seq() - 1,
                    commit_hash,
                }
            });
            super::store_outbound::activate_serial_commit_head(
                db,
                storage,
                coordination,
                base,
                &commit,
                &head,
            )
            .await?;
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            db.complete_serial_store_device_registration_activation(
                registration.revision,
                registration.registration_hash(),
                commit,
                authorization,
            )
            .await
            .map_err(database_error)
        }
    }
}

async fn required_store_root_hash(db: &Database) -> Result<ObjectHash, StoreRegistrationError> {
    db.required_store_root_hash_mapped(
        || StoreRegistrationError::MissingState {
            key: crate::database::STORE_ROOT_HASH_STATE_KEY,
        },
        |reason| StoreRegistrationError::Invalid(format!("store protocol root hash: {reason}")),
        database_error,
    )
    .await
}

async fn require_activated_registration(
    db: &Database,
    durable: &crate::database::DurableDeviceRegistration,
) -> Result<(), StoreRegistrationError> {
    let activated = db
        .activated_store_device_registrations()
        .await
        .map_err(database_error)?;
    if activated.iter().any(|registration| {
        registration.revision == durable.revision
            && registration.registration_hash() == durable.registration_hash
            && registration.state == durable.state
    }) {
        Ok(())
    } else {
        Err(StoreRegistrationError::Database(format!(
            "published Store device registration revision {} has no activated Store commit",
            durable.revision
        )))
    }
}

async fn protocol_value(
    db: &Database,
    key: &'static str,
) -> Result<String, StoreRegistrationError> {
    db.get_protocol_state(key)
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::MissingState { key })
}

fn database_error(error: crate::database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::MemberRole;
    use crate::sync::store_commit::{registration_semantic_prefix, StoreDeviceRegistration};
    use crate::sync::store_objects::{
        append_and_verify, list_latest_registration_chains, load_store_protocol_root_at_hash,
        StoreObjectError,
    };
    use crate::sync::test_helpers::{
        bootstrap_chain, open_test_db, pubkey_hex, publish_test_store_protocol_root,
        temp_store_dir, MockSyncStorage,
    };

    async fn initialized(
        source: &str,
    ) -> (
        InMemoryCloudHome,
        CloudSyncStorage,
        Database,
        UserKeypair,
        ObjectHash,
    ) {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "registration-store-test",
            signer.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(source)));
        let db = open_test_db();
        let store_root_hash = publish_test_store_protocol_root(
            &db,
            &storage,
            "registration-store-test",
            "dev-reader",
            &signer,
        )
        .await;
        (home, storage, db, signer, store_root_hash)
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_registration_error_variants() {
        let db = open_test_db();
        let storage = MockSyncStorage::new();
        let signer = UserKeypair::generate();

        assert!(matches!(
            drain_registration_outbox(
                &db,
                &storage,
                None,
                &signer,
                None,
                "2026-01-01T00:00:00Z",
            )
            .await,
            Err(StoreRegistrationError::MissingState { key })
                if key == crate::database::STORE_ROOT_HASH_STATE_KEY
        ));

        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            "not-an-object-hash",
        )
        .await
        .expect("write malformed Store root");
        assert!(matches!(
            drain_registration_outbox(
                &db,
                &storage,
                None,
                &signer,
                None,
                "2026-01-01T00:00:01Z",
            )
            .await,
            Err(StoreRegistrationError::Invalid(reason))
                if reason.contains("store protocol root hash")
        ));
    }

    #[tokio::test]
    async fn active_registration_and_retired_successor_form_one_verified_chain() {
        let (_home, storage, db, signer, store_root_hash) = initialized("active-retired").await;
        ensure_active_registration(&db, &storage, &signer)
            .await
            .unwrap();
        assert!(retire_registration(&db, &storage, &signer).await.unwrap());

        let chains = list_latest_registration_chains(&storage, store_root_hash)
            .await
            .unwrap();
        let latest = &chains.latest_by_device["dev-reader"].value;
        assert_eq!(latest.revision, 2);
        assert_eq!(latest.state, StoreDeviceRegistrationState::Retired);
        assert!(latest.previous_registration_hash.is_some());
    }

    #[tokio::test]
    async fn peer_materializes_registration_only_through_its_activating_store_commit() {
        let (_home, storage, source, signer, store_root_hash) =
            initialized("registration-activation-round-trip").await;
        ensure_active_registration(&source, &storage, &signer)
            .await
            .unwrap();

        let peer = open_test_db();
        let peer_root = publish_test_store_protocol_root(
            &peer,
            &storage,
            "registration-store-test",
            "dev-peer",
            &signer,
        )
        .await;
        assert_eq!(peer_root, store_root_hash);
        assert!(peer
            .activated_store_device_registrations()
            .await
            .unwrap()
            .is_empty());

        let (_temp, store_dir) = temp_store_dir();
        super::super::store_pull::pull_store_commits(
            &peer,
            peer.synced_tables(),
            &storage,
            store_root_hash,
            "dev-peer",
            &store_dir,
            None,
        )
        .await
        .unwrap();

        let activated = peer.activated_store_device_registrations().await.unwrap();
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0].device_id, "dev-reader");
        assert_eq!(activated[0].state, StoreDeviceRegistrationState::Active);
    }

    #[tokio::test]
    async fn merge_follower_activates_and_peer_materializes_exact_self_registration() {
        let (_home, storage, source, owner, store_root_hash) =
            initialized("merge-follower-registration").await;
        let root = load_store_protocol_root_at_hash(&storage, store_root_hash)
            .await
            .expect("load Store protocol root")
            .expect("Store protocol root exists")
            .value;
        let mut membership = bootstrap_chain(root.founder);
        let follower = UserKeypair::generate();
        let grant = membership
            .signed_set_member(
                &owner,
                pubkey_hex(&follower),
                None,
                MemberRole::Follower,
                "2026-07-15T00:00:00Z".to_string(),
            )
            .expect("owner grants Follower membership");
        membership.add_entry(grant).expect("apply Follower grant");

        ensure_active_registration_with_coordination(
            &source,
            &storage,
            None,
            &follower,
            Some(&membership),
            "2026-07-15T00:00:01Z",
        )
        .await
        .expect("Follower activates its own registration");

        let peer = open_test_db();
        let peer_root = publish_test_store_protocol_root(
            &peer,
            &storage,
            "registration-store-test",
            "dev-peer",
            &owner,
        )
        .await;
        assert_eq!(peer_root, store_root_hash);
        let (_temp, store_dir) = temp_store_dir();
        super::super::store_pull::pull_store_commits(
            &peer,
            peer.synced_tables(),
            &storage,
            store_root_hash,
            "dev-peer",
            &store_dir,
            Some(&membership),
        )
        .await
        .expect("pull Follower registration activation");

        let activated = peer.activated_store_device_registrations().await.unwrap();
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0].author_pubkey, pubkey_hex(&follower));
    }

    #[tokio::test]
    async fn failed_active_append_retries_the_owned_exact_bytes() {
        let (home, storage, db, signer, store_root_hash) = initialized("active-retry").await;
        home.fail_append_before_call(1);
        assert!(ensure_active_registration(&db, &storage, &signer)
            .await
            .is_err());
        let pending = db
            .oldest_unpublished_store_device_registration()
            .await
            .unwrap()
            .expect("Active bytes remain owned");
        assert_eq!(pending.revision, 1);
        assert_eq!(pending.state, StoreDeviceRegistrationState::Active);

        ensure_active_registration(&db, &storage, &signer)
            .await
            .unwrap();
        let chains = list_latest_registration_chains(&storage, store_root_hash)
            .await
            .unwrap();
        assert_eq!(
            chains.latest_by_device["dev-reader"].semantic_hash,
            pending.registration_hash,
        );
    }

    #[tokio::test]
    async fn registration_slot_fork_is_rejected() {
        let (_home, storage, _db, signer, store_root_hash) = initialized("registration-fork").await;
        let outsider = UserKeypair::generate();
        for author in [&signer, &outsider] {
            let registration = StoreDeviceRegistration::signed(
                store_root_hash,
                "dev-reader".to_string(),
                1,
                None,
                StoreDeviceRegistrationState::Active,
                author,
            )
            .unwrap();
            append_and_verify(
                &storage,
                &super::super::storage::ProtocolObjectContext::store(
                    store_root_hash,
                    super::super::storage::ProtocolObjectDomain::StoreDeviceRegistration,
                ),
                &registration_semantic_prefix("dev-reader", 1, registration.registration_hash()),
                ".json",
                &registration.to_bytes(),
            )
            .await
            .unwrap();
        }
        assert!(matches!(
            list_latest_registration_chains(&storage, store_root_hash).await,
            Err(StoreObjectError::SemanticFork { slot, .. })
                if slot == "store-v1/devices/dev-reader/1"
        ));
    }

    #[tokio::test]
    async fn registration_chain_missing_predecessor_is_rejected() {
        let (_home, storage, _db, signer, store_root_hash) = initialized("registration-gap").await;
        let registration = StoreDeviceRegistration::signed(
            store_root_hash,
            "dev-reader".to_string(),
            2,
            Some(ObjectHash::digest(b"missing registration")),
            StoreDeviceRegistrationState::Retired,
            &signer,
        )
        .unwrap();
        append_and_verify(
            &storage,
            &super::super::storage::ProtocolObjectContext::store(
                store_root_hash,
                super::super::storage::ProtocolObjectDomain::StoreDeviceRegistration,
            ),
            &registration_semantic_prefix("dev-reader", 2, registration.registration_hash()),
            ".json",
            &registration.to_bytes(),
        )
        .await
        .unwrap();
        assert!(matches!(
            list_latest_registration_chains(&storage, store_root_hash).await,
            Err(StoreObjectError::MissingRegistrationRevision { device_id, revision })
                if device_id == "dev-reader" && revision == 1
        ));
    }
}
