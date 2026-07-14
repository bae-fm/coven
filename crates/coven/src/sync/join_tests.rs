//! "Join then sync" end-to-end: a device that bootstraps from a snapshot and
//! then runs the real [`run_single_sync_cycle`] must not republish — and thereby
//! clobber — the shared snapshot.
//!
//! `multi_device_managed_edit_reaches_restore` (in `snapshot.rs`) covers
//! bootstrap plus a *hand-rolled* pull and snapshot push; it never drives the
//! real cycle, so it can't see that the cycle, run on a just-joined device whose
//! `sync_state` the join path left empty, trips `is_initial_sync` and overwrites
//! the owner's snapshot with its own.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::blob::{CacheFill, Provenance};
use crate::clock::SystemClock;
use crate::config::Config;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::id_provider::SequentialIdProvider;
use crate::join_code::{encode, InviteCode, JoinCodeError};
use crate::keys::{MasterKeyCustody, UserKeypair};
use crate::storage::cloud::CloudHomeJoinInfo;
use crate::sync::cloud_storage::{CloudCipher, PendingRotation};
use crate::sync::hlc::Hlc;
use crate::sync::join::{join_from_invite_code, open_db_and_pull, BootstrapError};
use crate::sync::session::BlobDecl;
use crate::sync::snapshot::{
    bootstrap_from_snapshot, create_snapshot, push_snapshot, SnapshotBlobPreflight,
};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::run_test_cycle;
use crate::sync::test_helpers::*;

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

/// A cancel receiver whose sender is dropped immediately: `borrow()` reads the
/// initial `false` forever, so the join/restore flows run to completion exactly
/// as an uncancelled caller's would.
fn never_cancelled() -> tokio::sync::watch::Receiver<bool> {
    tokio::sync::watch::channel(false).1
}

/// An invite code carrying an attacker-chosen `store_id` is the path-traversal
/// vector: the code is unsigned, and the id becomes a directory the joiner creates
/// and — on a bootstrap failure — recursively deletes. The id is refused the moment
/// the code is decoded, so a crafted code never reaches the directory step: decode
/// fails, nothing is created outside the stores root, and no network or
/// filesystem work runs.
fn invite_code_with_store_id(store_id: &str) -> InviteCode {
    InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: "Evil".to_string(),
        join_info: CloudHomeJoinInfo::S3 {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            // Port 1 / loopback: nothing listens, so a connect fails at once. A
            // normal id reaches the network unwrap and fails here fast, instead of
            // resolving a real AWS endpoint.
            endpoint: Some("http://127.0.0.1:1".to_string()),
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
            key_prefix: None,
        },
        owner_pubkey: hex::encode([0xAB_u8; 32]),
        membership_floor: Vec::new(),
    }
}

/// Mint a pending identity and encode the join-request code that names it —
/// the pair a real join flow carries from `generate_join_request` through to
/// `join_from_invite_code`, which decodes it back to find the same pending
/// identity to promote.
fn seed_join_request() -> String {
    crate::keys::test_keyring::install();
    let keypair = crate::keys::mint_pending_identity().expect("mint pending identity");
    crate::join_code::generate_join_request_for_keypair(&keypair, None)
}

/// Drive the full join path for an invite code string and return its result. Uses
/// an app dir under `tmp` and no-op blob source; a malicious id fails at decode
/// before any cloud home is built, so the cloud details never matter. Mints its
/// own fresh pending identity each call — every join here reaches at most its
/// own attempt, never another's.
async fn join_result_for(
    code_str: &str,
    app_dir: &std::path::Path,
) -> Result<Config, BootstrapError> {
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    join_from_invite_code(
        code_str,
        &seed_join_request(),
        &crate::store_dir::StoreLayout::new(app_dir),
        &test_synced_tables(),
        &test_migrations(),
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
        None,
        None,
        Arc::new(SystemClock),
        ids,
        |_| {},
        &never_cancelled(),
    )
    .await
}

/// Every traversal-shaped `store_id` is refused at the decode boundary: `decode`
/// returns `JoinCodeError::InvalidStoreId`, so a decoded `InviteCode` never
/// carries a traversal id. Driven end to end, the decode error propagates as
/// `BootstrapError::InvalidCode` and the join creates nothing outside the stores root.
///
/// The cases share one mechanism and differ only in the malicious id and the
/// directory it would escape to, so they run as a table:
/// - `../escape`: `app_dir/stores/../escape` resolves to `app_dir/escape`.
/// - an absolute path: `stores`.join("/abs") == "/abs" replaces the base.
/// - `.`: a trailing `.` normalizes away, so `stores/.` lands on the data dir.
#[tokio::test]
async fn join_rejects_traversal_store_id_at_decode() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    // The absolute case escapes to a path *inside* the temp dir, so even a
    // regressed guard never writes to a real shared location.
    let parent_escape = app_dir.join("escape");
    let abs_escape = app_dir.join("abs_escape");
    let abs_id = abs_escape.to_str().expect("utf8 path").to_string();

    let cases: [(&str, Option<&std::path::Path>); 3] = [
        ("../escape", Some(parent_escape.as_path())),
        (&abs_id, Some(abs_escape.as_path())),
        (".", None),
    ];

    for (store_id, escape_target) in cases {
        let encoded = encode(&invite_code_with_store_id(store_id));
        assert!(
            matches!(
                crate::join_code::decode(&encoded),
                Err(JoinCodeError::InvalidStoreId(_))
            ),
            "decode must refuse `{store_id}` with InvalidStoreId",
        );

        let result = join_result_for(&encoded, app_dir).await;
        assert!(
            matches!(result, Err(BootstrapError::InvalidCode(_))),
            "`{store_id}` must fail the join with the propagated decode error, got {result:?}",
        );
        if let Some(target) = escape_target {
            assert!(
                !target.exists(),
                "join must not create an escape directory at {}",
                target.display(),
            );
        }
    }
}

/// A normal `store_id` decodes and the join reaches the cloud step, where it
/// fails on the unreachable endpoint (`BootstrapError::Invite`, from the wrapped-key
/// download) rather than on the id — proving the decoder rejects only unsafe ids
/// and the directory the join would create sits under `stores/`.
///
/// Join reads its pending identity from the keyring before the cloud
/// download, so one must exist for execution to reach the cloud —
/// `join_result_for` mints one keyed to its own join request, fresh, before
/// every attempt.
#[tokio::test]
async fn join_accepts_a_normal_store_id_past_decode() {
    let encoded = encode(&invite_code_with_store_id("abc-123"));
    let decoded = crate::join_code::decode(&encoded).expect("a normal id decodes");
    assert_eq!(decoded.store_id, "abc-123");

    // End to end the join still fails — the S3 endpoint above is bogus — but it
    // fails at the cloud read past the decode boundary, not at the id.
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::Invite(_))),
        "the bogus cloud endpoint must fail the join at the cloud read, got {result:?}",
    );
}

/// A completed store already present locally is the data — re-joining it adds
/// nothing, and the old code would delete its database and blobs during the
/// failure-cleanup once bootstrap failed. The join refuses up front with a typed
/// error naming the store and leaves the existing files untouched. "Completed"
/// is what the saved `config.yaml` marks: the guard dispatches on that marker,
/// so the store carries one here.
#[tokio::test]
async fn join_refuses_when_completed_store_exists_and_leaves_it_untouched() {
    let encoded = encode(&invite_code_with_store_id("abc-123"));

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // A completed store with this id is already present locally, holding a saved
    // config (the completion marker), a database file, and a blob the join must
    // not touch.
    let store_dir = app_dir.join("stores").join("abc-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create existing store dir");
    std::fs::write(store_dir.join("config.yaml"), b"store_id: abc-123\n")
        .expect("seed completion marker");
    let db_path = store_dir.join("store.db");
    let blob_path = store_dir.join("storage").join("cover.blob");
    std::fs::write(&db_path, b"existing-db-bytes").expect("seed existing db");
    std::fs::write(&blob_path, b"existing-blob-bytes").expect("seed existing blob");

    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::StoreExists(ref id)) if id == "abc-123"),
        "join must refuse a store already present locally, got {result:?}",
    );
    assert_eq!(
        std::fs::read(&db_path).expect("existing db still present"),
        b"existing-db-bytes",
        "the existing database must be left untouched",
    );
    assert_eq!(
        std::fs::read(&blob_path).expect("existing blob still present"),
        b"existing-blob-bytes",
        "the existing blob must be left untouched",
    );
}

/// A store directory with no saved config is a torn bootstrap a crash left
/// behind, not a completed store: the join must NOT refuse it with
/// `StoreExists`. It clears the torn residue and proceeds — here reaching the
/// (bogus) cloud endpoint and failing there — and the torn directory is gone,
/// so a real retry could complete. Without the fix the config-less directory
/// would block every retry forever.
#[tokio::test]
async fn join_clears_a_torn_bootstrap_and_proceeds() {
    let encoded = encode(&invite_code_with_store_id("torn-123"));

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // A torn bootstrap: a store directory with a half-written database and a
    // blob, but no `config.yaml` marker — what a crash mid-bootstrap leaves.
    let store_dir = app_dir.join("stores").join("torn-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create torn store dir");
    std::fs::write(store_dir.join("store.db"), b"half-written-db").expect("seed torn db");
    std::fs::write(
        store_dir.join("storage").join("cover.blob"),
        b"partial-blob",
    )
    .expect("seed torn blob");

    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::Invite(_))),
        "a torn bootstrap must not be refused — the join clears it and reaches the cloud read, got {result:?}",
    );
    assert!(
        !store_dir.exists(),
        "the torn directory must be gone at {}, not left to block retries",
        store_dir.display(),
    );
}

/// A join for a store not present locally that fails at the cloud read still
/// removes the directory it created — the failure-cleanup keeps working for the
/// directory this invocation owns.
#[tokio::test]
async fn fresh_join_failure_cleans_up_its_own_directory() {
    let encoded = encode(&invite_code_with_store_id("fresh-123"));

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let store_dir = app_dir.join("stores").join("fresh-123");

    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::Invite(_))),
        "the bogus cloud endpoint must fail the join at the cloud read, got {result:?}",
    );
    assert!(
        !store_dir.exists(),
        "a fresh join that fails must remove the directory it created at {}",
        store_dir.display(),
    );
}

/// `build_cloud_home_for_join` persists the caller's OAuth token to the
/// store-scoped keyring BEFORE `join_store` ever creates the store directory
/// (persist happens in `join_from_invite_code`, directory creation happens
/// later, inside `join_store`). A failure between those two points used to
/// escape the failure-cleanup entirely (a bare `?`), leaving the persisted
/// token behind with no store directory to show for it. Here the persist
/// succeeds (a real Dropbox home is built) and the very next step — loading
/// the device signing keypair, `join_store`'s first line — is made to fail via
/// a one-shot keyring error, landing exactly between persist and
/// `create_dir_all`.
#[cfg(feature = "oauth-providers")]
#[tokio::test]
async fn join_failure_after_oauth_persist_but_before_create_dir_all_cleans_the_keyring() {
    crate::keys::test_keyring::install();
    crate::oauth::install_test_client_creds();

    // Mint the pending identity this join's request names, then prime a
    // one-shot error on its keyring account's next read.
    let pending = crate::keys::mint_pending_identity().expect("mint pending identity");
    let join_request = crate::join_code::generate_join_request_for_keypair(&pending, None);
    let account =
        crate::keys::KeyringSlot::PendingIdentity(crate::keys::public_key_hex(&pending)).account();
    let entry = crate::keys::entry_for(&account).expect("create pending-identity entry");
    let mock: &keyring_core::mock::Cred = entry
        .as_any()
        .downcast_ref()
        .expect("mock keyring credential");
    mock.set_error(keyring_core::Error::Invalid(
        "induced".to_string(),
        "keypair load fails right after OAuth persist, before create_dir_all".to_string(),
    ));

    let store_id = "oauth-persist-then-keypair-load-fails";
    let encoded = encode(&InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: "Evil".to_string(),
        join_info: CloudHomeJoinInfo::Dropbox {
            folder_path: "/Apps/coven/oauth-cleanup-test".to_string(),
        },
        owner_pubkey: hex::encode([0xAB_u8; 32]),
        membership_floor: Vec::new(),
    });
    let oauth_tokens = crate::oauth::OAuthTokens {
        access_token: "access-token".to_string(),
        refresh_token: Some("refresh-token".to_string()),
        expires_at: None,
    };

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    let result = join_from_invite_code(
        &encoded,
        &join_request,
        &crate::store_dir::StoreLayout::new(app_dir),
        &test_synced_tables(),
        &test_migrations(),
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
        Some(oauth_tokens),
        None,
        Arc::new(SystemClock),
        ids,
        |_| {},
        &never_cancelled(),
    )
    .await;

    assert!(
        matches!(result, Err(BootstrapError::Key(_))),
        "the induced keyring error must surface as a Key error, got {result:?}",
    );

    let store_dir = app_dir.join("stores").join(store_id);
    assert!(
        !store_dir.exists(),
        "no store directory should exist — the failure predates `create_dir_all`",
    );
    assert!(
        crate::keys::StoreKeys::new(store_id.to_string())
            .get_cloud_home_credentials()
            .expect("read keyring")
            .is_none(),
        "the OAuth token persisted before the keypair-load failure must be rolled back",
    );
}

/// `join_store` is the lower-level, reusable function `join_from_invite_code`
/// delegates to after its own marker-check — any other direct caller reaches
/// `join_store` without that check. Its failure-cleanup removes the store
/// directory unconditionally, so `join_store` must refuse a completed store
/// itself, independent of any check a wrapper does or doesn't do around it.
/// Nothing here ever reaches the pending-identity read — the completion marker
/// refuses first — so no join request needs to be seeded.
#[tokio::test]
async fn join_store_refuses_when_completed_store_exists_and_leaves_it_untouched() {
    let code = invite_code_with_store_id("abc-123");

    crate::keys::test_keyring::install();

    let tmp = tempfile::tempdir().expect("temp dir");
    let data_dir = tmp.path();

    // A completed store with this id is already present locally, holding a saved
    // config (the completion marker), a database file, and a blob join_store
    // must not touch.
    let store_dir = data_dir.join("stores").join("abc-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create existing store dir");
    std::fs::write(store_dir.join("config.yaml"), b"store_id: abc-123\n")
        .expect("seed completion marker");
    let db_path = store_dir.join("store.db");
    let blob_path = store_dir.join("storage").join("cover.blob");
    std::fs::write(&db_path, b"existing-db-bytes").expect("seed existing db");
    std::fs::write(&blob_path, b"existing-blob-bytes").expect("seed existing blob");

    // The S3 client builds without touching the network; its endpoint is never
    // reached when the exists-check refuses the join first.
    let cloud_home: Box<dyn crate::storage::cloud::CloudHome> = Box::new(
        crate::storage::cloud::s3::S3CloudHome::new(
            "b".to_string(),
            "us-east-1".to_string(),
            Some("http://127.0.0.1:1".to_string()),
            "ak".to_string(),
            "sk".to_string(),
            None,
        )
        .await
        .expect("construct S3 cloud home"),
    );
    let ids = crate::id_provider::SequentialIdProvider::new("dev");
    let layout = crate::store_dir::StoreLayout::new(data_dir);
    let custody =
        crate::custody::KeyCustody::Keyring.resolve("abc-123", &layout.store_dir("abc-123"));
    let identity_custody = crate::identity_custody::IdentityCustody::Keyring
        .resolve("abc-123", &layout.store_dir("abc-123"));

    let result = crate::sync::join::join_store(
        &layout,
        code,
        "unused-no-pending-identity-is-ever-read",
        &test_synced_tables(),
        &test_migrations(),
        custody,
        identity_custody,
        cloud_home,
        &ids,
        &SystemClock,
        |_| {},
        &never_cancelled(),
    )
    .await;

    assert!(
        matches!(result, Err(BootstrapError::StoreExists(ref id)) if id == "abc-123"),
        "join_store must refuse a store already present locally, got {result:?}",
    );
    assert_eq!(
        std::fs::read(&db_path).expect("existing db still present"),
        b"existing-db-bytes",
        "the existing database must be left untouched",
    );
    assert_eq!(
        std::fs::read(&blob_path).expect("existing blob still present"),
        b"existing-blob-bytes",
        "the existing blob must be left untouched",
    );
}

/// A recording [`MasterKeyCustody`] for tests that must observe what join
/// hands custody, distinct from what actually lands in the OS keyring.
#[derive(Default)]
struct RecordingCustody {
    persisted: std::sync::Mutex<Option<crate::encryption::MasterKeyring>>,
    forgotten: std::sync::atomic::AtomicBool,
}

impl crate::keys::MasterKeyCustody for RecordingCustody {
    fn unlock(&self) -> Result<Option<crate::encryption::MasterKeyring>, crate::keys::KeyError> {
        Ok(self.persisted.lock().unwrap().clone())
    }

    fn persist(
        &self,
        keyring: &crate::encryption::MasterKeyring,
    ) -> Result<(), crate::keys::KeyError> {
        *self.persisted.lock().unwrap() = Some(keyring.clone());
        Ok(())
    }

    fn forget(&self) -> Result<(), crate::keys::KeyError> {
        self.forgotten
            .store(true, std::sync::atomic::Ordering::SeqCst);
        *self.persisted.lock().unwrap() = None;
        Ok(())
    }
}

/// Join's shared bootstrap (`bootstrap_and_save_store`'s step 7) persists the
/// invite's unwrapped keyring through whatever custody the caller supplies —
/// never the OS keyring directly. Drives `bootstrap_and_save_store` itself —
/// the shared unit both `join_store` and `restore_from_cloud` funnel through —
/// with `BootstrapContext::Join`, so this pins the join-specific wiring without
/// the invite/sealed-box dance `join_store` layers on top.
#[tokio::test]
async fn join_bootstrap_persists_the_unwrapped_keyring_through_custody_never_the_os_keyring() {
    crate::keys::test_keyring::install();

    let store_id = "join-custody-recording-test";
    let cloud = crate::InMemoryCloudHome::new();
    let owner_keypair = UserKeypair::generate();
    let master_key = crate::encryption::MasterKeyring::generate();
    let cipher = CloudCipher::Encrypted(master_key.clone().into());
    let blob_paths =
        crate::sync::cloud_storage::BlobPathScheme::for_storage(crate::config::HomeStorage::Opaque);
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        Arc::new(cloud.clone()) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        owner_keypair.clone(),
    );

    // `BootstrapContext::Join` pins the owner before the pull, so
    // `bootstrap_from_snapshot`'s authorization check needs a real,
    // owner-anchored chain — a founder entry, published and headed.
    let owner_pk = crate::sync::test_helpers::pubkey_hex(&owner_keypair);
    let mut chain = crate::sync::membership::MembershipChain::new();
    let founder = crate::sync::test_helpers::founder_entry(&owner_keypair, "0000000001000-0000-A");
    chain
        .add_entry_at(
            crate::sync::membership::MembershipCoord {
                author_pubkey: owner_pk.clone(),
                seq: 1,
            },
            founder.clone(),
        )
        .expect("valid founder entry");
    crate::sync::storage::SyncStorage::put_membership_entry(
        &owner_storage,
        &owner_pk,
        1,
        serde_json::to_vec(&founder).expect("serialize founder entry"),
    )
    .await
    .expect("upload founder entry");
    crate::sync::membership_ops::publish_membership_head(&owner_storage, &chain, &owner_keypair)
        .await
        .expect("publish founder membership head");

    let tables = test_synced_tables();
    let db = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("create owner snapshot");
    push_snapshot(
        &owner_storage,
        store_id,
        snapshot,
        "owner-device",
        HashMap::new(),
        0,
        db.schema_version(),
        &owner_keypair,
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db,
            blobs: &[],
        },
    )
    .await
    .expect("push owner snapshot");

    let joiner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        Arc::new(cloud) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        UserKeypair::generate(),
    );
    let (_tmp_b, store_dir) = temp_store_dir();
    let store_keys = crate::keys::StoreKeys::new(store_id.to_string());
    let custody = Arc::new(RecordingCustody::default());
    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(store_id, &store_dir);
    let joiner_identity = UserKeypair::generate();
    let join_info = CloudHomeJoinInfo::CloudKit;

    crate::sync::join::bootstrap_and_save_store(
        &joiner_storage,
        &cipher,
        Some(&master_key),
        &store_dir,
        store_id,
        "device-b",
        crate::sync::join::BootstrapContext::Join {
            owner_pubkey: &owner_pk,
            keypair: &joiner_identity,
        },
        &chain.author_heads(),
        &tables,
        &test_migrations(),
        &join_info,
        "Joined Store",
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &SystemClock,
        &|_status: &str| {},
        &never_cancelled(),
    )
    .await
    .expect("join bootstrap persists the unwrapped keyring");

    assert_eq!(
        custody.unlock().unwrap().map(|k| k.fingerprint()),
        Some(master_key.fingerprint()),
        "the unwrapped keyring reached custody's persist",
    );
    assert_eq!(
        store_keys
            .get_encryption_key()
            .expect("read the OS keyring"),
        None,
        "the OS keyring is never touched when the host selects a different custody",
    );
}

/// A join that fails after custody already holds the unwrapped keyring rolls
/// it back — `cleanup_after_bootstrap_failure` calls `forget` on whatever
/// custody the host selected, not just the OS keyring's delete.
#[tokio::test]
async fn join_failure_calls_forget_on_the_selected_custody() {
    crate::keys::test_keyring::install();

    let encoded = encode(&invite_code_with_store_id("join-custody-forget-test"));
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    let custody = Arc::new(RecordingCustody::default());

    let result = join_from_invite_code(
        &encoded,
        &seed_join_request(),
        &crate::store_dir::StoreLayout::new(app_dir),
        &test_synced_tables(),
        &test_migrations(),
        crate::custody::KeyCustody::Custom(custody.clone()),
        crate::identity_custody::IdentityCustody::Keyring,
        None,
        None,
        Arc::new(SystemClock),
        ids,
        |_| {},
        &never_cancelled(),
    )
    .await;

    assert!(
        matches!(result, Err(BootstrapError::Invite(_))),
        "the bogus cloud endpoint must fail the join at the cloud read, got {result:?}",
    );
    assert!(
        custody.forgotten.load(std::sync::atomic::Ordering::SeqCst),
        "a failed join must call forget on the host-selected custody",
    );
}

#[tokio::test]
async fn joined_device_first_cycle_does_not_clobber_the_shared_snapshot() {
    let enc = CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32]));
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

    // Owner A leaves the cloud in the shape "empty store, then import" produces:
    // an empty snapshot at seq 0 (the initial-sync snapshot of the empty store),
    // and the imported row as changeset A/1. `should_create_snapshot` does not
    // refresh the snapshot for a single sub-threshold changeset, so the catalog
    // lives in the changeset — not the snapshot.
    let db_a = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let empty_snap = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner empty snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        empty_snap,
        "A",
        HashMap::new(),
        0,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("push empty snapshot");

    let cs1 = capture_bytes(
        &db_a,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Album Title', NULL, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &cs1, SCHEMA_VERSION);

    // The shared snapshot pointer before B joins. B must not republish (which
    // would flip the pointer to a new generation).
    let snapshot_before = storage
        .get_snapshot_pointer()
        .await
        .expect("snapshot pointer present");

    // Device B joins through the real path: bootstrap from the snapshot, then
    // pull the changesets published after it.
    let (_tmp_b, lib_b) = temp_store_dir();
    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &[],
        &storage,
        boot,
        &lib_b,
        &never_cancelled(),
    )
    .await
    .expect("B open_db_and_pull");

    let (db_b, _stamper) = Database::open(
        &lib_b.db_path(),
        tables.clone(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        "B".to_string(),
        &test_migrations(),
    )
    .expect("open B db");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Album Title",
        "B must receive the imported row through bootstrap + pull",
    );

    // B's first real sync cycle, with no local changes of its own.
    let enc_lock = RwLock::new(enc.clone());
    let keypair = UserKeypair::generate();
    let b_hlc = Hlc::new("B".to_string());
    run_test_cycle(
        &storage,
        "test-lib",
        "B",
        &b_hlc,
        &SystemClock,
        &db_b,
        &enc_lock,
        &PendingRotation::none(),
        &keypair,
        None,
        &lib_b,
        None,
        None,
    )
    .await
    .expect("B sync cycle");

    // A just-joined device with no local changes must leave the shared snapshot
    // untouched: the pointer still names the owner's generation, not a republished
    // one of B's own.
    let snapshot_after = storage
        .get_snapshot_pointer()
        .await
        .expect("snapshot pointer present");
    assert_eq!(
        snapshot_after, snapshot_before,
        "a just-joined device's first cycle must not republish/clobber the shared snapshot",
    );
}

#[tokio::test]
async fn bootstrap_installs_snapshot_cursors_before_ordinary_pull() {
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();
    let db_a = open_test_db();

    capture_bytes(
        &db_a,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'Snapshot', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    let snapshot_dir = tempfile::tempdir().expect("snapshot temp dir");
    let snapshot_path = snapshot_dir.path().to_path_buf();
    let snapshot_tables = tables.clone();
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snapshot_path, &snapshot_tables)
                .map_err(|error| DbError(error.to_string()))
        })
        .await
        .expect("create snapshot at A/1");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "A",
        HashMap::from([("A".to_string(), 1)]),
        1,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("publish snapshot at A/1");

    let second = capture_bytes(
        &db_a,
        &["UPDATE notes SET title = 'After snapshot', \
             _updated_at = '0000000002000-0000-A' WHERE id = 'n1'"],
    )
    .await;
    storage.store_changeset("A", 2, &second, SCHEMA_VERSION);

    let (_tmp_b, lib_b) = temp_store_dir();
    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("bootstrap B from A/1 snapshot");
    let applied = open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &[],
        &storage,
        boot,
        &lib_b,
        &never_cancelled(),
    )
    .await
    .expect("install snapshot cursor and pull A/2");
    assert_eq!(applied, 1);

    let (db_b, _stamper) = Database::open(
        &lib_b.db_path(),
        tables,
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        "B".to_string(),
        &test_migrations(),
    )
    .expect("open bootstrapped B");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "After snapshot",
    );
    let durable_cursor: i64 = db_b
        .call(|conn| {
            conn.query_row(
                "SELECT last_seq FROM sync_cursors WHERE device_id = 'A'",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
        })
        .await
        .expect("read durable A cursor");
    assert_eq!(durable_cursor, 2);
}

/// The per-reader membership-head watermark is the only defense against a
/// storage provider replaying an older, otherwise validly signed membership
/// state — but a high-water mark only defends state a reader has already seen.
/// A fresh joiner has no watermark yet, so without seeding it from the invite's
/// floor, a provider serving the pre-removal head (and its still-present
/// entries — internally consistent, correctly signed) would be accepted, and
/// the removed member would read as current.
#[tokio::test]
async fn a_fresh_joiner_refuses_a_rolled_back_membership_head() {
    use crate::sync::membership::{
        entry_hash, AuthorHead, MemberRole, MembershipAction, MembershipChain, MembershipCoord,
    };
    use crate::sync::test_helpers::{
        append_membership_entry, founder_entry, make_linked_entry, pubkey_hex,
        publish_membership_chain_head,
    };

    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let invitee = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);

    // Build the chain: found, add a member, then remove them — head at seq 3.
    let mut chain = MembershipChain::new();
    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "0000000002000-0000-owner",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member.clone()).await;
    let remove_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Remove,
        &member,
        MemberRole::Member,
        "0000000003000-0000-owner",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // Mint an invite AFTER the removal: its floor reflects the post-removal,
    // post-invite chain state (the invitee's own Add lands at seq 4).
    let hlc = crate::sync::hlc::Hlc::new("owner".to_string());
    let encryption = EncryptionService::from_key([7u8; 32]);
    let invite = crate::sync::membership_ops::invite_member(
        &storage,
        &storage,
        &owner,
        &hlc,
        &pubkey_hex(&invitee),
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Lib",
    )
    .await
    .expect("mint invite");
    assert_eq!(
        invite.membership_floor,
        vec![MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 4,
        }],
        "the invite's floor must reflect the post-removal, post-invite chain state",
    );

    // The owner publishes a snapshot so bootstrap has something to adopt.
    let db_owner = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "owner-device",
        HashMap::new(),
        0,
        db_owner.schema_version(),
        &owner,
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_owner,
            blobs: &[],
        },
    )
    .await
    .expect("push owner snapshot");

    // The provider now serves the PRE-removal head — seq 2, before the member
    // was removed. Entries 1-2 really exist and the head is correctly signed;
    // this is a rollback of the true chain's current state, not a forgery.
    let stale_head = AuthorHead::signed(2, entry_hash(&add_member), &owner);
    storage
        .put_membership_head(&owner_pk, serde_json::to_vec(&stale_head).unwrap())
        .await
        .expect("provider serves the rolled-back head");

    let (_tmp_b, lib_b) = temp_store_dir();
    let boot = bootstrap_from_snapshot(&storage, "test-lib", Some(&owner_pk), 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    let result = open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        Some(&owner_pk),
        &invite.membership_floor,
        &storage,
        boot,
        &lib_b,
        &never_cancelled(),
    )
    .await;

    assert!(
        matches!(result, Err(BootstrapError::Pull(_))),
        "a fresh joiner seeded from the invite's floor must refuse a membership \
         head that regresses below it, got {result:?}",
    );
}

/// The seeded floor is exactly that — a floor, not a ceiling. A joiner seeded
/// at the invite's mint-time seq must still accept a LATER head: the ordinary
/// case where the chain kept moving between mint and this device's first sync.
#[tokio::test]
async fn a_fresh_joiner_accepts_a_membership_head_later_than_its_seeded_floor() {
    use crate::sync::membership::{MemberRole, MembershipAction, MembershipChain};
    use crate::sync::test_helpers::{
        append_membership_entry, founder_entry, make_linked_entry, pubkey_hex,
        publish_membership_chain_head,
    };

    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();
    let owner = UserKeypair::generate();
    let invitee = UserKeypair::generate();
    let second_member = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);

    let mut chain = MembershipChain::new();
    let founder = founder_entry(&owner, "0000000001000-0000-owner");
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // Mint the invite right after founding: floor lands at seq 2 (the
    // invitee's own Add).
    let hlc = crate::sync::hlc::Hlc::new("owner".to_string());
    let encryption = EncryptionService::from_key([7u8; 32]);
    let invite = crate::sync::membership_ops::invite_member(
        &storage,
        &storage,
        &owner,
        &hlc,
        &pubkey_hex(&invitee),
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Lib",
    )
    .await
    .expect("mint invite");
    assert_eq!(
        invite.membership_floor[0].seq, 2,
        "floor lands at the invitee's Add"
    );

    // `invite_member` appended the invitee's Add straight to storage, so the
    // `chain` built above (founder only) is stale; re-derive it from storage
    // before appending further, so the next entry's `prev_hash` links to the
    // invitee's Add rather than skipping over it.
    let visible = storage.list_membership_entries().await.unwrap();
    let mut chain = crate::sync::membership_ops::download_chain(&storage, &visible)
        .await
        .expect("chain including the invite's own Add");

    // Between mint and this device's first sync, the chain moved forward
    // further: the owner adds a second member, advancing the real head to
    // seq 3.
    let add_second_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &second_member,
        MemberRole::Member,
        "0000000004000-0000-owner",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 3, add_second_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    let db_owner = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "owner-device",
        HashMap::new(),
        0,
        db_owner.schema_version(),
        &owner,
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_owner,
            blobs: &[],
        },
    )
    .await
    .expect("push owner snapshot");

    let (_tmp_b, lib_b) = temp_store_dir();
    let boot = bootstrap_from_snapshot(&storage, "test-lib", Some(&owner_pk), 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    let result = open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        Some(&owner_pk),
        &invite.membership_floor,
        &storage,
        boot,
        &lib_b,
        &never_cancelled(),
    )
    .await;

    assert!(
        result.is_ok(),
        "a head later than the seeded floor must be accepted normally, got {result:?}",
    );
}

/// A device that bootstraps from a snapshot must receive not just the catalog
/// rows but the blob *files* those rows reference. The snapshot is a whole-DB
/// image carrying a `note_photos` row, but no per-row blob file; the pull that
/// follows starts past the snapshot's cursors, so the INSERT changeset that first
/// carried the photo (seq <= cursor) is never re-walked and the per-changeset
/// blob download never fires for it. Without the bootstrap backfill, the
/// bootstrapped device has the photo row but no blob file in its cache — a synced
/// album renders a placeholder cover. Asserts the file lands.
#[tokio::test]
async fn bootstrap_backfills_blob_files_for_snapshot_rows() {
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));

    // Owner A: a shared note with a cover photo, both captured into the snapshot.
    let db_a = open_test_db();
    exec(
        &db_a,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo1', 'n1', 'cover', 11, '{}', '0000000001000-0000-A', '2026-01-01')",
            crate::blob::content_hash(b"cover-bytes"),
        ),
    )
    .await;
    // A recorded who uploaded the cover when it imported the album. The snapshot
    // carries this uploader index forward (it is member-global, not per-device),
    // so B resolves the blob's prefix from it — there is no listing scan.
    let location = test_blob_location(&storage.own_uploader().expect("mock uploader"), 1000);
    record_test_blob_location(&db_a, "photos", "photo1", &location).await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "A",
        HashMap::new(),
        1,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("push snapshot");

    // The cover blob exists in the cloud (uploaded when A first imported the
    // album), keyed `photos/photo1` master-scoped as the declaration maps a cover
    // row.
    storage
        .put_blob(
            "photos",
            &location,
            "photo1",
            crate::blob::BlobScope::Master,
            None,
            b"cover-bytes".to_vec(),
        )
        .await
        .expect("seed cover blob");

    // Device B bootstraps from the snapshot, then runs the real bootstrap path,
    // which derives `note_photos`'s blob from the declaration. A `CacheEager` blob
    // lands in B's evictable cache folder (`storage/cache/<id>`) on reconciliation,
    // which coven builds from the validated id.
    let (_tmp_b, lib_b) = temp_store_dir();
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &[],
        &storage,
        boot,
        &lib_b,
        &never_cancelled(),
    )
    .await
    .expect("B open_db_and_pull");

    assert!(
        expected_blob.exists(),
        "the cover blob file must be backfilled to {} after bootstrap",
        expected_blob.display(),
    );
    assert_eq!(
        std::fs::read(&expected_blob).expect("read backfilled blob"),
        b"cover-bytes",
        "the backfilled file must hold the blob's plaintext bytes",
    );
}

/// A blob whose download fails at bootstrap (its object isn't in the cloud yet)
/// refuses the bootstrap before the store is saved.
#[tokio::test]
async fn snapshot_blob_backfill_failure_aborts_bootstrap_pull() {
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));

    // Owner A: a shared note with a cover photo, both captured into the snapshot.
    let db_a = open_test_db();
    exec(
        &db_a,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo1', 'n1', 'cover', 11, '{}', '0000000001000-0000-A', '2026-01-01')",
            crate::blob::content_hash(b"cover-bytes"),
        ),
    )
    .await;
    // A recorded who uploaded the cover when it imported the album. The snapshot
    // carries this uploader index forward (it is member-global, not per-device),
    // so B resolves the blob's prefix from it — there is no listing scan.
    let location = test_blob_location(&storage.own_uploader().expect("mock uploader"), 1000);
    record_test_blob_location(&db_a, "photos", "photo1", &location).await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "A",
        HashMap::new(),
        1,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("push snapshot");

    // Unlike the happy-path test above, the cover blob is NOT in the cloud yet at
    // bootstrap time (e.g. A's upload of it hadn't landed). So the bootstrap's
    // download attempt fails.
    let (_tmp_b, lib_b) = temp_store_dir();
    // A reconciled `CacheEager` blob lands in B's evictable cache folder.
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &[],
        &storage,
        boot,
        &lib_b,
        &never_cancelled(),
    )
    .await
    .expect_err("B open_db_and_pull must fail when snapshot eager blob is missing");

    assert!(
        !expected_blob.exists(),
        "the missing cover blob must not be materialized at {}",
        expected_blob.display(),
    );
}

/// The per-blob cancel check inside snapshot blob reconciliation is load-bearing:
/// with an eager blob to download and the cancel signal already set, the check
/// fires *before* the download, so `open_db_and_pull` returns `Cancelled` — not
/// the missing-blob `Database` error the identical setup produces uncancelled
/// (`snapshot_blob_backfill_failure_aborts_bootstrap_pull`). The blob is never
/// materialized.
#[tokio::test]
async fn open_db_and_pull_cancel_stops_before_downloading_snapshot_blob() {
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));

    // Owner A: a shared note with a cover photo, both captured into the snapshot.
    // The cover blob is deliberately never uploaded, so an uncancelled bootstrap
    // would fail trying to download it — making the `Cancelled` outcome below
    // attributable to the cancel check, not to the blob happening to be present.
    let db_a = open_test_db();
    exec(
        &db_a,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo1', 'n1', 'cover', 11, '{}', '0000000001000-0000-A', '2026-01-01')",
            crate::blob::content_hash(b"cover-bytes"),
        ),
    )
    .await;
    // A recorded who uploaded the cover when it imported the album. The snapshot
    // carries this uploader index forward (it is member-global, not per-device),
    // so B resolves the blob's prefix from it — there is no listing scan.
    let location = test_blob_location(&storage.own_uploader().expect("mock uploader"), 1000);
    record_test_blob_location(&db_a, "photos", "photo1", &location).await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "A",
        HashMap::new(),
        1,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("push snapshot");

    let (_tmp_b, lib_b) = temp_store_dir();
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let (tx, cancel) = tokio::sync::watch::channel(false);
    tx.send(true)
        .expect("prime cancel before the reconcile loop");

    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    let result = open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &[],
        &storage,
        boot,
        &lib_b,
        &cancel,
    )
    .await;

    assert!(
        matches!(result, Err(BootstrapError::Cancelled)),
        "the per-blob cancel check must return Cancelled before the download, got {result:?}",
    );
    assert!(
        !expected_blob.exists(),
        "a cancelled reconcile must not materialize the blob at {}",
        expected_blob.display(),
    );
}

/// Everything `bootstrap_and_save_store` needs to bootstrap a joiner: an owner
/// has published a founder chain and a snapshot into one in-memory cloud, and a
/// joiner storage is built over that same cloud. Each reader-publish test supplies
/// only the custody whose success or failure it exercises.
struct JoinerFixture {
    joiner_storage: crate::sync::cloud_storage::CloudSyncStorage,
    cipher: CloudCipher,
    master_key: crate::encryption::MasterKeyring,
    owner_pk: String,
    author_heads: Vec<crate::sync::membership::MembershipCoord>,
    store_keys: crate::keys::StoreKeys,
    join_info: CloudHomeJoinInfo,
    tables: Vec<crate::sync::session::SyncedTable>,
    _tmp: tempfile::TempDir,
    store_dir: crate::store_dir::StoreDir,
    store_id: String,
}

async fn joiner_fixture() -> JoinerFixture {
    crate::keys::test_keyring::install();

    // Unique per call: the store-scoped keyring identity slot is process-global
    // in tests, and two fixtures sharing one id would collide on the identity
    // the bootstrap establishes (import refuses a different keypair for the
    // same store).
    static FIXTURE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let store_id = format!(
        "join-fixture-store-{}",
        FIXTURE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let cloud = crate::InMemoryCloudHome::new();
    let owner_keypair = UserKeypair::generate();
    let master_key = crate::encryption::MasterKeyring::generate();
    let cipher = CloudCipher::Encrypted(master_key.clone().into());
    let blob_paths =
        crate::sync::cloud_storage::BlobPathScheme::for_storage(crate::config::HomeStorage::Opaque);
    let owner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        Arc::new(cloud.clone()) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.clone(),
        owner_keypair.clone(),
    );

    // `BootstrapContext::Join` pins the owner before the pull, so bootstrap's
    // authorization check needs a real, owner-anchored chain.
    let owner_pk = crate::sync::test_helpers::pubkey_hex(&owner_keypair);
    let mut chain = crate::sync::membership::MembershipChain::new();
    let founder = crate::sync::test_helpers::founder_entry(&owner_keypair, "0000000001000-0000-A");
    chain
        .add_entry_at(
            crate::sync::membership::MembershipCoord {
                author_pubkey: owner_pk.clone(),
                seq: 1,
            },
            founder.clone(),
        )
        .expect("valid founder entry");
    crate::sync::storage::SyncStorage::put_membership_entry(
        &owner_storage,
        &owner_pk,
        1,
        serde_json::to_vec(&founder).expect("serialize founder entry"),
    )
    .await
    .expect("upload founder entry");
    crate::sync::membership_ops::publish_membership_head(&owner_storage, &chain, &owner_keypair)
        .await
        .expect("publish founder membership head");

    let tables = test_synced_tables();
    let db = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("create owner snapshot");
    push_snapshot(
        &owner_storage,
        &store_id,
        snapshot,
        "owner-device",
        HashMap::new(),
        0,
        db.schema_version(),
        &owner_keypair,
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db,
            blobs: &[],
        },
    )
    .await
    .expect("push owner snapshot");

    let joiner_storage = crate::sync::cloud_storage::CloudSyncStorage::new(
        Arc::new(cloud) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.clone(),
        UserKeypair::generate(),
    );
    let (_tmp, store_dir) = temp_store_dir();
    let store_keys = crate::keys::StoreKeys::new(store_id.clone());

    JoinerFixture {
        joiner_storage,
        cipher,
        master_key,
        owner_pk,
        author_heads: chain.author_heads(),
        store_keys,
        join_info: CloudHomeJoinInfo::CloudKit,
        tables,
        _tmp,
        store_dir,
        store_id,
    }
}

/// A successful bootstrap publishes the joiner's own head (at seq 0 — it has
/// authored nothing) and pull-ack BEFORE the store commits, so a peer's changeset
/// reclamation running during the join sees this reader and pins every floor at
/// what it still needs. Without this a joiner is invisible to reclamation for the
/// whole join window and any peer that reclaims can strand it.
#[tokio::test]
async fn bootstrap_publishes_the_joiners_head_and_ack() {
    let f = joiner_fixture().await;
    let custody = Arc::new(RecordingCustody::default());
    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(&f.store_id, &f.store_dir);
    let joiner_identity = UserKeypair::generate();

    crate::sync::join::bootstrap_and_save_store(
        &f.joiner_storage,
        &f.cipher,
        Some(&f.master_key),
        &f.store_dir,
        &f.store_id,
        "device-b",
        crate::sync::join::BootstrapContext::Join {
            owner_pubkey: &f.owner_pk,
            keypair: &joiner_identity,
        },
        &f.author_heads,
        &f.tables,
        &test_migrations(),
        &f.join_info,
        "Joined Store",
        &f.store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &SystemClock,
        &|_status: &str| {},
        &never_cancelled(),
    )
    .await
    .expect("bootstrap succeeds");

    let listing = crate::sync::storage::SyncStorage::list_heads(&f.joiner_storage)
        .await
        .expect("list heads");
    let head = listing
        .heads
        .iter()
        .find(|h| h.device_id == "device-b")
        .expect("the joiner published its own head");
    assert_eq!(
        head.seq, 0,
        "a joiner has authored nothing, so its head is 0"
    );

    let ack_bytes = crate::sync::storage::SyncStorage::get_ack(&f.joiner_storage, "device-b")
        .await
        .expect("the joiner published its ack");
    let ack: crate::sync::signed_control::AckJson =
        serde_json::from_slice(&ack_bytes).expect("ack parses");
    assert!(
        ack.verify("device-b"),
        "the published ack is signed for the joiner's slot",
    );
}

/// A join's saved config — the completion marker — implies a resolvable signing
/// identity: the bootstrap establishes the pending identity in the store's own
/// custody BEFORE the config save. A crash after the save can no longer leave a
/// completed store whose identity is stuck in the consumable pending slot, which
/// the marker guard refuses to clear (config present) — a store nothing could
/// open or retry.
#[tokio::test]
async fn bootstrap_establishes_the_joiners_identity_before_the_marker() {
    let f = joiner_fixture().await;
    let custody = Arc::new(RecordingCustody::default());
    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(&f.store_id, &f.store_dir);
    let joiner_identity = UserKeypair::generate();

    crate::sync::join::bootstrap_and_save_store(
        &f.joiner_storage,
        &f.cipher,
        Some(&f.master_key),
        &f.store_dir,
        &f.store_id,
        "device-b",
        crate::sync::join::BootstrapContext::Join {
            owner_pubkey: &f.owner_pk,
            keypair: &joiner_identity,
        },
        &f.author_heads,
        &f.tables,
        &test_migrations(),
        &f.join_info,
        "Joined Store",
        &f.store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &SystemClock,
        &|_status: &str| {},
        &never_cancelled(),
    )
    .await
    .expect("bootstrap succeeds");

    assert!(
        f.store_dir.config_path().exists(),
        "the completion marker was written"
    );
    let established = identity_custody
        .unlock()
        .expect("identity custody unlocks")
        .expect("a saved config implies the store's identity is already established");
    assert_eq!(
        established.public_key(),
        joiner_identity.public_key(),
        "the established identity is the join request's pending keypair",
    );
}

/// A bootstrap that fails after publishing the reader deletes both the head and
/// the ack, so a never-completed join leaves no reader pinning a peer's
/// reclamation. The failure is induced at the custody-persist step, which runs
/// after the reader publish.
#[tokio::test]
async fn bootstrap_failure_removes_the_published_head_and_ack() {
    let f = joiner_fixture().await;

    /// A custody whose persist always fails, to fail the bootstrap after the
    /// reader publish.
    struct FailPersistCustody;
    impl crate::keys::MasterKeyCustody for FailPersistCustody {
        fn unlock(
            &self,
        ) -> Result<Option<crate::encryption::MasterKeyring>, crate::keys::KeyError> {
            Ok(None)
        }
        fn persist(
            &self,
            _keyring: &crate::encryption::MasterKeyring,
        ) -> Result<(), crate::keys::KeyError> {
            Err(crate::keys::KeyError::Persistence("induced".to_string()))
        }
        fn forget(&self) -> Result<(), crate::keys::KeyError> {
            Ok(())
        }
    }

    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(&f.store_id, &f.store_dir);
    let joiner_identity = UserKeypair::generate();
    let result = crate::sync::join::bootstrap_and_save_store(
        &f.joiner_storage,
        &f.cipher,
        Some(&f.master_key),
        &f.store_dir,
        &f.store_id,
        "device-b",
        crate::sync::join::BootstrapContext::Join {
            owner_pubkey: &f.owner_pk,
            keypair: &joiner_identity,
        },
        &f.author_heads,
        &f.tables,
        &test_migrations(),
        &f.join_info,
        "Joined Store",
        &f.store_keys,
        &FailPersistCustody,
        identity_custody.as_ref(),
        &SystemClock,
        &|_status: &str| {},
        &never_cancelled(),
    )
    .await;

    assert!(
        matches!(result, Err(BootstrapError::Key(_))),
        "the induced persist failure surfaces as a Key error, got {result:?}",
    );

    let listing = crate::sync::storage::SyncStorage::list_heads(&f.joiner_storage)
        .await
        .expect("list heads");
    assert!(
        listing.heads.iter().all(|h| h.device_id != "device-b"),
        "the joiner's head must be deleted after a failed bootstrap",
    );
    assert_eq!(
        listing.unreadable, 0,
        "the deleted head leaves no unreadable slot behind",
    );
    assert!(
        matches!(
            crate::sync::storage::SyncStorage::get_ack(&f.joiner_storage, "device-b").await,
            Err(crate::sync::storage::StorageError::NotFound(_)),
        ),
        "the joiner's ack must be deleted after a failed bootstrap",
    );
}
