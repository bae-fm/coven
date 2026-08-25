use crate::*;
use coven_protocol::blob::{Provenance, BLOB_TOMBSTONE_GRACE};
use coven_protocol::store_commit::{commit_semantic_prefix, StreamActivationId};

pub(crate) fn reclaim_test_object(path: &str) -> ExactObjectRef {
    let bytes = path.as_bytes();
    ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(path.to_string())
            .expect("valid reclaim test slot"),
        u64::try_from(bytes.len()).expect("reclaim test object length fits u64"),
        ObjectHash::digest(bytes),
    )
}

pub(crate) fn reclaim_test_activation(
    commit: StoreBatchCommitRef,
    label: &str,
) -> ReclaimCommitActivation {
    ReclaimCommitActivation::new(
        commit,
        coven_protocol::store_commit::StoreDeviceHeadRef {
            head_hash: ObjectHash::digest(format!("{label} reclaim head").as_bytes()),
            object: reclaim_test_object(&format!("store-v1/test/{label}/reclaim-head.json")),
        },
    )
    .expect("valid reclaim activation")
}

pub(crate) fn snapshot_activation(label: &str) -> StreamActivationId {
    let registration_bytes = format!("{label} snapshot registration");
    let registration = coven_protocol::store_commit::StoreDeviceRegistrationRef {
        device_id: format!("{:0>64}", label.len())
            .parse()
            .expect("valid snapshot test device id"),
        registration_hash: ObjectHash::digest(registration_bytes.as_bytes()),
        object: reclaim_test_object(&format!("store-v1/test/{label}/snapshot-registration.json")),
    };
    coven_protocol::store_commit::StreamActivation::device_authorized(
        ObjectHash::digest(format!("{label} Store root").as_bytes()),
        registration,
        coven_protocol::store_commit::DeviceStreamAnchor::StoreSnapshots {
            first_slot: coven_protocol::objects::ObjectSlot::logical(format!(
                "store-v1/test/{label}/snapshots/1.json"
            ))
            .expect("valid snapshot activation slot"),
        },
    )
    .activation_id()
}

pub(crate) fn notes_migration() -> Migration {
    Migration::sql(
        1,
        "notes",
        "CREATE TABLE notes (
            id TEXT PRIMARY KEY,
            body TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )
}

pub(crate) fn things_migration() -> Migration {
    Migration::sql(
        1,
        "things",
        "CREATE TABLE things (
            id TEXT PRIMARY KEY,
            body TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )
}

pub(crate) fn things_table(identity: coven_protocol::synced_schema::RowIdentity) -> SyncedTable {
    SyncedTable::new("things", identity)
}

pub(crate) fn scoped_things_migration() -> Migration {
    Migration::sql(
        1,
        "scoped things",
        "CREATE TABLE things (
            id TEXT PRIMARY KEY,
            audience TEXT,
            body TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )
}

pub(crate) fn scoped_things_table() -> SyncedTable {
    SyncedTable::new(
        "things",
        coven_protocol::synced_schema::RowIdentity::SharedKey,
    )
    .scoped_by("audience")
}

pub(crate) fn exact_blob_binding(row_id: &str, stamp: &str, bytes: &[u8]) -> RowBlobLocatorBinding {
    let plaintext_hash = ObjectHash::digest(bytes);
    let uploader_bytes = b"database test uploader registration";
    let uploader = coven_protocol::store_commit::StoreDeviceRegistrationRef {
        device_id: "aa".repeat(32).parse().expect("valid test device id"),
        registration_hash: ObjectHash::digest(uploader_bytes),
        object: ExactObjectRef::new(
            coven_protocol::objects::ObjectSlot::logical(
                "store-v1/devices/database-test-uploader.json".to_string(),
            )
            .expect("valid uploader registration slot"),
            uploader_bytes.len() as u64,
            ObjectHash::digest(uploader_bytes),
        ),
    };
    let locator = BlobLocator::browsable(
        "images",
        row_id,
        uploader,
        format!("photos/{row_id}.bin"),
        bytes.len() as u64,
        plaintext_hash,
    )
    .expect("valid locator");
    let stored = b"stored representation".to_vec();
    let slot = coven_protocol::objects::ObjectSlot::logical(locator.semantic_key())
        .expect("valid exact slot");
    let object = ExactObjectRef::new(slot, stored.len() as u64, ObjectHash::digest(&stored));
    RowBlobLocatorBinding::new(
        "photos",
        row_id,
        stamp,
        "id",
        StoredBlobRef::new(locator, object).expect("valid stored blob"),
    )
    .expect("valid row binding")
}

pub(crate) fn local_row_blob(row_id: &str, stamp: &str, bytes: &[u8]) -> RowBlobRef {
    let binding = exact_blob_binding(row_id, stamp, bytes);
    let locator = binding.blob().locator();
    RowBlobRef::new(
        "photos".to_string(),
        row_id.to_string(),
        stamp.to_string(),
        "id".to_string(),
        BlobRef {
            namespace: locator.namespace().to_string(),
            id: locator.blob_id().to_string(),
            scope: coven_protocol::blob::BlobScope::Master,
            cloud_path: locator.cloud_path().map(str::to_string),
            provenance: Provenance::HostProvided,
            fill: coven_protocol::blob::CacheFill::CacheLazy,
        },
        locator.plaintext_size(),
        locator.plaintext_hash(),
        RowBlobAuthority::Local,
        None,
    )
    .expect("valid Local row blob")
}

pub(crate) fn open_outbox_database(device_id: &str) -> Database {
    Database::open(
        Path::new(":memory:"),
        Vec::new(),
        BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        CovenMigrationPolicy::ApplyPending,
        &[],
    )
    .expect("open outbox database")
}

pub(crate) fn test_candidate_family() -> coven_protocol::store_commit::CandidateFamilyId {
    coven_protocol::store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(
        b"database test candidate family",
    ))
}

pub(crate) fn test_commit_coord() -> StoreCommitCoord {
    StoreCommitCoord {
        stream_id: coven_protocol::membership::AuthorStreamId::from_bytes([7; 32]),
        sequence: 1,
    }
}

pub(crate) fn test_commit_ref() -> StoreBatchCommitRef {
    let coord = test_commit_coord();
    let commit_hash = ObjectHash::digest(b"database test commit");
    let StoreCommitCoord { stream_id, .. } = &coord;
    let slot = coven_protocol::objects::ObjectSlot::logical(format!(
        "{}.json",
        commit_semantic_prefix(
            test_candidate_family(),
            &stream_id.to_string(),
            coord.sequence(),
            commit_hash,
        )
    ))
    .expect("valid database test commit slot");
    StoreBatchCommitRef {
        coord,
        commit_hash,
        object: ExactObjectRef::new(slot, 1, ObjectHash::digest(b"x")),
    }
}

pub(crate) fn blob_binding_table() -> SyncedTable {
    SyncedTable::new(
        "photos",
        coven_protocol::synced_schema::RowIdentity::SharedKey,
    )
    .carries_blob(
        coven_protocol::synced_schema::BlobDecl::new(
            "images",
            Provenance::HostProvided,
            coven_protocol::blob::CacheFill::CacheLazy,
        )
        .with_cloud_path_column("cloud_path"),
    )
}
