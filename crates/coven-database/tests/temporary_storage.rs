//! SQLite's temporary tables spill to files in the folder the app names, so
//! a staged snapshot waits on disk (§21.2). This runs alone in its process:
//! the folder is named before any store opens.
use coven_database::{set_temporary_directory, DatabaseBuilder, TemporaryDirectoryError};
use coven_format::{
    admin::SnapshotId,
    sealed_snapshot::SnapshotObjectPrefix,
    sequence::Purpose,
    snapshot::{LastClaim, SnapshotEncoder, SnapshotHeader, SnapshotRecord, SECTIONS},
};
use coven_foundation::{
    files::StoreDirectories,
    id_source::{DeviceId, IdSource, SequentialIds, StoreId},
};
use coven_merge::Audience;

/// A store snapshot of `claims` last-claim records, about 57 bytes each.
fn claims_snapshot(id: &SnapshotId, covers: u64, claims: u64) -> Vec<u8> {
    let mut counts = [0; SECTIONS];
    counts[11] = claims;
    let (mut encoder, mut bytes) = SnapshotEncoder::start(SnapshotHeader {
        id: id.clone(),
        schema_version: 1,
        covers,
        counts,
    })
    .unwrap();
    for device in 1..=claims {
        bytes.extend(
            encoder
                .record(SnapshotRecord::Claim(LastClaim {
                    device: DeviceId(device),
                    number: 1,
                    digest: Some([7; 32]),
                }))
                .unwrap(),
        );
    }
    bytes.extend(encoder.finish().unwrap());
    bytes
}

#[tokio::test]
async fn staged_records_spill_to_files_in_the_named_temporary_folder() {
    let folder = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let missing = folder.path().join("missing");
    assert!(matches!(
        set_temporary_directory(&missing),
        Err(TemporaryDirectoryError::NotAFolder { directory }) if directory == missing
    ));
    set_temporary_directory(folder.path()).unwrap();
    set_temporary_directory(folder.path()).unwrap();
    assert!(matches!(
        set_temporary_directory(other.path()),
        Err(TemporaryDirectoryError::AlreadyNamed { directory }) if directory == folder.path()
    ));

    let store = tempfile::tempdir().unwrap();
    let ids = SequentialIds::new();
    let directory = StoreDirectories::new(store.path().into())
        .create_store_dir(StoreId(ids.new_id()), &ids)
        .unwrap();
    let db = DatabaseBuilder::new(directory)
        .synced_tables(coven_database::test_utils::notes_tables())
        .migrations(coven_database::test_utils::notes_migrations())
        .open()
        .await
        .unwrap();
    assert_eq!(db.input_temp_store().await.unwrap(), 1, "temp_store=FILE");

    // 200,000 records, about 11 MB staged: far past the connection's 2 MiB
    // cache, so the temporary database must be a file.
    let id = SnapshotId {
        device: DeviceId(99),
        number: 1,
        audience: Audience::Store,
    };
    let prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(1)),
        covers: 2,
        purpose: Purpose::Ordinary,
    };
    let bytes = claims_snapshot(&id, prefix.covers, 200_000);
    let staged =
        coven_database::test_utils::stage_plaintext(&db, id, prefix, std::io::Cursor::new(bytes))
            .await
            .unwrap();
    assert!(db.staged_snapshot_records().await.unwrap() >= 200_000);
    let open = coven_foundation::files::open_files_under(folder.path());
    assert!(
        !open.is_empty(),
        "no temporary file open under {}",
        folder.path().display()
    );
    staged.discard().await.unwrap();
    db.close().await.unwrap();
}
