use crate::*;
use coven_replication::sync::test_helpers::{test_migrations, test_synced_tables_with_blob};
use std::error::Error;

fn fixture() -> (CovenHandle, StoreDir, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let directory = StoreDir::new_ephemeral(temp.path());
    let handle = crate::blob_facade_tests::builder(directory.clone())
        .synced_tables(test_synced_tables_with_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheLazy,
        )))
        .migrations(test_migrations())
        .key_custody(KeyCustody::InMemory(MasterKeyring::from(
            EncryptionService::from_key([42; 32]),
        )))
        .identity_custody(IdentityCustody::InMemory(UserKeypair::generate()))
        .open()
        .unwrap();
    (handle, directory, temp)
}

#[tokio::test]
async fn host_callback_error_survives_a_failed_blob_rollback() {
    let (handle, directory, _temp) = fixture();
    let path = directory.local_blob_path("photos", "rejected").unwrap();
    let obstructed = path.clone();
    let result: CovenResult<WriteReceipt<()>> = handle.write_with_blobs(
        |batch| {
            batch.put_blob("photos", "rejected", b"uncommitted image".to_vec());
            Ok(())
        },
        move |sql| {
            sql.execute("INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES ('rejected', 'Rejected write', 0, ?1, ?1)", [sql.stamp()])?;
            std::fs::remove_file(&obstructed)?;
            std::fs::create_dir(&obstructed)?;
            Err(CovenError::TestFailure("the accepted draft changed"))
        },
    ).await;
    let error = result.expect_err("the callback and installed-file rollback fail");
    assert!(matches!(&error, CovenError::WriteRollbackFailed { .. }));
    let count: i64 = handle
        .read(|sql| {
            Ok(sql.query_row(
                "SELECT COUNT(*) FROM notes WHERE id = 'rejected'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "SQL mutations roll back even when file cleanup fails"
    );
    assert!(path.is_dir(), "the cleanup failure remains visible");
    let callback = error
        .source()
        .and_then(|source| source.downcast_ref::<Box<CovenError>>())
        .map(Box::as_ref);
    assert!(
        matches!(
            callback,
            Some(CovenError::TestFailure("the accepted draft changed"))
        ),
        "the original typed callback error must remain in the error source chain: {error}"
    );
}

#[derive(Debug, thiserror::Error)]
#[error("the accepted draft changed from revision {accepted} to {current}")]
struct DraftChanged {
    accepted: u64,
    current: u64,
}

#[tokio::test]
async fn typed_host_rejection_rolls_back_rows_and_blobs() {
    for obstruct_cleanup in [false, true] {
        let (handle, directory, _temp) = fixture();
        let path = directory.local_blob_path("photos", "rejected").unwrap();
        let installed = path.clone();
        let result: CovenResult<WriteReceipt<()>> = handle
            .write_with_blobs(
                |batch| {
                    batch.put_blob("photos", "rejected", b"uncommitted image".to_vec());
                    Ok(())
                },
                move |sql| {
                    sql.execute(
                        "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES ('rejected', 'Rejected write', 0, ?1, ?1)",
                        [sql.stamp()],
                    )?;
                    if obstruct_cleanup {
                        std::fs::remove_file(&installed)?;
                        std::fs::create_dir(&installed)?;
                    }
                    Err(CovenError::Host(Box::new(DraftChanged {
                        accepted: 17,
                        current: 18,
                    })))
                },
            )
            .await;
        let error = result.expect_err("the host rejects the write");
        assert_eq!(
            matches!(&error, CovenError::WriteRollbackFailed { .. }),
            obstruct_cleanup,
            "the public error must retain the actual rollback outcome"
        );
        let mut source: &dyn Error = &error;
        let rejected = loop {
            if let Some(rejected) = source.downcast_ref::<DraftChanged>() {
                break rejected;
            }
            source = source
                .source()
                .expect("the typed host error remains reachable");
        };
        assert_eq!((rejected.accepted, rejected.current), (17, 18));
        let count: i64 = handle
            .read(|sql| {
                Ok(sql.query_row(
                    "SELECT COUNT(*) FROM notes WHERE id = 'rejected'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(path.exists(), obstruct_cleanup);
    }
}
