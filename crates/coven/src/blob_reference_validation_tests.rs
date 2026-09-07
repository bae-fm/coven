use super::*;

async fn fixture() -> (
    crate::CovenHandle,
    crate::StoreDir,
    tempfile::TempDir,
    coven_keys::keys::UserKeypair,
) {
    let tmp = tempfile::tempdir().expect("store directory");
    let dir = crate::StoreDir::new_ephemeral(tmp.path());
    let owner = coven_keys::keys::UserKeypair::generate();
    let handle = builder(dir.clone())
        .synced_tables(test_synced_tables_with_blob(crate::BlobDecl::new(
            "photos",
            crate::Provenance::HostProvided,
            crate::CacheFill::CacheLazy,
        )))
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(crate::MasterKeyring::from(
            crate::EncryptionService::from_key([42; 32]),
        )))
        .identity_custody(crate::IdentityCustody::InMemory(owner.clone()))
        .open()
        .expect("open store");
    let bytes = b"original photo".to_vec();
    let size = bytes.len() as i64;
    let hash = crate::content_hash(&bytes);
    handle.write_with_blobs(move |batch| {
        batch.put_blob("photos", "photo-1", bytes);
        Ok(())
    }, move |sql| {
        sql.execute("INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES ('note-1', 'Original', 0, ?1, ?1)", [sql.stamp()])?;
        sql.execute("INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) VALUES ('photo-1', 'note-1', 'photo', ?1, ?2, ?3, ?3)", rusqlite::params![size, hash, sql.stamp()])?;
        Ok(())
    }).await.expect("write original photo");
    (handle, dir, tmp, owner)
}

#[tokio::test]
async fn transaction_validation_accepts_current_refs_after_unrelated_writes() {
    let (handle, _dir, _tmp, _owner) = fixture().await;
    let reference = handle.row_blob_ref("note_photos", "photo-1").await.unwrap();
    handle
        .write(move |sql| {
            sql.execute(
                "UPDATE notes SET title = 'Changed', _updated_at = ?1 WHERE id = 'note-1'",
                [sql.stamp()],
            )?;
            sql.validate_row_blob_ref(&reference)?;
            Ok(())
        })
        .await
        .expect("unrelated parent metadata does not invalidate a local photo");
}

#[tokio::test]
async fn transaction_validation_rejects_changed_rows_and_rolls_back_staged_blobs() {
    for (change, hash_only) in [
        (
            "UPDATE note_photos SET kind = 'edited', _updated_at = ?1 WHERE id = 'photo-1'",
            false,
        ),
        (
            "UPDATE note_photos SET hash = ?1 WHERE id = 'photo-1'",
            true,
        ),
        (
            "DELETE FROM note_photos WHERE id = 'photo-1' AND _updated_at != ?1",
            false,
        ),
    ] {
        let (handle, dir, _tmp, _owner) = fixture().await;
        let reference = handle.row_blob_ref("note_photos", "photo-1").await.unwrap();
        handle
            .write(move |sql| {
                let value = if hash_only {
                    crate::content_hash(b"changed photo")
                } else {
                    sql.stamp()
                };
                sql.execute(change, [value])?;
                Ok(())
            })
            .await
            .expect("change photo after planning");
        let result = handle.write_with_blobs(|batch| {
            batch.put_blob("photos", "uncommitted-photo", b"staged replacement".to_vec());
            Ok(())
        }, move |sql| {
            sql.execute("UPDATE notes SET title = 'Must roll back', _updated_at = ?1 WHERE id = 'note-1'", [sql.stamp()])?;
            sql.validate_row_blob_ref(&reference)?;
            Ok(())
        }).await;
        assert!(
            result.is_err(),
            "changed photo must abort the entire write: {change}"
        );
        let title: String = handle
            .read(|sql| {
                Ok(
                    sql.query_row("SELECT title FROM notes WHERE id = 'note-1'", [], |row| {
                        row.get(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(title, "Original");
        assert!(!dir
            .local_blob_path("photos", "uncommitted-photo")
            .unwrap()
            .exists());
    }
}

#[tokio::test]
async fn transaction_validation_rejects_a_reference_from_before_remote_publication() {
    let (handle, _dir, _tmp, owner) = fixture().await;
    let home = test_cloud_home();
    let store = handle
        .create_test_store("blob-facade-test", owner, home.clone())
        .await
        .unwrap();
    handle
        .connect_sync_with_test_home_caller_driven(
            home,
            coven_storage::CloudCipher::Encrypted(crate::EncryptionService::from_key([42; 32])),
        )
        .await
        .unwrap();
    let before = handle.row_blob_ref("note_photos", "photo-1").await.unwrap();
    assert!(before.stored().is_none());
    handle
        .make_remote_with_discovered_order_for_test("notes", "note-1", "Photo", false)
        .await
        .unwrap();
    handle.drain_uploads().await.unwrap();
    handle.publish_test_store(&store).await.unwrap();
    let published = handle.row_blob_ref("note_photos", "photo-1").await.unwrap();
    assert!(published.stored().is_some());
    handle
        .write(move |sql| {
            sql.execute(
                "UPDATE notes SET title = 'Stale', _updated_at = ?1 WHERE id = 'note-1'",
                [sql.stamp()],
            )?;
            sql.validate_row_blob_ref(&before)?;
            Ok(())
        })
        .await
        .expect_err("a retained local reference does not describe the published object");
    let title: String = handle
        .read(|sql| {
            Ok(
                sql.query_row("SELECT title FROM notes WHERE id = 'note-1'", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(title, "Original");
    handle
        .write(move |sql| {
            sql.execute(
                "UPDATE notes SET title = 'Validated', _updated_at = ?1 WHERE id = 'note-1'",
                [sql.stamp()],
            )?;
            sql.validate_row_blob_ref(&published)?;
            Ok(())
        })
        .await
        .expect("the current stored reference validates inside the transaction");
}
