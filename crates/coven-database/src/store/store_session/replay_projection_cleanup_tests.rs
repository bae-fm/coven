use super::*;
use crate::local_blob_cleanup_intents::LocalBlobCleanupIntent;
use crate::tests::fixtures::{
    blob_binding_table, exact_blob_binding, test_candidate_family, test_commit_coord,
    test_commit_ref,
};
use coven_protocol::audience_package::AudiencePackage;
use coven_protocol::blob::{DeferredLocalBlobDisposition, DeferredLocalBlobDrop};
use coven_protocol::store_commit::ObjectHash;

const STAMP: &str = "0000000001000-0000-a";
const CONTENT: &[u8] = b"current bytes";

enum CleanupCopy {
    ObsoleteExact,
    ReferencedExact,
    ObsoleteLocal,
    ReferencedLocal,
    PublishedLocal,
}

fn connection() -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().expect("open copy projection");
    crate::apply_coven_schema(&connection).expect("create cleanup and locator records");
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE photos (
                 id TEXT PRIMARY KEY, size INTEGER NOT NULL, hash TEXT NOT NULL,
                 cloud_path TEXT NOT NULL, _updated_at TEXT NOT NULL
             ) STRICT;",
        )
        .expect("create blob-bearing rows");
    connection
}

fn assert_projection_cleanup(copy: CleanupCopy) {
    let (_directory, store_dir) = coven_foundation::store_dir::temp_store_dir();
    let mut source = connection();
    let mut target = connection();
    let tables = [blob_binding_table()];
    let declarations = crate::BlobDecls::from_tables(&source, &tables).expect("resolve blobs");
    let gates = crate::Gates::from_tables(&source, &tables).expect("resolve row audience");
    crate::DatabaseTestSql::new(&source)
        .insert_blob_row("blob", STAMP, CONTENT)
        .expect("populate projected row");
    let binding = exact_blob_binding("blob", STAMP, CONTENT);
    let projected_remote = !matches!(copy, CleanupCopy::ReferencedLocal);
    if projected_remote {
        let package = AudiencePackage::store(
            ObjectHash::digest(b"cleanup projection root"),
            test_candidate_family(),
            coven_protocol::write::WriteId::from_generated("cleanup-projection".into()),
            test_commit_coord(),
            1,
            Vec::new(),
            vec![binding.clone()],
        )
        .expect("prepare exact projected blob");
        // Accepted object provenance is installed before live row projection.
        let target_transaction = target
            .transaction()
            .expect("retain incoming blob authority");
        crate::Database::install_pulled_blob_activations_on(
            &target_transaction,
            &package,
            &test_commit_ref(),
        )
        .expect("retain accepted object before installing its rows");
        target_transaction
            .commit()
            .expect("commit accepted object inventory");
        let transaction = source.transaction().expect("begin blob activation");
        crate::Database::install_pulled_blob_activations_on(
            &transaction,
            &package,
            &test_commit_ref(),
        )
        .expect("retain exact remote object");
        crate::store::test_install_winning_blob_bindings(
            &transaction,
            &store_dir,
            &gates,
            &tables,
            &package,
            &crate::BlobActivation {
                coord: test_commit_coord(),
            },
            &[crate::WinningRow {
                table: "photos".into(),
                row_id: "blob".into(),
                row_stamp: Some(STAMP.into()),
            }],
        )
        .expect("bind projected content through its owner");
        transaction.commit().expect("commit projected binding");
    }
    declarations
        .install_cleanup_guards(&target)
        .expect("install real cleanup guards");
    let current_hash = binding.blob().locator().locator_hash();
    let intent = match copy {
        CleanupCopy::ObsoleteExact => LocalBlobCleanupIntent::exact(
            "images",
            "blob",
            exact_blob_binding("blob", STAMP, b"obsolete bytes")
                .blob()
                .locator()
                .locator_hash(),
        ),
        CleanupCopy::ReferencedExact => {
            LocalBlobCleanupIntent::exact("images", "blob", current_hash)
        }
        _ => LocalBlobCleanupIntent::local("images", "blob"),
    };
    let published = PublishedBlobDropIntent {
        seq: 1,
        drop: DeferredLocalBlobDrop {
            namespace: "images".into(),
            id: "blob".into(),
            size: CONTENT.len() as u64,
            plaintext_hash: ObjectHash::digest(CONTENT),
            locator_hash: current_hash,
            disposition: DeferredLocalBlobDisposition::Drop,
        },
    };
    if matches!(copy, CleanupCopy::PublishedLocal) {
        reinsert_published_blob_drop_intent_on(&target, &published)
            .expect("record published local-source deletion");
    } else {
        crate::DatabaseTestSql::new(&target)
            .insert_cleanup_intent(
                intent.namespace(),
                intent.blob_id(),
                &intent.persisted_identity().unwrap(),
            )
            .expect("record in-flight copy deletion");
    }
    let transaction = target
        .transaction()
        .expect("begin guarded projection installation");
    let suspended = suspend_projection_blob_cleanup_on(&source, &transaction, &declarations)
        .expect("select only safely restorable copies");
    let mut projection_tables = crate::projection_table_names(false);
    projection_tables.push("photos".into());
    projection_tables.sort();
    transaction
        .pragma_update(None, "defer_foreign_keys", "ON")
        .unwrap();
    let installed = replace_tables_from_connection_on(&source, &transaction, &projection_tables);
    let must_refuse = matches!(
        copy,
        CleanupCopy::ReferencedExact | CleanupCopy::ReferencedLocal
    );
    if must_refuse {
        let error = installed.expect_err("cannot restore a copy an in-flight drain may delete");
        assert!(
            error.to_string().contains("blob local cleanup in progress"),
            "{error}"
        );
        transaction
            .rollback()
            .expect("roll back guarded installation");
        assert_eq!(
            target
                .query_row("SELECT count(*) FROM photos", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    } else {
        installed.expect("a different copy does not block the projected row");
        reevaluate_suspended_blob_cleanup_on(&transaction, &declarations, &suspended)
            .expect("retain every obsolete copy obligation");
        transaction
            .commit()
            .expect("commit projection and cleanup together");
        let restored = crate::Database::row_blob_ref_on(&target, &gates, &tables[0], "blob")
            .expect("resolve the installed remote copy");
        assert_eq!(restored.stored(), Some(binding.blob()));
        assert_eq!(restored.plaintext_hash(), ObjectHash::digest(CONTENT));
    }
    if matches!(copy, CleanupCopy::PublishedLocal) {
        let actual = target
            .query_row(
                "SELECT seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition
             FROM published_blob_drop_intents",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .expect("published cleanup remains exact after installation");
        assert_eq!(
            actual,
            (
                1,
                published.drop.namespace,
                published.drop.id,
                CONTENT.len() as i64,
                published.drop.plaintext_hash.to_string(),
                current_hash.to_string(),
                published.drop.disposition.as_db().into()
            )
        );
    } else {
        assert_eq!(
            local_blob_cleanup_intents_on(&target).expect("read unchanged cleanup ownership"),
            vec![(intent, false)]
        );
    }
}

#[test]
fn projection_preserves_cleanup_of_another_exact_copy() {
    assert_projection_cleanup(CleanupCopy::ObsoleteExact);
}

#[test]
fn projection_refuses_an_exact_copy_already_owned_by_cleanup() {
    assert_projection_cleanup(CleanupCopy::ReferencedExact);
}

#[test]
fn remote_projection_preserves_obsolete_local_source_cleanup() {
    assert_projection_cleanup(CleanupCopy::ObsoleteLocal);
}

#[test]
fn projection_refuses_an_unleased_local_source_owned_by_cleanup() {
    assert_projection_cleanup(CleanupCopy::ReferencedLocal);
}

#[test]
fn remote_projection_preserves_published_local_source_cleanup() {
    assert_projection_cleanup(CleanupCopy::PublishedLocal);
}
