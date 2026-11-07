//! What a large value costs in a snapshot, section by section (D7), and the
//! bytes this device keeps for it beside the store. Run explicitly:
//!
//!   scripts/cargo.sh test -p coven --all-features --test snapshot_size -- \
//!     --ignored --nocapture
//!
//! A note of 1 MiB is written and replaced by one of 4 MiB, each in a write of
//! its own with a sync pass after it, so the snapshot holds a row whose value
//! one change set and another replaced.
#![cfg(feature = "test-utils")]

use coven::{
    Coven, CovenHandle, MemoryStorage, Migration, ObjectPrefix, RetryOutcome, RowIdentity,
    SecretText, Storage, SyncedTable,
};
use coven_format::snapshot::section;
use std::sync::Arc;

const REPLACED: usize = 1 << 20;
const WINNING: usize = 4 << 20;

const SECTIONS: [(&str, usize); 11] = [
    ("applied writes", section::WRITE),
    ("synced columns", section::COLUMN),
    ("merge rows", section::MERGE),
    ("lost values", section::LOSS),
    ("authored changes", section::AUTHORED),
    ("creations", section::CREATION),
    ("write evidence", section::EVIDENCE),
    ("admin slots", section::ADMIN),
    ("key-copy slots", section::KEY_COPY),
    ("announcement slots", section::ANNOUNCEMENT),
    ("last claims", section::CLAIM),
];

async fn pass(handle: &CovenHandle) {
    match handle.retry_now().await.unwrap().await {
        RetryOutcome::Synced { .. } => (),
        outcome => panic!("{outcome:?}"),
    }
}

/// Every file under `root` and its size, largest first.
fn files(root: &std::path::Path) -> Vec<(u64, std::path::PathBuf)> {
    let mut found = Vec::new();
    let mut folders = vec![root.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(folder).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                folders.push(entry.path());
            } else {
                found.push((entry.metadata().unwrap().len(), entry.path()));
            }
        }
    }
    found.sort();
    found.reverse();
    found
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one size measurement, run explicitly"]
async fn a_large_value_in_a_snapshot_costs() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let root = tempfile::tempdir().unwrap();
    let handle = Coven::in_memory(root.path().into())
        .builder()
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL);",
        )])
        .storage_connector(storage.clone())
        .create_store("Notes")
        .await
        .unwrap();
    handle
        .setup_s3_storage(
            storage.config(),
            "Ana's laptop",
            "key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await
        .unwrap();
    handle.start_sync().await.unwrap();
    for (size, statement) in [
        (REPLACED, "INSERT INTO notes VALUES('note', ?1)"),
        (WINNING, "UPDATE notes SET body = ?1 WHERE id = 'note'"),
    ] {
        handle
            .write(move |sql| {
                sql.execute(statement, ["n".repeat(size)])?;
                Ok::<_, coven::CovenError>(())
            })
            .await
            .unwrap();
        pass(&handle).await;
    }
    pass(&handle).await;

    let sections = handle.test_snapshot_section_bytes().await.unwrap();
    for (name, at) in SECTIONS {
        eprintln!("{name:>20}: {:>10} bytes", sections[at]);
    }
    eprintln!("{:>20}: {:>10} bytes", "total", sections.iter().sum::<u64>());
    for object in storage.list(&ObjectPrefix::all(handle.store_id())).await.unwrap() {
        eprintln!("{:>10} bytes stored at {}", object.size, object.path.as_str());
    }
    for (size, file) in files(root.path()).into_iter().take(8) {
        eprintln!("{size:>10} bytes in {}", file.strip_prefix(root.path()).unwrap().display());
    }
    handle.close().await.unwrap();
}
