//! What a value costs in a snapshot, section by section (D7), and the bytes
//! this device keeps for it beside the store.
//!
//! `a_snapshot_holds_a_rows_current_value_and_no_edit_every_device_wrote_past`
//! runs with every test run and bounds a snapshot's size by the row's
//! current value and a fixed overhead, with no term per edit, for a lone
//! device and for one whose second device writes past each batch of edits.
//! The measurements run explicitly:
//!
//!   scripts/cargo.sh test -p coven --all-features --test snapshot_size -- \
//!     --ignored --nocapture --test-threads 1
//!
//! `a_large_value_in_a_snapshot_costs` writes a note of 1 MiB and replaces it
//! by one of 4 MiB, each in a write of its own with a sync pass after it, so
//! the snapshot holds a row whose value one change set and another replaced.
//!
//! `snapshot_growth_with_edit_history` shows how the bytes grow with a cell's
//! history: one note's body written once and then edited n times, at a few
//! sizes, and a note deleted after its edits. The app's tables end the same
//! size in every case, one row or none, so whatever grows with n is history.
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

/// A new store of one synced table, `notes(id, body)`, set up on `storage`
/// and syncing.
async fn notes_store(storage: &Arc<MemoryStorage>, root: &std::path::Path) -> CovenHandle {
    let handle = Coven::in_memory(root.into())
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
    handle
}

/// Run `statement` with one text parameter of `size` bytes, all `fill`, in
/// a write of its own.
async fn write(handle: &CovenHandle, statement: &'static str, size: usize, fill: u8) {
    let value = String::from_utf8(vec![fill; size]).unwrap();
    handle
        .write(move |sql| {
            sql.execute(statement, [value])?;
            Ok::<_, coven::CovenError>(())
        })
        .await
        .unwrap();
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

/// The bytes of every file under `root` whose name is `name`.
fn file_bytes(root: &std::path::Path, name: &str) -> u64 {
    files(root)
        .into_iter()
        .filter(|(_, path)| path.file_name().is_some_and(|file| file == name))
        .map(|(size, _)| size)
        .sum()
}

/// The store database's pages by table or index, largest first, as SQLite's
/// `dbstat` counts them: the bytes each one's pages take in the store file.
async fn store_pages(handle: &CovenHandle) -> Vec<(String, i64)> {
    handle
        .read(|sql| {
            Ok::<_, coven::CovenError>(sql.query(
                "SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name ORDER BY bytes DESC",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one size measurement, run explicitly"]
async fn a_large_value_in_a_snapshot_costs() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let root = tempfile::tempdir().unwrap();
    let handle = notes_store(&storage, root.path()).await;
    for (size, statement) in [
        (REPLACED, "INSERT INTO notes VALUES('note', ?1)"),
        (WINNING, "UPDATE notes SET body = ?1 WHERE id = 'note'"),
    ] {
        write(&handle, statement, size, b'n').await;
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
    for (name, bytes) in store_pages(&handle).await.into_iter().take(12) {
        eprintln!("{bytes:>10} bytes of store.db pages in {name}");
    }
    handle.close().await.unwrap();
    for (size, file) in files(root.path()).into_iter().take(8) {
        eprintln!("{size:>10} bytes in {}", file.strip_prefix(root.path()).unwrap().display());
    }
}

/// The bytes stored under the store's prefix: snapshot bodies, sequence
/// slots, and the floor with everything else.
async fn stored(storage: &MemoryStorage, store: coven::StoreId) -> (u64, u64, u64) {
    let mut bytes = (0, 0, 0);
    for object in storage.list(&ObjectPrefix::all(store)).await.unwrap() {
        let path = object.path.as_str();
        if path.contains("/snapshots/") {
            bytes.0 += object.size;
        } else if path.contains("/seq/") {
            bytes.1 += object.size;
        } else {
            bytes.2 += object.size;
        }
    }
    bytes
}

/// Print every table or index of `pages` holding more than one page.
fn print_pages(pages: &[(String, i64)]) {
    for (name, bytes) in pages.iter().filter(|(_, bytes)| *bytes > 4096) {
        eprintln!("  {bytes:>11} {name}");
    }
}

/// One case of the growth table: what the store holds after `edits`
/// replacements of one note's `size`-byte body, and, with `delete`, the
/// note's delete after them.
struct Case {
    size: usize,
    edits: usize,
    delete: bool,
}

/// The case whose store pages are listed by table, and which a second
/// device restores.
const LISTED: usize = 5;

/// What a snapshot of the notes store holds besides its rows' current
/// values and its snapshot announcements: the rows' identities and
/// generations, the synced column, each row's creation, the records and
/// entries of the writes still current and of each device's last write, the
/// original admin and key-copy slots, and the last claims. Nothing in it
/// grows with the number of edits. Measured at about 1.5 KB alone, and
/// 2.3 KB with a second device, whose addition, row and writes the snapshot
/// holds too.
const OVERHEAD: u64 = 4096;

/// The notes store restored on a second device of the same member, syncing.
async fn second_device(
    storage: &Arc<MemoryStorage>,
    handle: &CovenHandle,
    root: &std::path::Path,
) -> CovenHandle {
    let code = handle.restore_code_for_transfer().await.unwrap();
    let (_cancelling, cancel) = tokio::sync::watch::channel(false);
    let restored = coven::restore_from_code(
        Coven::in_memory(root.into())
            .builder()
            .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
            .migrations(vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL);",
            )])
            .storage_connector(Arc::new(storage.for_device())),
        &code,
        None,
        "Ana's phone",
        |_| {},
        &cancel,
    )
    .await
    .unwrap();
    restored.start_sync().await.unwrap();
    restored
}

/// The phone reads what the laptop landed, then lands a write past it: its
/// own row's first value, or a new one after `written` of the laptop's
/// edits.
async fn write_past(phone: &CovenHandle, written: usize) {
    pass(phone).await;
    let value = written.to_string();
    phone
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes VALUES('phone', ?1) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
                [value],
            )?;
            Ok::<_, coven::CovenError>(())
        })
        .await
        .unwrap();
    pass(phone).await;
}

/// A snapshot holds a row's current value once, and nothing of the edits
/// that replaced it once every device that can still write has written past
/// them (coven.md §16.7): no value a write replaced, no kept record, and no
/// applied write's entry. A lone device writes past its own edits; with a
/// second device, that device lands a write after each batch of edits. A
/// snapshot that kept a fixed record per edit would grow by about 200 bytes
/// an edit, which `OVERHEAD` leaves no room for at a hundred edits.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_holds_a_rows_current_value_and_no_edit_every_device_wrote_past() {
    let mut cases = Vec::new();
    for size in [1 << 10, 256 << 10] {
        for edits in [1, 10, 100] {
            cases.push(Case { size, edits, delete: false });
        }
    }
    cases.push(Case { size: 256 << 10, edits: 10, delete: true });
    for case in cases {
        for second in [false, true] {
            let storage = Arc::new(MemoryStorage::builder().build().unwrap());
            let root = tempfile::tempdir().unwrap();
            let phone_root = tempfile::tempdir().unwrap();
            let handle = notes_store(&storage, root.path()).await;
            write(&handle, "INSERT INTO notes VALUES('note', ?1)", case.size, b'a').await;
            pass(&handle).await;
            let phone = match second {
                true => Some(second_device(&storage, &handle, phone_root.path()).await),
                false => None,
            };
            let mut written = 0;
            while written < case.edits {
                let batch = (case.edits - written).min(10);
                for edit in written..written + batch {
                    let fill = b'b' + (edit % 24) as u8;
                    write(&handle, "UPDATE notes SET body = ?1 WHERE id = 'note'", case.size, fill)
                        .await;
                }
                written += batch;
                pass(&handle).await;
                if let Some(phone) = &phone {
                    write_past(phone, written).await;
                }
            }
            if case.delete {
                handle
                    .write(|sql| {
                        sql.execute("DELETE FROM notes WHERE id = 'note'", [])?;
                        Ok::<_, coven::CovenError>(())
                    })
                    .await
                    .unwrap();
                pass(&handle).await;
                if let Some(phone) = &phone {
                    write_past(phone, written + 1).await;
                }
            }
            pass(&handle).await;
            pass(&handle).await;
            // Each snapshot the passes took adds its announcement slot, which
            // retention deletes in time (§16.6): they follow the passes this
            // test makes, not the edits.
            let sections = handle.test_snapshot_section_bytes().await.unwrap();
            let total: u64 = sections.iter().sum::<u64>() - sections[section::ANNOUNCEMENT];
            handle.close().await.unwrap();
            if let Some(phone) = phone {
                phone.close().await.unwrap();
            }
            let current = if case.delete { 0 } else { case.size as u64 };
            eprintln!(
                "{:>7} {:>4} {:>5} {second:>5}: {:>7} beyond the value",
                case.size,
                case.edits,
                case.delete,
                total.saturating_sub(current)
            );
            let bound = current + OVERHEAD;
            assert!(
                total <= bound,
                "a value of {} bytes, {} edits, deleted {}, a second device {second}: {total} snapshot bytes, bound {bound}",
                case.size,
                case.edits,
                case.delete
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "one size measurement, run explicitly"]
async fn snapshot_growth_with_edit_history() {
    let mut cases = Vec::new();
    for size in [1 << 10, 16 << 10, 256 << 10] {
        for edits in [1, 10, 100] {
            cases.push(Case { size, edits, delete: false });
        }
    }
    cases.push(Case { size: 16 << 10, edits: 10, delete: true });
    cases.push(Case { size: 16 << 10, edits: 100, delete: true });

    eprintln!("Snapshot sections, plaintext bytes; storage objects; this device's files.");
    eprintln!("store.db is its file's size, and in brackets the pages its tables use.");
    eprintln!(
        "{:>7} {:>5} {:>6} | {:>10} {:>10} {:>10} {:>10} | {:>10} {:>10} {:>7} | {:>23} {:>10}",
        "value",
        "edits",
        "delete",
        "merge rows",
        "lost",
        "authored",
        "total",
        "snapshots",
        "slots",
        "floor",
        "store.db [used]",
        "inputs.db",
    );
    for (at, case) in cases.iter().enumerate() {
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let root = tempfile::tempdir().unwrap();
        let handle = notes_store(&storage, root.path()).await;
        write(&handle, "INSERT INTO notes VALUES('note', ?1)", case.size, b'a').await;
        for edit in 0..case.edits {
            let fill = b'b' + (edit % 24) as u8;
            write(&handle, "UPDATE notes SET body = ?1 WHERE id = 'note'", case.size, fill).await;
        }
        if case.delete {
            handle
                .write(|sql| {
                    sql.execute("DELETE FROM notes WHERE id = 'note'", [])?;
                    Ok::<_, coven::CovenError>(())
                })
                .await
                .unwrap();
        }
        pass(&handle).await;
        pass(&handle).await;

        let sections = handle.test_snapshot_section_bytes().await.unwrap();
        let (snapshots, slots, floor) = stored(&storage, handle.store_id()).await;
        let pages = store_pages(&handle).await;
        let used: i64 = pages.iter().map(|(_, bytes)| bytes).sum();
        let code = (at == LISTED).then(|| handle.restore_code_for_transfer());
        let code = match code {
            Some(code) => Some(code.await.unwrap()),
            None => None,
        };
        handle.close().await.unwrap();
        eprintln!(
            "{:>7} {:>5} {:>6} | {:>10} {:>10} {:>10} {:>10} | {:>10} {:>10} {:>7} | {:>11} [{:>9}] {:>10}",
            case.size,
            case.edits,
            case.delete,
            sections[section::MERGE],
            sections[section::LOSS],
            sections[section::AUTHORED],
            sections.iter().sum::<u64>(),
            snapshots,
            slots,
            floor,
            file_bytes(root.path(), "store.db"),
            used,
            file_bytes(root.path(), "inputs.db"),
        );
        let Some(code) = code else {
            continue;
        };
        eprintln!("  store.db pages by table, value of {} bytes, {} edits:", case.size, case.edits);
        print_pages(&pages);

        // A second device of the same member restores the store from its
        // snapshot: what a device that only loaded the history keeps.
        let device_root = tempfile::tempdir().unwrap();
        let (_cancelling, cancel) = tokio::sync::watch::channel(false);
        let restored = coven::restore_from_code(
            Coven::in_memory(device_root.path().into())
                .builder()
                .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
                .migrations(vec![Migration::addition(
                    1,
                    "notes",
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, body TEXT NOT NULL);",
                )])
                .storage_connector(Arc::new(storage.for_device())),
            &code,
            None,
            "Ana's phone",
            |_| {},
            &cancel,
        )
        .await
        .unwrap();
        let pages = store_pages(&restored).await;
        let used: i64 = pages.iter().map(|(_, bytes)| bytes).sum();
        restored.close().await.unwrap();
        eprintln!(
            "  a restored device: store.db {} [{used} used], inputs.db {}; its pages by table:",
            file_bytes(device_root.path(), "store.db"),
            file_bytes(device_root.path(), "inputs.db"),
        );
        print_pages(&pages);
    }
}
