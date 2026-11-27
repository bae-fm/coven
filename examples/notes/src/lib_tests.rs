use super::*;
use coven::{Coven, Migration, RowIdentity, SyncedTable};

/// A new store with the app's schema, in a folder of its own.
async fn store(root: &std::path::Path) -> CovenHandle {
    Coven::in_memory(root.into())
        .builder()
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)])
        .migrations(vec![Migration::addition(1, "initial", SCHEMA)])
        .create_store("Household", "Ana's phone", "Ana", |_| "Private".into(), &[], |_, _| Ok(()))
        .await
        .unwrap()
}

/// The titles of the notes a delivery lists, in its order.
fn titles(rows: &[NoteListRow]) -> Vec<&str> {
    rows.iter().map(|row| row.note.title()).collect()
}

/// Compiled writes and reads run on coven's handle: a write counts the rows
/// it changes, a RETURNING write gives its rows, a live query of a compiled
/// read follows both, and a read of a view of coven's runs on the writer.
#[tokio::test]
async fn compiled_statements_run_on_a_store() {
    let root = tempfile::tempdir().unwrap();
    let handle = store(root.path()).await;
    let mut list = notes(&handle);
    assert!(titles(&list.next_inline().await.unwrap().unwrap()).is_empty());

    let id = UuidText(uuid::Uuid::now_v7());
    assert_eq!(add_note(&handle, id, "Groceries".into()).await.unwrap(), 1);
    let shown = list.next_inline().await.unwrap().unwrap();
    assert_eq!(titles(&shown), ["Groceries"]);
    assert_eq!(shown[0].note.id(), id.0.to_string());

    assert_eq!(retitle(&handle, id, "Shopping".into()).await.unwrap(), ["Shopping"]);
    assert_eq!(titles(&list.next_inline().await.unwrap().unwrap()), ["Shopping"]);

    assert!(!sync_state(&handle).await.unwrap().is_empty());
    handle.close().await.unwrap();
}
