//! An operation an app's write journals is listed in `coven_operations`
//! until a write acknowledges it, and its journal row ends only with its
//! outcome (§11.4).

use crate::tests::TestStore;
use crate::{Database, DbError, NewOperation};

fn operation() -> NewOperation {
    NewOperation {
        kind: "sequence".into(),
        data: b"{}".to_vec(),
        started_by: "operation".into(),
        pauses_writes: false,
    }
}

async fn listed(db: &Database) -> Vec<(i64, String, Option<Vec<u8>>)> {
    db.read(|sql| {
        Ok(sql.query(
            "SELECT operation,kind,outcome FROM coven_operations ORDER BY operation",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn an_app_operation_ends_with_its_outcome_and_stays_listed_until_acknowledged() {
    let store = TestStore::new();
    let db = store.builder(Vec::new(), Vec::new()).open().await.unwrap();
    let id = db
        .write(|sql| {
            sql.journal_step(|journal| journal.start_app_operation(&operation(), "RenameStore"))
        })
        .await
        .unwrap();
    assert_eq!(listed(&db).await, [(id.0, "RenameStore".to_owned(), None)]);
    // A running operation has no outcome to acknowledge.
    let running = db.write(move |sql| sql.acknowledge_operation(id)).await;
    assert!(matches!(running, Err(DbError::OperationRunning(found)) if found == id), "{running:?}");
    // Its journal row can't end without its outcome.
    assert!(db.finish_operation(id).await.is_err());
    assert!(db.operation(id).await.unwrap().is_some());
    db.finish_operation_with_outcome(id, crate::OperationOutcome::Satisfied).await.unwrap();
    assert!(db.operation(id).await.unwrap().is_none());
    assert_eq!(
        listed(&db).await,
        [(id.0, "RenameStore".to_owned(), Some(b"\"Satisfied\"".to_vec()))]
    );
    db.write(move |sql| sql.acknowledge_operation(id)).await.unwrap();
    assert!(listed(&db).await.is_empty());
    // Acknowledged once, it is gone; a write that goes on after the refusal
    // still fails.
    let again = db
        .write(move |sql| {
            let refused = sql.acknowledge_operation(id);
            assert!(matches!(refused, Err(DbError::OperationUnknown(found)) if found == id));
            Ok(())
        })
        .await;
    assert!(matches!(again, Err(DbError::StepFailed)), "{again:?}");
    db.close().await.unwrap();
}

/// An operation a write's step journals starts like any other: the
/// observation of started operations, which the handle's invite expiry
/// follows, sees it in that write's commit (§13.2).
#[tokio::test]
async fn an_operation_a_step_journals_is_seen_as_started() {
    let store = TestStore::new();
    let db = store.builder(Vec::new(), Vec::new()).open().await.unwrap();
    let mut started = db.started_operations();
    assert!(started.take_changed().unwrap(), "the first observation reports the start");
    assert!(!started.take_changed().unwrap());
    db.write(|sql| {
        sql.journal_step(|journal| journal.start_app_operation(&operation(), "RenameStore"))
    })
    .await
    .unwrap();
    assert!(started.take_changed().unwrap());
    db.close().await.unwrap();
}
