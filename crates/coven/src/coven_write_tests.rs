//! Coven's steps in the app's write (§4): a failed step fails the write
//! even when the app's closure goes on, and its error comes back as the
//! step's own.
use crate::tests::Network;
use crate::*;

/// How many `parents` rows the device holds.
async fn parents(handle: &CovenHandle) -> i64 {
    handle
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM parents", [], |r| r.get(0))?))
        .await
        .unwrap()
}

/// Ana's app retries an operation that has already finished, beside a row
/// of its own, and goes on when the retry is refused: the write still fails,
/// and her row is not kept.
#[tokio::test]
async fn a_failed_step_fails_the_write_even_when_the_app_goes_on() {
    let network = Network::new(1).await;
    let ana = &network.devices[0].handle;
    let result = ana
        .write(|sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES('p','first',1)", [])?;
            let refused = sql.retry_pending_operation(OperationId(-1));
            assert!(matches!(refused, Err(OperationError::NotPending(OperationId(-1)))));
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(CovenError::Database(DbError::StepFailed))), "{result:?}");
    assert_eq!(parents(ana).await, 0);
    network.close().await;
}

/// The refusal a closure passes on with `?` is the step's own error, as the
/// one-call shortcut returns it.
#[tokio::test]
async fn a_steps_refusal_comes_back_as_its_own_error() {
    let network = Network::new(1).await;
    let ana = &network.devices[0].handle;
    let result = ana
        .write(|sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES('p','first',1)", [])?;
            sql.retry_pending_operation(OperationId(-1))?;
            Ok(())
        })
        .await;
    assert!(matches!(SyncError::from(result.unwrap_err()), SyncError::NotPending(OperationId(-1))),);
    assert!(matches!(
        ana.retry_pending_operation(OperationId(-1)).await,
        Err(OperationError::NotPending(OperationId(-1)))
    ));
    assert_eq!(parents(ana).await, 0);
    network.close().await;
}
