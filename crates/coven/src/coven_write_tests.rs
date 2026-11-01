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

/// The operation `coven_operations` lists, with its kind and outcome.
async fn listed(
    handle: &CovenHandle,
    operation: OperationId,
) -> Option<(OperationKind, Option<OperationOutcome>)> {
    handle
        .read(move |sql| {
            Ok(sql
                .query(
                    "SELECT kind,outcome FROM coven_operations WHERE operation=?1",
                    [operation.0],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?
                .pop())
        })
        .await
        .unwrap()
}

async fn stopped(handle: &CovenHandle) {
    handle.stop_sync().await.unwrap();
    handle.test_sync_status().wait_for(|s| matches!(s, SyncStatus::Stopped)).await;
}

/// Ana renames Gifts with sync stopped. Her write journals the rename beside
/// her app's row, and `coven_operations` lists it running; once sync runs it
/// lands, and the write that marks her row done acknowledges it (§11.1,
/// §11.4).
#[tokio::test]
async fn an_admin_change_journals_beside_the_apps_row_and_lands_once_sync_runs() {
    let network = Network::new(1).await;
    let ana = &network.devices[0].handle;
    let gifts = ana.circles().create("Gifts").await.unwrap();
    stopped(ana).await;
    let rename = ana
        .write(move |sql| {
            let rename = sql.rename_circle(gifts, "Presents")?.expect("Gifts has another name");
            sql.execute(
                "INSERT INTO parents(id,label,value) VALUES('rename','running',?1)",
                [rename.0],
            )?;
            Ok(rename)
        })
        .await
        .unwrap();
    assert_eq!(listed(ana, rename).await, Some((OperationKind::RenameCircle, None)));
    assert_eq!(ana.circles().list().await.unwrap()[0].name, "Gifts");
    // It has no outcome to acknowledge yet.
    let running = ana.write(move |sql| Ok(sql.acknowledge_operation(rename)?)).await;
    assert!(
        matches!(running, Err(CovenError::Database(DbError::OperationRunning(found))) if found == rename),
        "{running:?}"
    );
    ana.start_sync().await.unwrap();
    let slot = crate::tests::landed(ana, Some(rename)).await;
    assert_eq!(ana.circles().list().await.unwrap()[0].name, "Presents");
    assert_eq!(
        listed(ana, rename).await,
        Some((OperationKind::RenameCircle, Some(OperationOutcome::Landed(slot))))
    );
    ana.write(move |sql| {
        sql.acknowledge_operation(rename)?;
        sql.execute("UPDATE parents SET label='done' WHERE id='rename'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(listed(ana, rename).await, None);
    network.close().await;
}

/// Ben, a member, can't rename the store: the state his device has applied
/// refuses it, so his write fails with the rejection and keeps neither his
/// row nor an operation.
#[tokio::test]
async fn a_change_the_applied_state_refuses_fails_the_write() {
    let network = Network::new(2).await;
    let ben = &network.devices[1].handle;
    let result = ben
        .write(|sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES('p','renaming',1)", [])?;
            sql.rename_store("Ben's")?;
            Ok(())
        })
        .await;
    assert!(matches!(
        SyncError::from(result.unwrap_err()),
        SyncError::Rejected(OperationRejection::NotAllowed)
    ));
    assert_eq!(parents(ben).await, 0);
    let operations: i64 = ben
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM coven_operations", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(operations, 0);
    network.close().await;
}

/// Ana journals a rename of Gifts while her sync is stopped, and Ben deletes
/// Gifts meanwhile. The pass that catches Ana up rejects the rename, and the
/// rejection is listed, across reopening, until her app acknowledges it
/// (§11.2, §11.4).
#[tokio::test]
async fn a_rejection_met_in_a_pass_stays_listed_until_acknowledged() {
    let mut network = Network::new(2).await;
    let (ana, ben) = (&network.devices[0].handle, &network.devices[1].handle);
    let gifts = ana.circles().create("Gifts").await.unwrap();
    let bens = ben.members().await.unwrap().into_iter().find(|m| m.is_self).unwrap().id;
    crate::tests::landed(ana, ana.circles().add_member(gifts, &bens).await.unwrap()).await;
    network.sync(1).await;
    stopped(ana).await;
    let rename = ana.circles().rename(gifts, "Presents").await.unwrap();
    crate::tests::landed(ben, ben.circles().delete(gifts).await.unwrap()).await;
    let device = network.devices.remove(0);
    device.handle.close().await.unwrap();
    let ana = network.reopen(&device).open(device.store, |_| ()).await.unwrap();
    ana.start_sync().await.unwrap();
    let rejected = OperationOutcome::Rejected(OperationRejection::NotAllowed);
    assert_eq!(crate::tests::outcome(&ana, rename).await, rejected);
    ana.close().await.unwrap();
    let ana = network.reopen(&device).open(device.store, |_| ()).await.unwrap();
    let rename = rename.unwrap();
    assert_eq!(listed(&ana, rename).await, Some((OperationKind::RenameCircle, Some(rejected))));
    ana.write(move |sql| Ok(sql.acknowledge_operation(rename)?)).await.unwrap();
    assert_eq!(listed(&ana, rename).await, None);
    ana.close().await.unwrap();
    network.close().await;
}
