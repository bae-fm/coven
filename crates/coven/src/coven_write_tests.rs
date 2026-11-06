//! Coven's steps in the app's write (§4): a failed step fails the write
//! even when the app's closure goes on, and its error comes back as the
//! step's own.
use crate::tests::Network;
use crate::*;

/// How many `parents` rows the device holds.
async fn parents(handle: &CovenHandle) -> i64 {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query_row("SELECT count(*) FROM parents", [], |r| r.get(0))?)
        })
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
    let result: Result<_, CovenError> = ana
        .write(|sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES('p','first',1)", [])?;
            sql.retry_pending_operation(OperationId(-1))?;
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(CovenError::Sync(SyncError::NotPending(OperationId(-1))))));
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
            Ok::<_, CovenError>(
                sql.query(
                    "SELECT kind,outcome FROM coven_operations WHERE operation=?1",
                    [operation.0],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?
                .pop(),
            )
        })
        .await
        .unwrap()
}

/// Stop the device's sync, so no pass runs until it starts again.
async fn stopped(device: &crate::tests::Device) {
    device.stop_sync().await;
}

/// Ana renames Gifts with sync stopped. Her write journals the rename beside
/// her app's row, and `coven_operations` lists it running; once sync runs it
/// lands, and the write that marks her row done acknowledges it (§11.1,
/// §11.4).
#[tokio::test]
async fn an_admin_change_journals_beside_the_apps_row_and_lands_once_sync_runs() {
    let network = Network::new(1).await;
    let ana = &network.devices[0].handle;
    let gifts = crate::tests::circle(ana, "Gifts").await;
    stopped(&network.devices[0]).await;
    let rename = ana
        .write(move |sql| {
            let rename = sql.rename_circle(gifts, "Presents")?.expect("Gifts has another name");
            sql.execute(
                "INSERT INTO parents(id,label,value) VALUES('rename','running',?1)",
                [rename.0],
            )?;
            Ok::<_, CovenError>(rename)
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
        Ok::<_, CovenError>(())
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
    let result: Result<_, CovenError> = ben
        .write(|sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES('p','renaming',1)", [])?;
            sql.rename_store("Ben's")?;
            Ok(())
        })
        .await;
    assert!(matches!(
        result,
        Err(CovenError::Sync(SyncError::Rejected(OperationRejection::NotAllowed)))
    ));
    assert_eq!(parents(ben).await, 0);
    let operations: i64 = ben
        .read(|sql| {
            Ok::<_, CovenError>(sql.query_row(
                "SELECT count(*) FROM coven_operations",
                [],
                |r| r.get(0),
            )?)
        })
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
    let gifts = crate::tests::circle(ana, "Gifts").await;
    let bens = ben.members().await.unwrap().into_iter().find(|m| m.is_self).unwrap().id;
    crate::tests::landed(ana, ana.circles().add_member(gifts, &bens).await.unwrap()).await;
    network.sync(1).await;
    stopped(&network.devices[0]).await;
    let rename = ana.circles().rename(gifts, "Presents").await.unwrap();
    crate::tests::landed(ben, ben.circles().delete(gifts).await.unwrap()).await;
    let device = network.devices.remove(0);
    device.handle.close().await.unwrap();
    let ana = network.reopen(&device).open(device.store, |_| ()).await.unwrap();
    ana.start_sync().await.unwrap();
    let rejected =
        OperationOutcome::Rejected(OperationRejection::TargetGone(EntryTarget::Circle(gifts)));
    assert_eq!(crate::tests::outcome(&ana, rename).await, rejected);
    ana.close().await.unwrap();
    let ana = network.reopen(&device).open(device.store, |_| ()).await.unwrap();
    let rename = rename.unwrap();
    assert_eq!(listed(&ana, rename).await, Some((OperationKind::RenameCircle, Some(rejected))));
    ana.write(move |sql| Ok::<_, CovenError>(sql.acknowledge_operation(rename)?)).await.unwrap();
    assert_eq!(listed(&ana, rename).await, None);
    ana.close().await.unwrap();
    network.close().await;
}

/// Ana makes Gifts with sync stopped. Her write journals the circle and
/// returns its id, but a row into Gifts must wait: the write checks
/// membership against the slots her device has applied, and Gifts exists
/// once its slot applies (§15.5).
#[tokio::test]
async fn a_new_circle_takes_rows_once_its_slot_applies() {
    let network = Network::new(1).await;
    let ana = &network.devices[0].handle;
    stopped(&network.devices[0]).await;
    let note = |audience: String| {
        move |sql: CovenWrite<'_>| {
            sql.execute(
                "INSERT INTO circle_notes(id,value,audience) VALUES('00000000-0000-7000-8000-000000000100',1,?1)",
                [audience],
            )?;
            Ok(())
        }
    };
    let (gifts, created) = ana.circles().create("Gifts").await.unwrap();
    assert_eq!(listed(ana, created).await, Some((OperationKind::CreateCircle, None)));
    let early = ana.write(note(gifts.to_string())).await;
    assert!(
        matches!(early, Err(CovenError::Database(DbError::NotInCircle(circle))) if circle == gifts),
        "{early:?}"
    );
    ana.start_sync().await.unwrap();
    crate::tests::landed(ana, Some(created)).await;
    ana.write(note(gifts.to_string())).await.unwrap();
    assert_eq!(ana.circles().list().await.unwrap()[0].id, gifts);
    // A name that isn't one journals nothing.
    let unnamed = ana.circles().create("").await;
    assert!(matches!(unnamed, Err(SyncError::Format(_))), "{unnamed:?}");
    network.close().await;
}

/// Ana makes two invites with sync stopped, in a write beside a row of her
/// app's: both are listed at once, without codes, which a pass fixes once
/// storage is connected. She cancels one in a write of its own, and it
/// leaves her list. Once the other's lifetime has ended, cancelling it fails
/// her write, which keeps neither her row nor the cancel; coven writes that
/// invite's expiry itself (§13.2).
#[tokio::test]
async fn invites_are_made_and_ended_in_the_apps_writes_with_sync_stopped() {
    let network = Network::new(1).await;
    let ana = &network.devices[0].handle;
    stopped(&network.devices[0]).await;
    let invite = |key: &str| {
        (
            InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
            InviteAccess::S3AccessKey {
                access_key_id: key.into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            crate::InviteLifetime::Day,
        )
    };
    let (first, second) = (invite("first"), invite("second"));
    let made = ana
        .write(move |sql| {
            let first = sql.create_invite(first.0, first.1, first.2)?;
            let second = sql.create_invite(second.0, second.1, second.2)?;
            sql.execute("INSERT INTO parents(id,label,value) VALUES('invite','made',1)", [])?;
            Ok::<_, CovenError>((first, second))
        })
        .await
        .unwrap();
    let ((first, _, _), (second, _, second_operation)) = made;
    let mut invites = ana.test_invites();
    let listed = invites.next().await;
    assert_eq!(listed.iter().map(|invite| invite.id).collect::<Vec<_>>(), [first, second]);
    assert!(listed.iter().all(|invite| invite.stage == InviteStage::Granting));
    assert_eq!(listed[1].operation, second_operation);
    assert_eq!(parents(ana).await, 1);
    ana.write(move |sql| {
        sql.cancel_invite(&first)?;
        sql.execute("UPDATE parents SET label='cancelled' WHERE id='invite'", [])?;
        Ok::<_, CovenError>(())
    })
    .await
    .unwrap();
    assert_eq!(invites.next().await, listed[1..]);
    network.clock.set(listed[1].expires_at);
    let result: Result<_, CovenError> = ana
        .write(move |sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES('late','too late',2)", [])?;
            sql.cancel_invite(&second)?;
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(CovenError::Sync(SyncError::InvitationChanged))), "{result:?}");
    assert_eq!(parents(ana).await, 1);
    assert!(invites.next().await.is_empty());
    network.close().await;
}

/// Resetting a circle first catches up with the sequence, which needs
/// storage: with storage unreachable the reset fails and journals nothing
/// (§2.1, §11.1).
#[tokio::test]
async fn an_operation_that_catches_up_first_fails_without_storage_and_journals_nothing() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    let gifts = crate::tests::circle(&device.handle, "Gifts").await;
    let before = operations(&device.handle).await;

    device.storage.set_online(false);
    device.app.lose_network();
    let reset = device.handle.circles().reset(gifts).await;
    assert!(
        matches!(&reset, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network),
        "{reset:?}"
    );
    assert_eq!(operations(&device.handle).await, before);
    network.close().await;
}

/// The ids of the operations `coven_operations` lists.
async fn operations(handle: &CovenHandle) -> Vec<i64> {
    handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query(
                "SELECT operation FROM coven_operations ORDER BY operation",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}
