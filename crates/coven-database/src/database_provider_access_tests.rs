use crate::{pending::tests::Record, tests::TestStore, *};
use coven_crypto::SecretBytes;

fn change(intended: bool) -> ProviderAccessChange {
    ProviderAccessChange {
        account: "account@example.com".into(),
        intended,
        initially_granted: true,
        needs_invitation: false,
        operation: NewOperation {
            kind: "provider-access".into(),
            data: b"account@example.com".to_vec(),
            started_by: "coven".into(),
            pauses_writes: false,
        },
    }
}

#[tokio::test]
async fn confirmed_invitation_tokens_are_redacted() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut intended = change(true);
    intended.needs_invitation = true;
    db.start_operation_with_access::<Record>(
        NewOperation {
            kind: "invite".into(),
            data: vec![0],
            started_by: "test".into(),
            pauses_writes: false,
        },
        vec![intended],
    )
    .await
    .unwrap();
    let id = db
        .provider_access("account@example.com".into())
        .await
        .unwrap()
        .unwrap()
        .request
        .unwrap()
        .operation;
    db.start_provider_request::<Record>("account@example.com".into(), id).await.unwrap();
    let token = b"provider-share-token".to_vec();
    db.complete_provider_request::<Record>(
        "account@example.com".into(),
        id,
        true,
        Some(SecretBytes::new(token.clone())),
    )
    .await
    .unwrap();
    let state = db.provider_access("account@example.com".into()).await.unwrap().unwrap();
    let diagnostic = format!("{state:?}");
    assert!(
        !diagnostic.contains(&format!("{token:?}")),
        "provider invitation bytes must stay secret"
    );
    assert!(diagnostic.contains("REDACTED"));
}

#[tokio::test]
async fn operation_intention_journal_and_pending_roll_back_together() {
    for table in ["_coven_provider_access", "_coven_operations", "_coven_pending"] {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        db.inspect_writer(|sql|sql.batch(&format!("CREATE TEMP TRIGGER refuse_access BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'access failed'); END;")).unwrap());
        assert!(db
            .start_operation_with_access::<Record>(change(false).operation, vec![change(false)])
            .await
            .is_err());
        db.close().await.unwrap();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        assert!(db.provider_access("account@example.com".into()).await.unwrap().is_none());
        assert!(db.operations().await.unwrap().is_empty());
        assert!(db.pending::<Record>().await.unwrap().is_empty());
        db.start_operation_with_access::<Record>(change(false).operation, vec![change(false)])
            .await
            .unwrap();
        let state = db.provider_access("account@example.com".into()).await.unwrap().unwrap();
        assert!(!state.intended);
        assert_eq!(state.granted, Some(true));
        assert_eq!(db.pending::<Record>().await.unwrap().len(), 1);
        assert_eq!(db.operations().await.unwrap()[1].id, state.request.unwrap().operation);
    }
}

#[tokio::test]
async fn obsolete_completion_and_its_correction_are_one_commit() {
    for failed_table in ["_coven_provider_access", "_coven_operations", "_coven_pending"] {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let invite = db
            .start_operation_with_access::<Record>(
                NewOperation {
                    kind: "invite".into(),
                    data: vec![0],
                    started_by: "test".into(),
                    pauses_writes: false,
                },
                vec![change(false)],
            )
            .await
            .unwrap();
        let id = db
            .provider_access("account@example.com".into())
            .await
            .unwrap()
            .unwrap()
            .request
            .unwrap()
            .operation;
        assert!(!db
            .start_provider_request::<Record>("account@example.com".into(), id)
            .await
            .unwrap());
        assert!(db
            .start_provider_request::<Record>("account@example.com".into(), id)
            .await
            .is_err());
        db.advance_operation_with_access::<Record>(
            OperationUpdate { id: invite, previous: 0, last_step: 1, data: vec![1] },
            vec![change(true)],
            None,
        )
        .await
        .unwrap();
        let before = db.provider_access("account@example.com".into()).await.unwrap().unwrap();
        let pending = db.pending::<Record>().await.unwrap();
        let statement = if failed_table == "_coven_pending" { "INSERT" } else { "UPDATE" };
        db.inspect_writer(|sql|sql.batch(&format!("CREATE TEMP TRIGGER refuse_access BEFORE {statement} ON {failed_table} BEGIN SELECT RAISE(ABORT,'confirmation failed'); END;")).unwrap());
        assert!(db
            .complete_provider_request::<Record>("account@example.com".into(), id, false, None)
            .await
            .is_err());
        db.close().await.unwrap();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let after = db.provider_access("account@example.com".into()).await.unwrap().unwrap();
        assert_eq!(after.intended, before.intended);
        assert_eq!(after.granted, before.granted);
        assert_eq!(after.request, before.request);
        assert!(
            after.invitation.as_ref().map(SecretBytes::as_bytes)
                == before.invitation.as_ref().map(SecretBytes::as_bytes)
        );
        assert_eq!(db.pending::<Record>().await.unwrap(), pending);
        assert_eq!(db.operation(id).await.unwrap().unwrap().last_step, 1);
        assert!(!db
            .complete_provider_request::<Record>("account@example.com".into(), id, false, None)
            .await
            .unwrap());
        let state = db.provider_access("account@example.com".into()).await.unwrap().unwrap();
        assert_eq!(state.granted, Some(false));
        assert_eq!(state.request.unwrap(), ProviderAccessRequest { operation: id, target: None });
        assert_eq!(db.pending::<Record>().await.unwrap(), pending);
        assert!(db
            .start_provider_request::<Record>("account@example.com".into(), id)
            .await
            .unwrap());
        assert!(db
            .complete_provider_request::<Record>(
                "account@example.com".into(),
                id,
                true,
                Some(SecretBytes::new(vec![1]))
            )
            .await
            .unwrap());
        assert!(db.pending::<Record>().await.unwrap().is_empty());
        assert!(db.operation(id).await.unwrap().is_none());
    }
}

/// Ana's device sends the revocation of Dan's account, and the provider
/// reports a parent folder's grant left in place. When history still means
/// to take the account back, the reply is listed as the operation's failure.
/// When an invite to Dan applied while the revocation ran, the account is
/// intended shared again: the reply queues the grant on the same pending
/// subject, and nothing tells Ana to remove grants from an account her store
/// shares (§20.9).
#[tokio::test]
async fn grants_left_in_place_fail_the_revocation_only_while_it_is_intended() {
    for reshared in [false, true] {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let invite = db
            .start_operation_with_access::<Record>(
                NewOperation {
                    kind: "invite".into(),
                    data: vec![0],
                    started_by: "test".into(),
                    pauses_writes: false,
                },
                vec![change(false)],
            )
            .await
            .unwrap();
        let id = db
            .provider_access("account@example.com".into())
            .await
            .unwrap()
            .unwrap()
            .request
            .unwrap()
            .operation;
        assert!(!db
            .start_provider_request::<Record>("account@example.com".into(), id)
            .await
            .unwrap());
        if reshared {
            db.advance_operation_with_access::<Record>(
                OperationUpdate { id: invite, previous: 0, last_step: 1, data: vec![1] },
                vec![change(true)],
                None,
            )
            .await
            .unwrap();
        }
        let failure = crate::pending::tests::failed_operation_record(id);
        let recorded = db
            .fail_provider_request::<Record>(
                "account@example.com".into(),
                id,
                b"account@example.com".to_vec(),
                failure.clone(),
            )
            .await
            .unwrap();
        assert_eq!(recorded, !reshared);
        let state = db.provider_access("account@example.com".into()).await.unwrap().unwrap();
        assert_eq!(state.intended, reshared);
        assert_eq!(state.granted, None);
        let pending = db.pending::<Record>().await.unwrap();
        if reshared {
            assert_eq!(
                state.request.unwrap(),
                ProviderAccessRequest { operation: id, target: None }
            );
            assert_eq!(pending, vec![crate::pending::tests::operation_record(id)]);
            assert!(db
                .start_provider_request::<Record>("account@example.com".into(), id)
                .await
                .unwrap());
        } else {
            assert_eq!(
                state.request.unwrap(),
                ProviderAccessRequest { operation: id, target: Some(false) }
            );
            assert_eq!(pending, vec![failure]);
            assert_eq!(
                db.operation(id).await.unwrap().unwrap().last_step,
                crate::provider_access::REQUEST_FAILED_STEP
            );
        }
        db.close().await.unwrap();
    }
}
