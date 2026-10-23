use super::*;
use crate::{pending::tests::Record, tests::TestStore, ProviderAccessChange};
use coven_foundation::id_source::DeviceId;

fn cleanup() -> (Vec<NewOperation>, ProviderChanges) {
    let operation = |kind: &str| NewOperation {
        kind: kind.into(),
        data: b"opaque sync data".to_vec(),
        started_by: "coven".into(),
        pauses_writes: false,
    };
    (
        vec![operation("invite")],
        ProviderChanges {
            accounts: vec![ProviderAccessChange {
                account: "carol@example.test".into(),
                intended: false,
                initially_granted: true,
                needs_invitation: false,
                owner: true,
                operation: operation("provider-access"),
            }],
            keys: vec![crate::AccessKeyDeletion {
                key: crate::AccessKeyToDelete {
                    access_key_id: "invite-key".into(),
                    member: None,
                },
                operation: operation("delete-access-key"),
            }],
        },
    )
}

#[tokio::test]
async fn reset_notice_and_carried_cleanup_commit_once_or_roll_back_together() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let reset = DeviceReset {
        old_device: DeviceId(1),
        new_device: DeviceId(2),
    };
    db.inspect_writer(|sql| {
        sql.batch(
            "CREATE TEMP TRIGGER fail_reset BEFORE INSERT ON _coven_device_reset
            BEGIN SELECT RAISE(ABORT,'injected reset notice failure'); END;",
        )
        .unwrap();
    });
    let (operations, provider) = cleanup();
    assert!(db
        .record_device_reset::<Record>(reset, operations, provider)
        .await
        .is_err());
    assert_eq!(db.device_reset().await.unwrap(), None);
    assert!(db.operations().await.unwrap().is_empty());
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    assert!(db
        .provider_access("carol@example.test".into())
        .await
        .unwrap()
        .is_none());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER fail_reset").unwrap());
    let (operations, provider) = cleanup();
    db.record_device_reset::<Record>(reset, operations, provider)
        .await
        .unwrap();
    assert_eq!(db.device_reset().await.unwrap(), Some(reset));
    let ids: Vec<_> = db
        .operations()
        .await
        .unwrap()
        .iter()
        .map(|row| row.id)
        .collect();
    assert_eq!(ids.len(), 3);
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let (operations, provider) = cleanup();
    db.record_device_reset::<Record>(reset, operations, provider)
        .await
        .unwrap();
    assert_eq!(
        db.operations()
            .await
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        ids
    );
    let invite = db.first_operation("invite").await.unwrap().unwrap().id;
    db.finish_operation(invite).await.unwrap();
    db.clear_access_key("invite-key".into()).await.unwrap();
    let (operations, provider) = cleanup();
    db.record_device_reset::<Record>(reset, operations, provider)
        .await
        .unwrap();
    assert!(db.first_operation("invite").await.unwrap().is_none());
    assert!(db
        .first_operation("delete-access-key")
        .await
        .unwrap()
        .is_none());
    db.close().await.unwrap();
}
