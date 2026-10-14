use super::*;
use coven_sync::JoiningIdentity;

#[tokio::test]
async fn joining_refuses_an_invite_for_another_store_before_keeping_an_identity() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
        .await
        .unwrap();
    let directory = pending.directory();
    let mut code = coven_sync::read_invite_code(&invite.code).unwrap();
    code.store = StoreId(UuidIds.new_id());
    assert!(matches!(
        JoiningIdentity::prepare(&directory, &code, "New phone"),
        Err(SyncError::WrongStore { expected, actual })
            if expected == directory.id() && actual == code.store
    ));
    assert!(directory
        .owned_file(StoreFile::Bootstrap)
        .read_optional()
        .unwrap()
        .is_none());
    install.absent(owner.directory.id()).await;
    pending.cancel().unwrap();
    owner.close().await;
}

#[tokio::test]
async fn resuming_join_authenticates_the_initial_key_and_writer_cursor() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
        .await
        .unwrap();
    let directory = pending.directory();
    let code = coven_sync::read_invite_code(&invite.code).unwrap();
    let mut identity = JoiningIdentity::prepare(&directory, &code, "New phone").unwrap();
    let file = directory.owned_file(StoreFile::Bootstrap);
    identity.record_attempt(&file, &code).unwrap();
    let saved = file.read_optional().unwrap().unwrap();
    // A live directory clone keeps the bootstrap lease, and the join below
    // would wait for it. The file handle names only the path.
    drop(directory);
    drop(pending);
    let (_, cancel) = watch::channel(false);
    for field in ["initial_key", "inviting_writer", "key_copy_number"] {
        let mut changed = coven_sync::read_invite_code(&invite.code).unwrap();
        match field {
            "initial_key" => changed.initial_key = KeyId(UuidIds.new_id()),
            "inviting_writer" => changed.inviting_writer.0 ^= 1,
            "key_copy_number" => changed.key_copy_number += 1,
            _ => unreachable!(),
        }
        let result = join_with_invite(
            install.builder(&owner, owner.recipient()),
            &changed.to_text().unwrap(),
            "New phone",
            |_| {},
            &cancel,
        )
        .await;
        assert!(
            matches!(result, Err(BootstrapError::Sync(SyncError::Crypto(_)))),
            "{field}: {result:?}"
        );
        assert_eq!(file.read_optional().unwrap().unwrap(), saved);
        install.absent(owner.directory.id()).await;
    }
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
        .await
        .unwrap();
    let directory = pending.directory();
    let resumed = JoiningIdentity::prepare(&directory, &code, "New phone").unwrap();
    assert!(resumed.attempted());
    assert_eq!(
        resumed.identity().keys().public_keys(),
        identity.identity().keys().public_keys()
    );
    // The resumed identity keeps the id the invite fixed.
    assert_eq!(resumed.identity().member(), code.member);
    assert_eq!(file.read_optional().unwrap().unwrap(), saved);
    drop(directory);
    pending.cancel().unwrap();
    owner.close().await;
}
