use super::*;
use crate::custody::block_store_test_utils::MemoryBlockStore;
use uuid::Uuid;

fn store() -> BlockStore<MemoryBlockStore> {
    BlockStore::new(MemoryBlockStore::new())
}

#[test]
fn entries_are_named_under_the_service() {
    let block_store = store();
    block_store.write("bae", "restore-code:1", b"code").unwrap();
    assert_eq!(block_store.api().keys(), ["bae/restore-code:1"]);
    assert_eq!(block_store.read("bae", "restore-code:1").unwrap().unwrap().as_bytes(), b"code");
    assert!(block_store.read("other", "restore-code:1").unwrap().is_none());
}

#[test]
fn listing_keeps_only_this_services_restore_codes() {
    let block_store = store();
    let store_id = StoreId(Uuid::from_u128(7));
    let account = format!("restore-code:{store_id}");
    block_store.write("bae", &account, b"code").unwrap();
    block_store.write("bae", &format!("s3-credentials:{store_id}"), b"key").unwrap();
    block_store.write("other", &account, b"other code").unwrap();
    // An entry the app keeps for itself, under no service.
    block_store.api().store("com.example.token", b"token", true).unwrap();
    let codes = block_store.restore_codes("bae").unwrap();
    assert_eq!(codes.len(), 1);
    assert_eq!(codes[0].0, store_id);
    assert_eq!(codes[0].1.as_bytes(), b"code");
}

#[test]
fn a_restore_code_account_without_a_store_id_fails_the_list() {
    let block_store = store();
    block_store.write("bae", "restore-code:nope", b"code").unwrap();
    assert!(matches!(block_store.restore_codes("bae"), Err(KeyError::RestoreCodeStoreId)));
}

#[test]
fn deleting_an_absent_entry_succeeds() {
    let block_store = store();
    block_store.delete("bae", "restore-code:1").unwrap();
    block_store.write("bae", "restore-code:1", b"code").unwrap();
    block_store.delete("bae", "restore-code:1").unwrap();
    assert!(block_store.read("bae", "restore-code:1").unwrap().is_none());
}

/// The code holds the person's private keys, so it goes to Google's cloud
/// backup only when that copy is end-to-end encrypted, asked on every write.
#[test]
fn cloud_backup_follows_end_to_end_encryption_on_every_write() {
    let block_store = store();
    block_store.api().set_end_to_end_encryption(false);
    block_store.write("bae", "restore-code:1", b"code").unwrap();
    assert_eq!(block_store.api().backs_up("bae/restore-code:1"), Some(false));
    block_store.api().set_end_to_end_encryption(true);
    block_store.write("bae", "restore-code:1", b"code").unwrap();
    assert_eq!(block_store.api().backs_up("bae/restore-code:1"), Some(true));
}

#[test]
fn an_entry_past_the_size_limit_is_refused_and_changes_nothing() {
    let block_store = store();
    let key = "bae/restore-code:1";
    let fits = vec![1; ENTRY_LIMIT - key.len()];
    assert_eq!(BlockStore::<MemoryBlockStore>::value_limit("bae", "restore-code:1"), fits.len());
    block_store.write("bae", "restore-code:1", &fits).unwrap();
    assert!(matches!(
        block_store.write("bae", "restore-code:1", &[2; ENTRY_LIMIT]),
        Err(KeyError::SavedEntryTooLarge)
    ));
    assert_eq!(
        block_store.read("bae", "restore-code:1").unwrap().unwrap().as_bytes(),
        fits.as_slice()
    );
}

#[test]
fn a_seventeenth_entry_is_refused_but_rewriting_one_is_not() {
    let block_store = store();
    for n in 0..16 {
        block_store.write("bae", &format!("restore-code:{n}"), b"code").unwrap();
    }
    assert!(matches!(
        block_store.write("bae", "restore-code:16", b"code"),
        Err(KeyError::SavedEntriesFull)
    ));
    block_store.write("bae", "restore-code:3", b"newer").unwrap();
    assert!(block_store.read("bae", "restore-code:16").unwrap().is_none());
}

#[test]
fn other_failures_keep_the_java_exception() {
    let failure = refused(BlockStoreFailure {
        exception: "com.google.android.gms.common.api.ApiException".to_owned(),
        status: Some(17),
        message: Some("API_NOT_CONNECTED".to_owned()),
    });
    let KeyError::Keychain(native) = failure else {
        panic!("expected a keychain failure, got {failure:?}");
    };
    let cause = std::error::Error::source(&native)
        .and_then(std::error::Error::source)
        .expect("the Java exception is kept");
    assert_eq!(
        cause.to_string(),
        "Block Store call failed with com.google.android.gms.common.api.ApiException, \
         status 17: API_NOT_CONNECTED"
    );
    assert!(matches!(
        refused(BlockStoreFailure {
            exception: "com.google.android.gms.common.api.ApiException".to_owned(),
            status: Some(FEATURE_NOT_SUPPORTED),
            message: None,
        }),
        KeyError::Unsupported
    ));
}

#[test]
fn a_new_phone_receives_every_entry_and_a_cloud_restore_only_backed_up_ones() {
    let old = store();
    old.write("bae", "restore-code:1", b"backed up").unwrap();
    old.api().set_end_to_end_encryption(false);
    old.write("bae", "restore-code:2", b"local only").unwrap();
    old.api().back_up_to_cloud();

    let transferred = old.api().set_up_new_phone();
    assert_eq!(transferred.keys(), ["bae/restore-code:1", "bae/restore-code:2"]);
    let restored = old.api().restore_new_phone_from_cloud();
    assert_eq!(restored.keys(), ["bae/restore-code:1"]);
}
