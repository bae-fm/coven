use super::*;
use crate::custody::{KeySession, MemberKeyCustody, StoreKeyCustody};
use std::sync::Arc;

thread_local! {
    pub(super) static DERIVATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
use crate::{MemberKeys, StoreKey, StoreKeyring};
use coven_foundation::id_source::{IdSource, KeyId};
use coven_foundation::{
    files::{StoreFile, StoreLayout},
    id_source::SequentialIds,
};
use uuid::Uuid;

#[test]
fn unavailable_derivation_memory_preserves_the_allocation_cause() {
    use std::error::Error;

    // Capacity overflow fails deterministically without exhausting the machine.
    let error = allocate_blocks(usize::MAX).unwrap_err();
    assert!(matches!(&error, KeyError::Unavailable(_)));
    assert!(error
        .source()
        .unwrap()
        .is::<std::collections::TryReserveError>());
}

#[test]
#[should_panic(expected = "custody Argon2id requires a 32-byte output")]
fn an_invalid_internal_derivation_request_panics() {
    let params = argon2::Params::new(8, 1, 1, Some(64)).unwrap();
    let _key = derive(&Passphrase::new("phrase".into()), &[0; SALT_LEN], params);
}

fn file() -> (tempfile::TempDir, AtomicFile, StoreId) {
    let temp = tempfile::tempdir().unwrap();
    let id = StoreId(Uuid::from_u128(1));
    let store = StoreLayout::new(temp.path().to_owned())
        .create_store_dir(id, "Household", &SequentialIds::new())
        .unwrap();
    (temp, store.owned_file(StoreFile::StoreKeys), id)
}

#[test]
fn wrong_passphrase_and_wrong_store_or_material_kind_fail_authentication() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let (_temp, file, id) = file();
    let writer =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("right".into()), file.clone(), id);
    writer.unlock().unwrap();
    writer
        .persist(&StoreKeyring::new(
            StoreKey::generate(KeyId(key_ids.new_id())).unwrap(),
        ))
        .unwrap();
    let wrong =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("wrong".into()), file.clone(), id);
    assert!(matches!(
        wrong.unlock(),
        Err(KeyError::PassphraseAuthentication)
    ));
    let foreign = PassphraseCustody::<StoreKeyring>::new(
        Passphrase::new("right".into()),
        file.clone(),
        StoreId(Uuid::from_u128(2)),
    );
    assert!(matches!(
        foreign.unlock(),
        Err(KeyError::PassphraseAuthentication)
    ));
    let swapped = PassphraseCustody::<MemberKeys>::new(Passphrase::new("right".into()), file, id);
    assert!(matches!(
        swapped.unlock(),
        Err(KeyError::PassphraseAuthentication)
    ));
}

#[test]
fn a_session_holds_its_material_and_saves_without_rereading_the_file() {
    let (_temp, file, id) = file();
    let custody = Arc::new(PassphraseCustody::<StoreKeyring>::new(
        Passphrase::new("phrase".into()),
        file.clone(),
        id,
    ));
    let session = KeySession::store(custody.clone()).unwrap();
    let first = StoreKeyring::new(StoreKey::generate(KeyId(Uuid::from_u128(1))).unwrap());
    session.persist(&first).unwrap();
    let header = file.read_optional().unwrap().unwrap()[..37].to_vec();
    // External changes cannot replace held keys. Saving uses the held sealing
    // capability, even when rereading the prior file would fail authentication.
    file.replace(b"damaged external replacement").unwrap();
    assert_eq!(
        session
            .read()
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        first.to_secret_bytes().as_bytes()
    );
    let mut replacement = first;
    replacement
        .insert_store_key(StoreKey::generate(KeyId(Uuid::from_u128(2))).unwrap())
        .unwrap();
    session.persist(&replacement).unwrap();
    assert_eq!(&file.read_optional().unwrap().unwrap()[..37], header);
    assert_eq!(DERIVATIONS.get(), 1);
    session.close();
    assert!(custody.protection.lock().unwrap().is_none());
    let reopened = KeySession::store(Arc::new(PassphraseCustody::new(
        Passphrase::new("phrase".into()),
        file,
        id,
    )))
    .unwrap();
    assert_eq!(
        reopened
            .read()
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        replacement.to_secret_bytes().as_bytes()
    );
    assert_eq!(DERIVATIONS.get(), 2);
}

#[test]
fn parameters_are_recorded_authenticated_and_bounded_before_derivation() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let (_temp, file, id) = file();
    let custody =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("phrase".into()), file.clone(), id);
    let keys = StoreKeyring::new(StoreKey::generate(KeyId(key_ids.new_id())).unwrap());
    custody.unlock().unwrap();
    custody.persist(&keys).unwrap();
    let open = || {
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("phrase".into()), file.clone(), id)
            .unlock()
    };
    let original = file.read_optional().unwrap().unwrap();
    assert_eq!(&original[..5], HEADER);
    assert_eq!(
        u32::from_le_bytes(original[9..13].try_into().unwrap()),
        WRITE_PARAMETERS[0]
    );
    assert_eq!(
        u32::from_le_bytes(original[13..17].try_into().unwrap()),
        WRITE_PARAMETERS[1]
    );
    assert_eq!(
        u32::from_le_bytes(original[17..21].try_into().unwrap()),
        WRITE_PARAMETERS[2]
    );
    for (offset, value) in [
        (9, 8u32),
        (9, u32::MAX),
        (13, 0),
        (13, u32::MAX),
        (17, 0),
        (17, u32::MAX),
    ] {
        let mut bytes = original.clone();
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        file.replace(&bytes).unwrap();
        assert!(matches!(open(), Err(KeyError::PassphraseParameters)));
    }
    // Valid but altered work parameters still fail the authentication check.
    let mut altered = original.clone();
    altered[13..17].copy_from_slice(&4u32.to_le_bytes());
    file.replace(&altered).unwrap();
    assert!(matches!(open(), Err(KeyError::PassphraseAuthentication)));
    for offset in [0, 5, 21, 37, original.len() - 1] {
        let mut altered = original.clone();
        altered[offset] ^= 1;
        file.replace(&altered).unwrap();
        assert!(open().is_err());
    }
    for length in [0, 4, 5, 20, 36, 37, 60, 76] {
        file.replace(&original[..length]).unwrap();
        assert!(open().is_err());
    }
}

#[test]
fn file_failures_are_not_absence_or_success() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let temp = tempfile::tempdir().unwrap();
    let store =
        StoreLayout::new(temp.path().join("absent")).store_dir(&StoreId(Uuid::from_u128(1)));
    let custody = PassphraseCustody::<StoreKeyring>::new(
        Passphrase::new("phrase".into()),
        store.owned_file(StoreFile::StoreKeys),
        store.id(),
    );
    custody.unlock().unwrap();
    assert!(matches!(
        custody.persist(&StoreKeyring::new(
            StoreKey::generate(KeyId(key_ids.new_id())).unwrap()
        )),
        Err(KeyError::File(_))
    ));
}

#[test]
fn saving_additions_reuses_the_unlocked_passphrase_key() {
    let ids = SequentialIds::new();
    let (_temp, file, id) = file();
    let custody =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("phrase".into()), file.clone(), id);
    assert!(custody.unlock().unwrap().is_none());
    let mut keys = StoreKeyring::new(StoreKey::generate(KeyId(ids.new_id())).unwrap());
    custody.persist(&keys).unwrap();
    let first = file.read_optional().unwrap().unwrap();
    keys.insert_store_key(StoreKey::generate(KeyId(ids.new_id())).unwrap())
        .unwrap();
    custody.persist(&keys).unwrap();
    let second = file.read_optional().unwrap().unwrap();
    assert_eq!(DERIVATIONS.get(), 1);
    assert_eq!(
        &first[..37],
        &second[..37],
        "saving must retain the unlocked salt and parameters"
    );
    assert_ne!(
        &first[37..61],
        &second[37..61],
        "each save needs a fresh nonce"
    );
}

#[test]
fn failed_save_and_forget_preserve_held_keys_and_reuse_the_sealing_capability() {
    let (temp, file, id) = file();
    let custody = Arc::new(PassphraseCustody::<StoreKeyring>::new(
        Passphrase::new("phrase".into()),
        file.clone(),
        id,
    ));
    let session = KeySession::store(custody.clone()).unwrap();
    let first = StoreKeyring::new(StoreKey::generate(KeyId(Uuid::from_u128(1))).unwrap());
    session.persist(&first).unwrap();
    file.remove().unwrap();
    let store_dir = temp.path().join("stores").join(id.to_string());
    let moved = temp.path().join("held-store");
    std::fs::rename(&store_dir, &moved).unwrap();
    std::fs::write(&store_dir, b"not a directory").unwrap();
    let second = StoreKeyring::new(StoreKey::generate(KeyId(Uuid::from_u128(2))).unwrap());
    assert!(matches!(session.persist(&second), Err(KeyError::File(_))));
    assert!(matches!(session.forget(), Err(KeyError::File(_))));
    assert_eq!(
        session
            .read()
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        first.to_secret_bytes().as_bytes()
    );
    std::fs::remove_file(&store_dir).unwrap();
    std::fs::rename(&moved, &store_dir).unwrap();
    session.persist(&second).unwrap();
    session.forget().unwrap();
    assert!(session.read().unwrap().is_none());
    assert!(file.read_optional().unwrap().is_none());
    session.persist(&first).unwrap();
    assert_eq!(DERIVATIONS.get(), 1);
    session.close();
    assert!(custody.protection.lock().unwrap().is_none());
    assert!(matches!(
        custody.persist(&first),
        Err(KeyError::StoreClosed)
    ));
}
