use super::*;
use crate::{CircleId, CircleKey, MemberIdentity, StoreKey};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::id_source::{IdSource, KeyId, MemberId};
use coven_foundation::{
    files::{StoreDir, StoreDirectories, StoreFile},
    id_source::{SequentialIds, StoreId},
};
use uuid::Uuid;

fn directory() -> (tempfile::TempDir, StoreDir) {
    let temp = tempfile::tempdir().unwrap();
    let store = StoreDirectories::new(temp.path().to_owned())
        .create_store_dir(StoreId(Uuid::from_u128(1)), &SequentialIds::new())
        .unwrap();
    (temp, store)
}

fn exercise_store_custody(custody: &KeySession<StoreKeyring>) {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let mut keys = StoreKeyring::new(StoreKey::generate(KeyId(key_ids.new_id())).unwrap());
    keys.insert_store_key(StoreKey::generate(KeyId(key_ids.new_id())).unwrap())
        .unwrap();
    keys.insert_circle_key(
        CircleKey::generate(CircleId(Uuid::from_u128(4)), KeyId(key_ids.new_id())).unwrap(),
    )
    .unwrap();
    custody.persist(&keys).unwrap();
    let unlocked = custody.read().unwrap().unwrap();
    assert_eq!(
        unlocked.to_secret_bytes().as_bytes(),
        keys.to_secret_bytes().as_bytes()
    );
    // Reads return independently owned keys and leave the shared session available.
    drop(unlocked);
    assert!(custody.read().unwrap().is_some());
    custody.forget().unwrap();
    custody.forget().unwrap();
    assert!(custody.read().unwrap().is_none());
}

fn identity() -> MemberIdentity {
    MemberIdentity::generate(MemberId(Uuid::from_u128(0x77))).unwrap()
}

fn exercise_member_custody(custody: &KeySession<MemberIdentity>) {
    let identity = identity();
    custody.persist(&identity).unwrap();
    let unlocked = custody.read().unwrap().unwrap();
    assert_eq!(unlocked.member(), identity.member());
    assert_eq!(unlocked.keys().public_keys(), identity.keys().public_keys());
    identity
        .keys()
        .public_keys()
        .signing
        .verify(
            b"custody round-trip",
            &unlocked.keys().sign(b"custody round-trip"),
        )
        .unwrap();
    custody.forget().unwrap();
    custody.forget().unwrap();
    assert!(custody.read().unwrap().is_none());
}

#[test]
fn in_memory_custody_keeps_both_material_kinds_for_one_session() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let store = InMemoryCustody::new(StoreKeyring::new(
        StoreKey::generate(KeyId(key_ids.new_id())).unwrap(),
    ));
    let member = InMemoryCustody::new(identity());
    assert!(store.unlock().unwrap().is_some());
    assert!(member.unlock().unwrap().is_some());
    exercise_store_custody(&KeySession::store(Arc::new(store)).unwrap());
    exercise_member_custody(&KeySession::member(Arc::new(member)).unwrap());
}

#[test]
fn passphrase_files_round_trip_both_material_kinds() {
    let (_temp, directory) = directory();
    let open_store = || {
        KeySession::store(Arc::new(PassphraseCustody::new(
            Passphrase::new("store passphrase".into()),
            directory.owned_file(StoreFile::StoreKeys),
            directory.id(),
        )))
        .unwrap()
    };
    let open_member = || {
        KeySession::member(Arc::new(PassphraseCustody::new(
            Passphrase::new("member passphrase".into()),
            directory.owned_file(StoreFile::MemberKeys),
            directory.id(),
        )))
        .unwrap()
    };
    let store = open_store();
    let member = open_member();
    assert!(store.read().unwrap().is_none());
    assert!(member.read().unwrap().is_none());
    let keys = StoreKeyring::new(StoreKey::generate(KeyId(Uuid::from_u128(3))).unwrap());
    let identity = identity();
    store.persist(&keys).unwrap();
    member.persist(&identity).unwrap();
    store.close();
    member.close();
    let store = open_store();
    let member = open_member();
    assert_eq!(
        store.read().unwrap().unwrap().to_secret_bytes().as_bytes(),
        keys.to_secret_bytes().as_bytes()
    );
    let reopened = member.read().unwrap().unwrap();
    assert_eq!(reopened.member(), identity.member());
    assert_eq!(reopened.keys().public_keys(), identity.keys().public_keys());
    identity
        .keys()
        .public_keys()
        .signing
        .verify(b"reopened", &reopened.keys().sign(b"reopened"))
        .unwrap();
    exercise_store_custody(&store);
    exercise_member_custody(&member);
    assert!(directory
        .owned_file(StoreFile::StoreKeys)
        .read_optional()
        .unwrap()
        .is_none());
    assert!(directory
        .owned_file(StoreFile::MemberKeys)
        .read_optional()
        .unwrap()
        .is_none());
}

#[test]
fn keyring_custody_uses_the_fake_and_isolates_stores_and_material_kinds() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let keychain = Keychain::in_memory("custody-tests").unwrap();
    let first = Arc::new(StoreKeychain::new(
        keychain.clone(),
        StoreId(Uuid::from_u128(1)),
    ));
    let second = Arc::new(StoreKeychain::new(keychain, StoreId(Uuid::from_u128(2))));
    let store = KeyringCustody::<StoreKeyring>::new(first.clone());
    let member = KeyringCustody::<MemberIdentity>::new(first);
    assert!(store.unlock().unwrap().is_none());
    assert!(member.unlock().unwrap().is_none());
    let keys = StoreKeyring::new(StoreKey::generate(KeyId(key_ids.new_id())).unwrap());
    store.persist(&keys).unwrap();
    assert!(member.unlock().unwrap().is_none());
    assert!(KeyringCustody::<StoreKeyring>::new(second.clone())
        .unlock()
        .unwrap()
        .is_none());
    member.persist(&identity()).unwrap();
    assert!(KeyringCustody::<MemberIdentity>::new(second)
        .unlock()
        .unwrap()
        .is_none());
    exercise_store_custody(&KeySession::store(Arc::new(store)).unwrap());
    exercise_member_custody(&KeySession::member(Arc::new(member)).unwrap());
}

#[test]
fn keychain_failure_preserves_material_and_reaches_the_caller() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let fake = Keychain::in_memory("failure").unwrap();
    let store = Arc::new(StoreKeychain::new(
        fake.clone(),
        StoreId(Uuid::from_u128(1)),
    ));
    let custody = KeyringCustody::<StoreKeyring>::new(store);
    let original = StoreKeyring::new(StoreKey::generate(KeyId(key_ids.new_id())).unwrap());
    custody.persist(&original).unwrap();
    fake.fail_next_operation();
    assert!(matches!(
        custody.forget(),
        Err(KeyError::KeychainTemporarilyUnavailable(_))
    ));
    assert_eq!(
        custody
            .unlock()
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        original.to_secret_bytes().as_bytes()
    );
    fake.fail_next_operation();
    assert!(matches!(
        custody.persist(&StoreKeyring::new(
            StoreKey::generate(KeyId(key_ids.new_id())).unwrap()
        )),
        Err(KeyError::KeychainTemporarilyUnavailable(_))
    ));
    assert_eq!(
        custody
            .unlock()
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        original.to_secret_bytes().as_bytes()
    );
    fake.fail_next_operation();
    assert!(matches!(
        custody.unlock(),
        Err(KeyError::KeychainTemporarilyUnavailable(_))
    ));
}

#[test]
fn custody_debug_never_prints_secrets() {
    let phrase = || Passphrase::new("passphrase-must-not-print".into());
    let keys = || {
        StoreKeyring::new(StoreKey::from_bytes(
            KeyId(uuid::Uuid::from_bytes([1; 16])),
            [88; 32],
        ))
    };
    let member = identity;
    let (_temp, directory) = directory();
    let fake = Keychain::in_memory("debug").unwrap();
    let keychain = Arc::new(StoreKeychain::new(fake.clone(), directory.id()));
    keychain
        .set_host_secret("api-token", "host-secret-must-not-print")
        .unwrap();
    let outputs = [
        format!("{:?}", phrase()),
        format!("{:?}", KeyCustody::Passphrase(phrase())),
        format!("{:?}", IdentityCustody::Passphrase(phrase())),
        format!("{:?}", KeyCustody::InMemory),
        format!("{:?}", IdentityCustody::InMemory),
        format!("{:?}", InMemoryCustody::new(keys())),
        format!(
            "{:?}",
            PassphraseCustody::<StoreKeyring>::new(
                phrase(),
                directory.owned_file(StoreFile::StoreKeys),
                directory.id()
            )
        ),
        format!("{fake:?}"),
        format!("{keychain:?}"),
        format!("{:?}", KeyringCustody::<MemberIdentity>::new(keychain)),
        format!(
            "{:?}",
            KeyCustody::Custom(Arc::new(InMemoryCustody::new(keys())))
        ),
        format!(
            "{:?}",
            IdentityCustody::Custom(Arc::new(InMemoryCustody::new(member())))
        ),
    ];
    for output in outputs {
        assert!(!output.contains("must-not-print"));
        assert!(!output.contains(&hex::encode([88; 32])));
        assert!(!output.contains(&format!("{:?}", [88; 32])));
    }
}
