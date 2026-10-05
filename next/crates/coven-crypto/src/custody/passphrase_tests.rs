use super::*;
use crate::custody::{MemberKeyCustody, StoreKeyCustody};
use crate::{MemberKeys, StoreKey, StoreKeyring};
use coven_foundation::{
    files::{StoreFile, StoreLayout},
    id_source::SequentialIds,
};
use std::num::NonZeroU64;
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
    let (_temp, file, id) = file();
    let writer =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("right".into()), file.clone(), id);
    writer
        .persist(&StoreKeyring::new(
            StoreKey::generate(NonZeroU64::MIN).unwrap(),
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
fn a_live_custody_reads_the_parameters_of_the_current_file() {
    let (_temp, file, id) = file();
    let original =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("phrase".into()), file.clone(), id);
    original
        .persist(&StoreKeyring::new(
            StoreKey::generate(NonZeroU64::MIN).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        original
            .unlock()
            .unwrap()
            .unwrap()
            .current_store_key()
            .number(),
        1
    );
    let reopened =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("phrase".into()), file, id);
    reopened
        .persist(&StoreKeyring::new(
            StoreKey::generate(NonZeroU64::new(2).unwrap()).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        original
            .unlock()
            .unwrap()
            .unwrap()
            .current_store_key()
            .number(),
        2
    );
}

#[test]
fn parameters_are_recorded_authenticated_and_bounded_before_derivation() {
    let (_temp, file, id) = file();
    let custody =
        PassphraseCustody::<StoreKeyring>::new(Passphrase::new("phrase".into()), file.clone(), id);
    let keys = StoreKeyring::new(StoreKey::generate(NonZeroU64::MIN).unwrap());
    custody.persist(&keys).unwrap();
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
        assert!(matches!(
            custody.unlock(),
            Err(KeyError::PassphraseParameters)
        ));
    }
    // Valid but altered work parameters still fail the authentication check.
    let mut altered = original.clone();
    altered[13..17].copy_from_slice(&4u32.to_le_bytes());
    file.replace(&altered).unwrap();
    assert!(matches!(
        custody.unlock(),
        Err(KeyError::PassphraseAuthentication)
    ));
    for offset in [0, 5, 21, 37, original.len() - 1] {
        let mut altered = original.clone();
        altered[offset] ^= 1;
        file.replace(&altered).unwrap();
        assert!(custody.unlock().is_err());
    }
    for length in [0, 4, 5, 20, 36, 37, 60, 76] {
        file.replace(&original[..length]).unwrap();
        assert!(custody.unlock().is_err());
    }
}

#[test]
fn file_failures_are_not_absence_or_success() {
    let temp = tempfile::tempdir().unwrap();
    let store =
        StoreLayout::new(temp.path().join("absent")).store_dir(&StoreId(Uuid::from_u128(1)));
    let custody = PassphraseCustody::<StoreKeyring>::new(
        Passphrase::new("phrase".into()),
        store.owned_file(StoreFile::StoreKeys),
        store.id(),
    );
    assert!(matches!(
        custody.persist(&StoreKeyring::new(
            StoreKey::generate(NonZeroU64::MIN).unwrap()
        )),
        Err(KeyError::File(_))
    ));
}
