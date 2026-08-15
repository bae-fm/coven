use std::sync::Arc;

use super::*;
use crate::custody::KeyCustody;
use crate::encryption::MasterKeyring;
use crate::keys::MasterKeyCustody;

fn custody(store_id: &str) -> Arc<dyn MasterKeyCustody> {
    crate::keys::test_keyring::install();
    let keys = crate::keys::StoreKeys::bind(store_id.to_string());
    KeyCustody::Keyring.resolve(
        &keys,
        &coven_foundation::store_dir::StoreDir::new_ephemeral("unused"),
    )
}

#[test]
fn proposed_master_key_remains_in_memory_until_commit() {
    let destination = custody("staged-master-commit");
    let proposed = MasterKeyring::generate();
    let fingerprint = proposed.fingerprint();
    let staged =
        StagedMasterKeyCustody::new(destination.clone(), proposed).expect("stage master key");

    assert_eq!(
        staged
            .unlock()
            .expect("unlock staged master key")
            .expect("staged key exists")
            .fingerprint(),
        fingerprint,
    );
    assert!(destination
        .unlock()
        .expect("unlock durable custody before commit")
        .is_none(),);

    staged.commit().expect("commit master key");
    assert_eq!(
        destination
            .unlock()
            .expect("unlock durable custody after commit")
            .expect("committed key exists")
            .fingerprint(),
        fingerprint,
    );
}

#[test]
fn rollback_forgets_a_committed_proposed_master_key() {
    let destination = custody("staged-master-rollback");
    let staged = StagedMasterKeyCustody::new(destination.clone(), MasterKeyring::generate())
        .expect("stage master key");
    staged.commit().expect("commit master key");

    staged.rollback().expect("roll back master key");

    assert!(destination
        .unlock()
        .expect("unlock durable custody after rollback")
        .is_none(),);
}
