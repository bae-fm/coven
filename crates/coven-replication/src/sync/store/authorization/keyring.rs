use coven_keys::encryption::EncryptionService;
use coven_keys::keys::{IdentityKeyAuthority, UserKeypair};
use coven_protocol::membership::{MembershipChain, MembershipGrantId};

use crate::sync::store::membership::MembershipMutationError;

/// Open every sealed Store keyring the verified membership activates for
/// `identity`, merged into one keyring.
pub fn open_store_keyring(
    identity: &dyn IdentityKeyAuthority,
    membership: &MembershipChain,
) -> Result<EncryptionService, MembershipMutationError> {
    let recipient = hex::encode(identity.public_key());
    let activated = membership.sealed_key_authority_for(&recipient)?;
    let mut merged: Option<EncryptionService> = None;
    for sealed in &activated {
        let keyring = sealed
            .key
            .open(identity, sealed.generation)
            .map_err(MembershipMutationError::SealedKey)?;
        merged = Some(match merged {
            Some(existing) => existing
                .merged_with(&keyring)
                .map_err(MembershipMutationError::Encryption)?,
            None => keyring,
        });
    }
    merged.ok_or_else(|| {
        MembershipMutationError::Bucket(coven_protocol::objects::StorageError::NotFound(format!(
            "no activated sealed Store key for {recipient}"
        )))
    })
}

/// Cold join: the admission names the grant it was issued under; that grant
/// must be active for this identity in the verified chain.
pub fn open_granted_store_keyring(
    identity: &UserKeypair,
    membership: &MembershipChain,
    grant: &MembershipGrantId,
) -> Result<EncryptionService, MembershipMutationError> {
    if !membership
        .active_grant_ids(&coven_keys::keys::public_key_hex(identity))
        .contains(grant)
    {
        return Err(MembershipMutationError::Crypto(
            "admission grant is not active for this member in the verified membership".to_string(),
        ));
    }
    open_store_keyring(identity, membership)
}

/// The activated keyring, or `initial` when membership activates none — the
/// founding Owner still holds the key its Store was created with.
pub(crate) fn open_store_keyring_or(
    identity: &dyn IdentityKeyAuthority,
    membership: &MembershipChain,
    initial: &EncryptionService,
) -> Result<EncryptionService, MembershipMutationError> {
    let recipient = hex::encode(identity.public_key());
    if membership.sealed_key_authority_for(&recipient)?.is_empty() {
        return Ok(initial.clone());
    }
    open_store_keyring(identity, membership)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_helpers::{
        open_test_db, pubkey_hex, test_cloud_home, test_store_dir, TestCustody,
    };
    use coven_protocol::membership::MemberRole;

    #[tokio::test]
    async fn a_member_opens_every_activated_sealed_key() {
        let db_store_dir = test_store_dir();
        let db = open_test_db(db_store_dir.clone());
        let owner = UserKeypair::generate();
        let retained = UserKeypair::generate();
        let removed = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            db_store_dir.clone(),
            "activated-sealed-keys",
            owner.clone(),
            test_cloud_home(),
        )
        .await
        .expect("create the Store that seals its keyring");
        let encryption = EncryptionService::from_key([42; 32]);
        for member in [&retained, &removed] {
            store
                .admit_member(
                    &db,
                    db_store_dir.clone(),
                    &owner,
                    &pubkey_hex(member),
                    None,
                    MemberRole::Member,
                    &encryption,
                    "Activated sealed keys",
                )
                .await
                .expect("admit a member at the current generation");
        }
        store
            .remove_member(
                &db,
                db_store_dir.clone(),
                &owner,
                &pubkey_hex(&removed),
                &encryption,
                &TestCustody::default(),
            )
            .await
            .expect("remove a member, rotating the Store keyring");
        let membership = store
            .bind_device_in(&db, db_store_dir.clone(), &owner)
            .await
            .expect("bind the Store")
            .membership_for_test()
            .await
            .expect("read the rotated membership");

        let keyring = open_store_keyring(&retained, &membership)
            .expect("the retained member opens its grant and the rotation");
        assert_eq!(keyring.current_generation(), 2);
        assert_eq!(keyring.key_count(), 2);
        assert!(open_store_keyring(&removed, &membership).is_err());
    }

    #[tokio::test]
    async fn an_admission_for_an_inactive_grant_is_refused() {
        let db_store_dir = test_store_dir();
        let db = open_test_db(db_store_dir.clone());
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            db_store_dir.clone(),
            "inactive-admission-grant",
            owner.clone(),
            test_cloud_home(),
        )
        .await
        .expect("create the Store that issues the admission");
        let encryption = EncryptionService::from_key([7; 32]);
        let admission = store
            .admit_member(
                &db,
                db_store_dir.clone(),
                &owner,
                &pubkey_hex(&member),
                None,
                MemberRole::Member,
                &encryption,
                "Inactive admission grant",
            )
            .await
            .expect("admit the member the removal then revokes");
        store
            .remove_member(
                &db,
                db_store_dir.clone(),
                &owner,
                &pubkey_hex(&member),
                &encryption,
                &TestCustody::default(),
            )
            .await
            .expect("remove the admitted member");
        let membership = store
            .bind_device_in(&db, db_store_dir.clone(), &owner)
            .await
            .expect("bind the Store")
            .membership_for_test()
            .await
            .expect("read the membership that removed the grant");

        assert!(matches!(
            open_granted_store_keyring(&member, &membership, &admission.grant_id),
            Err(MembershipMutationError::Crypto(_)),
        ));
    }
}
