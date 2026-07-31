use super::StoreCommitVerifier;
use crate::protocol::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipGrantId, MembershipHeadRef,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use crate::protocol::store_commit::StoreProtocolError;
use crate::storage::{
    run_blocking_object_verification, ProtocolObjectContext, ProtocolObjectDomain,
    StoreObjectError, VerifiedObject,
};

pub(crate) struct StoreMembershipObjectVerifier<'operation, 'storage> {
    commit_verifier: &'operation StoreCommitVerifier<'storage>,
}

impl<'operation, 'storage> StoreMembershipObjectVerifier<'operation, 'storage> {
    pub(super) fn new(commit_verifier: &'operation StoreCommitVerifier<'storage>) -> Self {
        Self { commit_verifier }
    }

    pub(crate) async fn load_entry(
        &self,
        reference: &MembershipEntryRef,
    ) -> Result<VerifiedObject<MembershipEntry>, StoreObjectError> {
        crate::storage::load_membership_entry_ref(
            self.commit_verifier.storage,
            self.commit_verifier.root.reference().store_root_hash,
            reference,
        )
        .await
    }

    pub(crate) async fn load_resolution(
        &self,
        reference: &StoreMembershipConflictResolutionRef,
    ) -> Result<VerifiedObject<StoreMembershipConflictResolution>, StoreObjectError> {
        crate::storage::load_membership_resolution_ref(
            self.commit_verifier.storage,
            self.commit_verifier.root.reference().store_root_hash,
            reference,
        )
        .await
    }

    pub(crate) async fn load_head(
        &self,
        reference: &MembershipHeadRef,
    ) -> Result<VerifiedObject<AuthorHead>, StoreObjectError> {
        let semantic_prefix = reference
            .object
            .slot()
            .logical_key()
            .strip_suffix(".json")
            .ok_or_else(|| StoreObjectError::InvalidObject {
                semantic_prefix: reference.object.slot().logical_key().to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "membership head exact slot has no .json suffix".to_string(),
                )),
            })?;
        let bytes = self
            .commit_verifier
            .storage
            .read_protocol_object(
                &ProtocolObjectContext::signed_plaintext(
                    self.commit_verifier.root.reference().store_root_hash,
                    ProtocolObjectDomain::StoreMembershipHead,
                ),
                &reference.object,
                semantic_prefix,
            )
            .await?;
        let parse_bytes = bytes.clone();
        let head: AuthorHead = run_blocking_object_verification(
            semantic_prefix,
            &reference.object,
            Box::new(move || crate::storage::decode_protocol_object(&parse_bytes)),
        )
        .await?;
        let registration = self
            .commit_verifier
            .load_registration(&head.body.author_registration)
            .await?;
        crate::storage::verify_membership_head_reference(
            &head,
            &reference.coord,
            reference.head_hash,
            &registration.value,
        )
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix: semantic_prefix.to_string(),
            key: reference.object.slot().logical_key().to_string(),
            source: Box::new(source),
        })?;
        Ok(VerifiedObject {
            value: head,
            bytes,
            semantic_hash: reference.head_hash,
            object: reference.object.clone(),
        })
    }

    pub(crate) async fn load_head_at_slot(
        &self,
        slot: &crate::storage::cloud::ObjectSlot,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: crate::protocol::membership::AuthorStreamId,
        sequence: u64,
    ) -> Result<VerifiedObject<AuthorHead>, StoreObjectError> {
        let semantic_prefix = slot.logical_key().strip_suffix(".json").ok_or_else(|| {
            StoreObjectError::InvalidObject {
                semantic_prefix: slot.logical_key().to_string(),
                key: slot.logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "membership head exact slot has no .json suffix".to_string(),
                )),
            }
        })?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.commit_verifier.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let (bytes, object) = self
            .commit_verifier
            .storage
            .read_protocol_slot(&context, slot, semantic_prefix)
            .await?;
        let parse_bytes = bytes.clone();
        let head: AuthorHead = run_blocking_object_verification(
            semantic_prefix,
            &object,
            Box::new(move || crate::storage::decode_protocol_object(&parse_bytes)),
        )
        .await?;
        let coord = head.entry_coord();
        if coord.author_pubkey != author
            || coord.author_owner_grant != *grant
            || coord.stream_id != stream_id
            || coord.seq != sequence
        {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.to_string(),
                key: object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(format!(
                    "membership head at sequence {sequence} selects coordinate {coord:?}"
                ))),
            });
        }
        let registration = self
            .commit_verifier
            .load_registration(&head.body.author_registration)
            .await?;
        let head_hash = head.head_hash();
        crate::storage::verify_membership_head_reference(
            &head,
            &coord,
            head_hash,
            &registration.value,
        )
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix: semantic_prefix.to_string(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(source),
        })?;
        if serde_json::to_vec(&head).map_err(|error| StoreObjectError::InvalidObject {
            semantic_prefix: semantic_prefix.to_string(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(error.to_string())),
        })? != bytes
        {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.to_string(),
                key: object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "membership head bytes are not canonical".to_string(),
                )),
            });
        }
        Ok(VerifiedObject {
            semantic_hash: head_hash,
            value: head,
            bytes,
            object,
        })
    }
}
