use super::StoreCommitVerifier;
use coven_protocol::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipGrantId, MembershipHeadRef,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use coven_protocol::objects::{
    ProtocolObjectContext, ProtocolObjectDomain, StoreObjectError, VerifiedObject,
};
use coven_protocol::store_commit::{
    membership_entry_semantic_prefix, membership_resolution_semantic_prefix, StoreProtocolError,
};
use coven_storage::run_blocking_object_verification;

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
        let coord = &reference.coord;
        let semantic_prefix = membership_entry_semantic_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.entry_hash,
        );
        let context = ProtocolObjectContext::signed_plaintext(
            self.commit_verifier.store_root_hash(),
            ProtocolObjectDomain::StoreMembershipEntry,
        );
        let expected_coord = coord.clone();
        self.commit_verifier
            .load_exact_object(
                &context,
                &reference.object,
                &semantic_prefix,
                coord.entry_hash,
                move |bytes| {
                    let entry: MembershipEntry =
                        coven_protocol::objects::decode_protocol_object(bytes)?;
                    if entry.coord() != expected_coord
                        || !coven_protocol::membership::verify_membership_entry(&entry)
                    {
                        return Err(StoreProtocolError::Malformed(
                            "exact membership entry differs from its reference".to_string(),
                        ));
                    }
                    Ok(entry)
                },
            )
            .await
    }

    pub(crate) async fn load_resolution(
        &self,
        reference: &StoreMembershipConflictResolutionRef,
    ) -> Result<VerifiedObject<StoreMembershipConflictResolution>, StoreObjectError> {
        let semantic_prefix = membership_resolution_semantic_prefix(
            reference.conflict_hash,
            &reference.resolver_pubkey,
            reference.resolution_hash,
        );
        let context = ProtocolObjectContext::signed_plaintext(
            self.commit_verifier.store_root_hash(),
            ProtocolObjectDomain::StoreMembershipResolution,
        );
        let expected = reference.clone();
        let store_root_hash = self.commit_verifier.store_root_hash();
        self.commit_verifier
            .load_exact_object(
                &context,
                &reference.object,
                &semantic_prefix,
                reference.resolution_hash,
                move |bytes| {
                    let resolution: StoreMembershipConflictResolution =
                        coven_protocol::objects::decode_protocol_object(bytes)?;
                    if resolution.store_root_hash != store_root_hash
                        || resolution.conflict_hash != expected.conflict_hash
                        || resolution.resolver_pubkey != expected.resolver_pubkey
                        || resolution.resolution_hash() != expected.resolution_hash
                        || !resolution.verify_signature()
                    {
                        return Err(StoreProtocolError::Malformed(
                            "exact membership resolution differs from its reference".to_string(),
                        ));
                    }
                    Ok(resolution)
                },
            )
            .await
    }

    pub(crate) async fn load_head_for_registration(
        &self,
        reference: &MembershipHeadRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
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
                    "membership head slot has no .json suffix".to_string(),
                )),
            })?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.commit_verifier.store_root_hash(),
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let expected_coord = reference.coord.clone();
        let expected_head_hash = reference.head_hash;
        let expected_registration = registration.clone();
        self.commit_verifier
            .load_exact_object(
                &context,
                &reference.object,
                semantic_prefix,
                reference.head_hash,
                move |bytes| {
                    let head: AuthorHead = coven_protocol::objects::decode_protocol_object(bytes)?;
                    coven_protocol::objects::verify_membership_head_reference(
                        &head,
                        &expected_coord,
                        expected_head_hash,
                        &expected_registration,
                    )?;
                    Ok(head)
                },
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
            .read_protocol_object(
                &ProtocolObjectContext::signed_plaintext(
                    self.commit_verifier.store_root_hash(),
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
            Box::new(move || coven_protocol::objects::decode_protocol_object(&parse_bytes)),
        )
        .await?;
        let registration = self
            .commit_verifier
            .load_registration(&head.body.author_registration)
            .await?;
        coven_protocol::objects::verify_membership_head_reference(
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
        slot: &coven_protocol::objects::ObjectSlot,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
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
            self.commit_verifier.store_root_hash(),
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let (bytes, object) = self
            .commit_verifier
            .read_protocol_slot(&context, slot, semantic_prefix)
            .await?;
        let parse_bytes = bytes.clone();
        let head: AuthorHead = run_blocking_object_verification(
            semantic_prefix,
            &object,
            Box::new(move || coven_protocol::objects::decode_protocol_object(&parse_bytes)),
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
        coven_protocol::objects::verify_membership_head_reference(
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
