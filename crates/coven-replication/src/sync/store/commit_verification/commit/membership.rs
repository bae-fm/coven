use super::StoreCommitVerifier;
use coven_protocol::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipGrantId, MembershipHeadRef,
};
use coven_protocol::objects::{
    ProtocolObjectContext, ProtocolObjectDomain, StoreObjectError, VerifiedObject,
};
use coven_protocol::store_commit::{membership_entry_semantic_prefix, StoreProtocolError};
use coven_storage::run_blocking_object_verification;

pub(crate) struct StoreMembershipObjectVerifier<'operation, 'storage> {
    commit_verifier: &'operation StoreCommitVerifier<'storage>,
}

impl<'operation, 'storage> StoreMembershipObjectVerifier<'operation, 'storage> {
    pub(super) fn new(commit_verifier: &'operation StoreCommitVerifier<'storage>) -> Self {
        Self { commit_verifier }
    }

    /// Retained history already owns these exact objects. Their readers still
    /// perform the ordinary signature, reference, and activation checks.
    pub(crate) fn remember_retained_proof(
        &self,
        proof: &coven_protocol::store_commit::RetainedMergeMembershipProof,
    ) -> Result<(), StoreProtocolError> {
        let objects = [
            (&proof.entry.object, serde_json::to_vec(&proof.entry_value)?),
            (&proof.head.object, serde_json::to_vec(&proof.head_value)?),
        ];
        for (object, bytes) in &objects {
            object.verify(bytes)?;
        }
        for (object, bytes) in objects {
            self.commit_verifier.remember_exact_object(object, &bytes);
        }
        Ok(())
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
        let context = ProtocolObjectContext::signed_plaintext(
            self.commit_verifier.store_root_hash(),
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let loaded: VerifiedObject<AuthorHead> = self
            .commit_verifier
            .load_exact_object(
                &context,
                &reference.object,
                semantic_prefix,
                reference.head_hash,
                coven_protocol::objects::decode_protocol_object,
            )
            .await?;
        let VerifiedObject {
            value: head, bytes, ..
        } = loaded;
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

    pub(crate) async fn load_head_acceptance(
        &self,
        reference: &MembershipHeadRef,
        head: &AuthorHead,
    ) -> Result<
        VerifiedObject<coven_protocol::membership::MembershipHeadAcceptance>,
        crate::sync::store::membership::AnchoredChainError,
    > {
        self.load_head_acceptance_at(reference, head, None).await
    }

    pub(crate) async fn load_head_acceptance_at(
        &self,
        reference: &MembershipHeadRef,
        head: &AuthorHead,
        exact: Option<&coven_protocol::objects::ExactObjectRef>,
    ) -> Result<
        VerifiedObject<coven_protocol::membership::MembershipHeadAcceptance>,
        crate::sync::store::membership::AnchoredChainError,
    > {
        let coven_protocol::membership::MembershipHeadActivation::StoreCommit {
            acceptance_slot,
            ..
        } = &head.activation
        else {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: reference.object.slot().logical_key().to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "direct membership head has no Store acceptance result".into(),
                )),
            }
            .into());
        };
        let prefix = coven_protocol::membership::membership_head_acceptance_semantic_prefix(
            &reference.coord,
        );
        let context = ProtocolObjectContext::signed_plaintext(
            self.commit_verifier.store_root_hash(),
            ProtocolObjectDomain::StoreMembershipHeadAcceptance,
        );
        let read = match exact {
            Some(object) => {
                if object.slot() != acceptance_slot {
                    return Err(
                        crate::sync::store::membership::AnchoredChainError::LoadFailed(
                            "successor names another predecessor acceptance slot".into(),
                        ),
                    );
                }
                self.commit_verifier
                    .load_exact_object(&context, object, &prefix, object.stored_hash(), |_| Ok(()))
                    .await
                    .map(|loaded| (loaded.bytes, loaded.object))
            }
            None => self
                .commit_verifier
                .read_protocol_slot(&context, acceptance_slot, &prefix)
                .await
                .map_err(StoreObjectError::from),
        };
        let (bytes, object) = read.map_err(|source| match source {
            StoreObjectError::Storage(
                source @ coven_protocol::objects::StorageError::NotFound(_),
            ) => crate::sync::store::membership::AnchoredChainError::IncompleteFinalization {
                head: Box::new(reference.clone()),
                source,
            },
            source => crate::sync::store::membership::AnchoredChainError::from_store_object(source),
        })?;
        let registration = self
            .commit_verifier
            .load_registration(&head.body.author_registration)
            .await?;
        let parse_bytes = bytes.clone();
        let expected_root = self.commit_verifier.store_root_hash();
        let expected_head = head.clone();
        let expected_reference = reference.clone();
        let value = run_blocking_object_verification(
            &prefix,
            &object,
            Box::new(move || {
                let value: coven_protocol::membership::MembershipHeadAcceptance =
                    coven_protocol::objects::decode_protocol_object(&parse_bytes)?;
                value.verify_for(
                    expected_root,
                    &expected_reference,
                    &expected_head,
                    &registration.value,
                )?;
                if value.to_bytes() != parse_bytes {
                    return Err(StoreProtocolError::Malformed(
                        "membership acceptance result is not canonical".into(),
                    ));
                }
                Ok(value)
            }),
        )
        .await?;
        Ok(VerifiedObject {
            value,
            semantic_hash: coven_protocol::store_commit::ObjectHash::digest(&bytes),
            bytes,
            object,
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
        let semantic_prefix = Self::head_semantic_prefix(slot)?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.commit_verifier.store_root_hash(),
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let (bytes, object) = self
            .commit_verifier
            .read_protocol_slot(&context, slot, semantic_prefix)
            .await?;
        self.verify_head_at_slot(&bytes, &object, author, grant, stream_id, sequence)
            .await
    }

    fn head_semantic_prefix(
        slot: &coven_protocol::objects::ObjectSlot,
    ) -> Result<&str, StoreObjectError> {
        slot.logical_key()
            .strip_suffix(".json")
            .ok_or_else(|| StoreObjectError::InvalidObject {
                semantic_prefix: slot.logical_key().to_string(),
                key: slot.logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "membership head exact slot has no .json suffix".to_string(),
                )),
            })
    }

    /// Every check [`load_head_at_slot`](Self::load_head_at_slot) makes once
    /// the slot's bytes are in hand, split out so a reader that fetched the
    /// stream's slots together runs the identical verification over what it
    /// already holds.
    pub(crate) async fn verify_head_at_slot(
        &self,
        bytes: &[u8],
        object: &coven_protocol::objects::ExactObjectRef,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
        sequence: u64,
    ) -> Result<VerifiedObject<AuthorHead>, StoreObjectError> {
        let slot = object.slot();
        let semantic_prefix = Self::head_semantic_prefix(slot)?;
        self.commit_verifier.remember_exact_object(object, bytes);
        let parse_bytes = bytes.to_vec();
        let head: AuthorHead = run_blocking_object_verification(
            semantic_prefix,
            object,
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
            source: Box::new(StoreProtocolError::from(error)),
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
            bytes: bytes.to_vec(),
            object: object.clone(),
        })
    }
}
