use super::*;
use crate::sync::store::membership::MembershipMutationError;
use crate::sync::store::StoreError;
use coven_protocol::membership::{self, MembershipEntry, MembershipHeadRef};
use coven_protocol::membership_mutation::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{self, membership_head_slot_prefix};
use coven_storage as store_objects;

pub(crate) struct MembershipPublicationSigner<'a> {
    registration: &'a coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
    device_signer: &'a UserKeypair,
    result_signer: MembershipResultSigner<'a>,
}

enum MembershipResultSigner<'a> {
    Device,
    OwnerRecovery(&'a UserKeypair),
}

impl<'a> MembershipPublicationSigner<'a> {
    pub(crate) fn device(
        registration: &'a coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: &'a UserKeypair,
    ) -> Self {
        Self {
            registration,
            device_signer,
            result_signer: MembershipResultSigner::Device,
        }
    }

    pub(crate) fn owner_recovery(
        registration: &'a coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: &'a UserKeypair,
        principal: &'a UserKeypair,
    ) -> Result<Self, StoreError> {
        if !matches!(
            registration.value().origin,
            coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery { .. }
        ) || registration.value().author_pubkey != coven_keys::keys::public_key_hex(principal)
            || registration.value().device_signing_pubkey
                != coven_keys::keys::public_key_hex(device_signer)
        {
            return Err(StoreError::InvalidOutbound(
                "Recovery membership signing keys differ from their exact registration".into(),
            ));
        }
        Ok(Self {
            registration,
            device_signer,
            result_signer: MembershipResultSigner::OwnerRecovery(principal),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_membership_transition(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        entry: &coven_protocol::membership::MembershipEntry,
        entry_ref: coven_protocol::membership::MembershipEntryRef,
        predecessor: Option<coven_protocol::membership::MembershipHeadPredecessor>,
        anchor: coven_protocol::store_commit::GrantStreamAnchor,
        next_slot: coven_protocol::objects::ObjectSlot,
        head_slot: coven_protocol::objects::ObjectSlot,
    ) -> Result<
        coven_protocol::membership::MergeMembershipHeadTransition,
        crate::sync::store::membership::MembershipMutationError,
    > {
        if self.registration.value().author_pubkey != entry.author_pubkey
            || self.registration.reference().device_id != self.registration.value().device_id
        {
            return Err(
                crate::sync::store::membership::MembershipMutationError::InvalidDurableMutation(
                    "membership author differs from the exact device registration".to_string(),
                ),
            );
        }
        let coord = entry.coord();
        Ok(coven_protocol::membership::MergeMembershipHeadTransition {
            body: coven_protocol::membership::MembershipHeadBody {
                author_registration: self.registration.reference().clone(),
                entry: entry_ref,
                predecessor: predecessor.clone(),
                resolutions: entry.resolution_dependencies.clone(),
                successor: coven_protocol::store_commit::SuccessorLink {
                    activation: coven_protocol::store_commit::StreamActivation::grant_authorized(
                        store_root_hash,
                        self.registration.reference().clone(),
                        coord.author_owner_grant.clone(),
                        anchor,
                    )
                    .activation_id(),
                    predecessor: predecessor.map(|reference| reference.head().object.clone()),
                    next_slot,
                },
            },
            head_slot,
        })
    }

    pub(crate) fn sign_membership_head(
        &self,
        entry: &coven_protocol::membership::MembershipEntry,
        transition: &coven_protocol::membership::MergeMembershipHeadTransition,
        activation: coven_protocol::membership::MembershipHeadActivation,
    ) -> Result<
        coven_protocol::membership::AuthorHead,
        crate::sync::store::membership::MembershipMutationError,
    > {
        if self.registration.value().author_pubkey != entry.author_pubkey
            || self.registration.reference() != &transition.body.author_registration
        {
            return Err(
                crate::sync::store::membership::MembershipMutationError::InvalidDurableMutation(
                    "membership transition author differs from the exact device registration"
                        .to_string(),
                ),
            );
        }
        Ok(coven_protocol::membership::AuthorHead::signed(
            entry.store_id.clone(),
            transition.body.clone(),
            activation,
            self.device_signer,
        ))
    }

    pub(crate) fn sign_membership_head_acceptance(
        &self,
        head_ref: coven_protocol::membership::MembershipHeadRef,
        head: &coven_protocol::membership::AuthorHead,
        accepted: &coven_database::AcceptedStoreCommitPublication,
        accepted_current: &store_commit::StoreCurrentPublicationRecord,
        accepted_predecessor: coven_protocol::membership::MembershipFloor,
    ) -> Result<coven_protocol::membership::MembershipHeadAcceptance, crate::sync::store::StoreError>
    {
        let (sign, key) = match self.result_signer {
            MembershipResultSigner::Device => (
                coven_protocol::membership::MembershipHeadAcceptance::signed
                    as fn(_, _, _, _, _, _, _, _) -> _,
                self.device_signer,
            ),
            MembershipResultSigner::OwnerRecovery(principal) => (
                coven_protocol::membership::MembershipHeadAcceptance::signed_owner_recovery
                    as fn(_, _, _, _, _, _, _, _) -> _,
                principal,
            ),
        };
        let result: Result<
            coven_protocol::membership::MembershipHeadAcceptance,
            coven_protocol::store_commit::StoreProtocolError,
        > = sign(
            self.registration.value().store_root.store_root_hash,
            head_ref,
            head,
            accepted.entry(),
            accepted_current,
            accepted_predecessor,
            self.registration.value(),
            key,
        );
        result.map_err(crate::sync::store::StoreError::from)
    }
}

impl AuthorizedStoreHistory<'_> {
    pub(crate) async fn select_membership_author_stream(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        author: &str,
    ) -> Result<
        coven_protocol::membership::AuthorStreamId,
        crate::sync::store::membership::MembershipMutationError,
    > {
        let grant = chain.active_owner_grant(author).ok_or_else(|| {
            coven_protocol::membership::MembershipError::SignerIsNotOwner(author.to_string())
        })?;
        let mut reusable = chain.reusable_author_streams(author, &grant);
        if let Some(anchored) = chain.membership_stream_id(&grant) {
            reusable.insert(anchored);
        }
        Ok(self
            .database
            .select_membership_author_stream(author, &grant, reusable)
            .await?)
    }

    pub(crate) async fn prepare_membership_transition(
        &mut self,
        signer: &MembershipPublicationSigner<'_>,
        chain: &MembershipChain,
        entry: MembershipEntry,
    ) -> Result<PreparedMembershipTransition, MembershipMutationError> {
        let root = self.root().clone();
        let storage = self.storage.as_ref();
        let (_, entry_ref) =
            store_objects::prepare_membership_entry(storage, root.store_root_hash, &entry)
                .await
                .map_err(MembershipMutationError::from)?;
        let coord = entry.coord();
        let mut predecessor_head = chain
            .head_ref_for_stream(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
            )
            .cloned();
        let mut predecessor = None;
        let current_slot = match predecessor_head.take() {
            Some(reference) => {
                let loaded = self
                    .membership_objects()
                    .load_head(&reference)
                    .await
                    .map_err(MembershipMutationError::from)?;
                predecessor = Some(match &loaded.value.activation {
                    membership::MembershipHeadActivation::Direct => {
                        membership::MembershipHeadPredecessor::Direct { head: reference }
                    }
                    membership::MembershipHeadActivation::StoreCommit { .. } => {
                        let result = self
                            .membership_objects()
                            .load_head_acceptance(&reference, &loaded.value)
                            .await?;
                        membership::MembershipHeadPredecessor::Accepted {
                            head: reference,
                            acceptance: result.object,
                        }
                    }
                });
                loaded.value.body.successor.next_slot.clone()
            }
            None => match chain.membership_anchor(&coord.author_owner_grant) {
                Some(store_commit::GrantStreamAnchor::StoreMembership { first_slot }) => {
                    first_slot.clone()
                }
                Some(
                    store_commit::GrantStreamAnchor::OwnerRecovery { .. }
                    | store_commit::GrantStreamAnchor::CircleControl { .. }
                    | store_commit::GrantStreamAnchor::CircleRoster { .. }
                    | store_commit::GrantStreamAnchor::CircleMetadata { .. },
                ) => {
                    return Err(MembershipMutationError::InvalidDurableMutation(format!(
                        "Owner grant {} uses another domain's anchor as its membership stream",
                        coord.author_owner_grant
                    )));
                }
                None => {
                    return Err(MembershipMutationError::InvalidDurableMutation(format!(
                        "Owner grant {} has no activated membership stream anchor",
                        coord.author_owner_grant
                    )));
                }
            },
        };
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
            MembershipMutationError::InvalidDurableMutation(
                "membership head sequence overflow".to_string(),
            )
        })?;
        let next_prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            next_sequence,
        );
        let next_slot = storage
            .allocate_protocol_slot(&context, &next_prefix, ".json")
            .await?;
        let anchor = chain
            .membership_anchor(&coord.author_owner_grant)
            .ok_or_else(|| {
                MembershipMutationError::InvalidDurableMutation(format!(
                    "Owner grant {} has no activated membership stream anchor",
                    coord.author_owner_grant
                ))
            })?;
        let transition = signer.build_membership_transition(
            root.store_root_hash,
            &entry,
            entry_ref.clone(),
            predecessor,
            anchor.clone(),
            next_slot,
            current_slot,
        )?;
        Ok(PreparedMembershipTransition {
            entry,
            entry_ref,
            transition,
        })
    }

    pub(crate) async fn finish_store_membership_transition(
        &mut self,
        signer: &MembershipPublicationSigner<'_>,
        prepared: PreparedMembershipTransition,
        commit: store_commit::StoreBatchCommitRef,
    ) -> Result<PreparedMembershipPublication, MembershipMutationError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root().store_root_hash,
            ProtocolObjectDomain::StoreMembershipHeadAcceptance,
        );
        let prefix =
            membership::membership_head_acceptance_semantic_prefix(&prepared.entry.coord());
        let acceptance_slot = self
            .storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await?;
        self.finish_membership_transition(
            signer,
            prepared,
            membership::MembershipHeadActivation::StoreCommit {
                commit,
                acceptance_slot,
            },
        )
        .await
    }

    pub(crate) async fn finish_membership_transition(
        &mut self,
        signer: &MembershipPublicationSigner<'_>,
        prepared: PreparedMembershipTransition,
        activation: membership::MembershipHeadActivation,
    ) -> Result<PreparedMembershipPublication, MembershipMutationError> {
        let root = self.root().clone();
        let head =
            signer.sign_membership_head(&prepared.entry, &prepared.transition, activation)?;
        let coord = prepared.entry.coord();
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let head_prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        );
        let head_bytes = serde_json::to_vec(&head).map_err(MembershipMutationError::Json)?;
        let head_object = self.storage.as_ref().prepare_protocol_object(
            &context,
            prepared.transition.head_slot.clone(),
            &head_prefix,
            head_bytes,
        )?;
        let head_ref = MembershipHeadRef {
            coord,
            head_hash: head.head_hash(),
            object: head_object.reference().clone(),
        };
        let publication = PreparedMembershipPublication {
            entry: prepared.entry,
            entry_ref: prepared.entry_ref,
            head,
            head_ref,
        };
        publication.validate()?;
        Ok(publication)
    }

    pub(crate) async fn finalize_membership_head_acceptance(
        &mut self,
        signer: &MembershipPublicationSigner<'_>,
        commit: &store_commit::VerifiedStoreBatchCommit,
        proof: &store_commit::RetainedMergeMembershipProof,
        publication: &coven_database::StoreCommitPublicationOutcome,
    ) -> Result<coven_protocol::remote_object::RemoteObjectRecord, StoreError> {
        let accepted: coven_database::AcceptedStoreCommitEvidence = match publication {
            coven_database::StoreCommitPublicationOutcome::Accepted { interval, .. } => {
                interval.accepted_commit(commit)?.into()
            }
            coven_database::StoreCommitPublicationOutcome::Installed(accepted) => accepted.clone(),
        };
        if accepted.commit_ref() != commit.reference() || proof.commit != *commit.reference() {
            return Err(StoreError::InvalidOutbound(
                "membership finalization differs from its accepted commit".into(),
            ));
        }
        let membership::MembershipHeadActivation::StoreCommit {
            acceptance_slot, ..
        } = &proof.head_value.activation
        else {
            return Err(StoreError::InvalidOutbound(
                "Store membership finalization has a direct head".into(),
            ));
        };
        let context = ProtocolObjectContext::signed_plaintext(
            self.root().store_root_hash,
            ProtocolObjectDomain::StoreMembershipHeadAcceptance,
        );
        let prefix = membership::membership_head_acceptance_semantic_prefix(&proof.head.coord);
        let signing = match publication {
            coven_database::StoreCommitPublicationOutcome::Accepted {
                interval,
                accepted_predecessor,
            } => Some((
                interval.interval().current().clone(),
                accepted_predecessor.clone(),
            )),
            coven_database::StoreCommitPublicationOutcome::Installed(_) => {
                match accepted.exact_publication() {
                    Some(exact) => {
                        let active = self.database.active_store_publication().await?;
                        match active.filter(|active| {
                            active.commit_reservation()
                                == Some((
                                    &commit.write_id,
                                    &commit.author_registration,
                                    &commit.reference().coord,
                                ))
                        }) {
                            Some(active) => {
                                let attempt = active.attempt()?;
                                attempt.verify_commit(commit)?;
                                if attempt.reference()? != *exact.reference() {
                                    return Err(StoreError::InvalidOutbound(
                                        "membership acceptance differs from its owned winning attempt".into(),
                                    ));
                                }
                                let predecessor = Box::pin(
                                    self.accepted_publication_membership_predecessor(exact, commit),
                                )
                                .await?;
                                Some((attempt.replacement.clone(), predecessor))
                            }
                            None => None,
                        }
                    }
                    None => None,
                }
            }
        };
        let (value, prepared) = match signing {
            Some((accepted_current, accepted_predecessor)) => {
                let exact = accepted.exact_publication().ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "membership acceptance has no exact publication".into(),
                    )
                })?;
                accepted_current.verify_accepted_commit(
                    exact.entry(),
                    exact.reference(),
                    commit,
                    &commit.author().device_signing_pubkey,
                )?;
                let value = signer.sign_membership_head_acceptance(
                    proof.head.clone(),
                    &proof.head_value,
                    exact,
                    &accepted_current,
                    accepted_predecessor,
                )?;
                let prepared = self
                    .storage
                    .prepare_protocol_object(
                        &context,
                        acceptance_slot.clone(),
                        &prefix,
                        value.to_bytes(),
                    )
                    .map_err(coven_protocol::objects::StoreObjectError::from)?;
                (value, prepared)
            }
            None => {
                // A completed or compacted publication owns its existing result;
                // it cannot supply another winning envelope for a new signature.
                let loaded = self
                    .membership_objects()
                    .load_head_acceptance(&proof.head, &proof.head_value)
                    .await?;
                if accepted.exact_publication().is_some_and(|exact| {
                    loaded.value.accepted_current.accepted() != Some(exact.reference())
                }) {
                    return Err(StoreError::InvalidOutbound(
                        "retained membership result differs from its exact accepted publication"
                            .into(),
                    ));
                }
                let prepared =
                    coven_protocol::objects::PreparedExactObject::new(loaded.object, loaded.bytes)
                        .map_err(coven_protocol::objects::StoreObjectError::from)?;
                (loaded.value, prepared)
            }
        };
        let remote =
            coven_protocol::remote_object::RemoteObjectRecord::prepared_membership_head_acceptance(
                &value,
                &proof.head_value,
                &prepared,
            )?;
        let remote = self
            .database
            .stage_membership_head_acceptance(accepted.clone(), remote)
            .await?;
        self.storage
            .create_protocol_object(&prepared)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let uploaded = self
            .membership_objects()
            .load_head_acceptance(&proof.head, &proof.head_value)
            .await?;
        if uploaded.object != *prepared.reference() || uploaded.bytes != value.to_bytes() {
            return Err(StoreError::InvalidOutbound(
                "membership acceptance upload differs from its exact result".into(),
            ));
        }
        self.database
            .mark_remote_object_uploaded(remote)
            .await
            .map_err(StoreError::from)
    }
}
