use super::*;
use crate::sync::store::commit_verification::commit::StoreCommitVerifier;
use coven_protocol::membership::{MembershipHeadAcceptance, MembershipHeadAcceptanceIssuer};
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, ReferencedStoreDeviceRegistration,
    StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionRef, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
};

use crate::sync::store::{protocol_root::VerifiedStoreRoot, StorePullError};
use coven_protocol::membership::{OwnerRecoveryAnchorRef, StoreMembershipRoleGrant};
use coven_protocol::store_commit::{
    OwnerRecoveryActivationId, ResolvedStoreDeviceState, RetainedMergeMembershipProof,
    RetainedStoreDeviceExclusionOutcome, RetainedStoreDeviceExclusionProposal,
    RetainedStoreDeviceOperations, SnapshotMeta, StoreDeviceExclusionOutcome,
    StoreDeviceRegistrationRef,
};

/// Results observed during one rooted authority walk. Pending heads stop their
/// stream but cannot block discovery of the other already activated streams.
#[derive(Default)]
pub(super) struct AcceptedDeviceAuthority {
    heads: BTreeMap<MembershipHeadRef, HeadAcceptance>,
    accepted: BTreeSet<MembershipCoord>,
}

enum HeadAcceptance {
    Accepted(MembershipHeadAcceptance),
    Pending {
        head: AuthorHead,
        entry: MembershipEntry,
        source: StorageError,
    },
}

struct AcceptedDeviceExclusion {
    exclusion: StoreDeviceExclusionRef,
    acceptance: MembershipHeadAcceptance,
}

impl AcceptedDeviceAuthority {
    pub(super) fn contains(&self, coord: &MembershipCoord) -> bool {
        self.accepted.contains(coord)
    }

    pub(super) async fn observe(
        &mut self,
        verifier: &StoreCommitVerifier<'_>,
        reference: &MembershipHeadRef,
        head: &AuthorHead,
        entry: &MembershipEntry,
        loaded: Option<LoadedHeadAcceptance>,
    ) -> Result<bool, AnchoredChainError> {
        let result = match loaded {
            Some(result) => result,
            None => {
                if let Some(observed) = self.heads.get(reference) {
                    return Ok(matches!(observed, HeadAcceptance::Accepted(_)));
                }
                verifier
                    .membership_objects()
                    .load_head_acceptance(reference, head)
                    .await
            }
        };
        let observed = match result {
            Ok(result) => HeadAcceptance::Accepted(result.value),
            Err(AnchoredChainError::IncompleteFinalization { source, .. }) => {
                HeadAcceptance::Pending {
                    head: head.clone(),
                    entry: entry.clone(),
                    source,
                }
            }
            Err(error) => return Err(error),
        };
        if let Some(previous) = self.heads.get(reference) {
            match (previous, &observed) {
                (HeadAcceptance::Accepted(previous), HeadAcceptance::Accepted(current))
                    if previous == current =>
                {
                    return Ok(true)
                }
                (HeadAcceptance::Pending { .. }, HeadAcceptance::Pending { .. }) => {
                    return Ok(false)
                }
                _ => {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership result changed within its authority walk".into(),
                    ))
                }
            }
        }
        let accepted = matches!(observed, HeadAcceptance::Accepted(_));
        self.heads.insert(reference.clone(), observed);
        Ok(accepted)
    }

    pub(super) fn receipt(
        &self,
        reference: &MembershipHeadRef,
    ) -> Result<&MembershipHeadAcceptance, AnchoredChainError> {
        match self.heads.get(reference) {
            Some(HeadAcceptance::Accepted(result)) => Ok(result),
            _ => Err(AnchoredChainError::LoadFailed(
                "authority head has no observed acceptance result".into(),
            )),
        }
    }

    pub(super) async fn validate(
        &mut self,
        verifier: &StoreCommitVerifier<'_>,
        root: &crate::sync::store::protocol_root::VerifiedStoreRoot,
        membership: &MembershipChain,
        streams: &[TraversedMembershipStream],
    ) -> Result<(), AnchoredChainError> {
        let nodes = streams
            .iter()
            .flat_map(|stream| stream.heads.iter())
            .map(|(reference, head, entry)| (reference.coord.clone(), (reference, head, entry)))
            .collect::<BTreeMap<_, _>>();
        for (reference, head, _) in nodes.values().copied() {
            if !matches!(
                head.activation,
                coven_protocol::membership::MembershipHeadActivation::StoreCommit { .. }
            ) {
                continue;
            }
            let acceptance = self.receipt(reference)?;
            for tip in &acceptance.accepted_predecessor.0 {
                let Some((actual, prior_head, _)) = nodes.get(&tip.coord) else {
                    return Err(AnchoredChainError::LoadFailed(
                        "authority acceptance predecessor is absent from rooted authority".into(),
                    ));
                };
                if *actual != tip {
                    return Err(AnchoredChainError::LoadFailed(
                        "authority acceptance predecessor differs from its exact rooted head"
                            .into(),
                    ));
                }
                if matches!(
                    prior_head.activation,
                    coven_protocol::membership::MembershipHeadActivation::StoreCommit { .. }
                ) && self.receipt(tip)?.publication()?.position
                    >= acceptance.publication()?.position
                {
                    return Err(AnchoredChainError::LoadFailed(
                        "authority acceptance predecessors do not precede its publication".into(),
                    ));
                }
            }
            let own_predecessor = acceptance
                .accepted_predecessor
                .0
                .iter()
                .find(|tip| tip.coord.stream_key() == reference.coord.stream_key());
            if own_predecessor != head.body.predecessor_head() {
                return Err(AnchoredChainError::LoadFailed(
                    "authority acceptance changes its own exact predecessor".into(),
                ));
            }
        }
        // A later accepted head witnesses the exact authority that preceded
        // its publication, even when its grant reduction retires that authority.
        // Unreferenced tails of retired streams do not acquire this witness.
        let mut accepted = BTreeSet::new();
        let mut pending = nodes
            .keys()
            .filter(|coord| membership.effectively_contains_coord(coord))
            .cloned()
            .collect::<Vec<_>>();
        while let Some(coord) = pending.pop() {
            if !accepted.insert(coord.clone()) {
                continue;
            }
            let (reference, head, _) = nodes.get(&coord).ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "accepted authority closure leaves its rooted head paths".into(),
                )
            })?;
            if let Some(previous) = head.body.predecessor_head() {
                if nodes
                    .get(&previous.coord)
                    .is_none_or(|(actual, _, _)| *actual != previous)
                {
                    return Err(AnchoredChainError::LoadFailed(
                        "accepted authority closure changes an exact predecessor".into(),
                    ));
                }
                pending.push(previous.coord.clone());
            }
            if matches!(
                head.activation,
                coven_protocol::membership::MembershipHeadActivation::StoreCommit { .. }
            ) {
                pending.extend(
                    self.receipt(reference)?
                        .accepted_predecessor
                        .0
                        .iter()
                        .map(|tip| tip.coord.clone()),
                );
            }
        }
        let mut founders = nodes.values().copied().filter(|(reference, _, entry)| {
            accepted.contains(&reference.coord)
                && matches!(entry.change, StoreAuthorityChange::Founder { .. })
        });
        let (founder_ref, founder_head, founder_entry) = founders.next().ok_or_else(|| {
            AnchoredChainError::LoadFailed("accepted authority omits its Founder".into())
        })?;
        if founders.next().is_some() {
            return Err(AnchoredChainError::LoadFailed(
                "accepted authority contains multiple Founders".into(),
            ));
        }
        let provider_admin = coven_protocol::provider::ProviderAdminState::founder_from_root(
            root.reference().clone(),
            founder_head.body.author_registration.clone(),
            &root.protocol().descriptor.founder_provider_admin,
        );
        let mut preceding = MembershipChain::from_entries_with_coords_and_heads_and_provider_admin(
            vec![(founder_ref.coord.clone(), founder_entry.clone())],
            vec![(founder_ref.clone(), founder_head.clone())],
            provider_admin,
        )?;
        let mut publications = BTreeMap::new();
        for (reference, head, entry) in nodes.values().copied() {
            if !accepted.contains(&reference.coord) || reference == founder_ref {
                continue;
            }
            let receipt = self.receipt(reference)?;
            if publications
                .insert(
                    receipt.publication()?.position,
                    (reference, head, entry, receipt),
                )
                .is_some()
            {
                return Err(AnchoredChainError::LoadFailed(
                    "membership authority repeats an accepted Store publication position".into(),
                ));
            }
        }
        // The receipt order supplies the actual predecessor for every accepted
        // control. Reuse the traversed exact entries instead of reading another
        // history or trusting the candidate's older preparation boundary.
        for (_, (reference, _, entry, receipt)) in publications {
            if preceding.head_refs() != receipt.accepted_predecessor.0 {
                return Err(AnchoredChainError::LoadFailed(
                    "membership result omits or changes an earlier accepted authority head".into(),
                ));
            }
            if let StoreAuthorityChange::ResolutionActivation { resolution } = &entry.change {
                let loaded = verifier
                    .membership_objects()
                    .load_resolution(resolution)
                    .await?;
                preceding.apply_resolutions(
                    root.reference().store_root_hash,
                    &[(resolution.clone(), loaded.value)],
                )?;
            }
            preceding.validate_publication_predecessor(entry)?;
            preceding.add_entry_at(reference.coord.clone(), entry.clone())?;
            preceding.activate_head_ref(reference.clone())?;
        }
        let mut registrations = BTreeMap::new();
        for (reference, _, entry) in nodes.values().copied() {
            if !accepted.contains(&reference.coord) {
                continue;
            }
            let StoreAuthorityChange::DeviceRegistrationActivation {
                registration: activation,
            } = &entry.change
            else {
                continue;
            };
            self.load_activated_registration(verifier, activation)
                .await?;
            if registrations
                .insert(activation.registration.clone(), reference)
                .is_some()
            {
                return Err(AnchoredChainError::LoadFailed(
                    "device registration has multiple accepted authority activations".into(),
                ));
            }
        }
        // A non-founder device cannot establish its own permission by signing a
        // result. Its admitting Owner's exact earlier head must already belong
        // to the predecessor authority claimed by that result.
        for (reference, head, entry) in nodes.values().copied() {
            if !accepted.contains(&reference.coord) {
                continue;
            }
            if matches!(
                &entry.change,
                StoreAuthorityChange::DeviceRegistrationActivation { registration }
                    if matches!(registration.authority, StoreDeviceRegistrationActivationRef::Recovery { .. })
            ) {
                let acceptance = self.receipt(reference)?;
                if acceptance.issuer != MembershipHeadAcceptanceIssuer::OwnerRecovery {
                    return Err(AnchoredChainError::LoadFailed(
                        "Recovery activation requires its principal acceptance".into(),
                    ));
                }
                self.validate_recovery_activation(
                    verifier, root, &accepted, reference, head, entry, acceptance,
                )
                .await?;
                continue;
            }
            let author = verifier
                .load_registration(&head.body.author_registration)
                .await
                .map_err(map_membership_object_error)?;
            if matches!(
                author.value.origin,
                StoreDeviceRegistrationOrigin::Founder { .. }
            ) {
                continue;
            }
            let acceptance = self.receipt(reference)?;
            if acceptance.issuer == MembershipHeadAcceptanceIssuer::OwnerRecovery {
                return Err(AnchoredChainError::LoadFailed(
                    "principal-signed recovery result accompanies another authority change".into(),
                ));
            }
            let activation = registrations
                .get(&head.body.author_registration)
                .ok_or_else(|| {
                    AnchoredChainError::LoadFailed(
                        "membership authority author has no accepted device registration activation"
                            .into(),
                    )
                })?;
            let acceptance = self.receipt(reference)?;
            let predecessor = acceptance
                .accepted_predecessor
                .0
                .iter()
                .find(|tip| tip.coord.stream_key() == activation.coord.stream_key());
            let predecessor_matches = predecessor.is_some_and(|tip| {
                nodes
                    .get(&tip.coord)
                    .is_some_and(|(actual, _, _)| *actual == tip)
                    && tip.coord.seq >= activation.coord.seq
            });
            if !predecessor_matches
                || self.receipt(activation)?.publication()?.position
                    >= acceptance.publication()?.position
            {
                return Err(AnchoredChainError::LoadFailed(
                    "membership authority precedes its device registration activation".into(),
                ));
            }
        }
        let mut exclusions = Vec::new();
        for (reference, head, entry) in nodes.values().copied() {
            if !accepted.contains(&reference.coord) {
                continue;
            }
            let StoreAuthorityChange::DeviceExclusionOutcome {
                outcome: StoreDeviceExclusionOutcomeRef::Excluded(exclusion),
            } = &entry.change
            else {
                continue;
            };
            let proposal = verifier
                .load_device_exclusion_proposal(&exclusion.proposal)
                .await
                .map_err(map_membership_object_error)?;
            let outcome = verifier
                .load_device_exclusion_outcome(
                    &StoreDeviceExclusionOutcomeRef::Excluded(exclusion.clone()),
                    &proposal,
                )
                .await
                .map_err(map_membership_object_error)?;
            let coven_protocol::store_commit::StoreDeviceExclusionOutcome::Excluded(value) =
                outcome.object.value
            else {
                return Err(AnchoredChainError::LoadFailed(
                    "device exclusion changed its exact outcome".into(),
                ));
            };
            if value.owner_registration != head.body.author_registration
                || value.owner_grant != reference.coord.author_owner_grant
            {
                return Err(AnchoredChainError::LoadFailed(
                    "device exclusion issuer differs from its authority head".into(),
                ));
            }
            let acceptance = self.receipt(reference)?;
            exclusions.push(AcceptedDeviceExclusion {
                exclusion: exclusion.clone(),
                acceptance: acceptance.clone(),
            });
        }
        // Validate every issuer before using any exclusion to dismiss a pending
        // tail. Mutually unsupported exclusions fail as a whole.
        for (reference, head, _) in nodes.values().copied() {
            if accepted.contains(&reference.coord)
                && exclusions
                    .iter()
                    .any(|exclusion| exclusion.excludes(reference, head))
            {
                return Err(AnchoredChainError::LoadFailed(
                    "membership authority head lies beyond its device exclusion boundary".into(),
                ));
            }
        }
        let activated_streams = membership
            .activated_membership_streams()
            .into_iter()
            .map(|(stream, _)| stream)
            .collect::<BTreeSet<_>>();
        let mut unresolved = None;
        for (reference, observed) in &self.heads {
            let HeadAcceptance::Pending { head, entry, .. } = observed else {
                continue;
            };
            if !activated_streams.contains(&reference.coord.stream_key()) {
                continue;
            }
            if exclusions
                .iter()
                .any(|exclusion| exclusion.excludes(reference, head))
            {
                continue;
            }
            let with_pending = membership.with_exact_entry(entry)?;
            if !with_pending.effectively_contains_coord(&reference.coord) {
                continue;
            }
            unresolved = Some(reference.clone());
            break;
        }
        if let Some(reference) = unresolved {
            let Some(HeadAcceptance::Pending { source, .. }) = self.heads.remove(&reference) else {
                unreachable!("selected pending authority head remains pending")
            };
            return Err(AnchoredChainError::IncompleteFinalization {
                head: Box::new(reference),
                source,
            });
        }
        self.accepted = accepted;
        Ok(())
    }

    async fn load_activated_registration(
        &self,
        verifier: &StoreCommitVerifier<'_>,
        activation: &coven_protocol::store_commit::ActivatedStoreDeviceRegistrationRef,
    ) -> Result<ActivatedStoreDeviceRegistration, AnchoredChainError> {
        let registration = verifier
            .load_registration(&activation.registration)
            .await
            .map_err(map_membership_object_error)?;
        let authority = match &activation.authority {
            StoreDeviceRegistrationActivationRef::Join { attempt_id } => {
                StoreDeviceRegistrationActivation::Join {
                    attempt_id: *attempt_id,
                }
            }
            StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node } => {
                StoreDeviceRegistrationActivation::Recovery {
                    recovery_id: *recovery_id,
                    node: node.clone(),
                }
            }
        };
        ActivatedStoreDeviceRegistration::verified(
            ReferencedStoreDeviceRegistration::verified(
                activation.registration.clone(),
                registration.value,
            )
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?,
            authority,
        )
        .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))
    }

    async fn validate_recovery_activation(
        &self,
        verifier: &StoreCommitVerifier<'_>,
        root: &crate::sync::store::protocol_root::VerifiedStoreRoot,
        accepted: &BTreeSet<MembershipCoord>,
        reference: &MembershipHeadRef,
        head: &AuthorHead,
        entry: &MembershipEntry,
        acceptance: &MembershipHeadAcceptance,
    ) -> Result<(), AnchoredChainError> {
        let StoreAuthorityChange::DeviceRegistrationActivation { registration } = &entry.change
        else {
            return Err(AnchoredChainError::LoadFailed(
                "principal-signed recovery result accompanies another authority change".into(),
            ));
        };
        let StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node } =
            &registration.authority
        else {
            return Err(AnchoredChainError::LoadFailed(
                "principal-signed recovery result accompanies another registration origin".into(),
            ));
        };
        let value = verifier
            .load_owner_recovery_node(node)
            .await
            .map_err(map_membership_object_error)?
            .value;
        if registration.registration != head.body.author_registration
            || value.readiness.registration != registration.registration
            || value.recovery_id != *recovery_id
            || value.owner_pubkey != reference.coord.author_pubkey
            || value.owner_grant != reference.coord.author_owner_grant
        {
            return Err(AnchoredChainError::LoadFailed(
                "recovery acceptance differs from its exact self-activation".into(),
            ));
        }
        let mut authority = MembershipActivationAuthority::AcceptedHeads {
            root: root.clone(),
            commit_verifier: verifier,
            device_authority: AcceptedDeviceAuthority::default(),
        };
        let resolutions = acceptance.accepted_predecessor.0.iter().map(|tip| async {
            verifier
                .membership_objects()
                .load_head(tip)
                .await
                .map(|loaded| loaded.value.body.resolutions.clone())
        });
        let mut exact_resolutions = BTreeSet::new();
        for loaded in resolutions {
            exact_resolutions.extend(loaded.await.map_err(map_membership_object_error)?);
        }
        let predecessor = Box::pin(authority.load_anchored_chain_at_exact_heads(
            &acceptance.accepted_predecessor.0,
            &exact_resolutions.into_iter().collect::<Vec<_>>(),
            None,
        ))
        .await?;
        let historical = Box::pin(authority.load_anchored_chain_at_exact_heads(
            &value.membership.heads,
            &value.membership.resolutions,
            None,
        ))
        .await?;
        crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier::verify_owner_recovery_node_authority(&value, &historical, &predecessor)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        let record = predecessor
            .active_grant(&value.owner_grant)
            .ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "Recovery principal's Owner grant is retired at acceptance".into(),
                )
            })?;
        let coven_protocol::membership::StoreMembershipRoleGrant::Owner { recovery } = &record.role
        else {
            return Err(AnchoredChainError::LoadFailed(
                "Recovery principal has no Owner recovery authority".into(),
            ));
        };
        let anchor = match recovery {
            coven_protocol::membership::OwnerRecoveryAnchorRef::Founder { creation_id }
                if *creation_id == root.protocol().descriptor.creation_id =>
            {
                &root.protocol().descriptor.founder_recovery
            }
            coven_protocol::membership::OwnerRecoveryAnchorRef::Founder { .. } => {
                return Err(AnchoredChainError::LoadFailed(
                    "Recovery principal names another Store creation".into(),
                ));
            }
            coven_protocol::membership::OwnerRecoveryAnchorRef::Promotion { acceptance } => {
                &acceptance.anchors.recovery
            }
            coven_protocol::membership::OwnerRecoveryAnchorRef::ConflictResolution {
                acceptance,
            } => &acceptance.recovery,
        };
        let GrantStreamAnchor::OwnerRecovery { first_slot } = anchor else {
            return Err(AnchoredChainError::LoadFailed(
                "Recovery grant has another authority anchor".into(),
            ));
        };
        let previous = predecessor
            .device_registration_activations()
            .filter_map(|(coord, activation)| match &activation.authority {
                StoreDeviceRegistrationActivationRef::Recovery { node, .. }
                    if node.owner_grant == value.owner_grant =>
                {
                    Some((coord, node))
                }
                _ => None,
            })
            .max_by_key(|(_, node)| node.sequence);
        if value.predecessor.as_ref() != previous.map(|(_, node)| node) {
            return Err(AnchoredChainError::LoadFailed(
                "Recovery activation does not extend its accepted recovery cursor".into(),
            ));
        }
        match previous {
            Some((coord, previous)) => {
                if !accepted.contains(coord) {
                    return Err(AnchoredChainError::LoadFailed(
                        "Recovery cursor is absent from the accepted rooted authority".into(),
                    ));
                }
                // The complete rooted walk validates this earlier activation's
                // issuer as well. Only its authenticated successor slot is
                // needed to extend that already accepted recovery cursor.
                let loaded = verifier
                    .load_owner_recovery_node(previous)
                    .await
                    .map_err(map_membership_object_error)?
                    .value;
                if loaded.next_slot != *node.object.slot() {
                    return Err(AnchoredChainError::LoadFailed(
                        "Recovery node leaves its exact anchored successor path".into(),
                    ));
                }
            }
            None if node.object.slot() != first_slot => {
                return Err(AnchoredChainError::LoadFailed(
                    "Recovery node is not rooted in its Owner grant".into(),
                ));
            }
            None => {}
        }
        Ok(())
    }
    /// Reconstruct the complete device state from accepted authority. The
    /// candidate's state is only compared after reconstruction; it never seeds it.
    pub(super) async fn verify_snapshot(
        &self,
        root: &VerifiedStoreRoot,
        verifier: &StoreCommitVerifier<'_>,
        snapshot: &SnapshotMeta,
        membership: &MembershipChain,
        traversed: &TraversedMembership,
    ) -> Result<(), StorePullError> {
        let founder = verifier.load_founder_registration().await?;
        let descriptor = &root.protocol().descriptor;
        let mut state = ResolvedStoreDeviceState::founder(
            root.reference(),
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object),
            &descriptor.founder_pubkey,
            descriptor.founder_grant.clone(),
            &descriptor.founder_recovery,
        )?;
        let selected = traversed
            .streams
            .iter()
            .flat_map(|stream| &stream.heads)
            .filter(|(reference, _, _)| {
                self.contains(&reference.coord) && membership.contains_coord(&reference.coord)
            })
            .collect::<Vec<_>>();
        // Registrations must be established independently before any exclusion
        // contribution is allowed to refer to its target device.
        for (reference, head, entry) in &selected {
            let StoreAuthorityChange::DeviceRegistrationActivation { registration } = &entry.change
            else {
                continue;
            };
            let proof = snapshot_proof(snapshot, reference, head, entry)?;
            if proof.commit_value.device_registrations() != std::slice::from_ref(registration) {
                return Err(StorePullError::InvalidState(
                    "snapshot registration differs from its accepted authority entry".into(),
                ));
            }
            let activated = self
                .load_activated_registration(verifier, registration)
                .await?;
            let effect = ResolvedStoreDeviceState::merge([])?.apply_verified_lifecycle(
                &proof.commit_value,
                &[activated],
                None,
                None,
            )?;
            state = ResolvedStoreDeviceState::merge([state, effect])?;
        }
        for (reference, head, entry) in selected {
            let effect = match &entry.change {
                StoreAuthorityChange::SetMember {
                    user_pubkey,
                    grant_id,
                    role:
                        StoreMembershipRoleGrant::Owner {
                            recovery: OwnerRecoveryAnchorRef::Promotion { acceptance },
                        },
                    ..
                } => recovery_effect(root, user_pubkey, grant_id, &acceptance.anchors.recovery)?,
                StoreAuthorityChange::ResolutionActivation { resolution } => {
                    let loaded = verifier
                        .membership_objects()
                        .load_resolution(resolution)
                        .await?;
                    recovery_effect(
                        root,
                        &loaded.value.resolver_pubkey,
                        &loaded.value.replacement_grant,
                        &loaded.value.replacement_acceptance.recovery,
                    )?
                }
                StoreAuthorityChange::DeviceExclusionProposal { proposal } => {
                    let loaded = verifier.load_device_exclusion_proposal(proposal).await?;
                    if loaded.object.value.owner_registration != head.body.author_registration
                        || loaded.object.value.owner_grant != entry.author_owner_grant
                    {
                        return Err(StorePullError::InvalidState(
                            "snapshot exclusion proposal differs from its accepted authority author".into(),
                        ));
                    }
                    RetainedStoreDeviceOperations::from_sources(
                        vec![RetainedStoreDeviceExclusionProposal::from_verified(&loaded)],
                        Vec::new(),
                    )
                    .verify_for(
                        root.reference(),
                        &snapshot_proof(snapshot, reference, head, entry)?.commit_value,
                    )?
                    .accepted_effect()?
                }
                StoreAuthorityChange::DeviceExclusionOutcome { outcome } => {
                    let proposal = verifier
                        .load_device_exclusion_proposal(outcome.proposal())
                        .await?;
                    let loaded = verifier
                        .load_device_exclusion_outcome(outcome, &proposal)
                        .await?;
                    let (owner, grant) = match &loaded.object.value {
                        StoreDeviceExclusionOutcome::Excluded(value) => {
                            (&value.owner_registration, &value.owner_grant)
                        }
                        StoreDeviceExclusionOutcome::Cancelled(value) => {
                            (&value.owner_registration, &value.owner_grant)
                        }
                    };
                    if owner != &head.body.author_registration || grant != &entry.author_owner_grant
                    {
                        return Err(StorePullError::InvalidState(
                            "snapshot exclusion outcome differs from its accepted authority author"
                                .into(),
                        ));
                    }
                    RetainedStoreDeviceOperations::from_sources(
                        Vec::new(),
                        vec![RetainedStoreDeviceExclusionOutcome::from_verified(
                            outcome,
                            RetainedStoreDeviceExclusionProposal::from_verified(&proposal),
                            &loaded,
                        )?],
                    )
                    .verify_for(
                        root.reference(),
                        &snapshot_proof(snapshot, reference, head, entry)?.commit_value,
                    )?
                    .accepted_effect()?
                }
                _ => continue,
            };
            if effect.devices.iter().any(|(id, record)| {
                state
                    .devices
                    .get(id)
                    .is_none_or(|accepted| accepted.registration != record.registration)
            }) {
                return Err(StorePullError::InvalidState(
                    "snapshot exclusion target has no independently accepted registration activation".into(),
                ));
            }
            state = ResolvedStoreDeviceState::merge([state, effect])?;
        }
        for (id, accepted) in &state.devices {
            if snapshot
                .state
                .devices
                .devices
                .get(id)
                .is_none_or(|claimed| claimed.registration != accepted.registration)
            {
                return Err(StorePullError::InvalidState(
                    "snapshot omits an independently accepted registration".into(),
                ));
            }
        }
        if snapshot
            .state
            .devices
            .devices
            .keys()
            .any(|id| !state.devices.contains_key(id))
        {
            return Err(StorePullError::InvalidState(
                "snapshot device has no independently accepted registration activation".into(),
            ));
        }
        if snapshot.state.devices != state {
            return Err(StorePullError::InvalidState(
                "snapshot device state omits or changes an independently accepted authority effect, or includes an unaccepted authority effect".into(),
            ));
        }
        Ok(())
    }
}

impl AcceptedDeviceExclusion {
    fn excludes(&self, reference: &MembershipHeadRef, head: &AuthorHead) -> bool {
        if head.body.author_registration != self.exclusion.proposal.target
            || reference == &self.acceptance.head
        {
            return false;
        }
        match self
            .acceptance
            .accepted_predecessor
            .0
            .iter()
            .find(|tip| tip.coord.stream_key() == reference.coord.stream_key())
        {
            Some(tip) => reference.coord.seq > tip.coord.seq,
            None => true,
        }
    }
}

fn recovery_effect(
    root: &VerifiedStoreRoot,
    owner: &str,
    grant: &MembershipGrantId,
    anchor: &GrantStreamAnchor,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let activation = OwnerRecoveryActivationId::derive(root.reference(), owner, grant, anchor)?;
    Ok(ResolvedStoreDeviceState::merge([])?.activate_owner_recovery(grant.clone(), activation)?)
}

fn snapshot_proof<'a>(
    snapshot: &'a SnapshotMeta,
    reference: &MembershipHeadRef,
    head: &AuthorHead,
    entry: &MembershipEntry,
) -> Result<&'a RetainedMergeMembershipProof, StorePullError> {
    let coven_protocol::membership::MembershipHeadActivation::StoreCommit { commit, .. } =
        &head.activation
    else {
        return Err(StorePullError::InvalidState(
            "device authority has no Store activation".into(),
        ));
    };
    let proof = snapshot
        .history_summary
        .membership_proofs
        .get(commit)
        .ok_or_else(|| {
            StorePullError::InvalidState(
                "snapshot omits its independently accepted device authority proof".into(),
            )
        })?;
    if &proof.head != reference || &proof.head_value != head || &proof.entry_value != entry {
        return Err(StorePullError::InvalidState(
            "snapshot device authority proof differs from its rooted accepted head".into(),
        ));
    }
    commit.verify_commit(&proof.commit_value)?;
    Ok(proof)
}
