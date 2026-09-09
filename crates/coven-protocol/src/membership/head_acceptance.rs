use super::{
    AuthorHead, MembershipCoord, MembershipFloor, MembershipHeadActivation, MembershipHeadRef,
};
use crate::store_commit::{
    ObjectHash, Signed, SignedBody, StoreCurrentPublicationRecord, StoreDeviceRegistration,
    StoreProtocolError, StorePublicationEntry, StorePublicationPayload, StorePublicationRef,
};
use coven_keys::keys::UserKeypair;
use serde::{Deserialize, Serialize};

/// Issued by the head's author after exact Store publication acceptance.
/// The head binds the commit and transition; this result binds the winning
/// publication envelope without retaining ordinary publication history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipHeadAcceptanceBody {
    pub store_root_hash: ObjectHash,
    pub head: MembershipHeadRef,
    pub issuer: MembershipHeadAcceptanceIssuer,
    pub accepted_current: StoreCurrentPublicationRecord,
    /// Exact authority heads before the winning publication, including controls
    /// accepted while this candidate waited for its publication position.
    pub accepted_predecessor: MembershipFloor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipHeadAcceptanceIssuer {
    Device,
    OwnerRecovery,
}

impl SignedBody for MembershipHeadAcceptanceBody {
    const DOMAIN: &'static [u8] = b"coven.store-membership-head-acceptance.v1\0";
}

pub type MembershipHeadAcceptance = Signed<MembershipHeadAcceptanceBody>;

impl MembershipHeadAcceptance {
    /// The publication owner must supply already accepted evidence. Entry
    /// signatures alone establish preparation, so callers gate signing through
    /// their accepted-publication capability.
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        head_ref: MembershipHeadRef,
        head: &AuthorHead,
        accepted_entry: &StorePublicationEntry,
        accepted_current: &StoreCurrentPublicationRecord,
        accepted_predecessor: MembershipFloor,
        author: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_by(
            store_root_hash,
            head_ref,
            head,
            accepted_entry,
            accepted_current,
            accepted_predecessor,
            author,
            MembershipHeadAcceptanceIssuer::Device,
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_owner_recovery(
        store_root_hash: ObjectHash,
        head_ref: MembershipHeadRef,
        head: &AuthorHead,
        accepted_entry: &StorePublicationEntry,
        accepted_current: &StoreCurrentPublicationRecord,
        accepted_predecessor: MembershipFloor,
        author: &StoreDeviceRegistration,
        principal: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_by(
            store_root_hash,
            head_ref,
            head,
            accepted_entry,
            accepted_current,
            accepted_predecessor,
            author,
            MembershipHeadAcceptanceIssuer::OwnerRecovery,
            principal,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn signed_by(
        store_root_hash: ObjectHash,
        head_ref: MembershipHeadRef,
        head: &AuthorHead,
        accepted_entry: &StorePublicationEntry,
        accepted_current: &StoreCurrentPublicationRecord,
        accepted_predecessor: MembershipFloor,
        author: &StoreDeviceRegistration,
        issuer: MembershipHeadAcceptanceIssuer,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let MembershipHeadActivation::StoreCommit { commit, .. } = &head.activation else {
            return Err(StoreProtocolError::Malformed(
                "direct membership head cannot have a Store acceptance result".into(),
            ));
        };
        let accepted_ref = accepted_current.accepted().ok_or_else(|| {
            StoreProtocolError::Malformed("membership acceptance cannot name genesis".into())
        })?;
        accepted_current.verify_by(&author.device_signing_pubkey)?;
        let parsed = StorePublicationEntry::parse_at(
            &accepted_entry.to_bytes(),
            store_root_hash,
            accepted_ref,
            &author.device_signing_pubkey,
        )?;
        let reference = StorePublicationRef::from_entry(&parsed, accepted_ref.object.clone())?;
        if reference != *accepted_ref
            || accepted_entry.author_registration != head.body.author_registration
            || accepted_entry.payload != StorePublicationPayload::Commit(commit.clone())
        {
            return Err(StoreProtocolError::Malformed(
                "membership head acceptance differs from its exact accepted commit".into(),
            ));
        }
        let value = Signed::sign(
            MembershipHeadAcceptanceBody {
                store_root_hash,
                head: head_ref.clone(),
                issuer,
                accepted_current: accepted_current.clone(),
                accepted_predecessor,
            },
            signer,
        );
        value.verify_for(store_root_hash, &head_ref, head, author)?;
        Ok(value)
    }

    pub fn publication(&self) -> Result<&StorePublicationRef, StoreProtocolError> {
        self.accepted_current.accepted().ok_or_else(|| {
            StoreProtocolError::Malformed("membership acceptance cannot name genesis".into())
        })
    }

    pub fn verify_for(
        &self,
        expected_store_root_hash: ObjectHash,
        head_ref: &MembershipHeadRef,
        head: &AuthorHead,
        author: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.require_version()?;
        self.accepted_current
            .verify_by(&author.device_signing_pubkey)?;
        let publication = self.publication()?;
        self.accepted_predecessor.validate().map_err(|error| {
            StoreProtocolError::Malformed(format!(
                "invalid accepted membership predecessor: {error}"
            ))
        })?;
        match self.issuer {
            MembershipHeadAcceptanceIssuer::Device => {
                self.verify_by(&author.device_signing_pubkey)?
            }
            MembershipHeadAcceptanceIssuer::OwnerRecovery => {
                if !matches!(
                    author.origin,
                    crate::store_commit::StoreDeviceRegistrationOrigin::Recovery { .. }
                ) {
                    return Err(StoreProtocolError::Malformed(
                        "Owner recovery acceptance has another registration origin".into(),
                    ));
                }
                self.verify_by(&author.author_pubkey)?;
            }
        }
        head_ref.object.verify(&head.to_bytes())?;
        let MembershipHeadActivation::StoreCommit {
            acceptance_slot, ..
        } = &head.activation
        else {
            return Err(StoreProtocolError::Malformed(
                "direct membership head carries a Store acceptance result".into(),
            ));
        };
        let expected_key = format!(
            "{}.json",
            membership_head_acceptance_semantic_prefix(&head_ref.coord)
        );
        if acceptance_slot.logical_key() != expected_key {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: expected_key,
                actual: acceptance_slot.logical_key().to_string(),
            });
        }
        if self.store_root_hash != expected_store_root_hash
            || self.accepted_current.store_root_hash != expected_store_root_hash
            || publication.store_root_hash != expected_store_root_hash
            || author.store_root.store_root_hash != expected_store_root_hash
            || self.head != *head_ref
            || head_ref.head_hash != head.head_hash()
            || head_ref.coord != head.entry_coord()
            || !head.verify(author)
        {
            return Err(StoreProtocolError::Malformed(
                "membership acceptance result differs from its exact rooted head".into(),
            ));
        }
        Ok(())
    }
}

pub fn membership_head_acceptance_semantic_prefix(coord: &MembershipCoord) -> String {
    format!(
        "store-v1/membership/acceptances/{}/{}/{}/{}",
        coord.author_pubkey, coord.author_owner_grant, coord.stream_id, coord.seq,
    )
}
