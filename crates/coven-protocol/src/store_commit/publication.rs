use super::*;

pub fn store_current_publication_semantic_prefix() -> &'static str {
    "store-v1/publications/current"
}

pub fn store_current_publication_logical_key() -> &'static str {
    "store-v1/publications/current.json"
}

pub fn store_publication_entry_semantic_prefix(entry: &StorePublicationEntry) -> String {
    format!(
        "store-v1/publications/entries/{}/{}",
        entry.position.get(),
        entry.entry_hash()
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StorePublicationPosition(u64);

impl StorePublicationPosition {
    pub fn new(value: u64) -> Result<Self, StoreProtocolError> {
        if value == 0 {
            return Err(StoreProtocolError::InvalidSequence(value));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }

    fn successor(self) -> Result<Self, StoreProtocolError> {
        self.0
            .checked_add(1)
            .ok_or_else(|| {
                StoreProtocolError::Malformed("Store publication position overflow".to_string())
            })
            .and_then(Self::new)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePublicationRef {
    pub store_root_hash: ObjectHash,
    pub position: StorePublicationPosition,
    pub entry_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl StorePublicationRef {
    pub fn from_entry(
        entry: &StorePublicationEntry,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        object.verify(&entry.to_bytes())?;
        Ok(Self {
            store_root_hash: entry.store_root_hash,
            position: entry.position,
            entry_hash: entry.entry_hash(),
            object,
        })
    }

    fn verify_entry(&self, entry: &StorePublicationEntry) -> Result<(), StoreProtocolError> {
        self.object.verify(&entry.to_bytes())?;
        if self.store_root_hash != entry.store_root_hash
            || self.position != entry.position
            || self.entry_hash != entry.entry_hash()
        {
            return Err(StoreProtocolError::Malformed(
                "Store publication reference differs from its entry".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedStoreSnapshotRef {
    pub snapshot: StoreSnapshotRef,
    pub publication: StorePublicationRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StorePublicationBase {
    Genesis,
    Snapshot(AcceptedStoreSnapshotRef),
}

impl StorePublicationBase {
    pub fn validate_for_store(
        &self,
        expected_store_root_hash: ObjectHash,
    ) -> Result<(), StoreProtocolError> {
        if let Self::Snapshot(snapshot) = self {
            crate::objects::verify_store_root(
                expected_store_root_hash,
                snapshot.publication.store_root_hash,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StorePublicationPayload {
    Commit(StoreBatchCommitRef),
    Snapshot {
        snapshot: StoreSnapshotRef,
        covers: StorePublicationRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePublicationEntryBody {
    pub store_root_hash: ObjectHash,
    pub position: StorePublicationPosition,
    pub predecessor: Option<StorePublicationRef>,
    pub previous_record_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub payload: StorePublicationPayload,
}

impl SignedBody for StorePublicationEntryBody {
    const DOMAIN: &'static [u8] = STORE_PUBLICATION_ENTRY_DOMAIN;
}

pub type StorePublicationEntry = Signed<StorePublicationEntryBody>;

impl StorePublicationEntry {
    pub fn signed_commit(
        current: &StoreCurrentPublicationRecord,
        commit: &VerifiedStoreBatchCommit,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let entry = Self::signed_payload(
            current,
            commit.author_registration.clone(),
            StorePublicationPayload::Commit(commit.reference().clone()),
            signer,
        )?;
        entry.validate_commit_against(current, commit, &keys::public_key_hex(signer))?;
        Ok(entry)
    }

    pub fn signed_snapshot(
        current: &StoreCurrentPublicationRecord,
        author_registration: StoreDeviceRegistrationRef,
        snapshot: StoreSnapshotRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let covers = current.accepted().cloned().ok_or_else(|| {
            StoreProtocolError::Malformed(
                "Store snapshot cannot cover an empty publication history".to_string(),
            )
        })?;
        Self::signed_payload(
            current,
            author_registration,
            StorePublicationPayload::Snapshot { snapshot, covers },
            signer,
        )
    }

    fn signed_payload(
        current: &StoreCurrentPublicationRecord,
        author_registration: StoreDeviceRegistrationRef,
        payload: StorePublicationPayload,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let position = current.next_position()?;
        let entry = Signed::sign(
            StorePublicationEntryBody {
                store_root_hash: current.store_root_hash,
                position,
                predecessor: current.accepted().cloned(),
                previous_record_hash: current.record_hash(),
                author_registration,
                payload,
            },
            signer,
        );
        entry.validate_against(current)?;
        Ok(entry)
    }

    pub fn entry_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        reference: &StorePublicationRef,
        expected_signing_pubkey: &str,
    ) -> Result<Self, StoreProtocolError> {
        let entry: Self = crate::objects::decode_protocol_object(bytes)?;
        entry.require_version()?;
        entry.verify_by(expected_signing_pubkey)?;
        if entry.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: entry.store_root_hash,
            });
        }
        reference.verify_entry(&entry)?;
        entry.validate_shape()?;
        Ok(entry)
    }

    fn validate_against(
        &self,
        current: &StoreCurrentPublicationRecord,
    ) -> Result<(), StoreProtocolError> {
        self.validate_shape()?;
        if self.store_root_hash != current.store_root_hash
            || self.predecessor.as_ref() != current.accepted()
            || self.position != current.next_position()?
            || self.previous_record_hash != current.record_hash()
        {
            return Err(StoreProtocolError::Malformed(
                "Store publication entry does not extend the current accepted boundary".to_string(),
            ));
        }
        if let StorePublicationPayload::Snapshot { covers, .. } = &self.payload {
            if Some(covers) != current.accepted() {
                return Err(StoreProtocolError::Malformed(
                    "Store snapshot does not cover its complete accepted predecessor".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn validate_commit_against(
        &self,
        current: &StoreCurrentPublicationRecord,
        commit: &VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        self.validate_against(current)?;
        let StorePublicationPayload::Commit(reference) = &self.payload else {
            return Err(StoreProtocolError::Malformed(
                "Store publication entry is not a commit".to_string(),
            ));
        };
        reference.verify_commit(commit.value())?;
        commit.value().verify_by(publisher_signing_pubkey)?;
        if commit.store_root_hash() != self.store_root_hash
            || commit.author_registration != self.author_registration
            || commit.publication_base() != &current.publication_base()
        {
            return Err(StoreProtocolError::Malformed(
                "Store commit differs from its accepted publication boundary".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_commit_against_record(
        &self,
        commit: &VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        self.validate_shape()?;
        let StorePublicationPayload::Commit(reference) = &self.payload else {
            return Err(StoreProtocolError::Malformed(
                "Store publication entry is not a commit".to_string(),
            ));
        };
        reference.verify_commit(commit.value())?;
        commit.value().verify_by(publisher_signing_pubkey)?;
        commit
            .publication_base()
            .validate_for_store(self.store_root_hash)?;
        if commit.store_root_hash() != self.store_root_hash
            || commit.author_registration != self.author_registration
        {
            return Err(StoreProtocolError::Malformed(
                "Store commit differs from its publication entry".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        self.position.get().checked_add(1).ok_or_else(|| {
            StoreProtocolError::Malformed("Store publication position overflow".to_string())
        })?;
        let expected_position = match &self.predecessor {
            Some(predecessor) => {
                if predecessor.store_root_hash != self.store_root_hash {
                    return Err(StoreProtocolError::StoreRootMismatch {
                        expected: self.store_root_hash,
                        actual: predecessor.store_root_hash,
                    });
                }
                predecessor.position.successor()?
            }
            None => StorePublicationPosition::new(1)?,
        };
        if self.position != expected_position {
            return Err(StoreProtocolError::Malformed(
                "Store publication entry position is not its predecessor's successor".to_string(),
            ));
        }
        match &self.payload {
            StorePublicationPayload::Snapshot { covers, .. }
                if Some(covers) != self.predecessor.as_ref() =>
            {
                return Err(StoreProtocolError::Malformed(
                    "Store snapshot does not cover its complete accepted predecessor".to_string(),
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StorePublicationPublisher {
    Founder { public_key: String },
    Device(StoreDeviceRegistrationRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StorePublicationState {
    Genesis,
    Accepted {
        entry: StorePublicationRef,
        latest_snapshot: Option<AcceptedStoreSnapshotRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCurrentPublicationRecordBody {
    pub store_root_hash: ObjectHash,
    pub previous_record_hash: Option<ObjectHash>,
    pub publisher: StorePublicationPublisher,
    pub state: StorePublicationState,
}

impl SignedBody for StoreCurrentPublicationRecordBody {
    const DOMAIN: &'static [u8] = STORE_CURRENT_PUBLICATION_DOMAIN;
}

pub type StoreCurrentPublicationRecord = Signed<StoreCurrentPublicationRecordBody>;

impl StoreCurrentPublicationRecord {
    pub fn genesis(store_root_hash: ObjectHash, founder: &UserKeypair) -> Self {
        Signed::sign(
            StoreCurrentPublicationRecordBody {
                store_root_hash,
                previous_record_hash: None,
                publisher: StorePublicationPublisher::Founder {
                    public_key: keys::public_key_hex(founder),
                },
                state: StorePublicationState::Genesis,
            },
            founder,
        )
    }

    pub fn advance_commit(
        previous: &Self,
        entry: &StorePublicationEntry,
        reference: StorePublicationRef,
        commit: &VerifiedStoreBatchCommit,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        entry.validate_commit_against(previous, commit, &keys::public_key_hex(signer))?;
        Self::advance(previous, entry, reference, signer)
    }

    pub fn advance_snapshot(
        previous: &Self,
        entry: &StorePublicationEntry,
        reference: StorePublicationRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        if !matches!(entry.payload, StorePublicationPayload::Snapshot { .. }) {
            return Err(StoreProtocolError::Malformed(
                "Store publication entry is not a snapshot".to_string(),
            ));
        }
        Self::advance(previous, entry, reference, signer)
    }

    fn advance(
        previous: &Self,
        entry: &StorePublicationEntry,
        reference: StorePublicationRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        entry.validate_against(previous)?;
        reference.verify_entry(entry)?;
        let latest_snapshot = match &entry.payload {
            StorePublicationPayload::Commit(_) => previous.latest_snapshot().cloned(),
            StorePublicationPayload::Snapshot { snapshot, .. } => Some(AcceptedStoreSnapshotRef {
                snapshot: snapshot.clone(),
                publication: reference.clone(),
            }),
        };
        Ok(Signed::sign(
            StoreCurrentPublicationRecordBody {
                store_root_hash: previous.store_root_hash,
                previous_record_hash: Some(previous.record_hash()),
                publisher: StorePublicationPublisher::Device(entry.author_registration.clone()),
                state: StorePublicationState::Accepted {
                    entry: reference,
                    latest_snapshot,
                },
            },
            signer,
        ))
    }

    pub fn record_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn accepted(&self) -> Option<&StorePublicationRef> {
        match &self.state {
            StorePublicationState::Genesis => None,
            StorePublicationState::Accepted { entry, .. } => Some(entry),
        }
    }

    pub fn latest_snapshot(&self) -> Option<&AcceptedStoreSnapshotRef> {
        match &self.state {
            StorePublicationState::Genesis => None,
            StorePublicationState::Accepted {
                latest_snapshot, ..
            } => latest_snapshot.as_ref(),
        }
    }

    pub fn publication_base(&self) -> StorePublicationBase {
        match self.latest_snapshot() {
            Some(snapshot) => StorePublicationBase::Snapshot(snapshot.clone()),
            None => StorePublicationBase::Genesis,
        }
    }

    pub fn next_position(&self) -> Result<StorePublicationPosition, StoreProtocolError> {
        match self.accepted() {
            Some(reference) => reference.position.successor(),
            None => StorePublicationPosition::new(1),
        }
    }

    pub fn verify_genesis(
        &self,
        expected_store_root_hash: ObjectHash,
        founder_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        if self.store_root_hash != expected_store_root_hash
            || self.previous_record_hash.is_some()
            || self.state != StorePublicationState::Genesis
            || self.publisher
                != (StorePublicationPublisher::Founder {
                    public_key: founder_pubkey.to_string(),
                })
        {
            return Err(StoreProtocolError::Malformed(
                "Store genesis publication record differs from its descriptor".to_string(),
            ));
        }
        self.verify_by(founder_pubkey)
    }

    pub fn verify_commit_transition(
        &self,
        previous: &Self,
        entry: &StorePublicationEntry,
        reference: &StorePublicationRef,
        commit: &VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        entry.validate_commit_against(previous, commit, publisher_signing_pubkey)?;
        self.verify_transition(previous, entry, reference, publisher_signing_pubkey)
    }

    fn verify_transition(
        &self,
        previous: &Self,
        entry: &StorePublicationEntry,
        reference: &StorePublicationRef,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        entry.validate_against(previous)?;
        reference.verify_entry(entry)?;
        self.verify_by(publisher_signing_pubkey)?;
        let expected_latest = match &entry.payload {
            StorePublicationPayload::Commit(_) => previous.latest_snapshot().cloned(),
            StorePublicationPayload::Snapshot { snapshot, .. } => Some(AcceptedStoreSnapshotRef {
                snapshot: snapshot.clone(),
                publication: reference.clone(),
            }),
        };
        let expected_state = StorePublicationState::Accepted {
            entry: reference.clone(),
            latest_snapshot: expected_latest,
        };
        if self.store_root_hash != previous.store_root_hash
            || self.previous_record_hash != Some(previous.record_hash())
            || self.publisher
                != StorePublicationPublisher::Device(entry.author_registration.clone())
            || self.state != expected_state
        {
            return Err(StoreProtocolError::Malformed(
                "Store current publication record differs from its accepted transition".to_string(),
            ));
        }
        Ok(())
    }

    pub fn verify_accepted_commit(
        &self,
        entry: &StorePublicationEntry,
        reference: &StorePublicationRef,
        commit: &VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        let base = commit.publication_base();
        entry.validate_commit_against_record(commit, publisher_signing_pubkey)?;
        self.verify_accepted_with_latest(
            entry,
            reference,
            match base {
                StorePublicationBase::Genesis => None,
                StorePublicationBase::Snapshot(snapshot) => Some(snapshot.clone()),
            },
            publisher_signing_pubkey,
        )
    }

    fn verify_accepted_with_latest(
        &self,
        entry: &StorePublicationEntry,
        reference: &StorePublicationRef,
        expected_latest: Option<AcceptedStoreSnapshotRef>,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        entry.validate_shape()?;
        reference.verify_entry(entry)?;
        self.verify_by(publisher_signing_pubkey)?;
        let expected_state = StorePublicationState::Accepted {
            entry: reference.clone(),
            latest_snapshot: expected_latest,
        };
        if self.store_root_hash != entry.store_root_hash
            || self.previous_record_hash != Some(entry.previous_record_hash)
            || self.publisher
                != StorePublicationPublisher::Device(entry.author_registration.clone())
            || self.state != expected_state
        {
            return Err(StoreProtocolError::Malformed(
                "Store current publication record differs from its accepted entry".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "publication_tests.rs"]
mod tests;
