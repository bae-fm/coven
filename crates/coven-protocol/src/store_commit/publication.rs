use super::*;

pub fn store_current_publication_semantic_prefix() -> &'static str {
    "store-v1/publications/current"
}

pub fn store_current_publication_logical_key() -> &'static str {
    "store-v1/publications/current.json"
}

pub fn store_publication_entry_semantic_prefix(entry: &StorePublicationEntry) -> String {
    publication_entry_prefix(entry.position, entry.entry_hash())
}

fn publication_entry_prefix(position: StorePublicationPosition, hash: ObjectHash) -> String {
    format!("store-v1/publications/entries/{}/{hash}", position.get())
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
    pub fn validate_slot(&self) -> Result<(), StoreProtocolError> {
        let expected = format!(
            "{}.json",
            publication_entry_prefix(self.position, self.entry_hash)
        );
        if self.object.slot().logical_key() != expected {
            return Err(StoreProtocolError::RelocatedSlot {
                expected,
                actual: self.object.slot().logical_key().into(),
            });
        }
        Ok(())
    }

    pub fn from_entry(
        entry: &StorePublicationEntry,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        entry.validate_shape()?;
        object.verify(&entry.to_bytes())?;
        let expected_key = format!("{}.json", store_publication_entry_semantic_prefix(entry));
        if object.slot().logical_key() != expected_key {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: expected_key,
                actual: object.slot().logical_key().to_string(),
            });
        }
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
    Snapshot(StoreSnapshotRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePublicationEntryBody {
    pub store_root_hash: ObjectHash,
    pub position: StorePublicationPosition,
    pub predecessor: Option<StorePublicationRef>,
    pub previous_state_hash: ObjectHash,
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
        Self::signed_payload(
            current,
            author_registration,
            StorePublicationPayload::Snapshot(snapshot),
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
                previous_state_hash: current.state_hash(),
                author_registration,
                payload,
            },
            signer,
        );
        entry.validate_against(current.body())?;
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
        current: &StoreCurrentPublicationRecordBody,
    ) -> Result<(), StoreProtocolError> {
        self.validate_shape()?;
        if self.store_root_hash != current.store_root_hash
            || self.predecessor.as_ref() != current.accepted()
            || self.position != current.next_position()?
            || self.previous_state_hash != current.state_hash()
        {
            return Err(StoreProtocolError::Malformed(
                "Store publication entry does not extend the current accepted boundary".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_commit_against(
        &self,
        current: &StoreCurrentPublicationRecord,
        commit: &VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        self.validate_against(current.body())?;
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

    pub fn verify_published_commit(
        &self,
        commit: &VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        self.validate_commit_against_record(commit, publisher_signing_pubkey)
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
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreCommitPublication {
    entry: StorePublicationEntry,
    reference: StorePublicationRef,
}

impl StoreCommitPublication {
    pub fn verified(
        entry: StorePublicationEntry,
        reference: StorePublicationRef,
        commit: &VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<Self, StoreProtocolError> {
        reference.verify_entry(&entry)?;
        entry.verify_published_commit(commit, publisher_signing_pubkey)?;
        Ok(Self { entry, reference })
    }

    pub fn entry(&self) -> &StorePublicationEntry {
        &self.entry
    }

    pub fn reference(&self) -> &StorePublicationRef {
        &self.reference
    }

    pub fn into_parts(self) -> (StorePublicationEntry, StorePublicationRef) {
        (self.entry, self.reference)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePublicationIntervalEntry {
    entry: StorePublicationEntry,
    reference: StorePublicationRef,
    author: ReferencedStoreDeviceRegistration,
}

impl StorePublicationIntervalEntry {
    pub fn new(
        entry: StorePublicationEntry,
        reference: StorePublicationRef,
        author: ReferencedStoreDeviceRegistration,
    ) -> Self {
        Self {
            entry,
            reference,
            author,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedStorePublicationEntry {
    entry: StorePublicationEntry,
    reference: StorePublicationRef,
    author: ReferencedStoreDeviceRegistration,
}

impl AcceptedStorePublicationEntry {
    pub fn entry(&self) -> &StorePublicationEntry {
        &self.entry
    }

    pub fn reference(&self) -> &StorePublicationRef {
        &self.reference
    }

    pub fn author(&self) -> &ReferencedStoreDeviceRegistration {
        &self.author
    }

    fn accepted_commit(
        &self,
        commit: &VerifiedStoreBatchCommit,
    ) -> Result<AcceptedStoreCommitPublication, StoreProtocolError> {
        let publication = StoreCommitPublication::verified(
            self.entry.clone(),
            self.reference.clone(),
            commit,
            &self.author.value().device_signing_pubkey,
        )?;
        Ok(AcceptedStoreCommitPublication { publication })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedStoreCommitPublication {
    publication: StoreCommitPublication,
}

impl AcceptedStoreCommitPublication {
    pub fn entry(&self) -> &StorePublicationEntry {
        self.publication.entry()
    }

    pub fn reference(&self) -> &StorePublicationRef {
        self.publication.reference()
    }

    pub fn into_publication(self) -> StoreCommitPublication {
        self.publication
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedStorePublicationInterval {
    previous: StoreCurrentPublicationRecordBody,
    current: StoreCurrentPublicationRecord,
    entries: Vec<AcceptedStorePublicationEntry>,
}

impl VerifiedStorePublicationInterval {
    /// Continue from an already authenticated record. An empty interval must
    /// preserve that exact record, including its signature.
    pub fn verified(
        previous: StoreCurrentPublicationRecord,
        current: StoreCurrentPublicationRecord,
        entries: Vec<StorePublicationIntervalEntry>,
    ) -> Result<Self, StoreProtocolError> {
        if entries.is_empty() {
            if current != previous {
                return Err(StoreProtocolError::Malformed(
                    "empty Store publication interval changes the accepted boundary".to_string(),
                ));
            }
            return Ok(Self {
                previous: previous.body().clone(),
                current,
                entries: Vec::new(),
            });
        }

        Self::from_nonempty_history(previous.body().clone(), current, entries)
    }

    /// A carried historical interval authenticates its starting state through
    /// the first entry's state hash and its terminal signed record. It carries
    /// no provider observation or conditional publication authority.
    pub fn from_nonempty_history(
        previous: StoreCurrentPublicationRecordBody,
        current: StoreCurrentPublicationRecord,
        entries: Vec<StorePublicationIntervalEntry>,
    ) -> Result<Self, StoreProtocolError> {
        if entries.is_empty() {
            return Err(StoreProtocolError::Malformed(
                "carried Store publication history has no entries".into(),
            ));
        }
        Self::fold(previous, current, entries)
    }

    /// Start from the pinned Store root without requiring its founder's private
    /// key. An empty Store still authenticates the actual remote genesis record.
    pub fn from_genesis(
        store_root_hash: ObjectHash,
        founder_pubkey: &str,
        current: StoreCurrentPublicationRecord,
        entries: Vec<StorePublicationIntervalEntry>,
    ) -> Result<Self, StoreProtocolError> {
        let previous = StoreCurrentPublicationRecordBody::genesis(store_root_hash);
        if entries.is_empty() {
            current.verify_genesis(store_root_hash, founder_pubkey)?;
            return Ok(Self {
                previous,
                current,
                entries: Vec::new(),
            });
        }
        Self::fold(previous, current, entries)
    }

    fn fold(
        previous: StoreCurrentPublicationRecordBody,
        current: StoreCurrentPublicationRecord,
        entries: Vec<StorePublicationIntervalEntry>,
    ) -> Result<Self, StoreProtocolError> {
        let mut folded = previous.clone();
        let mut accepted = Vec::with_capacity(entries.len());
        let mut commit_coordinates = std::collections::BTreeSet::new();
        let mut final_publisher = None;
        for candidate in entries {
            let registration_bytes = candidate.author.value().to_bytes();
            candidate
                .author
                .reference()
                .object
                .verify(&registration_bytes)?;
            let parsed_registration = StoreDeviceRegistration::parse_at(
                &registration_bytes,
                &candidate.author.value().store_root,
                candidate.author.reference().device_id,
            )?;
            candidate
                .author
                .reference()
                .verify_registration(&parsed_registration)?;
            if parsed_registration != *candidate.author.value()
                || candidate.author.value().store_root.store_root_hash != folded.store_root_hash
                || candidate.entry.author_registration != *candidate.author.reference()
            {
                return Err(StoreProtocolError::Malformed(
                    "Store publication entry differs from its exact author registration"
                        .to_string(),
                ));
            }
            let parsed_entry = StorePublicationEntry::parse_at(
                &candidate.entry.to_bytes(),
                folded.store_root_hash,
                &candidate.reference,
                &candidate.author.value().device_signing_pubkey,
            )?;
            if parsed_entry != candidate.entry {
                return Err(StoreProtocolError::Malformed(
                    "Store publication entry differs from its canonical bytes".to_string(),
                ));
            }
            if let StorePublicationPayload::Commit(commit) = &candidate.entry.payload {
                if !commit_coordinates.insert(commit.coord.clone()) {
                    return Err(StoreProtocolError::Malformed(
                        "Store publication interval repeats an author sequence".to_string(),
                    ));
                }
            }
            folded = folded.advance(&candidate.entry, candidate.reference.clone())?;
            final_publisher = Some(candidate.author.value().device_signing_pubkey.clone());
            accepted.push(AcceptedStorePublicationEntry {
                entry: candidate.entry,
                reference: candidate.reference,
                author: candidate.author,
            });
        }

        let final_publisher = final_publisher.expect("a non-empty interval has a publisher");
        current.verify_by(&final_publisher)?;
        if current.body() != &folded {
            return Err(StoreProtocolError::Malformed(
                "Store current publication record differs from its verified interval".to_string(),
            ));
        }
        Ok(Self {
            previous,
            current,
            entries: accepted,
        })
    }

    /// The authenticated starting state, derived from the prior record or root.
    pub fn previous(&self) -> &StoreCurrentPublicationRecordBody {
        &self.previous
    }

    pub fn current(&self) -> &StoreCurrentPublicationRecord {
        &self.current
    }

    pub fn entries(&self) -> &[AcceptedStorePublicationEntry] {
        &self.entries
    }

    pub fn accepted_commit(
        &self,
        commit: &VerifiedStoreBatchCommit,
    ) -> Result<AcceptedStoreCommitPublication, StoreProtocolError> {
        let mut base = self.previous.publication_base();
        for entry in &self.entries {
            match &entry.entry.payload {
                StorePublicationPayload::Snapshot(snapshot) => {
                    base = StorePublicationBase::Snapshot(AcceptedStoreSnapshotRef {
                        snapshot: snapshot.clone(),
                        publication: entry.reference.clone(),
                    });
                }
                StorePublicationPayload::Commit(reference) if reference == commit.reference() => {
                    if commit.publication_base() != &base {
                        return Err(StoreProtocolError::Malformed(
                            "Store commit differs from the snapshot base at its accepted publication"
                                .to_string(),
                        ));
                    }
                    return entry.accepted_commit(commit);
                }
                StorePublicationPayload::Commit(_) => {}
            }
        }
        Err(StoreProtocolError::Malformed(
            "Store commit is absent from the accepted publication interval".to_string(),
        ))
    }
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
    pub state: StorePublicationState,
}

impl StoreCurrentPublicationRecordBody {
    pub fn genesis(store_root_hash: ObjectHash) -> Self {
        Self {
            store_root_hash,
            state: StorePublicationState::Genesis,
        }
    }

    pub fn state_hash(&self) -> ObjectHash {
        super::signed::signed_body_hash(STORE_PROTOCOL_VERSION, self)
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

    fn advance(
        &self,
        entry: &StorePublicationEntry,
        reference: StorePublicationRef,
    ) -> Result<Self, StoreProtocolError> {
        entry.validate_against(self)?;
        reference.verify_entry(entry)?;
        let latest_snapshot = match &entry.payload {
            StorePublicationPayload::Commit(_) => self.latest_snapshot().cloned(),
            StorePublicationPayload::Snapshot(snapshot) => Some(AcceptedStoreSnapshotRef {
                snapshot: snapshot.clone(),
                publication: reference.clone(),
            }),
        };
        Ok(Self {
            store_root_hash: self.store_root_hash,
            state: StorePublicationState::Accepted {
                entry: reference,
                latest_snapshot,
            },
        })
    }
}

impl SignedBody for StoreCurrentPublicationRecordBody {
    const DOMAIN: &'static [u8] = STORE_CURRENT_PUBLICATION_DOMAIN;
}

pub type StoreCurrentPublicationRecord = Signed<StoreCurrentPublicationRecordBody>;

impl StoreCurrentPublicationRecord {
    pub fn state_hash(&self) -> ObjectHash {
        self.body().state_hash()
    }

    pub fn genesis(store_root_hash: ObjectHash, founder: &UserKeypair) -> Self {
        Signed::sign(
            StoreCurrentPublicationRecordBody::genesis(store_root_hash),
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
        if !matches!(entry.payload, StorePublicationPayload::Snapshot(_)) {
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
        Ok(Signed::sign(
            previous.body().advance(entry, reference)?,
            signer,
        ))
    }

    pub fn record_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn accepted(&self) -> Option<&StorePublicationRef> {
        self.body().accepted()
    }

    pub fn latest_snapshot(&self) -> Option<&AcceptedStoreSnapshotRef> {
        self.body().latest_snapshot()
    }

    pub fn publication_base(&self) -> StorePublicationBase {
        self.body().publication_base()
    }

    pub fn next_position(&self) -> Result<StorePublicationPosition, StoreProtocolError> {
        self.body().next_position()
    }

    pub fn verify_genesis(
        &self,
        expected_store_root_hash: ObjectHash,
        founder_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        if self.store_root_hash != expected_store_root_hash
            || self.state != StorePublicationState::Genesis
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
        entry.validate_against(previous.body())?;
        reference.verify_entry(entry)?;
        self.verify_by(publisher_signing_pubkey)?;
        let expected_latest = match &entry.payload {
            StorePublicationPayload::Commit(_) => previous.latest_snapshot().cloned(),
            StorePublicationPayload::Snapshot(snapshot) => Some(AcceptedStoreSnapshotRef {
                snapshot: snapshot.clone(),
                publication: reference.clone(),
            }),
        };
        let expected_state = StorePublicationState::Accepted {
            entry: reference.clone(),
            latest_snapshot: expected_latest,
        };
        if self.store_root_hash != previous.store_root_hash || self.state != expected_state {
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
        if self.store_root_hash != entry.store_root_hash || self.state != expected_state {
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
