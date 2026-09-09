mod retained_materialization;

use crate::DbError;
use coven_protocol::objects::ExactObjectVersion;
use coven_protocol::store_commit::StoreCurrentPublicationRecord;
use rusqlite::OptionalExtension;

use super::StoreSession;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedStorePublication {
    record: StoreCurrentPublicationRecord,
    version: ExactObjectVersion,
}

/// An installed accepted prefix may be handed to a joining device without a
/// provider observation. Only an observed boundary can start a conditional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorePublicationBoundary {
    AcceptedPrefix(StoreCurrentPublicationRecord),
    Observed(ObservedStorePublication),
}

impl StorePublicationBoundary {
    pub fn record(&self) -> &StoreCurrentPublicationRecord {
        match self {
            Self::AcceptedPrefix(record) => record,
            Self::Observed(observed) => observed.record(),
        }
    }

    pub fn observed_version(&self) -> Option<&ExactObjectVersion> {
        match self {
            Self::AcceptedPrefix(_) => None,
            Self::Observed(observed) => Some(observed.version()),
        }
    }

    pub fn require_observed(&self) -> Result<&ObservedStorePublication, DbError> {
        match self {
            Self::Observed(observed) => Ok(observed),
            Self::AcceptedPrefix(_) => Err(DbError::Message(
                "conditional Store publication requires a current provider observation".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedStoreCommitPublication {
    publication: coven_protocol::store_commit::StoreCommitPublication,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedStoreCommitEvidence {
    acceptance: StoreCommitAcceptance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StoreCommitAcceptance {
    Exact(AcceptedStoreCommitPublication),
    SnapshotCovered {
        commit: coven_protocol::store_commit::StoreBatchCommitRef,
        snapshot: coven_protocol::store_commit::StoreSnapshotRef,
    },
}

impl AcceptedStoreCommitEvidence {
    pub(crate) fn from_snapshot(
        authority: &coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
        commit: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<Self, DbError> {
        authority.validate()?;
        if !authority.metadata.coverage.covers_commit(commit)
            || authority
                .metadata
                .history_summary
                .causal_cut
                .get(&commit.coord)
                != Some(commit)
        {
            return Err(DbError::Message(
                "retained Store commit is absent from its exact snapshot history".to_string(),
            ));
        }
        Ok(Self {
            acceptance: StoreCommitAcceptance::SnapshotCovered {
                commit: commit.clone(),
                snapshot: authority.snapshot.clone(),
            },
        })
    }

    pub fn commit_ref(&self) -> &coven_protocol::store_commit::StoreBatchCommitRef {
        match &self.acceptance {
            StoreCommitAcceptance::Exact(publication) => match &publication.entry().payload {
                coven_protocol::store_commit::StorePublicationPayload::Commit(commit) => commit,
                coven_protocol::store_commit::StorePublicationPayload::Snapshot(_) => {
                    unreachable!("verified commit publication contains a commit")
                }
            },
            StoreCommitAcceptance::SnapshotCovered { commit, .. } => commit,
        }
    }

    pub fn exact_publication(&self) -> Option<&AcceptedStoreCommitPublication> {
        match &self.acceptance {
            StoreCommitAcceptance::Exact(publication) => Some(publication),
            StoreCommitAcceptance::SnapshotCovered { .. } => None,
        }
    }
}

impl From<AcceptedStoreCommitPublication> for AcceptedStoreCommitEvidence {
    fn from(publication: AcceptedStoreCommitPublication) -> Self {
        Self {
            acceptance: StoreCommitAcceptance::Exact(publication),
        }
    }
}

impl AcceptedStoreCommitPublication {
    pub fn from_verified(
        accepted: coven_protocol::store_commit::AcceptedStoreCommitPublication,
    ) -> Self {
        Self {
            publication: accepted.into_publication(),
        }
    }

    pub fn entry(&self) -> &coven_protocol::store_commit::StorePublicationEntry {
        self.publication.entry()
    }

    pub fn reference(&self) -> &coven_protocol::store_commit::StorePublicationRef {
        self.publication.reference()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedStorePublicationInterval {
    interval: coven_protocol::store_commit::VerifiedStorePublicationInterval,
    current_version: Option<ExactObjectVersion>,
}

/// Whether this publisher advanced the provider or found its commit already installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreCommitPublicationOutcome {
    Accepted {
        interval: AcceptedStorePublicationInterval,
        accepted_predecessor: coven_protocol::membership::MembershipFloor,
    },
    Installed(AcceptedStoreCommitEvidence),
}

impl StoreCommitPublicationOutcome {
    /// Resolve against the installation transaction: pull may install the
    /// commit or replace its retained acceptance proof with a covering snapshot
    /// after the publisher obtained its outcome.
    pub(super) fn resolve_installed_on(
        self,
        store: super::StoreTransaction<'_, '_>,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<Self, DbError> {
        let expected = match &self {
            Self::Accepted { interval, .. } => interval.accepted_commit(commit)?.into(),
            Self::Installed(evidence) => {
                if evidence.commit_ref() != commit.reference() {
                    return Err(DbError::Message(
                        "Store publication receipt belongs to another commit".to_string(),
                    ));
                }
                evidence.clone()
            }
        };
        match installed_store_commit_evidence_on(store, commit)? {
            Some(installed) => {
                if let (Some(current), Some(expected)) =
                    (installed.exact_publication(), expected.exact_publication())
                {
                    if current != expected {
                        return Err(DbError::Message(
                            "installed Store commit has a different accepted publication"
                                .to_string(),
                        ));
                    }
                }
                Ok(Self::Installed(installed))
            }
            None => match self {
                Self::Accepted { .. } => Ok(self),
                Self::Installed(_) => Err(DbError::Message(
                    "Store publication receipt has no installed commit authority".to_string(),
                )),
            },
        }
    }

    pub(super) fn requires_materialization(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    pub(super) fn install_on(
        &self,
        store: super::StoreTransaction<'_, '_>,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<AcceptedStoreCommitEvidence, DbError> {
        match self {
            Self::Accepted { interval, .. } => {
                install_accepted_store_commit_interval_on(store.transaction, interval, commit)
                    .map(Into::into)
            }
            Self::Installed(evidence) => {
                let installed = installed_store_commit_evidence_on(store, commit)?;
                if installed.as_ref() != Some(evidence) {
                    return Err(DbError::Message(
                        "Store publication receipt differs from installed commit authority"
                            .to_string(),
                    ));
                }
                Ok(evidence.clone())
            }
        }
    }
}

fn installed_store_commit_evidence_on(
    store: super::StoreTransaction<'_, '_>,
    commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
) -> Result<Option<AcceptedStoreCommitEvidence>, DbError> {
    let connection = store.transaction;
    let current = load_store_current_publication_on(connection)?;
    if current.record().store_root_hash != commit.store_root_hash() {
        return Err(DbError::Message(
            "Store commit belongs to another installed Store".to_string(),
        ));
    }
    let reference = commit.reference();
    let stream_id = reference.coord.stream_id.to_string();
    let coverage = super::materialized_commit_index::snapshot_coverage_on(connection)?;
    if coverage
        .get(&stream_id)
        .is_some_and(|tip| reference.coord.sequence() <= tip.coord.sequence())
    {
        let baseline = super::retained_replay::load_replay_baseline_metadata_on(
            super::StoreRecords::new(connection, store.store_dir),
        )?
        .ok_or_else(|| {
            DbError::Message("covered Store commit has no installed replay baseline".to_string())
        })?;
        let crate::RetainedReplayAuthority::InstalledSnapshot(authority) = baseline.authority
        else {
            return Err(DbError::Message(
                "covered Store commit has a genesis replay baseline".to_string(),
            ));
        };
        return AcceptedStoreCommitEvidence::from_snapshot(&authority, reference).map(Some);
    }
    let installed = super::materialized_commit_index::materialized_commit_ref_on(
        connection,
        &stream_id,
        reference.coord.sequence(),
    )?;
    match installed {
        Some(installed) if installed == *reference => load_accepted_store_commit_on(
            connection,
            commit,
            &commit.author().device_signing_pubkey,
        )
        .map(|accepted| Some(accepted.into())),
        Some(_) => Err(DbError::Message(
            "Store commit coordinate is installed with another exact commit".to_string(),
        )),
        None => Ok(None),
    }
}

impl AcceptedStorePublicationInterval {
    pub fn from_verified(
        interval: coven_protocol::store_commit::VerifiedStorePublicationInterval,
        current_version: Option<ExactObjectVersion>,
    ) -> Self {
        Self {
            interval,
            current_version,
        }
    }

    pub fn interval(&self) -> &coven_protocol::store_commit::VerifiedStorePublicationInterval {
        &self.interval
    }

    pub fn current_version(&self) -> Option<&ExactObjectVersion> {
        self.current_version.as_ref()
    }

    pub fn accepted_commit(
        &self,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<AcceptedStoreCommitPublication, coven_protocol::store_commit::StoreProtocolError>
    {
        self.interval
            .accepted_commit(commit)
            .map(AcceptedStoreCommitPublication::from_verified)
    }
}

impl ObservedStorePublication {
    pub fn from_parts(record: StoreCurrentPublicationRecord, version: ExactObjectVersion) -> Self {
        Self { record, version }
    }

    pub fn verified_genesis(
        record: StoreCurrentPublicationRecord,
        version: ExactObjectVersion,
        expected_store_root_hash: coven_protocol::store_commit::ObjectHash,
        founder_pubkey: &str,
    ) -> Result<Self, coven_protocol::store_commit::StoreProtocolError> {
        record.verify_genesis(expected_store_root_hash, founder_pubkey)?;
        Ok(Self { record, version })
    }

    pub fn record(&self) -> &StoreCurrentPublicationRecord {
        &self.record
    }

    pub fn version(&self) -> &ExactObjectVersion {
        &self.version
    }

    pub fn verified_commit_successor(
        previous: &Self,
        record: StoreCurrentPublicationRecord,
        version: ExactObjectVersion,
        entry: &coven_protocol::store_commit::StorePublicationEntry,
        reference: &coven_protocol::store_commit::StorePublicationRef,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        publisher_signing_pubkey: &str,
    ) -> Result<Self, coven_protocol::store_commit::StoreProtocolError> {
        record.verify_commit_transition(
            &previous.record,
            entry,
            reference,
            commit,
            publisher_signing_pubkey,
        )?;
        Ok(Self { record, version })
    }
}

pub(super) fn load_store_current_publication_on(
    connection: &rusqlite::Connection,
) -> Result<StorePublicationBoundary, DbError> {
    load_store_publication_boundary_on(connection)?
        .ok_or_else(|| DbError::Message("Store publication current record is absent".to_string()))
}

fn load_store_publication_boundary_on(
    connection: &rusqlite::Connection,
) -> Result<Option<StorePublicationBoundary>, DbError> {
    connection
        .query_row(
            "SELECT record_hash, record_bytes, provider_version
             FROM store_publication_current WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(DbError::from)?
        .map(|(hash, bytes, version)| {
            let record: StoreCurrentPublicationRecord = serde_json::from_slice(&bytes)
                .map_err(|error| DbError::context("Store current publication record", error))?;
            if record.to_bytes() != bytes || record.record_hash().to_string() != hash {
                return Err(DbError::Message(
                    "Store current publication row differs from its canonical record".to_string(),
                ));
            }
            match version {
                Some(version) => Ok(StorePublicationBoundary::Observed(
                    ObservedStorePublication {
                        record,
                        version: ExactObjectVersion::from_provider(version)
                            .map_err(DbError::from)?,
                    },
                )),
                None => Ok(StorePublicationBoundary::AcceptedPrefix(record)),
            }
        })
        .transpose()
}

fn require_unobserved_genesis_publication(
    connection: &rusqlite::Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> Result<(), DbError> {
    let baseline = super::retained_replay::load_generation_zero_replay_baseline_on(
        super::StoreRecords::new(connection, store_dir),
    )?
    .ok_or_else(|| DbError::Message("unobserved Store has no replay baseline".to_string()))?;
    let crate::RetainedReplayAuthority::Genesis(_) = &baseline.authority else {
        return Err(DbError::Message(
            "installed Store snapshot has no publication boundary".to_string(),
        ));
    };
    if !baseline.exact_cut.commits().is_empty() {
        return Err(DbError::Message(
            "only an unopened genesis history may lack its publication boundary".to_string(),
        ));
    }
    let has_accepted_history: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM materialized_commits)
             OR EXISTS(SELECT 1 FROM retained_merge_materializations)
             OR EXISTS(SELECT 1 FROM snapshot_coverage)
             OR EXISTS(SELECT 1 FROM store_publication_entries)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if has_accepted_history {
        return Err(DbError::Message(
            "accepted Store history has no publication boundary".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn observe_store_publication_interval_on(
    store: super::StoreTransaction<'_, '_>,
    accepted: &AcceptedStorePublicationInterval,
) -> Result<(), DbError> {
    let expected = load_store_publication_boundary_on(store.transaction)?;
    match &expected {
        Some(expected) => {
            if accepted.interval().previous() != &**expected.record()
                && accepted.interval().current() != expected.record()
            {
                // The pull captured its previous boundary before reading packages.
                // Restart against the concurrent installation before making any
                // durable changes, including acceptance and replay retirement.
                return Err(DbError::StorePublicationChanged);
            }
            install_store_publication_interval_on(store.transaction, expected, accepted)
        }
        None => {
            require_unobserved_genesis_publication(store.transaction, store.store_dir)?;
            let (root, protocol) = crate::load_store_root_authority_on(store.transaction)?
                .ok_or(DbError::StoreRootHashMissing)?;
            if accepted.interval().previous()
                != &coven_protocol::store_commit::StoreCurrentPublicationRecordBody::genesis(
                    root.store_root_hash,
                )
            {
                return Err(DbError::Message(
                    "initial Store publication interval does not start at its rooted genesis"
                        .to_string(),
                ));
            }
            if accepted.interval().entries().is_empty() {
                accepted
                    .interval()
                    .current()
                    .verify_genesis(root.store_root_hash, &protocol.descriptor.founder_pubkey)?;
            }
            persist_store_publication_interval_on(store.transaction, None, accepted)
        }
    }
}

pub(super) fn install_genesis_store_publication_on(
    transaction: &rusqlite::Transaction<'_>,
    record: &StoreCurrentPublicationRecord,
    version: &ExactObjectVersion,
) -> Result<(), DbError> {
    if record.accepted().is_some() {
        return Err(DbError::Message(
            "initial Store publication record is not genesis".to_string(),
        ));
    }
    let bytes = record.to_bytes();
    let inserted = transaction
        .execute(
            "INSERT INTO store_publication_current
             (singleton, record_hash, record_bytes, provider_version)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(singleton) DO NOTHING",
            rusqlite::params![
                record.record_hash().to_string(),
                bytes,
                version.as_provider()
            ],
        )
        .map_err(DbError::from)?;
    if inserted == 0 {
        let current = load_store_current_publication_on(transaction)?;
        if current.record() != record || current.observed_version() != Some(version) {
            return Err(DbError::Message(
                "Store publication genesis differs from installed current record".to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) fn install_store_publication_interval_on(
    transaction: &rusqlite::Transaction<'_>,
    expected: &StorePublicationBoundary,
    accepted: &AcceptedStorePublicationInterval,
) -> Result<(), DbError> {
    if accepted.interval().current() == expected.record() {
        // Another pull can install this exact interval while its packages are
        // loading. Its authenticated tip already commits to the same entry
        // chain; preserve the installed provider revision and retained prefix.
        if load_store_current_publication_on(transaction)? != *expected {
            return Err(DbError::Message(
                "Store publication boundary changed before local completion".to_string(),
            ));
        }
        load_store_publication_entries_on(transaction)?;
        if expected.observed_version().is_none() {
            if let Some(version) = accepted.current_version() {
                let changed = transaction.execute(
                    "UPDATE store_publication_current SET provider_version = ?1
                     WHERE singleton = 1 AND record_hash = ?2 AND record_bytes = ?3
                       AND provider_version IS NULL",
                    rusqlite::params![
                        version.as_provider(),
                        expected.record().record_hash().to_string(),
                        expected.record().to_bytes()
                    ],
                )?;
                if changed != 1 {
                    return Err(DbError::StorePublicationChanged);
                }
            }
        }
        return Ok(());
    }
    if accepted.interval().previous() != &**expected.record() {
        return Err(DbError::Message(
            "Store publication interval starts from another local boundary".to_string(),
        ));
    }
    if accepted.interval().entries().is_empty()
        && accepted.interval().current() != expected.record()
    {
        return Err(DbError::Message(
            "empty Store publication interval changes its authenticated current record".to_string(),
        ));
    }
    persist_store_publication_interval_on(transaction, Some(expected), accepted)
}

fn persist_store_publication_interval_on(
    transaction: &rusqlite::Transaction<'_>,
    expected: Option<&StorePublicationBoundary>,
    accepted: &AcceptedStorePublicationInterval,
) -> Result<(), DbError> {
    let retained = load_store_publication_entry_objects_on(transaction)?;
    let mut coordinates = std::collections::BTreeMap::new();
    for entry in &retained {
        if let coven_protocol::store_commit::StorePublicationPayload::Commit(commit) =
            &entry.value.payload
        {
            if coordinates
                .insert(commit.coord.clone(), entry.prepared.reference())
                .is_some()
            {
                return Err(DbError::Message(
                    "retained Store publications repeat an author sequence".to_string(),
                ));
            }
        }
    }
    let coverage = super::materialized_commit_index::snapshot_coverage_on(transaction)?;
    for accepted_entry in accepted.interval().entries() {
        let reference = accepted_entry.reference();
        if let coven_protocol::store_commit::StorePublicationPayload::Commit(commit) =
            &accepted_entry.entry().payload
        {
            match coordinates.get(&commit.coord) {
                Some(existing) if **existing == reference.object => {}
                Some(_) => {
                    return Err(DbError::Message(
                        "Store publication reuses an already accepted author sequence".to_string(),
                    ));
                }
                None => {
                    if coverage
                        .get(&commit.coord.stream_id.to_string())
                        .is_some_and(|tip| commit.coord.sequence() <= tip.coord.sequence())
                    {
                        return Err(DbError::Message(
                            "Store publication reuses a snapshot-covered author sequence"
                                .to_string(),
                        ));
                    }
                }
            }
        }
        let position = i64::try_from(reference.position.get()).map_err(|_| {
            DbError::Message("Store publication position exceeds SQLite integer".to_string())
        })?;
        let encoded_reference = serde_json::to_string(reference)
            .map_err(|error| DbError::context("Store publication reference", error))?;
        let entry_bytes = accepted_entry.entry().to_bytes();
        let inserted = transaction
            .execute(
                "INSERT INTO store_publication_entries (position, entry_ref, entry_bytes)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(position) DO NOTHING",
                rusqlite::params![position, encoded_reference, entry_bytes],
            )
            .map_err(DbError::from)?;
        if inserted == 0 {
            let existing: (String, Vec<u8>) = transaction
                .query_row(
                    "SELECT entry_ref, entry_bytes FROM store_publication_entries
                     WHERE position = ?1",
                    [position],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            if existing != (encoded_reference, entry_bytes) {
                return Err(DbError::Message(
                    "Store publication position already retains another entry".to_string(),
                ));
            }
        }
    }
    let current = accepted.interval().current();
    let current_bytes = current.to_bytes();
    let updated = match expected {
        Some(expected) => transaction
            .execute(
                "UPDATE store_publication_current
             SET record_hash = ?1, record_bytes = ?2, provider_version = ?3
             WHERE singleton = 1 AND record_hash = ?4 AND record_bytes = ?5
               AND provider_version IS ?6",
                rusqlite::params![
                    current.record_hash().to_string(),
                    current_bytes,
                    accepted
                        .current_version()
                        .map(ExactObjectVersion::as_provider),
                    expected.record().record_hash().to_string(),
                    expected.record().to_bytes(),
                    expected
                        .observed_version()
                        .map(ExactObjectVersion::as_provider),
                ],
            )
            .map_err(DbError::from)?,
        None => transaction
            .execute(
                "INSERT INTO store_publication_current
             (singleton, record_hash, record_bytes, provider_version)
             VALUES (1, ?1, ?2, ?3)",
                rusqlite::params![
                    current.record_hash().to_string(),
                    current_bytes,
                    accepted
                        .current_version()
                        .map(ExactObjectVersion::as_provider),
                ],
            )
            .map_err(DbError::from)?,
    };
    if updated != 1 {
        return Err(DbError::Message(
            "Store publication boundary changed before local completion".to_string(),
        ));
    }
    load_store_publication_entries_on(transaction)?;
    Ok(())
}

/// Replace a retired publication prefix using an independently authenticated
/// checkpoint. The checkpoint is the replay authority; this does not assert
/// acceptance of any individual commit omitted by its retained interval.
pub(super) fn install_store_checkpoint_publication_on(
    transaction: &rusqlite::Transaction<'_>,
    expected: &StorePublicationBoundary,
    accepted: &AcceptedStorePublicationInterval,
    checkpoint: &coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
) -> Result<(), DbError> {
    use coven_protocol::store_commit::{StorePublicationPayload, StorePublicationRef};

    if load_store_current_publication_on(transaction)? != *expected {
        return Err(DbError::Message(
            "Store publication changed while the checkpoint image was being prepared".into(),
        ));
    }
    let interval = accepted.interval();
    let first = interval.entries().first().ok_or_else(|| {
        DbError::Message("checkpoint installation has no accepted snapshot entry".into())
    })?;
    if checkpoint.store_root.store_root_hash != expected.record().store_root_hash
        || interval.previous() != &*checkpoint.metadata.publication_predecessor
        || first.entry().payload != StorePublicationPayload::Snapshot(checkpoint.snapshot.clone())
        || interval.current().latest_snapshot().is_none_or(|snapshot| {
            snapshot.snapshot != checkpoint.snapshot || &snapshot.publication != first.reference()
        })
    {
        return Err(DbError::Message(
            "checkpoint image and accepted publication name different Store boundaries".into(),
        ));
    }
    if let Some(previous) = expected.record().accepted() {
        let current = interval.current().accepted().ok_or_else(|| {
            DbError::Message("an accepted checkpoint cannot return the Store to genesis".into())
        })?;
        if current.position < previous.position
            || (current.position == previous.position && interval.current() != expected.record())
        {
            return Err(DbError::Message(
                "checkpoint publication regresses or conflicts with the observed Store boundary"
                    .into(),
            ));
        }
    }
    for local in load_store_publication_entries_on(transaction)? {
        if local.value.position < first.reference().position {
            continue;
        }
        let reference =
            StorePublicationRef::from_entry(&local.value, local.prepared.reference().clone())?;
        if !interval
            .entries()
            .iter()
            .any(|entry| entry.reference() == &reference && entry.entry() == &local.value)
        {
            return Err(DbError::Message(
                "checkpoint publication conflicts with an overlapping accepted Store entry".into(),
            ));
        }
    }
    // The old entries may have a retired gap before this checkpoint. Keeping
    // them would represent a contiguous interval that was never verified.
    transaction.execute("DELETE FROM store_publication_entries", [])?;
    let accepted =
        if interval.current() == expected.record() && expected.observed_version().is_some() {
            AcceptedStorePublicationInterval {
                interval: interval.clone(),
                current_version: expected.observed_version().cloned(),
            }
        } else {
            accepted.clone()
        };
    persist_store_publication_interval_on(transaction, Some(expected), &accepted)
}

pub(super) fn retire_store_publication_prefix_before_snapshot_on(
    store: super::StoreTransaction<'_, '_>,
    lookup: &mut dyn super::verified_store_authority::VerifiedRegistrationLookup,
    snapshot: &coven_protocol::store_commit::StorePublicationRef,
) -> Result<(), DbError> {
    let transaction = store.transaction;
    let baseline = super::retained_replay::load_replay_baseline_metadata_on(
        super::StoreRecords::new(transaction, store.store_dir),
    )?
    .ok_or_else(|| {
        DbError::Message(
            "Store publication retirement has no installed replay baseline".to_string(),
        )
    })?;
    let crate::RetainedReplayAuthority::InstalledSnapshot(authority) = baseline.authority else {
        return Err(DbError::Message(
            "Store publication retirement requires an installed snapshot".to_string(),
        ));
    };
    let entries = load_store_publication_entries_on(transaction)?;
    let installed = entries.iter().any(|entry| {
        entry.prepared.reference() == &snapshot.object
            && entry.value.position == snapshot.position
            && entry.value.entry_hash() == snapshot.entry_hash
            && entry.value.previous_state_hash == authority.metadata.publication_predecessor.state_hash()
            && matches!(&entry.value.payload, coven_protocol::store_commit::StorePublicationPayload::Snapshot(reference) if reference == &authority.snapshot)
    });
    if !installed {
        return Err(DbError::Message(
            "Store publication prefix retirement requires the installed accepted snapshot"
                .to_string(),
        ));
    }
    let superseded = store.retire_snapshot_artifact_ownership(
        lookup,
        &authority.store_root,
        &coven_protocol::store_commit::AcceptedStoreSnapshotRef {
            snapshot: authority.snapshot.clone(),
            publication: snapshot.clone(),
        },
        &authority.metadata,
    )?;
    store.retire_store_blob_snapshot_ownership(authority.snapshot.object.slot(), &superseded)?;
    let position = i64::try_from(snapshot.position.get()).map_err(|_| {
        DbError::Message("Store publication position exceeds SQLite integer".to_string())
    })?;
    transaction
        .execute(
            "DELETE FROM store_publication_entries WHERE position < ?1",
            [position],
        )
        .map_err(DbError::from)?;
    load_store_publication_entries_on(transaction)?;
    Ok(())
}

pub(super) fn install_accepted_store_commit_interval_on(
    transaction: &rusqlite::Transaction<'_>,
    accepted: &AcceptedStorePublicationInterval,
    commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
) -> Result<AcceptedStoreCommitPublication, DbError> {
    let previous = load_store_current_publication_on(transaction)?;
    let publication = accepted
        .accepted_commit(commit)
        .map_err(|error| DbError::context("accepted Store commit publication", error))?;
    install_store_publication_interval_on(transaction, &previous, accepted)?;
    Ok(publication)
}

fn load_store_publication_entry_objects_on(
    connection: &rusqlite::Connection,
) -> Result<
    Vec<
        coven_protocol::objects::ExactProtocolObject<
            coven_protocol::store_commit::StorePublicationEntry,
        >,
    >,
    DbError,
> {
    let rows = crate::query_mapped_rows(
        connection,
        "SELECT position, entry_ref, entry_bytes
         FROM store_publication_entries ORDER BY position",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        },
    )?;
    let mut entries = Vec::with_capacity(rows.len());
    for (position, encoded_reference, bytes) in rows {
        let reference: coven_protocol::store_commit::StorePublicationRef =
            serde_json::from_str(&encoded_reference)
                .map_err(|error| DbError::context("Store publication reference", error))?;
        if i64::try_from(reference.position.get()).ok() != Some(position) {
            return Err(DbError::Message(
                "Store publication entry position differs from its index".to_string(),
            ));
        }
        let value: coven_protocol::store_commit::StorePublicationEntry =
            serde_json::from_slice(&bytes)
                .map_err(|error| DbError::context("Store publication entry", error))?;
        if value.to_bytes() != bytes {
            return Err(DbError::Message(
                "Store publication entry bytes are not canonical".to_string(),
            ));
        }
        let verified_reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
            &value,
            reference.object.clone(),
        )
        .map_err(|error| DbError::context("Store publication entry reference", error))?;
        if verified_reference != reference {
            return Err(DbError::Message(
                "Store publication entry differs from its exact reference".to_string(),
            ));
        }
        let prepared = coven_protocol::objects::PreparedExactObject::new(
            reference.object.clone(),
            bytes.clone(),
        )
        .map_err(DbError::from)?;
        entries.push(coven_protocol::objects::ExactProtocolObject {
            value,
            bytes,
            prepared,
        });
    }
    Ok(entries)
}

pub(super) fn load_store_publication_entries_on(
    connection: &rusqlite::Connection,
) -> Result<
    Vec<
        coven_protocol::objects::ExactProtocolObject<
            coven_protocol::store_commit::StorePublicationEntry,
        >,
    >,
    DbError,
> {
    let entries = load_store_publication_entry_objects_on(connection)?;
    let current = load_store_current_publication_on(connection)?;
    match (current.record().accepted(), entries.first(), entries.last()) {
        (None, None, None) => {}
        (Some(accepted), Some(first), Some(last)) => {
            let last_reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
                &last.value,
                last.prepared.reference().clone(),
            )
            .map_err(|error| DbError::context("last Store publication entry", error))?;
            if &last_reference != accepted {
                return Err(DbError::Message(
                    "retained Store publication interval does not reach current".to_string(),
                ));
            }
            let first_reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
                &first.value,
                first.prepared.reference().clone(),
            )
            .map_err(|error| DbError::context("first Store publication entry", error))?;
            if first.value.predecessor.is_some()
                && !matches!(
                    &first.value.payload,
                    coven_protocol::store_commit::StorePublicationPayload::Snapshot(_)
                )
            {
                return Err(DbError::Message(format!(
                    "retained Store publication interval begins at non-snapshot {first_reference:?}"
                )));
            }
            for pair in entries.windows(2) {
                let predecessor = coven_protocol::store_commit::StorePublicationRef::from_entry(
                    &pair[0].value,
                    pair[0].prepared.reference().clone(),
                )
                .map_err(|error| {
                    DbError::context("Store publication interval predecessor", error)
                })?;
                if pair[1].value.predecessor.as_ref() != Some(&predecessor) {
                    return Err(DbError::Message(
                        "retained Store publication interval is not contiguous".to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(DbError::Message(
                "Store publication current and retained interval disagree".to_string(),
            ));
        }
    }
    Ok(entries)
}

pub(super) fn load_accepted_store_commit_on(
    connection: &rusqlite::Connection,
    commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    publisher_signing_pubkey: &str,
) -> Result<AcceptedStoreCommitPublication, DbError> {
    let entries = load_store_publication_entries_on(connection)?;
    let mut matches = entries.into_iter().filter(|candidate| {
        matches!(
            &candidate.value.payload,
            coven_protocol::store_commit::StorePublicationPayload::Commit(reference)
                if reference == commit.reference()
        )
    });
    let entry = matches.next().ok_or_else(|| {
        DbError::Message("accepted Store publication is outside the retained interval".to_string())
    })?;
    if matches.next().is_some() {
        return Err(DbError::Message(
            "Store commit appears more than once in the accepted publication interval".to_string(),
        ));
    }
    let reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
        &entry.value,
        entry.prepared.reference().clone(),
    )
    .map_err(DbError::from)?;
    let publication = coven_protocol::store_commit::StoreCommitPublication::verified(
        entry.value,
        reference,
        commit,
        publisher_signing_pubkey,
    )
    .map_err(DbError::from)?;
    Ok(AcceptedStoreCommitPublication { publication })
}

impl StoreSession<'_> {
    fn store_publication_boundary(&self) -> Result<Option<StorePublicationBoundary>, DbError> {
        let observed = load_store_publication_boundary_on(self.conn)?;
        if observed.is_none() {
            require_unobserved_genesis_publication(self.conn, self.store_dir)?;
        }
        Ok(observed)
    }

    pub(crate) fn store_current_publication(&self) -> Result<StorePublicationBoundary, DbError> {
        load_store_current_publication_on(self.conn)
    }

    pub(crate) fn store_publication_entries(
        &self,
    ) -> Result<
        Vec<
            coven_protocol::objects::ExactProtocolObject<
                coven_protocol::store_commit::StorePublicationEntry,
            >,
        >,
        DbError,
    > {
        load_store_publication_entries_on(self.conn)
    }
}

impl super::StoreDatabase {
    /// Read the replay floor, accepted history, materialized frontier, and inputs in one
    /// database job. A concurrent baseline advance cannot retire the prefix
    /// between these reads.
    pub async fn retained_store_replay(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<
        (
            crate::InstalledReplayBaseline,
            Option<StorePublicationBoundary>,
            Vec<
                coven_protocol::objects::ExactProtocolObject<
                    coven_protocol::store_commit::StorePublicationEntry,
                >,
            >,
            Vec<crate::OwnedVerifiedMergeMaterialization>,
            coven_protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        self.call_store(move |session| {
            let baseline = session.installed_replay_baseline()?;
            let observed = session.store_publication_boundary()?;
            let entries = match &observed {
                Some(_) => session.store_publication_entries()?,
                None => Vec::new(),
            };
            let inputs = session.retained_merge_replay_inputs(root)?;
            let materialized = coven_protocol::store_commit::CommitFrontier::from_refs(
                super::StoreRecords::new(session.conn, session.store_dir)
                    .materialized_frontier()?,
            )?;
            Ok((baseline, observed, entries, inputs, materialized))
        })
        .await
    }

    pub async fn installed_store_commit_evidence(
        &self,
        commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<Option<AcceptedStoreCommitEvidence>, DbError> {
        self.call_store(move |session| {
            let transaction = session
                .conn
                .unchecked_transaction()
                .map_err(DbError::from)?;
            installed_store_commit_evidence_on(
                super::StoreTransaction::new(&transaction, session.store_dir),
                &commit,
            )
        })
        .await
    }

    pub async fn retained_store_publication(
        &self,
    ) -> Result<
        (
            StorePublicationBoundary,
            Vec<
                coven_protocol::objects::ExactProtocolObject<
                    coven_protocol::store_commit::StorePublicationEntry,
                >,
            >,
        ),
        DbError,
    > {
        self.call_store(|session| {
            Ok((
                session.store_current_publication()?,
                session.store_publication_entries()?,
            ))
        })
        .await
    }

    pub async fn store_publication_boundary(
        &self,
    ) -> Result<Option<StorePublicationBoundary>, DbError> {
        self.call_store(|session| session.store_publication_boundary())
            .await
    }
}
