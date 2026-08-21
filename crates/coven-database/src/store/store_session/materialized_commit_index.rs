use crate::query_mapped_rows;
use crate::*;
#[cfg(any(test, feature = "test-utils"))]
use coven_protocol::store_commit::StoreDeviceRegistration;
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CommitFrontier, ReferencedStoreDeviceRegistration,
    ResolvedStoreDeviceState, StoreBatchCommitRef, StoreDeviceProposalAck,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut,
};
use rusqlite::{Connection, OptionalExtension};
use std::collections::BTreeMap;

use super::*;

impl StoreSession<'_> {
    fn materialized_frontier(&mut self) -> Result<BTreeMap<String, StoreBatchCommitRef>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .materialized_frontier()
    }

    fn retained_merge_replay_inputs(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        self.verified_store_authority.retained_replay_inputs_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            &root,
        )
    }

    fn retained_merge_materialization_refs(&mut self) -> Result<Vec<StoreBatchCommitRef>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .retained_merge_materialization_refs()
    }

    fn retained_merge_materialization(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        reference: StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        self.verified_store_authority
            .retained_replay_inputs_on(
                crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
                &root,
            )?
            .into_iter()
            .find(|materialization| materialization.commit_ref() == &reference)
            .ok_or_else(|| {
                DbError::Message(
                    "retained Merge materialization is absent at its exact coordinate".to_string(),
                )
            })
    }

    fn retained_merge_history_frontier(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        references: Vec<StoreBatchCommitRef>,
    ) -> Result<Vec<RetainedMergeHistoryCheckpoint>, DbError> {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let authority = &mut *self.verified_store_authority;
        let retained = authority.retained_replay_inputs_on(records, &root)?;
        // Read once from the connection's verified baseline; every reference the
        // walk finds below the snapshot cut resolves against this same one.
        let baseline = authority.retained_replay_baseline_on(records)?.clone();
        let by_reference = retained
            .iter()
            .map(|materialization| (materialization.commit_ref().clone(), materialization))
            .collect::<BTreeMap<_, _>>();
        let mut pending = references;
        let mut visited = std::collections::BTreeSet::new();
        let mut checkpoints = Vec::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            match by_reference.get(&reference) {
                Some(materialization) => {
                    pending.extend(
                        materialization
                            .commit()
                            .order
                            .predecessor_cut()
                            .map_err(DbError::from)?
                            .0
                            .into_values(),
                    );
                    checkpoints
                        .push(authority.retained_history_checkpoint_on(records, &reference)?);
                }
                None => checkpoints.push(StoreDatabase::load_retained_merge_history_checkpoint_on(
                    records, &root, authority, &baseline, &reference,
                )?),
            }
        }
        Ok(checkpoints)
    }

    fn exact_materialized_ref(
        &mut self,
        stream_id: String,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .materialized_commit_ref(&stream_id, sequence)
    }

    fn snapshot_coverage_frontier(&mut self) -> Result<CommitFrontier, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .snapshot_coverage_frontier()
    }

    fn installed_replay_baseline(&mut self) -> Result<crate::InstalledReplayBaseline, DbError> {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let coverage = self.snapshot_coverage_frontier()?;
        let covered_states =
            crate::store::store_device_state::load_covered_store_device_snapshots_on(
                self.conn, &coverage,
            )?;
        // A genesis baseline covers nothing, so there is nothing under it to
        // summarize and every walk runs to the bottom as it always did.
        let (summary, snapshot) =
            match crate::store::retained_replay::load_replay_baseline_metadata_on(records)? {
                Some(baseline) => match &baseline.authority {
                    crate::RetainedReplayAuthority::InstalledSnapshot(authority) => {
                        let snapshot = authority.snapshot.clone();
                        (
                            Some(
                                crate::StoreDatabase::open_installed_baseline_history_summary(
                                    records, &baseline,
                                )?,
                            ),
                            Some(snapshot),
                        )
                    }
                    crate::RetainedReplayAuthority::Genesis(_) => (None, None),
                },
                None => (None, None),
            };
        Ok(crate::InstalledReplayBaseline::new(
            coverage,
            covered_states,
            summary,
            snapshot,
        ))
    }

    /// The accepted announcement each stream's installed snapshot restates at
    /// its covered tip.
    ///
    /// The announcement chain is slot-linked, so a walker that starts below a
    /// position cannot skip to it — it has to read every head in between. This
    /// is where a walk resumes instead: the head the snapshot's owner signed
    /// into its history summary, at the coverage this device stands on. A
    /// device on a genesis baseline has none, and its walks start at the stream
    /// anchor as they always did.
    fn snapshot_announcement_frontier(
        &mut self,
    ) -> Result<
        BTreeMap<
            coven_protocol::causal_grants::AuthorStreamId,
            coven_protocol::store_commit::RetainedAcceptedStoreAnnouncement,
        >,
        DbError,
    > {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let baseline = self
            .verified_store_authority
            .retained_replay_baseline_on(records)?;
        let crate::RetainedReplayAuthority::InstalledSnapshot(authority) = &baseline.authority
        else {
            return Ok(BTreeMap::new());
        };
        let summary = &authority.metadata.history_summary;
        let coverage = &authority.metadata.coverage.0;
        let frontier = summary.announcement_frontier.clone();
        for (stream_id, announcement) in &frontier {
            // The summary carries both, so they can disagree; the coverage is
            // what every other position question answers from, and an
            // announcement naming a different commit would resume a walk on the
            // wrong chain.
            if coverage.get(stream_id) != Some(&announcement.value.commit) {
                return Err(DbError::Message(
                    "snapshot announcement frontier differs from its own coverage".to_string(),
                ));
            }
        }
        Ok(frontier)
    }

    fn store_device_state_for_history_cut(
        &mut self,
        cut: StoreHistoryCut,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .store_device_state_for_history_cut(&cut)
    }

    fn resolved_store_device_state(
        &mut self,
        reference: StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .declared_store_device_state(&reference)
    }

    fn store_device_exclusion_freezes(&mut self) -> Result<Vec<StoreDeviceProposalAck>, DbError> {
        let root = self
            .root_authority()?
            .map(|(reference, _)| reference)
            .ok_or_else(|| {
                DbError::Message("Store root is absent while loading exclusion freezes".to_string())
            })?;
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .store_device_exclusion_freezes(&root)
    }

    fn activated_store_device_registration_records(
        &mut self,
    ) -> Result<Vec<ReferencedStoreDeviceRegistration>, DbError> {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let root = self
            .verified_store_authority
            .root_authority_on(records)?
            .map(|(reference, _)| reference)
            .ok_or_else(|| {
                DbError::Message("Store root is absent while loading activated devices".to_string())
            })?;
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .activated_registration_references()?
            .into_iter()
            .map(|reference| {
                let device_id = reference.device_id;
                let registration = self
                    .verified_store_authority
                    .activated_registration_on(records, &root, &reference)?;
                ReferencedStoreDeviceRegistration::verified(reference, registration).map_err(
                    |error| {
                        DbError::context(
                            format!(
                                "activated Store device registration {device_id} exact reference"
                            ),
                            error,
                        )
                    },
                )
            })
            .collect::<Result<Vec<_>, DbError>>()
    }

    fn activated_store_device_registration(
        &mut self,
        reference: StoreDeviceRegistrationRef,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        let root = self
            .root_authority()?
            .map(|(reference, _)| reference)
            .ok_or_else(|| {
                DbError::Message(
                    "Store root is absent while loading an activated device".to_string(),
                )
            })?;
        let registration = self.verified_store_authority.activated_registration_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            &root,
            &reference,
        )?;
        ReferencedStoreDeviceRegistration::verified(reference, registration).map_err(DbError::from)
    }

    fn local_activated_registration_ref(
        &mut self,
    ) -> Result<Option<StoreDeviceRegistrationRef>, DbError> {
        crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .local_activated_registration_ref()
    }

    fn activated_store_device_registration_with_authority(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        reference: StoreDeviceRegistrationRef,
    ) -> Result<ActivatedStoreDeviceRegistration, DbError> {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let registration = self
            .verified_store_authority
            .activated_registration_on(records, &root, &reference)?;
        let authority = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .activated_registration_authority(&reference)?;
        let authority = serde_json::from_str(&authority)
            .map_err(|error| DbError::context("activated Store registration authority", error))?;
        let registration = ReferencedStoreDeviceRegistration::verified(reference, registration)
            .map_err(DbError::from)?;
        ActivatedStoreDeviceRegistration::verified(registration, authority).map_err(DbError::from)
    }

    fn activated_store_device_registration_for_device(
        &mut self,
        device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<Option<ActivatedStoreDeviceRegistration>, DbError> {
        let records = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir);
        let root = self
            .verified_store_authority
            .root_authority_on(records)?
            .map(|(reference, _)| reference)
            .ok_or_else(|| {
                DbError::Message(
                    "Store root is absent while loading an activated device".to_string(),
                )
            })?;
        let stored = crate::store::store_session::StoreRecords::new(self.conn, self.store_dir)
            .activated_registration_row_for_device(device_id)?;
        let Some((reference, authority)) = stored else {
            return Ok(None);
        };
        let reference: StoreDeviceRegistrationRef = serde_json::from_str(&reference)
            .map_err(|error| DbError::context("activated Store registration ref", error))?;
        if reference.device_id != device_id {
            return Err(DbError::Message(
                "activated Store registration row names another device".to_string(),
            ));
        }
        let registration = self
            .verified_store_authority
            .activated_registration_on(records, &root, &reference)?;
        let authority = serde_json::from_str(&authority)
            .map_err(|error| DbError::context("activated Store registration authority", error))?;
        let registration = ReferencedStoreDeviceRegistration::verified(reference, registration)
            .map_err(DbError::from)?;
        ActivatedStoreDeviceRegistration::verified(registration, authority)
            .map(Some)
            .map_err(DbError::from)
    }
}

impl StoreDatabase {
    pub async fn materialized_frontier(
        &self,
    ) -> Result<BTreeMap<String, StoreBatchCommitRef>, DbError> {
        self.call_store(|session| session.materialized_frontier())
            .await
    }

    pub async fn retained_merge_replay_inputs(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        self.call_store(move |session| session.retained_merge_replay_inputs(root))
            .await
    }

    pub async fn retained_merge_materialization_refs(
        &self,
    ) -> Result<Vec<StoreBatchCommitRef>, DbError> {
        self.call_store(|session| session.retained_merge_materialization_refs())
            .await
    }

    pub async fn retained_merge_materialization(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        reference: StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        self.call_store(move |session| session.retained_merge_materialization(root, reference))
            .await
    }

    pub async fn retained_merge_history_frontier(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        references: Vec<StoreBatchCommitRef>,
    ) -> Result<Vec<RetainedMergeHistoryCheckpoint>, DbError> {
        self.call_store(move |session| session.retained_merge_history_frontier(root, references))
            .await
    }

    pub async fn exact_materialized_ref(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        let stream_id = stream_id.to_string();
        self.call_store(move |session| session.exact_materialized_ref(stream_id, sequence))
            .await
    }

    pub async fn snapshot_coverage_frontier(&self) -> Result<CommitFrontier, DbError> {
        self.call_store(|session| session.snapshot_coverage_frontier())
            .await
    }

    /// The baseline a history walk stops at, with the device states it keeps
    /// for the covered positions commits above it still name.
    pub async fn installed_replay_baseline(
        &self,
    ) -> Result<crate::InstalledReplayBaseline, DbError> {
        self.call_store(|session| session.installed_replay_baseline())
            .await
    }

    pub async fn snapshot_announcement_frontier(
        &self,
    ) -> Result<
        BTreeMap<
            coven_protocol::causal_grants::AuthorStreamId,
            coven_protocol::store_commit::RetainedAcceptedStoreAnnouncement,
        >,
        DbError,
    > {
        self.call_store(|session| session.snapshot_announcement_frontier())
            .await
    }

    pub async fn store_device_state_for_order(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), DbError> {
        let cut = order.predecessor_cut().map_err(DbError::from)?;
        self.call_store(move |session| session.store_device_state_for_history_cut(cut))
            .await
    }

    pub async fn store_device_state_for_history_cut(
        &self,
        cut: &StoreHistoryCut,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), DbError> {
        let cut = cut.clone();
        self.call_store(move |session| session.store_device_state_for_history_cut(cut))
            .await
    }

    pub async fn resolved_store_device_state(
        &self,
        reference: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, DbError> {
        let reference = reference.clone();
        self.call_store(move |session| session.resolved_store_device_state(reference))
            .await
    }

    pub async fn store_device_exclusion_freezes(
        &self,
    ) -> Result<Vec<StoreDeviceProposalAck>, DbError> {
        self.call_store(|session| session.store_device_exclusion_freezes())
            .await
    }

    pub async fn activated_store_device_registration_records(
        &self,
    ) -> Result<Vec<ReferencedStoreDeviceRegistration>, DbError> {
        self.call_store(|session| session.activated_store_device_registration_records())
            .await
    }

    pub async fn activated_store_device_registration(
        &self,
        reference: StoreDeviceRegistrationRef,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        self.call_store(move |session| session.activated_store_device_registration(reference))
            .await
    }

    /// The exact registration this device is activated under, or `None` before
    /// it has one. This is the identity a signed artifact names when it names a
    /// device, so it is what a role check compares against.
    pub async fn local_activated_registration_ref(
        &self,
    ) -> Result<Option<StoreDeviceRegistrationRef>, DbError> {
        self.call_store(|session| session.local_activated_registration_ref())
            .await
    }

    pub async fn local_blob_write_authority(
        &self,
    ) -> Result<ReferencedStoreDeviceRegistration, DbError> {
        self.call_store(|session| session.local_store_authority())
            .await
    }

    pub async fn activated_store_device_registration_with_authority(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        reference: StoreDeviceRegistrationRef,
    ) -> Result<ActivatedStoreDeviceRegistration, DbError> {
        let root = root.clone();
        self.call_store(move |session| {
            session.activated_store_device_registration_with_authority(root, reference)
        })
        .await
    }

    pub async fn activated_store_device_registration_for_device(
        &self,
        device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<Option<ActivatedStoreDeviceRegistration>, DbError> {
        self.call_store(move |session| {
            session.activated_store_device_registration_for_device(device_id)
        })
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn activated_store_device_registrations(
        &self,
    ) -> Result<Vec<StoreDeviceRegistration>, DbError> {
        Ok(self
            .activated_store_device_registration_records()
            .await?
            .into_iter()
            .map(|registration| registration.value().clone())
            .collect())
    }
}

pub(crate) fn materialized_frontier_on(
    conn: &Connection,
    exclude_device: Option<&str>,
) -> Result<BTreeMap<String, StoreBatchCommitRef>, DbError> {
    let mut frontier = BTreeMap::new();
    let rows = query_mapped_rows(
        conn,
        "SELECT m.device_id, m.seq, m.commit_ref,
                    m.retained_commit_ref, m.retained_input_hash \
             FROM materialized_commits m \
             JOIN (SELECT device_id, MAX(seq) AS seq FROM materialized_commits \
                   GROUP BY device_id) latest \
               ON latest.device_id = m.device_id AND latest.seq = m.seq",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        },
    )?;
    for row in rows {
        let (device_id, seq, reference, retained_commit_ref, retained_input_hash) = row;
        if exclude_device == Some(device_id.as_str()) {
            continue;
        }
        let seq = Database::sequence_from_sqlite(&device_id, seq)?;
        frontier.insert(
            device_id.clone(),
            parse_materialized_commit_row_on(
                &device_id,
                seq,
                &reference,
                retained_commit_ref.as_deref(),
                retained_input_hash.as_deref(),
            )?,
        );
    }

    for (device_id, reference) in snapshot_coverage_on(conn)? {
        if exclude_device == Some(device_id.as_str()) {
            continue;
        }
        if frontier
            .get(&device_id)
            .is_none_or(|current| current.coord.sequence() < reference.coord.sequence())
        {
            frontier.insert(device_id, reference);
        }
    }
    Ok(frontier)
}

/// The exact commit each stream's installed snapshot image reaches. The image
/// records one tip per stream and nothing below it, so this is the whole of
/// what a snapshot says it materialized.
pub(crate) fn snapshot_coverage_on(
    conn: &Connection,
) -> Result<BTreeMap<String, StoreBatchCommitRef>, DbError> {
    let rows = query_mapped_rows(
        conn,
        "SELECT device_id, seq, commit_ref FROM snapshot_coverage",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;
    let mut coverage = BTreeMap::new();
    for (device_id, seq, reference) in rows {
        let seq = Database::sequence_from_sqlite(&device_id, seq)?;
        let reference = parse_stored_commit_ref(&device_id, seq, &reference)?;
        coverage.insert(device_id, reference);
    }
    Ok(coverage)
}

pub(crate) fn parse_stored_commit_ref(
    stream_id: &str,
    sequence: u64,
    encoded: &str,
) -> Result<StoreBatchCommitRef, DbError> {
    let reference: StoreBatchCommitRef = serde_json::from_str(encoded)
        .map_err(|error| DbError::context("stored exact Store commit ref", error))?;
    let coordinate_matches =
        reference.coord.stream_id.to_string() == stream_id && reference.coord.sequence == sequence;
    if !coordinate_matches {
        return Err(DbError::Message(format!(
            "stored exact Store commit ref differs from {stream_id}/{sequence}"
        )));
    }
    Ok(reference)
}

fn parse_materialized_commit_row_on(
    stream_id: &str,
    sequence: u64,
    encoded: &str,
    retained_commit_ref: Option<&str>,
    retained_input_hash: Option<&str>,
) -> Result<StoreBatchCommitRef, DbError> {
    let reference = parse_stored_commit_ref(stream_id, sequence, encoded)?;
    if retained_commit_ref != Some(encoded) {
        return Err(DbError::Message(format!(
            "materialized coordinate {stream_id}/{sequence} does not bind its exact retained commit"
        )));
    }
    let input_hash = retained_input_hash.ok_or_else(|| {
        DbError::Message(format!(
            "materialized coordinate {stream_id}/{sequence} has no retained input hash"
        ))
    })?;
    input_hash
        .parse::<coven_protocol::store_commit::ObjectHash>()
        .map_err(|error| {
            DbError::context(
                format!(
                    "materialized coordinate {stream_id}/{sequence} retained input hash is invalid"
                ),
                error,
            )
        })?;
    Ok(reference)
}

pub(crate) fn materialized_commit_ref_on(
    conn: &Connection,
    stream_id: &str,
    sequence: u64,
) -> Result<Option<StoreBatchCommitRef>, DbError> {
    let seq = Database::sequence_to_sqlite(stream_id, sequence)?;
    conn.query_row(
        "SELECT commit_ref, retained_commit_ref, retained_input_hash
         FROM materialized_commits WHERE device_id = ?1 AND seq = ?2",
        (stream_id, seq),
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        },
    )
    .optional()
    .map_err(DbError::from)?
    .map(|(encoded, retained_commit_ref, retained_input_hash)| {
        parse_materialized_commit_row_on(
            stream_id,
            sequence,
            &encoded,
            retained_commit_ref.as_deref(),
            retained_input_hash.as_deref(),
        )
    })
    .transpose()
}

pub(crate) fn latest_position_for_device_on(
    conn: &Connection,
    device_id: &str,
) -> Result<Option<StoreBatchCommitRef>, DbError> {
    let materialized = conn
        .query_row(
            "SELECT seq, commit_ref, retained_commit_ref, retained_input_hash
             FROM materialized_commits
             WHERE device_id = ?1 ORDER BY seq DESC LIMIT 1",
            [device_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(DbError::from)?;
    let coverage = conn
        .query_row(
            "SELECT seq, commit_ref FROM snapshot_coverage WHERE device_id = ?1",
            [device_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(DbError::from)?;
    let mut references = Vec::new();
    if let Some((seq, reference, retained_commit_ref, retained_input_hash)) = materialized {
        let seq = Database::sequence_from_sqlite(device_id, seq)?;
        references.push(parse_materialized_commit_row_on(
            device_id,
            seq,
            &reference,
            retained_commit_ref.as_deref(),
            retained_input_hash.as_deref(),
        )?);
    }
    if let Some((seq, reference)) = coverage {
        let seq = Database::sequence_from_sqlite(device_id, seq)?;
        references.push(parse_stored_commit_ref(device_id, seq, &reference)?);
    }
    if references.len() == 2
        && references[0].coord.sequence() == references[1].coord.sequence()
        && references[0] != references[1]
    {
        return Err(DbError::Message(format!(
            "materialized ledger and snapshot coverage fork {device_id:?} at sequence {}",
            references[0].coord.sequence()
        )));
    }
    Ok(references
        .into_iter()
        .max_by_key(|reference| reference.coord.sequence()))
}
