use rusqlite::Connection;

use super::{MergeMaterializationTransaction, StoreDatabase};
use crate::{
    candidate_graph_exact_objects, circle_operation_ids_in_phase_on,
    load_activated_registration_on, load_circle_operation_on, load_remote_object_on,
    persist_prepared_remote_object_on, required_store_root_authority_on, update_remote_object_on,
    DbError, PreparedCircleOperationRow, VerifiedMergeMaterialization,
};
use coven_protocol::circle::{CircleOperationId, CircleOperationState};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::circle_journal::{CircleOperationJournal, CircleOperationProgress};
use coven_protocol::objects::PreparedExactObject;
use coven_protocol::remote_object::remote_object_id;
use coven_protocol::store_commit::{
    commit_semantic_prefix, StoreBatchCommit, StoreDeviceHead, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
};

/// The stored bytes of one operation's objects, supplied alongside the
/// operation that names them.
///
/// The operation holds references; `remote_objects` still stores the bytes
/// those references name inline, so the flows that write those rows are handed
/// the bytes by whoever prepared or read them.
pub type PreparedCircleObjects = std::collections::BTreeMap<String, PreparedExactObject>;

impl StoreDatabase {
    pub async fn insert_circle_operation(
        &self,
        journal: CircleOperationJournal,
        prepared_objects: PreparedCircleObjects,
    ) -> Result<(), DbError> {
        let remotes = journal
            .closed_remote_objects(&prepared_objects)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let owner = journal.operation().commit_ref.clone();
        let row = PreparedCircleOperationRow::from_journal(&journal)?;
        let store_dir = self.store_dir.clone();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                for remote in &remotes {
                    persist_prepared_remote_object_on(
                        &tx,
                        &store_dir,
                        remote,
                        &owner,
                        "Circle candidate graph",
                    )?;
                }
                claim_operation_payloads_on(&tx, &journal.operation_id, journal.operation())?;
                insert_circle_operation_row_on(&tx, &row)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    /// Insert the terminal deletion operation, superseding the operation that
    /// currently holds the Circle's single operation slot. A closing Circle keeps
    /// a waiting close operation there; the deletion removes it and takes the slot
    /// in one transaction, so no window leaves the Circle carrying both a pending
    /// close and a pending deletion.
    pub async fn insert_circle_operation_superseding(
        &self,
        journal: CircleOperationJournal,
        superseded: CircleOperationId,
        prepared_objects: PreparedCircleObjects,
    ) -> Result<(), DbError> {
        let remotes = journal
            .closed_remote_objects(&prepared_objects)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let owner = journal.operation().commit_ref.clone();
        let row = PreparedCircleOperationRow::from_journal(&journal)?;
        let superseded = superseded.as_str().to_string();
        let circle_id = row.circle_id.clone();
        let store_dir = self.store_dir.clone();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let discarded = load_circle_operation_on(&tx, &superseded)?.ok_or_else(|| {
                    DbError::Message(
                        "superseded Circle operation is absent from its slot".to_string(),
                    )
                })?;
                if discarded.circle_id.to_string() != circle_id {
                    return Err(DbError::Message(
                        "superseded Circle operation belongs to another circle".to_string(),
                    ));
                }
                release_operation_payloads_on(&tx, &discarded.operation_id)?;
                let removed = tx
                    .execute(
                        "DELETE FROM circle_operations WHERE operation_id = ?1 AND circle_id = ?2",
                        rusqlite::params![superseded, circle_id],
                    )
                    .map_err(DbError::from)?;
                if removed != 1 {
                    return Err(DbError::Message(
                        "superseded Circle operation is absent from its slot".to_string(),
                    ));
                }
                for remote in &remotes {
                    persist_prepared_remote_object_on(
                        &tx,
                        &store_dir,
                        remote,
                        &owner,
                        "Circle candidate graph",
                    )?;
                }
                claim_operation_payloads_on(&tx, &journal.operation_id, journal.operation())?;
                insert_circle_operation_row_on(&tx, &row)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn circle_operation(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<Option<CircleOperationJournal>, DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| load_circle_operation_on(conn, &operation_id))
            .await
    }

    pub async fn oldest_pending_circle_operation(
        &self,
    ) -> Result<Option<CircleOperationJournal>, DbError> {
        self.connection
            .call(|conn| {
                let Some(operation_id) = circle_operation_ids_in_phase_on(conn, |progress| {
                    matches!(
                        progress,
                        CircleOperationProgress::Ready | CircleOperationProgress::Finalizing
                    )
                })?
                .into_iter()
                .next() else {
                    return Ok(None);
                };
                load_circle_operation_on(conn, &operation_id)
            })
            .await
    }

    pub async fn waiting_circle_operations(&self) -> Result<Vec<CircleOperationJournal>, DbError> {
        self.connection
            .call(|conn| {
                let waiting = circle_operation_ids_in_phase_on(conn, |progress| {
                    matches!(progress, CircleOperationProgress::WaitingForCloseResponses)
                })?;
                waiting
                    .iter()
                    .map(|operation_id| {
                        load_circle_operation_on(conn, operation_id)?.ok_or_else(|| {
                            DbError::Message(format!(
                                "Circle operation {operation_id} disappeared while being listed"
                            ))
                        })
                    })
                    .collect()
            })
            .await
    }

    /// Record that one upload step finished: the step's row, and — for a step
    /// carrying an object the candidate owns — that object's uploaded state.
    ///
    /// A step whose object is a shared Circle object rather than a
    /// candidate-exclusive one records only its row: no `remote_objects` record
    /// exists for it to mark, which is why the operation's own commit decides
    /// that rather than an absent lookup.
    ///
    /// The operation beside it is untouched. Both writes are idempotent, so a
    /// retry of a step whose transaction already committed is a no-op rather
    /// than a conflict, and the foreign key is what refuses a step for an
    /// operation that is no longer there.
    pub async fn complete_circle_operation_upload_step(
        &self,
        operation_id: &CircleOperationId,
        step: &str,
    ) -> Result<(), DbError> {
        let operation_id = operation_id.as_str().to_string();
        let step = step.to_string();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let journal = load_circle_operation_on(&tx, &operation_id)?.ok_or_else(|| {
                    DbError::Message(format!(
                        "Circle operation {operation_id} disappeared before its upload step"
                    ))
                })?;
                let object = journal
                    .operation()
                    .prepared_objects
                    .get(&step)
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "Circle upload step {step:?} names no object of operation {operation_id}"
                        ))
                    })?
                    .clone();
                let candidate_owned = journal
                    .candidate_owned_objects()
                    .map_err(|error| DbError::Message(error.to_string()))?;
                tx.execute(
                    "INSERT OR IGNORE INTO circle_operation_uploads (operation_id, step)
                     VALUES (?1, ?2)",
                    rusqlite::params![operation_id, step],
                )
                .map_err(DbError::from)?;
                if candidate_owned.contains(&object) {
                    mark_uploaded_object_on(&tx, remote_object_id(&object)).map_err(|error| {
                        DbError::Message(format!(
                            "Circle upload step {step:?} of operation {operation_id}: {error}"
                        ))
                    })?;
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    /// Replace a closed operation with its freshly prepared finalization.
    ///
    /// This is the one transition that rewrites the prepared operation, so it
    /// is also the one that has to retire what it replaces: the superseded
    /// operation's upload rows go, because the finalization reuses their step
    /// names for different objects, and its spool files go, because nothing
    /// names them once the operation that did is gone.
    pub async fn begin_circle_operation_finalization(
        &self,
        journal: CircleOperationJournal,
        prepared_objects: PreparedCircleObjects,
    ) -> Result<(), DbError> {
        if !matches!(journal.state(), CircleOperationState::Finalizing) {
            return Err(DbError::Message(
                "Circle finalization journal is not in finalizing state".to_string(),
            ));
        }
        let remotes = journal
            .closed_remote_objects(&prepared_objects)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let owner = journal.operation().commit_ref.clone();
        let row = PreparedCircleOperationRow::from_journal(&journal)?;
        let store_dir = self.store_dir.clone();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let durable = load_circle_operation_on(&tx, journal.operation_id.as_str())?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "Circle operation {} disappeared before finalization",
                            journal.operation_id
                        ))
                    })?;
                if !matches!(
                    durable.state(),
                    CircleOperationState::WaitingForCloseResponses
                ) || durable.circle_id != journal.circle_id
                    || durable.intent != journal.intent
                {
                    return Err(DbError::Message(format!(
                        "Circle operation {} changed before finalization",
                        journal.operation_id
                    )));
                }
                for remote in &remotes {
                    persist_prepared_remote_object_on(
                        &tx,
                        &store_dir,
                        remote,
                        &owner,
                        "Circle close-finalization candidate graph",
                    )?;
                }
                claim_operation_payloads_on(&tx, &journal.operation_id, journal.operation())?;
                tx.execute(
                    "DELETE FROM circle_operation_uploads WHERE operation_id = ?1",
                    [&row.operation_id],
                )
                .map_err(DbError::from)?;
                let updated = tx
                    .execute(
                        "UPDATE circle_operations SET prepared = ?3, phase = ?4
                         WHERE operation_id = ?1 AND circle_id = ?2",
                        rusqlite::params![row.operation_id, row.circle_id, row.prepared, row.phase],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "Circle operation disappeared during finalization".to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    /// Replace one operation's prepared payload with a substituted one, leaving
    /// its phase and upload rows where they are.
    ///
    /// Production rewrites `prepared` only at the close-to-finalization
    /// boundary. This is how a test hands the publication and activation paths
    /// a durable operation that contradicts what it names, to check that they
    /// refuse it rather than trusting the row.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn substitute_circle_operation_for_test(
        &self,
        journal: CircleOperationJournal,
    ) -> Result<(), DbError> {
        let row = PreparedCircleOperationRow::from_journal(&journal)?;
        self.connection
            .call(move |conn| {
                let updated = conn
                    .execute(
                        "UPDATE circle_operations SET prepared = ?3
                         WHERE operation_id = ?1 AND circle_id = ?2",
                        rusqlite::params![row.operation_id, row.circle_id, row.prepared],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "Circle operation {} is absent from its slot",
                        row.operation_id
                    )));
                }
                Ok(())
            })
            .await
    }

    pub async fn block_circle_operation(
        &self,
        operation_id: &CircleOperationId,
        block: coven_protocol::circle::CircleOperationBlock,
    ) -> Result<(), DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let mut journal =
                    load_circle_operation_on(&tx, &operation_id)?.ok_or_else(|| {
                        DbError::Message(format!("circle operation {operation_id} is absent"))
                    })?;
                journal
                    .block(block)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                update_circle_operation_phase_on(&tx, &journal)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn unblock_circle_operation(
        &self,
        operation_id: &CircleOperationId,
    ) -> Result<(), DbError> {
        let operation_id = operation_id.as_str().to_string();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let mut journal =
                    load_circle_operation_on(&tx, &operation_id)?.ok_or_else(|| {
                        DbError::Message(format!("circle operation {operation_id} is absent"))
                    })?;
                journal
                    .unblock()
                    .map_err(|error| DbError::Message(error.to_string()))?;
                update_circle_operation_phase_on(&tx, &journal)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn activate_circle_operation(
        &self,
        journal: CircleOperationJournal,
        verified: VerifiedCircleActivations,
    ) -> Result<(), DbError> {
        let gates = self.gates();
        let store_dir = self.store_dir.clone();
        self.with_retained_replay(move |records, retained_cache| {
                let conn = records.conn();
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                journal
                    .validate_identity()
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let durable = load_circle_operation_on(&tx, journal.operation_id.as_str())?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "circle operation {} disappeared during activation",
                            journal.operation_id
                        ))
                    })?;
                if durable != journal {
                    return Err(DbError::Message(format!(
                        "circle operation {} changed before activation",
                        journal.operation_id
                    )));
                }
                if !journal.is_publishable() {
                    return Err(DbError::Message(
                        "blocked circle operation cannot activate".to_string(),
                    ));
                }
                let operation = journal.operation();
                let creation = &operation.creation;
                let resolved_roster = creation.resolved_roster();
                if !creation.control.verify()
                    || !creation.metadata.verify()
                    || !resolved_roster.verify()
                {
                    return Err(DbError::Message(
                        "circle operation contains invalid signed objects".to_string(),
                    ));
                }
                let unverified_commit: StoreBatchCommit =
                    serde_json::from_slice(&operation.commit_bytes).map_err(|error| {
                        DbError::context("parse circle Store commit", error)
                    })?;
                let root = required_store_root_authority_on(&tx)?;
                let author = load_activated_registration_on(
                    &tx,
                    &root,
                    &unverified_commit.author_registration,
                )?;
                let [activation] = verified.circles() else {
                    return Err(DbError::Message(
                        "local Circle publication must carry one common-verifier result"
                            .to_string(),
                    ));
                };
                let verify_commit = || {
                    let commit = VerifiedStoreBatchCommit::parse(
                        &operation.commit_bytes,
                        root.store_root_hash,
                        &operation.commit_ref,
                        &author,
                    )
                    .map_err(|error| {
                        DbError::context("verify circle Store commit", error)
                    })?;
                    if operation.commit_ref.object.slot().logical_key()
                        != commit_semantic_prefix(
                            commit.candidate_family(),
                            &operation.commit_ref.coord.stream_id.to_string(),
                            commit.seq(),
                            commit.commit_hash(),
                        ) + ".json"
                    {
                        return Err(DbError::Message(
                            "circle commit exact object occupies a different semantic slot"
                                .to_string(),
                        ));
                    }
                    let [control_ref] = commit.circle_controls() else {
                        return Err(DbError::Message(
                            "circle creation Store commit is not an exact control-only batch"
                                .to_string(),
                        ));
                    };
                    let expected_ref = creation.control_ref(
                        control_ref.objects().clone(),
                        Some(control_ref.head_object().clone()),
                    );
                    if control_ref != &expected_ref
                        || !commit.operations().is_some_and(
                            coven_protocol::store_commit::StoreCommitOperations::is_circle_control_activation_only,
                        )
                    {
                        return Err(DbError::Message(
                            "circle creation Store commit is not an exact control-only batch"
                                .to_string(),
                        ));
                    }
                    if activation.reference != *control_ref
                        || activation.circle_id != creation.circle_id
                        || activation.control != creation.control
                        || verified.stream_activations().activating_commit()
                            != &operation.commit_ref
                        || verified.stream_activations().as_slice() != commit.stream_activations()
                    {
                        return Err(DbError::Message(
                            "common-verifier Circle result differs from the durable signed operation"
                                .to_string(),
                        ));
                    }
                    Ok(commit)
                };
                let (commit, activation, head_object_id, retained) = {
                    let head = &operation.policy.head;
                    let history_evidence = &operation.policy.history_evidence;
                    let commit = verify_commit()?;
                    let parsed = StoreDeviceHead::parse_at(
                        &head.to_bytes(),
                        commit.store_root_hash,
                        &author,
                        &operation.commit_ref,
                    )
                    .map_err(|error| {
                        DbError::context("verify circle activation head", error)
                    })?;
                    if parsed.commit != operation.commit_ref {
                        return Err(DbError::Message(
                            "circle activation head names a different commit".to_string(),
                        ));
                    }
                    let device_operations = VerifiedStoreDeviceOperations::without_exclusions(
                        &commit,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                    let prepared_head = operation.prepared_objects.get("store-head").ok_or_else(
                        || {
                            DbError::Message(
                                "Merge Circle operation lacks its prepared Store head".to_string(),
                            )
                        },
                    )?;
                    let materialization = VerifiedMergeMaterialization::verify(
                        &root,
                        &commit,
                        &[],
                        &device_operations,
                        &verified,
                        head,
                        prepared_head,
                        history_evidence,
                        None,
                        &[],
                        None,
                    )?;
                    let retained = MergeMaterializationTransaction::new(&tx, &store_dir)
                        .record_verified_merge_materialization(materialization)?;
                    (
                        commit,
                        activation.clone(),
                        Some(remote_object_id(prepared_head)),
                        retained,
                    )
                };
                retained_cache.validate_insert_verified(&retained)?;
                let mut object_ids = candidate_graph_exact_objects(&commit)?
                    .iter()
                    .map(remote_object_id)
                    .collect::<Vec<_>>();
                for access in &creation.access {
                    if let coven_protocol::circle::CircleAccessDisposition::Active {
                        bootstrap: Some(bootstrap),
                        ..
                    } = &access.leaf.value.disposition
                    {
                        object_ids.extend(
                            bootstrap
                                .blobs
                                .iter()
                                .map(|blob| {
                                    remote_object_id(
                                        blob.stored()
                                            .expect("verified bootstrap remote blob has a locator")
                                            .object(),
                                    )
                                }),
                        );
                    }
                }
                object_ids.push(remote_object_id(&operation.commit_ref.object));
                if let Some(head_object_id) = head_object_id {
                    object_ids.push(head_object_id);
                }
                let store_transaction = MergeMaterializationTransaction::new(&tx, &store_dir);
                store_transaction.activate_store_operation_remote_objects(
                    &operation.commit_ref,
                    &object_ids,
                )?;
                store_transaction.record_verified_circle_activations(
                    &commit,
                    &[activation],
                )?;
                // A deletion the local device authored prunes its own rows,
                // routes, and blob bindings in this activation transaction.
                // Recording the verified activation above already removed its
                // live access cache while retaining the control activation spine.
                if store_transaction.circle_current_state_is_deleted(creation.circle_id)? {
                    crate::prune_ineligible_scoped_rows(
                        &tx,
                        &gates,
                        &std::collections::BTreeSet::from([creation.circle_id]),
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                }
                if !journal.is_finalizing()
                    && matches!(
                    journal.intent,
                    coven_protocol::circle_journal::CircleOperationIntent::RemoveMember { .. }
                )
                {
                    let mut waiting = journal;
                    waiting
                        .wait_for_close_responses()
                        .map_err(|error| DbError::Message(error.to_string()))?;
                    update_circle_operation_phase_on(&tx, &waiting)?;
                } else {
                    release_operation_payloads_on(&tx, &journal.operation_id)?;
                    let deleted = tx
                        .execute(
                            "DELETE FROM circle_operations WHERE operation_id = ?1 AND circle_id = ?2",
                            rusqlite::params![
                                journal.operation_id.as_str(),
                                creation.circle_id.to_string()
                            ],
                        )
                        .map_err(DbError::from)?;
                    if deleted != 1 {
                        return Err(DbError::Message(
                            "circle operation disappeared during activation".to_string(),
                        ));
                    }
                }
                tx.commit().map_err(DbError::from)?;
                retained_cache
                    .insert_verified(retained)
                    .expect("committed Circle materialization passed cache validation");
                Ok(())
            })
            .await
    }
}

fn insert_circle_operation_row_on(
    conn: &Connection,
    row: &PreparedCircleOperationRow,
) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO circle_operations (operation_id, circle_id, prepared, phase)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![row.operation_id, row.circle_id, row.prepared, row.phase],
    )
    .map_err(DbError::from)
    .map(|_| ())
}

/// Move one operation to the phase it now stands in, leaving the prepared
/// operation and its completed upload steps as they are.
pub(crate) fn update_circle_operation_phase_on(
    conn: &Connection,
    journal: &CircleOperationJournal,
) -> Result<(), DbError> {
    let updated = conn
        .execute(
            "UPDATE circle_operations SET phase = ?3
             WHERE operation_id = ?1 AND circle_id = ?2",
            rusqlite::params![
                journal.operation_id.as_str(),
                journal.circle_id.to_string(),
                crate::circle_operation_phase_json(&journal.progress)?
            ],
        )
        .map_err(DbError::from)?;
    if updated != 1 {
        return Err(DbError::Message(format!(
            "circle operation {} disappeared during publication",
            journal.operation_id
        )));
    }
    Ok(())
}

/// Claim the spool file behind every object this operation names.
///
/// Called in the transaction that writes the operation row, so the row and its
/// claims commit together. An object the operation shares with a surviving
/// `remote_objects` record is claimed twice over, which is what keeps the file
/// alive when the operation lets go of it.
///
/// The whole claim set is replaced rather than added to, so the finalization
/// boundary — which hands one operation id a new object graph — never puts an
/// object carried across it through a moment of being owed a deletion.
pub(crate) fn claim_operation_payloads_on(
    conn: &Connection,
    operation_id: &CircleOperationId,
    operation: &coven_protocol::circle_journal::PreparedCircleOperation,
) -> Result<(), DbError> {
    crate::payload_spool::set_payload_owner_claims_on(
        conn,
        &crate::payload_spool::circle_operation_owner_key(operation_id.as_str()),
        &operation
            .prepared_objects
            .values()
            .map(coven_protocol::objects::ExactObjectRef::stored_hash)
            .collect(),
    )
}

/// Let go of every spool file this operation claimed.
///
/// Called in the transaction that stops the operation naming them, so a file no
/// row names any more is owed its deletion by the same commit.
pub(crate) fn release_operation_payloads_on(
    conn: &Connection,
    operation_id: &CircleOperationId,
) -> Result<(), DbError> {
    crate::payload_spool::release_payload_owner_on(
        conn,
        &crate::payload_spool::circle_operation_owner_key(operation_id.as_str()),
    )
}

/// Record that the object one upload step carried is now in cloud storage.
///
/// The durable row is the truth being advanced, so it is read and transitioned
/// in place rather than compared against a reconstruction of what the operation
/// says it should be.
fn mark_uploaded_object_on(
    conn: &Connection,
    object_id: coven_protocol::store_commit::ObjectHash,
) -> Result<(), DbError> {
    let current = load_remote_object_on(conn, object_id)?;
    let mut uploaded = current.clone();
    uploaded
        .mark_uploaded_verified()
        .map_err(|error| DbError::context(format!("mark {object_id} uploaded"), error))?;
    if uploaded == current {
        return Ok(());
    }
    update_remote_object_on(conn, object_id, &uploaded)
}
