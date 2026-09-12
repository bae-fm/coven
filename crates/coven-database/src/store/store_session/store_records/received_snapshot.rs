use super::*;
use crate::store::store_session::{
    merge_materialization_transaction::replay_effect_public_rows, replay_sql::ReplaySql,
};
use coven_protocol::circle::Audience;
use rusqlite::OptionalExtension;
use std::collections::BTreeSet;

impl StoreRecords<'_> {
    /// Keep the receiver's folded Local rows when replacing the shared replay
    /// image. Unsettled writes remain in the journal and are replayed later.
    pub(super) fn received_snapshot_image_with_local_rows_records(
        self,
        local_image: &[u8],
        gates: &crate::Gates,
        covered_suffix: &[crate::MergeReplayWriteEffect],
    ) -> Result<Vec<u8>, DbError> {
        let baseline = load_replay_baseline_on(self)?
            .ok_or_else(|| DbError::Message("received snapshot has no replay image".into()))?;
        let mut image = Connection::open_in_memory()?;
        crate::connection_io::deserialize_database_image_into(
            &mut image,
            &self.verified_payload(baseline.image_payload_hash)?,
        )?;
        let mut local = Connection::open_in_memory()?;
        crate::connection_io::deserialize_database_image_into(&mut local, local_image)?;
        let mut folded_rows = local_rows(&local, gates)?;
        // Only effects beyond the captured prefix can transfer its Local rows.
        // Their Local partitions remain in the journal for ordered replay.
        for effect in covered_suffix {
            for row in replay_effect_public_rows(&local, effect)? {
                folded_rows.remove(&row);
            }
        }
        let image_local_rows = local_rows(&image, gates)?;
        ReplaySql::begin(&local)?.run(|| {
            crate::gate::retain_projection_rows(&local, gates, &folded_rows)?;
            Ok(())
        })?;
        ReplaySql::begin(&image)?.run(|| {
            // Receiver migrations may create Local defaults in the disposable
            // image. Their live counterparts belong to this receiving device.
            for (table, row_id) in image_local_rows {
                image.execute(
                    &format!("DELETE FROM {} WHERE id = ?1", crate::quote_ident(&table)),
                    [&row_id],
                )?;
                if gates.has_scoped_graph() {
                    image.execute(
                        "DELETE FROM _coven_row_routes WHERE table_name = ?1 AND row_id = ?2",
                        (&table, &row_id),
                    )?;
                }
            }
            for (table, row_id) in &folded_rows {
                let exists: bool = image.query_row(
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM {} WHERE id = ?1)",
                        crate::quote_ident(table)
                    ),
                    [row_id],
                    |row| row.get(0),
                )?;
                if exists {
                    return Err(DbError::Message(format!(
                        "accepted checkpoint conflicts with folded Local row {table}/{row_id}"
                    )));
                }
            }
            let mut tables = gates.sorted_synced_table_names();
            if gates.has_scoped_graph() {
                tables.extend([
                    "_coven_row_routes".to_string(),
                    "_coven_audience".to_string(),
                ]);
            }
            for table in tables {
                crate::copy_table_with_conflicts(&local, &image, &table, false)?;
            }
            Ok(())
        })?;
        let violations: bool = image.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )?;
        if violations {
            return Err(DbError::Message(
                "accepted checkpoint and folded Local rows violate foreign keys".into(),
            ));
        }
        crate::connection_io::serialize_database_image(&image)
    }
}

impl StoreTransaction<'_, '_> {
    pub(super) fn import_received_snapshot_inputs_records(
        self,
        source: StoreRecords<'_>,
        inputs: &[crate::OwnedVerifiedMergeMaterialization],
        baseline: &crate::RetainedReplayBaseline,
    ) -> Result<(), DbError> {
        let coverage =
            crate::store::retained_merge_replay::RetainedReplayObjectCoverage::from_baseline(Some(
                baseline,
            ));
        for input in inputs {
            let reference = input.commit_ref();
            let stream_id = reference.coord.stream_id.to_string();
            let sequence = Database::sequence_to_sqlite(&stream_id, reference.coord.sequence)?;
            let existing: Option<String> = self.transaction.query_row(
                "SELECT commit_ref FROM retained_merge_materializations WHERE device_id = ?1 AND seq = ?2",
                (&stream_id, sequence),
                |row| row.get(0),
            ).optional()?;
            if let Some(existing) = existing {
                let existing: coven_protocol::store_commit::StoreBatchCommitRef =
                    serde_json::from_str(&existing)?;
                if existing != *reference {
                    return Err(DbError::Message(
                        "received checkpoint reuses a retained author coordinate".into(),
                    ));
                }
                // The local input can carry recipient packages deliberately
                // omitted from the Store image. Keep its exact existing bytes.
                continue;
            }
            let objects = input.membership_remote_objects()?;
            let verified = crate::VerifiedMergeMaterialization::verify(
                input.root(),
                input.verified_commit(),
                input.registrations(),
                input.device_operations(),
                input.circle_activations(),
                input.acceptance(),
                input.history_evidence(),
                input.membership_objects(),
                input.packages(),
                input.package_application(),
            )?;
            crate::store::store_session::MergeMaterializationTransaction::from_store(self)
                .record_verified_materialization_authority(&verified, &objects)?;
            let canonical: Vec<u8> = source.conn.query_row(
                "SELECT canonical_input FROM retained_merge_materializations WHERE device_id = ?1 AND seq = ?2 AND input_hash = ?3",
                (&stream_id, sequence, input.input_hash().to_string()),
                |row| row.get(0),
            )?;
            let decoded = serde_json::from_slice(&canonical)?;
            self.persist_retained_materialization(
                reference,
                &decoded,
                input.input_hash(),
                canonical,
                &coverage,
            )?;
        }
        Ok(())
    }

    /// Blob inventory includes accepted objects with no current row binding.
    /// Import their provenance without inventing rows or foreign pending owners.
    pub(super) fn import_received_snapshot_blob_inventory_records(
        self,
        source: StoreRecords<'_>,
    ) -> Result<(), DbError> {
        for encoded in crate::query_mapped_rows(
            source.conn,
            "SELECT remote_object_id FROM blob_locators ORDER BY remote_object_id",
            [],
            |row| row.get::<_, String>(0),
        )? {
            let object_id = encoded.parse()?;
            let received = crate::load_remote_object_on(source.conn, object_id)?;
            let locator = crate::blob_records::carried_blob_locator(
                &received,
                "received snapshot inventory",
            )?;
            let stored = coven_protocol::blob::locator::StoredBlobRef::new(
                locator,
                received.object().clone(),
            )?;
            let owners = received.stored_blob_commit_owners();
            let first = owners.first().ok_or_else(|| {
                DbError::Message(
                    "received snapshot blob inventory has no accepted provenance".into(),
                )
            })?;
            let exists: bool = self.transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [&encoded],
                |row| row.get(0),
            )?;
            let mut remote = if exists {
                crate::load_remote_object_on(self.transaction, object_id)?
            } else {
                coven_protocol::remote_object::RemoteObjectRecord::activated_blob(
                    &stored,
                    first.clone(),
                )?
                .into_record()
            };
            for owner in owners {
                remote.merge_blob_activation(&stored, &owner)?;
            }
            for owner in received.snapshot_owners() {
                remote.merge_snapshot_owner(&stored, owner.clone())?;
            }
            if exists {
                crate::update_remote_object_on(self.transaction, object_id, &remote)?;
            } else {
                let closed = coven_protocol::remote_object::ClosedRemoteObject::with_payloads(
                    remote,
                    std::collections::BTreeMap::new(),
                )?;
                crate::persist_exact_remote_object_on(
                    self.transaction,
                    self.store_dir,
                    &closed,
                    "received snapshot blob inventory",
                )?;
            }
            crate::blob_records::record_stored_locator_on(self.transaction, &stored)?;
        }
        Ok(())
    }

    pub(super) fn replace_received_snapshot_baseline_records(
        self,
        source: StoreRecords<'_>,
        baseline: &crate::RetainedReplayBaseline,
        image: Vec<u8>,
        folded: &[crate::SettledStoreWrite],
        retained_inputs: &[crate::OwnedVerifiedMergeMaterialization],
        blob_decls: &crate::BlobDecls,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        let crate::RetainedReplayAuthority::InstalledSnapshot(authority) = &baseline.authority
        else {
            return Err(DbError::Message(
                "received image has no accepted snapshot authority".into(),
            ));
        };
        let records = self.records();
        let previous = load_replay_baseline_on(records)?.ok_or_else(|| {
            DbError::Message("checkpoint replacement has no installed replay baseline".into())
        })?;
        if !baseline.coverage().covers(previous.coverage()) {
            return Err(DbError::Message(
                "received checkpoint regresses the installed replay coverage".into(),
            ));
        }
        let prepared = super::retained_replay::PreparedRetainedReplayBaseline::new(
            baseline.schema_version,
            baseline.routing_hash,
            baseline.authority.clone(),
            image,
        )
        .validate_image(source.store_dir, blob_decls)?;
        let retained = retained_inputs
            .iter()
            .map(|input| serde_json::to_string(input.commit_ref()))
            .collect::<Result<BTreeSet<_>, _>>()?;
        self.retire_history_outside_baseline(baseline.coverage(), &retained)?;
        self.fold_settled_store_writes(baseline.coverage(), folded)?;
        self.rewrite_snapshot_coverage(baseline.coverage(), authority.snapshot.snapshot_hash)?;
        self.transaction
            .execute("DELETE FROM retained_replay_baselines", [])?;
        let mut timings =
            coven_foundation::stage_timing::StageTimings::start("Received replay checkpoint");
        let installed = records.install_prepared_replay_baseline(prepared, &mut timings)?;
        self.replace_snapshot_replay_object_ownership(&installed)?;
        timings.report();
        Ok(installed)
    }
}

fn local_rows(
    connection: &Connection,
    gates: &crate::Gates,
) -> Result<BTreeSet<(String, String)>, DbError> {
    gates
        .private_rows(connection)?
        .into_iter()
        .map(|(table, row_id)| {
            crate::live_row_audience(connection, gates, &table, &row_id)
                .map(|audience| (audience == Audience::Local).then_some((table, row_id)))
                .map_err(DbError::from)
        })
        .filter_map(Result::transpose)
        .collect()
}
