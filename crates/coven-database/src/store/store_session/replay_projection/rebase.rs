use super::*;
use crate::store::store_session::host_write_capture::{
    capture_partition_blob_facts_on, partition_captured_write_on,
};
use coven_protocol::store_commit::StorePublicationBase;
use std::collections::{BTreeMap, BTreeSet};

impl ReplayProjection {
    pub(super) fn rebase_write(
        &self,
        live: &mut VerifiedStoreTransaction<'_, '_, '_, '_>,
        effect: crate::MergeReplayWriteEffect,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        publication_base: &StorePublicationBase,
    ) -> Result<(), DbError> {
        let source = StoreRecords::new(live.store.transaction, live.store.store_dir);
        let (original_hash, encoded_facts): (String, String) = live.store.transaction.query_row(
            "SELECT changeset_hash, blob_facts FROM store_writes WHERE write_id = ?1",
            [effect.write_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let original_hash = original_hash.parse::<crate::ObjectHash>()?;
        let original = source.payload(original_hash)?;
        let original_facts: crate::StoreWriteBlobFacts = serde_json::from_str(&encoded_facts)
            .map_err(|error| DbError::context("recorded write blob facts", error))?;
        let previous_facts = match source.rebased_store_write(&effect.write_id)? {
            Some(rebased) => rebased.blob_facts,
            None => original_facts.clone(),
        };
        let prepared_audiences = crate::load_prepared_audience_objects_on(
            live.store.transaction,
            live.store.store_dir,
            &effect.write_id,
        )?;
        let host_edits = crate::gate::recorded_host_changeset(&original)?;
        let schema = self.table_schema(live.synced_tables, live.gates)?;
        let tx = self.connection.unchecked_transaction()?;
        tx.pragma_update(None, "defer_foreign_keys", "ON")?;
        ReplaySql::begin(&tx)?.run(|| {
            let mut authority = VerifiedStoreAuthority::for_replay_baseline(self.baseline.clone());
            let materializer = MergeMaterializationTransaction::from_store(StoreTransaction::new(
                &tx,
                &self.store_dir,
            ));
            materializer.validate_recorded_replay_context(
                &mut authority,
                live.authority.root(),
                &effect,
                live.gates,
            )?;
            let row_keys = crate::walk_changeset(&host_edits)?
                .into_iter()
                .map(|row| {
                    let id = row
                        .pk()
                        .ok_or_else(|| {
                            DbError::Message(format!(
                                "recorded write {} has no identity in {}",
                                effect.write_id, row.table,
                            ))
                        })?
                        .to_string();
                    Ok((row.table, id))
                })
                .collect::<Result<BTreeSet<_>, DbError>>()?;
            let mut before_facts = BTreeMap::new();
            for (table, id) in &row_keys {
                if let Some(publication) =
                    live.blob_decls.publication_blob_for_row(&tx, table, id)?
                {
                    let fact = StoreDatabase::capture_store_write_blob_fact_on(&tx, publication)?;
                    before_facts.insert((table.clone(), id.clone(), fact.column.clone()), fact);
                }
            }
            let mut capture = rusqlite::session::Session::new(&tx)?;
            for table in live.synced_tables {
                capture.attach(Some(table.name()))?;
            }
            if live.gates.has_scoped_graph() {
                capture.attach(Some("_coven_audience"))?;
                capture.attach(Some("_coven_row_routes"))?;
            }
            if let Some(floor) = live.clock_floor.as_ref() {
                live.clock.advance_past(floor);
            }
            let stamp = live.clock.now();
            live.clock_floor = Some(stamp.clone());
            materializer.apply_recorded_changeset(
                crate::ValidatedChangeset::new(host_edits, schema.clone())?,
                &effect.write_id,
                &stamp,
            )?;
            let mut captured = crate::capture_changeset(&mut capture)?;
            crate::validate_scoped_foreign_key_audiences(&tx, live.gates)?;
            let moves = crate::audience_moves(&tx, &captured, live.gates)?;
            if StoreDatabase::advance_moved_blob_row_stamps_on(&tx, &moves, live.blob_decls)? {
                captured = crate::capture_changeset(&mut capture)?;
            }
            live.blob_decls.validate_changed_rows(&tx, &captured)?;
            let partitioned = partition_captured_write_on(&tx, &captured, live.gates, routing_key)?;
            materializer.validate_recorded_foreign_keys(&effect.write_id, &schema)?;
            let mut blob_facts = StoreDatabase::capture_audience_move_blob_facts_on(
                &tx,
                &partitioned.moves,
                live.blob_decls,
                capture_partition_blob_facts_on(&tx, &partitioned.partitions, live.blob_decls)?,
            )?;
            for fact in &mut blob_facts.blobs {
                let exact_content = |prior: &&crate::StoreWriteBlobFact| {
                    prior.table == fact.table
                        && prior.row_id == fact.row_id
                        && prior.column == fact.column
                        && prior.blob == fact.blob
                        && prior.plaintext_size == fact.plaintext_size
                        && prior.plaintext_hash == fact.plaintext_hash
                };
                let recorded = previous_facts.blobs.iter().find(exact_content);
                let before = before_facts
                    .get(&(fact.table.clone(), fact.row_id.clone(), fact.column.clone()))
                    .filter(|prior| {
                        prior.blob == fact.blob
                            && prior.plaintext_hash == fact.plaintext_hash
                            && prior.plaintext_size == fact.plaintext_size
                    });
                if let Some(prior) = recorded {
                    fact.external_path = prior.external_path.clone();
                    fact.previous = prior.previous.clone();
                    fact.audience_move = prior.audience_move.clone();
                }
                if let Some(prior) = before {
                    if prior.previous.is_some() {
                        fact.previous = prior.previous.clone();
                    }
                    if prior.external_path.is_some() {
                        fact.external_path = prior.external_path.clone();
                    }
                }
                for package in &prepared_audiences.packages {
                    for binding in package.package().blob_bindings() {
                        if binding.table() != fact.table
                            || binding.row_id() != fact.row_id
                            || binding.column() != fact.column
                            || !coven_protocol::blob::locator_describes_row(
                                binding.blob().locator(),
                                &fact.blob,
                                fact.plaintext_size,
                                fact.plaintext_hash,
                            )
                        {
                            continue;
                        }
                        let object = crate::load_remote_object_on(
                            live.store.transaction,
                            coven_protocol::remote_object::remote_object_id(
                                binding.blob().object(),
                            ),
                        )?;
                        if object.records_verified_upload() {
                            fact.previous = Some(crate::StoreWriteRemoteBlob {
                                authority: package.package().audience().clone(),
                                stored: binding.blob().clone(),
                            });
                            fact.audience_move = None;
                        }
                    }
                }
            }
            let actual = crate::capture_changeset(&mut capture)?;
            crate::changeset_identity::validate_captured_row_identities(
                &actual,
                live.synced_tables,
            )?;
            let changeset_hash = live.store.install_payload(&actual)?;
            let current_base = crate::StoreWriteBase {
                dependencies: self.materialized_frontier()?.into_refs(),
            };
            let rebased = crate::write_models::RebasedStoreWrite {
                base: current_base,
                publication_base: publication_base.clone(),
                changeset_hash,
                blob_facts,
            };
            live.store.replace_rebased_store_write(
                &effect.write_id,
                original_hash,
                &original_facts,
                &rebased,
                &partitioned.partitions,
            )?;
            drop(capture);
            Ok(())
        })?;
        tx.commit()?;
        Ok(())
    }
}
