use super::*;
use coven_protocol::store_commit::StorePublicationBase;

impl ReplayProjection {
    pub(super) fn rebase_write(
        &self,
        live: &mut VerifiedStoreTransaction<'_, '_, '_, '_>,
        effect: crate::MergeReplayWriteEffect,
        publication_base: &StorePublicationBase,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<(), DbError> {
        let source = StoreRecords::new(live.store.transaction, live.store.store_dir);
        let (original_hash, encoded_facts): (String, String) = live.store.transaction.query_row(
            "SELECT changeset_hash, blob_facts FROM store_writes WHERE write_id = ?1",
            [effect.write_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let original_hash = original_hash.parse::<crate::ObjectHash>()?;
        let original_facts: crate::StoreWriteBlobFacts = serde_json::from_str(&encoded_facts)
            .map_err(|error| DbError::context("recorded write blob facts", error))?;
        let mut blob_facts = match source.rebased_store_write(&effect.write_id)? {
            Some(rebased) => rebased.blob_facts,
            None => original_facts.clone(),
        };
        let prepared_audiences = crate::load_prepared_audience_objects_on(
            live.store.transaction,
            live.store.store_dir,
            &effect.write_id,
        )?;
        let schema = self.table_schema(live.synced_tables, live.gates)?;
        let tx = self.connection.unchecked_transaction()?;
        tx.pragma_update(None, "defer_foreign_keys", "ON")?;
        ReplaySql::begin(&tx)?.run(|| {
            let mut authority = VerifiedStoreAuthority::for_replay_baseline(self.baseline.clone());
            let materializer = MergeMaterializationTransaction::from_store(StoreTransaction::new(
                &tx,
                &self.store_dir,
            ));
            let mut capture = rusqlite::session::Session::new(&tx)?;
            for table in live.synced_tables {
                capture.attach(Some(table.name()))?;
            }
            if live.gates.has_scoped_graph() {
                capture.attach(Some("_coven_audience"))?;
            }
            let mut private_rows = materializer.capture_replay_rows(live.gates, &schema)?;
            materializer.apply_unaccepted_replay_effect(
                &mut authority,
                live.authority.root(),
                effect.clone(),
                schema,
                live.gates,
                routing_key,
                &mut private_rows,
            )?;
            let actual = crate::capture_changeset(&mut capture)?;
            crate::validate_scoped_foreign_key_audiences(&tx, live.gates)?;
            live.blob_decls.validate_changed_rows(&tx, &actual)?;
            // Publication retains the captured operations and their timestamps,
            // even where merging omits them. The actual effect below is only
            // the inverse-discard input at this new base.
            for fact in &mut blob_facts.blobs {
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
                        }
                    }
                }
            }
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
                &effect
                    .partitions
                    .store
                    .into_iter()
                    .chain(effect.partitions.circles)
                    .chain(effect.partitions.local)
                    .collect::<Vec<_>>(),
            )?;
            drop(capture);
            Ok(())
        })?;
        tx.commit()?;
        Ok(())
    }
}
