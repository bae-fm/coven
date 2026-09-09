use std::path::{Path, PathBuf};

use super::snapshot_image::{snapshot_image_db_error, verify_circle_bootstrap_image};
use super::*;
use crate::*;

impl VerifiedStoreTransaction<'_, '_, '_, '_> {
    /// Reconstruct the Store as of `cut` and serialize it as a replay baseline,
    /// alongside the write-journal prefix the image now states.
    ///
    /// The live database is left exactly as it was: the projection is built
    /// from this transaction into a separate replay database. Its frontier is
    /// `cut` — checked, not assumed — which is the one property a baseline
    /// image must have, because replay applies the retained commits the cut
    /// does not cover on top of it.
    ///
    /// It also folds in the local partitions of the writes settled at `cut`.
    /// A local partition is stated nowhere else — no commit carries one, and an
    /// image projected for an audience may not — so without this the journal is
    /// the durable home of every local row a device has ever written, replayed
    /// in full on every rebuild and never shorter. Folding them in is what lets
    /// the advance adopting this image delete them, and the returned write ids
    /// are exactly what it may delete.
    pub(super) fn capture_replay_baseline_at_cut(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        cut: &coven_protocol::store_commit::CommitFrontier,
        current_cut: &coven_protocol::store_commit::CommitFrontier,
        snapshot_hash: crate::ObjectHash,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<(Vec<u8>, Vec<crate::SettledStoreWrite>), DbError> {
        if self.authority.root() != root {
            return Err(DbError::Message(
                "replay baseline belongs to another Store root".to_string(),
            ));
        }
        let routing_key = if self.gates.has_scoped_graph() {
            let encryption = routing_encryption.ok_or_else(|| {
                DbError::Message(
                    "scoped replay baseline capture requires Store routing encryption".to_string(),
                )
            })?;
            Some(
                coven_protocol::circle::derive_row_routing_key(encryption, root.store_root_hash)
                    .map_err(DbError::from)?,
            )
        } else {
            None
        };
        let folded = crate::StoreDatabase::settled_store_write_prefix_on(
            crate::store::store_session::StoreRecords::new(
                self.store.transaction,
                self.store.store_dir,
            ),
            cut,
        )?;
        let current_replay = self.authority.replay_projection_result_on(
            self.store,
            self.blob_decls,
            self.gates,
            self.synced_tables,
            routing_key.as_ref(),
            Some(current_cut),
            crate::ReplayJournal::Omit,
            coven_protocol::membership::LocalStoreMembership::Current,
        )?;
        if current_replay.materialized_frontier()? != *current_cut {
            return Err(DbError::Message(
                "replay retirement proof does not cover the current Store frontier".to_string(),
            ));
        }
        let mut crossed_cut = false;
        for reference in current_replay.applied_order() {
            if cut.covers_commit(reference) {
                if crossed_cut {
                    return Err(DbError::ReplayRetirementCutNotPrefix);
                }
            } else {
                crossed_cut = true;
            }
        }
        let replay = self.authority.replay_projection_result_on(
            self.store,
            self.blob_decls,
            self.gates,
            self.synced_tables,
            routing_key.as_ref(),
            Some(cut),
            crate::ReplayJournal::Folded(&folded),
            coven_protocol::membership::LocalStoreMembership::Current,
        )?;
        let replay_frontier = replay.materialized_frontier()?;
        if replay_frontier != *cut {
            return Err(DbError::Message(
                "retained replay baseline cut is not an exact Store frontier".to_string(),
            ));
        }
        Ok((
            replay.capture_replay_baseline(root, cut, snapshot_hash)?,
            folded,
        ))
    }
}

impl StoreSession<'_> {
    fn capture_shared_snapshot_cut(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        temp_dir: &Path,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: coven_protocol::circle::Audience,
    ) -> Result<
        (
            CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        let routing_key = if self.gates.has_scoped_graph() {
            let encryption = routing_encryption.ok_or_else(|| {
                DbError::Message(
                    "scoped snapshot capture requires Store routing encryption".to_string(),
                )
            })?;
            Some(
                coven_protocol::circle::derive_row_routing_key(encryption, root.store_root_hash)
                    .map_err(DbError::from)?,
            )
        } else {
            None
        };
        self.verified_store_transaction(|transaction| {
            if transaction.authority.root() != root {
                return Err(DbError::Message(
                    "snapshot capture belongs to another Store root".to_string(),
                ));
            }
            let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
                crate::store::materialized_commit_index::materialized_frontier_on(
                    transaction.store.transaction,
                    None,
                )?,
            )
            .map_err(|error| DbError::context("snapshot coverage", error))?;
            for entry in super::observed_store_publication::load_store_publication_entries_on(
                transaction.store.transaction,
            )? {
                if let coven_protocol::store_commit::StorePublicationPayload::Commit(reference) =
                    &entry.value.payload
                {
                    if !coverage.covers_commit(reference) {
                        return Err(DbError::Message(format!(
                            "snapshot capture is missing accepted commit {reference:?}"
                        )));
                    }
                }
            }
            let replay = transaction.authority.replay_projection_result_on(
                transaction.store,
                transaction.blob_decls,
                transaction.gates,
                transaction.synced_tables,
                routing_key.as_ref(),
                Some(&coverage),
                crate::ReplayJournal::Omit,
                coven_protocol::membership::LocalStoreMembership::Current,
            )?;
            if replay.materialized_frontier()? != coverage {
                return Err(DbError::Message(
                    "snapshot capture could not reconstruct the accepted Store frontier"
                        .to_string(),
                ));
            }
            let snapshot = SnapshotDatabaseImage::prepare_snapshot(temp_dir)
                .and_then(|image| {
                    replay.capture_snapshot(
                        image,
                        root,
                        transaction.synced_tables,
                        routing_encryption,
                        &audience,
                    )
                })
                .map_err(snapshot_image_db_error)?;
            Ok(StoreTransactionOutcome::Rollback((snapshot, coverage)))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn capture_circle_snapshot_at_cutoff(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        temp_dir: &Path,
        routing_encryption: &coven_keys::encryption::EncryptionService,
        routing_key: &coven_protocol::circle::RowRoutingKey,
        circle_id: coven_protocol::circle::CircleId,
        cutoff: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<CreatedSnapshot, DbError> {
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let replay =
            crate::store::store_session::StoreTransaction::new(&transaction, self.store_dir)
                .replay_projection_with_authority(
                    self.verified_store_authority,
                    root,
                    self.blob_decls,
                    self.gates,
                    self.synced_tables,
                    Some(routing_key),
                    &std::collections::BTreeSet::new(),
                    Some(cutoff),
                    crate::ReplayJournal::Omit,
                    coven_protocol::membership::LocalStoreMembership::Current,
                )?;
        transaction.rollback().map_err(DbError::from)?;
        let replay_frontier = replay.materialized_frontier()?;
        if replay_frontier != *cutoff {
            return Err(DbError::Message(
                "Circle close cutoff is not an exact retained Store frontier".to_string(),
            ));
        }
        SnapshotDatabaseImage::prepare_snapshot(temp_dir)
            .and_then(|image| {
                replay.capture_snapshot(
                    image,
                    root,
                    self.synced_tables,
                    Some(routing_encryption),
                    &coven_protocol::circle::Audience::Circle(circle_id),
                )
            })
            .map_err(snapshot_image_db_error)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn capture_snapshot_image_for_test(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        temp_dir: &Path,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: coven_protocol::circle::Audience,
    ) -> Result<Vec<u8>, DbError> {
        SnapshotDatabaseImage::prepare_snapshot(temp_dir)
            .and_then(|image| {
                StoreRecords::new(self.conn, self.store_dir).capture_snapshot(
                    image,
                    root,
                    self.synced_tables,
                    routing_encryption,
                    &audience,
                )
            })
            .and_then(|snapshot| snapshot.into_parts().0.read_and_discard())
            .map_err(snapshot_image_db_error)
    }
}

impl StoreDatabase {
    pub async fn capture_store_snapshot_cut(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    ) -> Result<
        (
            CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        self.call_store(move |session| {
            session.capture_shared_snapshot_cut(
                &root,
                &temp_dir,
                routing_encryption.as_ref(),
                coven_protocol::circle::Audience::Store,
            )
        })
        .await
    }

    pub async fn capture_circle_snapshot_cut(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: coven_keys::encryption::EncryptionService,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<
        (
            CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        self.call_store(move |session| {
            session.capture_shared_snapshot_cut(
                &root,
                &temp_dir,
                Some(&routing_encryption),
                coven_protocol::circle::Audience::Circle(circle_id),
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn capture_circle_snapshot_at_cutoff(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: coven_keys::encryption::EncryptionService,
        routing_key: coven_protocol::circle::RowRoutingKey,
        circle_id: coven_protocol::circle::CircleId,
        cutoff: coven_protocol::store_commit::CommitFrontier,
    ) -> Result<CreatedSnapshot, DbError> {
        self.call_store(move |session| {
            session.capture_circle_snapshot_at_cutoff(
                &root,
                &temp_dir,
                &routing_encryption,
                &routing_key,
                circle_id,
                &cutoff,
            )
        })
        .await
    }

    pub async fn verify_circle_bootstrap_image(
        &self,
        image: Vec<u8>,
        reference: coven_protocol::circle::CircleBootstrapRef,
        circle_id: coven_protocol::circle::CircleId,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
    ) -> Result<Vec<u8>, SnapshotImageError> {
        self.call_store(move |session| {
            let verification = verify_circle_bootstrap_image(
                &image,
                &reference,
                circle_id,
                session.synced_tables,
                routing_key.as_ref(),
            );
            Ok(verification.map(|()| image))
        })
        .await
        .map_err(SnapshotImageError::from)?
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn capture_snapshot_image_for_test(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    ) -> Result<Vec<u8>, DbError> {
        self.call_store(move |session| {
            session.capture_snapshot_image_for_test(
                &root,
                &temp_dir,
                routing_encryption.as_ref(),
                coven_protocol::circle::Audience::Store,
            )
        })
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn capture_circle_snapshot_image_for_test(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: coven_keys::encryption::EncryptionService,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Vec<u8>, DbError> {
        self.call_store(move |session| {
            session.capture_snapshot_image_for_test(
                &root,
                &temp_dir,
                Some(&routing_encryption),
                coven_protocol::circle::Audience::Circle(circle_id),
            )
        })
        .await
    }
}
