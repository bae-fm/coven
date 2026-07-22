use crate::database::blob_records::live_blob_row;
use crate::database::blob_records::validate_live_blob_locator;
use crate::database::blob_records::validate_live_blob_row;
use crate::database::blob_records::validate_stored_locator_on;
use crate::database::blob_records::validate_stored_row_binding_on;
use crate::database::cloud_outbox_records::created_upload_handoff_on;
use crate::database::cloud_outbox_records::upload_entry_for_identity_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::persist_exact_remote_object_on;
use crate::database::remote_object_records::update_remote_object_on;
use crate::database::remote_object_records::validate_remote_object_on;
use crate::database::remote_object_records::RemoteStoredRepresentationRef;

use super::*;

impl Database {
    // ---- Materialized Store commit ledger ----

    /// Install only blob bindings whose exact row stamp won the enclosing
    /// changeset. The caller owns the transaction, so app rows, locator facts,
    /// and the materialized commit position either all commit or all roll back.
    pub(crate) fn install_winning_blob_bindings_on(
        conn: &Connection,
        gates: &Gates,
        synced_tables: &[SyncedTable],
        package: &AudiencePackage,
        activation: &BlobActivation,
        winning_rows: &[crate::sync::apply::WinningRow],
    ) -> Result<usize, DbError> {
        if package.commit_coord() != &activation.coord {
            return Err(DbError::Message(format!(
                "blob activation {:?} does not match audience package {:?}",
                activation.coord,
                package.commit_coord()
            )));
        }

        let package_audience = package.audience().remote_audience();
        for winner in winning_rows {
            if gate::is_routing_table(&winner.table) {
                continue;
            }
            let Some(table) = synced_tables
                .iter()
                .find(|table| table.name() == winner.table)
            else {
                return Err(DbError::Message(format!(
                    "winning changeset row names undeclared table {:?}",
                    winner.table
                )));
            };
            let Some(declaration) = table.blob() else {
                continue;
            };
            match winner.row_stamp.as_deref() {
                Some(row_stamp) => conn.execute(
                    "DELETE FROM row_blob_locators
                     WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                       AND row_stamp <> ?4",
                    rusqlite::params![
                        winner.table,
                        winner.row_id,
                        declaration.id_column,
                        row_stamp,
                    ],
                ),
                None => conn.execute(
                    "DELETE FROM row_blob_locators
                     WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3",
                    rusqlite::params![winner.table, winner.row_id, declaration.id_column],
                ),
            }
            .map_err(DbError::from)?;
        }
        let mut installed = 0;
        for binding in package.blob_bindings() {
            let Some(table) = synced_tables
                .iter()
                .find(|table| table.name() == binding.table())
            else {
                return Err(DbError::Message(format!(
                    "blob binding names undeclared table {:?}",
                    binding.table()
                )));
            };
            let declaration = table.blob().ok_or_else(|| {
                DbError::Message(format!(
                    "blob binding names table {:?}, which has no blob declaration",
                    binding.table()
                ))
            })?;
            if binding.column() != declaration.id_column {
                return Err(DbError::Message(format!(
                    "blob binding column {:?} does not match declared blob-id column {:?} on table {:?}",
                    binding.column(), declaration.id_column, binding.table()
                )));
            }

            let Some(row) = live_blob_row(conn, binding.table(), binding.row_id(), declaration)?
            else {
                continue;
            };
            if row.stamp != binding.row_stamp() {
                continue;
            }

            let live_audience =
                gate::live_row_audience(conn, gates, binding.table(), binding.row_id()).map_err(
                    |error| {
                        DbError::Message(format!(
                            "resolve winning blob row audience for {:?}/{:?}: {error}",
                            binding.table(),
                            binding.row_id()
                        ))
                    },
                )?;
            let live_audience = RemoteAudience::try_from(live_audience).map_err(|error| {
                DbError::Message(format!(
                    "winning blob row {:?}/{:?} is not remote: {error}",
                    binding.table(),
                    binding.row_id()
                ))
            })?;
            if live_audience != package_audience {
                return Err(DbError::Message(format!(
                    "winning blob row {:?}/{:?} belongs to {:?}, but its package belongs to {:?}",
                    binding.table(),
                    binding.row_id(),
                    live_audience,
                    package_audience
                )));
            }
            validate_live_blob_row(binding, declaration, &row, &live_audience)?;

            let locator = binding.blob().locator();
            let locator_hash = locator.locator_hash();
            let remote_object_id = remote_object_id(binding.blob().object());
            let remote = load_remote_object_on(conn, remote_object_id)?;
            if !remote.is_activated_stored_blob() {
                return Err(DbError::Message(format!(
                    "blob locator {locator_hash} does not reference an activated uploaded blob"
                )));
            }
            validate_remote_object_on(
                conn,
                remote_object_id,
                binding.blob().object(),
                &locator.to_bytes(),
                RemoteStoredRepresentationRef::Blob,
            )?;
            conn.execute(
                "INSERT INTO blob_locators
                 (remote_object_id, locator_hash)
                 VALUES (?1, ?2)
                 ON CONFLICT(remote_object_id) DO NOTHING",
                rusqlite::params![remote_object_id.to_string(), locator_hash.to_string(),],
            )
            .map_err(DbError::from)?;
            validate_stored_locator_on(conn, binding.blob())?;

            let audience_authority =
                serde_json::to_string(package.audience()).map_err(|error| {
                    DbError::Message(format!("serialize row blob audience authority: {error}"))
                })?;
            conn.execute(
                "INSERT INTO row_blob_locators
                 (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(table_name, row_id, column_name, row_stamp) DO NOTHING",
                rusqlite::params![
                    binding.table(),
                    binding.row_id(),
                    binding.column(),
                    binding.row_stamp(),
                    audience_authority,
                    remote_object_id.to_string(),
                ],
            )
            .map_err(DbError::from)?;
            validate_stored_row_binding_on(conn, binding, package.audience(), remote_object_id)?;
            installed += 1;
        }
        Ok(installed)
    }

    pub(crate) fn install_pulled_blob_activations_on(
        conn: &Connection,
        package: &AudiencePackage,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        if package.commit_coord() != &owner.coord {
            return Err(DbError::Message(
                "pulled blob package coordinate differs from its activating commit".to_string(),
            ));
        }
        for binding in package.blob_bindings() {
            let stored = binding.blob();
            let object_id = remote_object_id(stored.object());
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                    [object_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let remote = if exists {
                let mut remote = load_remote_object_on(conn, object_id)?;
                remote
                    .merge_blob_activation(stored, owner)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "merge pulled blob activation {object_id}: {error}"
                        ))
                    })?;
                remote
            } else {
                RemoteObjectRecord::activated_blob(stored, owner.clone()).map_err(|error| {
                    DbError::Message(format!(
                        "construct pulled blob activation {object_id}: {error}"
                    ))
                })?
            };
            let state = serde_json::to_string(&remote).map_err(|error| {
                DbError::Message(format!("serialize pulled blob activation: {error}"))
            })?;
            conn.execute(
                "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2) \
                 ON CONFLICT(object_id) DO UPDATE SET state = excluded.state",
                rusqlite::params![object_id.to_string(), state],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(crate) fn install_pulled_package_activation_on(
        conn: &Connection,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        package: &AudiencePackage,
    ) -> Result<(), DbError> {
        commit_ref
            .verify_commit(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let retained = Self::retained_audience_package(commit, commit_ref, package.clone())?;
        let domain = retained.domain();
        let object_id = remote_object_id(retained.object());
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if exists {
            let remote = load_remote_object_on(conn, object_id)?;
            let mut remote = if matches!(remote, RemoteObjectRecord::CandidateExclusive(_)) {
                remote.into_activated(commit_ref).map_err(|error| {
                    DbError::Message(format!(
                        "activate locally prepared pulled package {object_id}: {error}"
                    ))
                })?
            } else {
                remote
            };
            remote
                .merge_package_activation(&domain, retained.package(), commit_ref)
                .map_err(|error| {
                    DbError::Message(format!(
                        "merge pulled package activation {object_id}: {error}"
                    ))
                })?;
            update_remote_object_on(conn, object_id, &remote)
        } else {
            let remote = RemoteObjectRecord::activated_external_package(
                domain,
                retained.package(),
                commit_ref.clone(),
            )
            .map_err(|error| {
                DbError::Message(format!(
                    "construct pulled package activation {object_id}: {error}"
                ))
            })?;
            persist_exact_remote_object_on(conn, &remote, "pulled audience package")
        }
    }

    pub(crate) fn install_pulled_merge_membership_activations_on(
        conn: &Connection,
        commit_ref: &StoreBatchCommitRef,
        remotes: &[RemoteObjectRecord],
    ) -> Result<(), DbError> {
        let mut object_ids = BTreeSet::new();
        for expected in remotes {
            let object_id = expected.object_id();
            if !object_ids.insert(object_id) {
                return Err(DbError::Message(
                    "pulled Merge membership closure repeats an exact object".to_string(),
                ));
            }
            let existing = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                    [object_id.to_string()],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DbError::from)?;
            if existing {
                let mut remote = load_remote_object_on(conn, object_id)?;
                remote
                    .merge_retained_authority_activation(expected, commit_ref)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "merge pulled Merge membership authority {object_id}: {error}"
                        ))
                    })?;
                update_remote_object_on(conn, object_id, &remote)?;
            } else {
                persist_exact_remote_object_on(
                    conn,
                    expected,
                    "pulled Merge membership authority",
                )?;
            }
        }
        Ok(())
    }

    pub async fn row_blob_ref(&self, table: &str, row_id: &str) -> Result<RowBlobRef, DbError> {
        let table = self
            .synced_tables()
            .iter()
            .find(|candidate| candidate.name() == table)
            .cloned()
            .ok_or_else(|| DbError::Message(format!("undeclared synced table {table:?}")))?;
        if table.blob().is_none() {
            return Err(DbError::Message(format!(
                "synced table {:?} has no blob declaration",
                table.name()
            )));
        }
        let row_id = row_id.to_string();
        let gates = self.gates();
        self.call(move |conn| Self::row_blob_ref_on(conn, &gates, &table, &row_id))
            .await
    }

    pub async fn validate_row_blob_ref(&self, reference: &RowBlobRef) -> Result<(), DbError> {
        let current = self
            .row_blob_ref(reference.table(), reference.row_id())
            .await?;
        if &current != reference {
            return Err(DbError::Message(format!(
                "row blob reference {:?}/{:?}/{:?} at {:?} is stale",
                reference.table(),
                reference.row_id(),
                reference.column(),
                reference.row_stamp()
            )));
        }
        Ok(())
    }

    pub async fn row_blob_refs_for_root(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<RowBlobRef>, DbError> {
        let root_table = root_table.to_string();
        let root_id = root_id.to_string();
        let gates = self.gates();
        let tables = self.synced_tables().to_vec();
        self.call(move |conn| {
            Self::row_blob_refs_for_root_on(conn, &gates, &tables, &root_table, &root_id)
        })
        .await
    }

    pub(crate) fn row_blob_refs_for_root_on(
        conn: &Connection,
        gates: &Gates,
        tables: &[SyncedTable],
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<RowBlobRef>, DbError> {
        let mut rows = gates
            .subtree_rows(conn, root_table, root_id)
            .map_err(|error| DbError::Message(error.to_string()))?
            .into_iter()
            .collect::<Vec<_>>();
        rows.sort();
        let tables = tables
            .iter()
            .map(|table| (table.name(), table))
            .collect::<BTreeMap<_, _>>();
        rows.into_iter()
            .filter_map(|(table_name, row_id)| {
                tables
                    .get(table_name.as_str())
                    .filter(|table| table.blob().is_some())
                    .map(|table| Self::row_blob_ref_on(conn, gates, table, &row_id))
            })
            .collect()
    }

    pub(crate) fn upload_entries_for_rows_on(
        conn: &Connection,
        rows: &[RowBlobRef],
    ) -> Result<Vec<OutboxEntry>, DbError> {
        rows.iter()
            .filter_map(|row| {
                match upload_entry_for_identity_on(
                    conn,
                    row.table(),
                    row.row_id(),
                    row.column(),
                    row.row_stamp(),
                ) {
                    Ok(Some(entry)) => Some(Ok(entry)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .collect()
    }

    pub(crate) fn upload_entries_for_root_on(
        conn: &Connection,
        gates: &Gates,
        tables: &[SyncedTable],
        root_table: &str,
        root_id: &str,
    ) -> Result<Vec<OutboxEntry>, DbError> {
        let rows = Self::row_blob_refs_for_root_on(conn, gates, tables, root_table, root_id)?;
        Self::upload_entries_for_rows_on(conn, &rows)
    }

    pub(crate) fn stored_blob_reference_state_on(
        conn: &Connection,
        gates: &Gates,
        tables: &[SyncedTable],
        stored: &StoredBlobRef,
    ) -> Result<StoredBlobReferenceState, DbError> {
        let exact_object_id = remote_object_id(stored.object()).to_string();
        let mut statement = conn
            .prepare(
                "SELECT table_name, row_id, row_stamp FROM row_blob_locators
                 WHERE remote_object_id = ?1",
            )
            .map_err(DbError::from)?;
        let bindings = statement
            .query_map([exact_object_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut unresolved = false;
        for (table_name, row_id, row_stamp) in bindings {
            let table = tables
                .iter()
                .find(|candidate| candidate.name() == table_name)
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "stored blob binding names undeclared table {table_name:?}"
                    ))
                })?;
            let declaration = table.blob().ok_or_else(|| {
                DbError::Message(format!(
                    "stored blob binding names table {table_name:?} without a blob declaration"
                ))
            })?;
            let Some(live) = live_blob_row(conn, &table_name, &row_id, declaration)? else {
                continue;
            };
            if live.stamp != row_stamp {
                continue;
            }
            match gates
                .root_kept_of(conn, &table_name, &row_id)
                .map_err(|error| DbError::Message(error.to_string()))?
            {
                Some(false) => continue,
                None => {
                    unresolved = true;
                    continue;
                }
                Some(true) => {}
            }
            let reference = Self::row_blob_ref_on(conn, gates, table, &row_id)?;
            if matches!(reference.authority(), RowBlobAuthority::Remote(_))
                && reference.stored() == Some(stored)
            {
                return Ok(StoredBlobReferenceState::LiveRemote);
            }
        }
        Ok(if unresolved {
            StoredBlobReferenceState::Unresolved
        } else {
            StoredBlobReferenceState::NotLiveRemote
        })
    }

    pub(crate) fn row_blob_ref_on(
        conn: &Connection,
        gates: &Gates,
        table: &SyncedTable,
        row_id: &str,
    ) -> Result<RowBlobRef, DbError> {
        let declaration = table.blob().ok_or_else(|| {
            DbError::Message(format!(
                "synced table {:?} has no blob declaration",
                table.name()
            ))
        })?;
        let row = live_blob_row(conn, table.name(), row_id, declaration)?.ok_or_else(|| {
            DbError::Message(format!(
                "blob-bearing row {:?}/{row_id:?} does not exist",
                table.name()
            ))
        })?;
        let audience =
            gate::live_row_audience(conn, gates, table.name(), row_id).map_err(|error| {
                DbError::Message(format!(
                    "resolve blob row audience for {:?}/{row_id:?}: {error}",
                    table.name()
                ))
            })?;
        let (authority, stored) = match RemoteAudience::try_from(audience.clone()) {
            Err(_) if audience == Audience::Local => (RowBlobAuthority::Local, None),
            Err(error) => {
                return Err(DbError::Message(format!(
                    "blob row {:?}/{row_id:?} has invalid audience: {error}",
                    table.name()
                )));
            }
            Ok(remote_audience) => {
                let installed: Option<(String, String)> = conn
                    .query_row(
                        "SELECT binding.audience_authority, locator.remote_object_id
                         FROM row_blob_locators AS binding
                         JOIN blob_locators AS locator
                           ON locator.remote_object_id = binding.remote_object_id
                         WHERE binding.table_name = ?1
                           AND binding.row_id = ?2
                           AND binding.column_name = ?3
                           AND binding.row_stamp = ?4",
                        rusqlite::params![table.name(), row_id, declaration.id_column, row.stamp,],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                let exact = if let Some((authority_json, remote_object_id)) = installed {
                    let package_authority: crate::sync::audience_package::PackageAudience =
                        serde_json::from_str(&authority_json).map_err(|error| {
                            DbError::Message(format!(
                                "remote blob row {:?}/{row_id:?} has invalid audience authority: {error}",
                                table.name()
                            ))
                        })?;
                    let remote_object_id = remote_object_id.parse().map_err(|error| {
                        DbError::Message(format!(
                            "remote blob row {:?}/{row_id:?} has invalid prepared object id: {error}",
                            table.name()
                        ))
                    })?;
                    let remote = load_remote_object_on(conn, remote_object_id)?;
                    if !remote.is_activated_stored_blob() {
                        return Err(DbError::Message(format!(
                            "remote blob row {:?}/{row_id:?} references a blob without activated ownership",
                            table.name()
                        )));
                    }
                    let locator = BlobLocator::parse(remote.bytes().canonical_semantic_bytes())
                        .map_err(|error| {
                            DbError::Message(format!(
                                "remote blob row {:?}/{row_id:?} has invalid locator: {error}",
                                table.name()
                            ))
                        })?;
                    let stored = StoredBlobRef::new(locator, remote.object().clone()).map_err(
                        |error| {
                            DbError::Message(format!(
                                "remote blob row {:?}/{row_id:?} has invalid stored blob reference: {error}",
                                table.name()
                            ))
                        },
                    )?;
                    Some((package_authority, stored))
                } else {
                    created_upload_handoff_on(
                        conn,
                        table.name(),
                        row_id,
                        &declaration.id_column,
                        &row.stamp,
                    )?
                    .map(|handoff| (handoff.authority, handoff.stored))
                };
                let Some((package_authority, stored)) = exact else {
                    return RowBlobRef::new(
                        table.name().to_string(),
                        row_id.to_string(),
                        row.stamp,
                        declaration.id_column.clone(),
                        BlobRef {
                            namespace: declaration.namespace.clone(),
                            id: row.blob_id,
                            scope: declaration.scope.clone(),
                            cloud_path: row.cloud_path,
                            provenance: declaration.provenance,
                            fill: declaration.fill,
                        },
                        row.plaintext_size,
                        row.plaintext_hash,
                        RowBlobAuthority::PendingRemote(remote_audience),
                        None,
                    )
                    .map_err(DbError::Message);
                };
                if package_authority.remote_audience() != remote_audience {
                    return Err(DbError::Message(format!(
                        "remote blob row {:?}/{row_id:?} has audience authority {:?}, expected {remote_audience:?}",
                        table.name(), package_authority
                    )));
                }
                validate_live_blob_locator(
                    table.name(),
                    row_id,
                    &declaration.id_column,
                    &row.stamp,
                    &stored,
                    declaration,
                    &row,
                    &remote_audience,
                )?;
                (RowBlobAuthority::Remote(package_authority), Some(stored))
            }
        };
        let blob = BlobRef {
            namespace: declaration.namespace.clone(),
            id: row.blob_id.clone(),
            scope: declaration.scope.clone(),
            cloud_path: row.cloud_path.clone(),
            provenance: declaration.provenance,
            fill: declaration.fill,
        };
        RowBlobRef::new(
            table.name().to_string(),
            row_id.to_string(),
            row.stamp,
            declaration.id_column.clone(),
            blob,
            row.plaintext_size,
            row.plaintext_hash,
            authority,
            stored,
        )
        .map_err(DbError::Message)
    }
}
