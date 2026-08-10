use crate::blob_records::live_blob_row;
use crate::blob_records::validate_live_blob_locator;
use crate::cloud_outbox_records::CloudOutboxRecords;
use crate::remote_object_records::load_remote_object_on;
use crate::remote_object_records::persist_exact_remote_object_on;
use crate::remote_object_records::update_remote_object_on;

use super::*;

pub(crate) fn install_pulled_package_activation_on(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    commit_ref: &StoreBatchCommitRef,
    domain: SharedLiveSetObjectDomain,
    object: &ExactObjectRef,
    package: &AudiencePackage,
) -> Result<(), DbError> {
    let object_id = remote_object_id(object);
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
                DbError::context(
                    format!("activate locally prepared pulled package {object_id}"),
                    error,
                )
            })?
        } else {
            remote
        };
        remote
            .merge_package_activation(&domain, package, commit_ref)
            .map_err(|error| {
                DbError::context(
                    format!("merge pulled package activation {object_id}"),
                    error,
                )
            })?;
        update_remote_object_on(conn, object_id, &remote)
    } else {
        let remote =
            RemoteObjectRecord::activated_external_package(domain, package, commit_ref.clone())
                .map_err(|error| {
                    DbError::context(
                        format!("construct pulled package activation {object_id}"),
                        error,
                    )
                })?;
        persist_exact_remote_object_on(conn, store_dir, &remote, "pulled audience package")
    }
}

pub(crate) fn install_pulled_merge_membership_activations_on(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    commit_ref: &StoreBatchCommitRef,
    remotes: &[coven_protocol::remote_object::ClosedRemoteObject],
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
                    DbError::context(
                        format!("merge pulled Merge membership authority {object_id}"),
                        error,
                    )
                })?;
            update_remote_object_on(conn, object_id, &remote)?;
        } else {
            persist_exact_remote_object_on(
                conn,
                store_dir,
                expected,
                "pulled Merge membership authority",
            )?;
        }
    }
    Ok(())
}

impl Database {
    // ---- Materialized Store commit ledger ----

    pub fn install_pulled_blob_activations_on(
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
                        DbError::context(format!("merge pulled blob activation {object_id}"), error)
                    })?;
                remote
            } else {
                RemoteObjectRecord::activated_blob(stored, owner.clone())
                    .map_err(|error| {
                        DbError::context(
                            format!("construct pulled blob activation {object_id}"),
                            error,
                        )
                    })?
                    .into_record()
            };
            let state = serde_json::to_string(&remote)
                .map_err(|error| DbError::context("serialize pulled blob activation", error))?;
            conn.execute(
                "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2) \
                 ON CONFLICT(object_id) DO UPDATE SET state = excluded.state",
                rusqlite::params![object_id.to_string(), state],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub fn row_blob_refs_for_root_on(
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

    pub fn stored_blob_reference_state_on(
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
            // A row reaches the cloud in two different ways: a gated root is
            // kept, while an audience-scoped root is addressed to a non-Local
            // audience. An absent row or unreachable audience parent leaves the
            // locality unresolved; it cannot prove that the blob is unreferenced.
            let remote = if !gates.table_is_scoped(&table_name) {
                gates
                    .root_kept_of(conn, &table_name, &row_id)
                    .map_err(|error| DbError::Message(error.to_string()))?
            } else {
                match crate::live_row_audience(conn, gates, &table_name, &row_id) {
                    Ok(audience) => Some(audience != coven_protocol::circle::Audience::Local),
                    Err(
                        crate::GateError::MissingAudienceRow { .. }
                        | crate::GateError::MissingAudienceParent { .. },
                    ) => None,
                    Err(error) => return Err(DbError::Message(error.to_string())),
                }
            };
            match remote {
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

    pub fn row_blob_ref_on(
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
                DbError::context(
                    format!(
                        "resolve blob row audience for {:?}/{row_id:?}",
                        table.name()
                    ),
                    error,
                )
            })?;
        let (authority, stored) = match RemoteAudience::try_from(audience.clone()) {
            Err(_) if audience == Audience::Local => (RowBlobAuthority::Local, None),
            Err(error) => {
                return Err(DbError::context(
                    format!(
                        "blob row {:?}/{row_id:?} has invalid audience",
                        table.name()
                    ),
                    error,
                ));
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
                    let package_authority: coven_protocol::audience_package::PackageAudience =
                        serde_json::from_str(&authority_json).map_err(|error| {
                            DbError::context(format!("remote blob row {:?}/{row_id:?} has invalid audience authority", table.name()), error)
                        })?;
                    let remote_object_id = remote_object_id.parse().map_err(|error| {
                        DbError::context(
                            format!(
                                "remote blob row {:?}/{row_id:?} has invalid prepared object id",
                                table.name()
                            ),
                            error,
                        )
                    })?;
                    let remote = load_remote_object_on(conn, remote_object_id)?;
                    if !remote.is_activated_stored_blob() {
                        return Err(DbError::Message(format!(
                            "remote blob row {:?}/{row_id:?} references a blob without activated ownership",
                            table.name()
                        )));
                    }
                    let locator = crate::blob_records::carried_blob_locator(
                        &remote,
                        &format!(
                            "remote blob row {:?}/{row_id:?} has invalid locator",
                            table.name()
                        ),
                    )?;
                    let stored = StoredBlobRef::new(locator, remote.object().clone()).map_err(
                        |error| {
                            DbError::context(format!("remote blob row {:?}/{row_id:?} has invalid stored blob reference", table.name()), error)
                        },
                    )?;
                    Some((package_authority, stored))
                } else {
                    CloudOutboxRecords::new(conn)
                        .created_upload_handoff(
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

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn row_blob_ref(&self, table: &str, row_id: &str) -> Result<RowBlobRef, DbError> {
        let table = table.to_string();
        let row_id = row_id.to_string();
        self.call_database(move |session| session.row_blob_ref(&table, &row_id))
            .await
    }
}
