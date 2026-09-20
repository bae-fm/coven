//! Schema identity belongs to retained write effects, not their terminal receipts.

use super::publication_state::PreparedStoreWriteState;
use super::verified_store_authority::{verify_prepared_store_commit_on, VerifiedStoreAuthority};
use super::StoreRecords;
use crate::{DbError, Migration};
use coven_foundation::store_dir::StoreDir;
use rusqlite::Connection;

pub(crate) fn validate_store_write_schemas(conn: &Connection) -> Result<(), DbError> {
    let invalid: bool = conn.query_row(
        "SELECT EXISTS (
            SELECT 1 FROM store_writes w LEFT JOIN store_write_schemas s USING (write_id)
            WHERE (w.changeset_hash IS NOT NULL) != (s.schema_version IS NOT NULL)
        ) OR EXISTS (
            SELECT 1 FROM store_write_schemas s LEFT JOIN store_writes w USING (write_id)
            WHERE w.write_id IS NULL
        )",
        [],
        |row| row.get(0),
    )?;
    if invalid {
        return Err(DbError::Message(
            "retained write effects and captured schema records differ".into(),
        ));
    }
    Ok(())
}

pub(crate) fn recover_store_write_schemas(
    conn: &Connection,
    store_dir: &StoreDir,
    migrations: &[Migration],
) -> Result<(), DbError> {
    let records = StoreRecords::new(conn, store_dir);
    let rows = crate::query_mapped_rows(conn,
        "SELECT write_id, changeset_hash, prepared, rebased, status FROM store_writes WHERE changeset_hash IS NOT NULL ORDER BY ordinal",
        [], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?, row.get::<_, Option<String>>(3)?, row.get::<_, String>(4)?)))?;
    let mut authority = VerifiedStoreAuthority::default();
    for (write_id, changeset_hash, prepared, rebased, status) in rows {
        let mut changesets = vec![records.verified_payload(changeset_hash.parse()?)?];
        let hashes = crate::query_mapped_rows(conn,
            "SELECT changeset_hash FROM store_write_partitions WHERE write_id = ?1 ORDER BY audience",
            [&write_id], |row| row.get::<_, String>(0))?;
        for hash in hashes {
            changesets.push(records.verified_payload(hash.parse()?)?);
        }
        let authenticated_version = match prepared {
            Some(encoded) => {
                let prepared: PreparedStoreWriteState = serde_json::from_str(&encoded)?;
                let root = authority.required_root_authority_on(records)?;
                let commit =
                    verify_prepared_store_commit_on(&mut authority, records, &root, &prepared)?;
                let packages =
                    crate::load_prepared_audience_objects_on(conn, store_dir, &commit.write_id)?;
                Some(authenticated_write_schema_version(
                    records,
                    &write_id,
                    &commit,
                    packages
                        .packages
                        .iter()
                        .map(crate::PreparedAudiencePackage::package),
                )?)
            }
            None => match serde_json::from_str::<coven_protocol::write::WriteStatus>(&status)? {
                coven_protocol::write::WriteStatus::Published(published) => match *published {
                    coven_protocol::write::PublishedWrite::Commit(position) => {
                        let reference = position.commit();
                        let retained = conn.query_row(
                            "SELECT EXISTS(SELECT 1 FROM retained_merge_materializations WHERE device_id = ?1 AND seq = ?2)",
                            rusqlite::params![reference.coord.stream_id.to_string(), crate::Database::sequence_to_sqlite(&reference.coord.stream_id.to_string(), reference.coord.sequence())?],
                            |row| row.get::<_, bool>(0),
                        )?;
                        if retained {
                            let materialization =
                                authority.retained_materialization_by_ref_on(records, reference)?;
                            Some(authenticated_write_schema_version(
                                records,
                                &write_id,
                                materialization.verified_commit(),
                                materialization.packages().iter(),
                            )?)
                        } else {
                            None
                        }
                    }
                    coven_protocol::write::PublishedWrite::Snapshot(_) => None,
                },
                _ => None,
            },
        };
        let slices = changesets.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let version = crate::changeset_migration::recover_changeset_version(
            conn,
            migrations,
            &slices,
            authenticated_version,
        )?;
        if let Some(encoded) = rebased {
            let old: UnversionedRebasedStoreWrite = serde_json::from_str(&encoded)?;
            let actual = records.verified_payload(old.changeset_hash)?;
            let actual_version = crate::changeset_migration::recover_changeset_version(
                conn,
                migrations,
                &[&actual],
                None,
            )?;
            let versioned = crate::write_models::RebasedStoreWrite {
                schema_version: actual_version,
                base: old.base,
                publication_base: old.publication_base,
                changeset_hash: old.changeset_hash,
                blob_facts: old.blob_facts,
            };
            conn.execute(
                "UPDATE store_writes SET rebased = ?2 WHERE write_id = ?1",
                rusqlite::params![write_id, serde_json::to_string(&versioned)?],
            )?;
        }
        conn.execute(
            "INSERT INTO store_write_schemas (write_id, schema_version) VALUES (?1, ?2)",
            rusqlite::params![write_id, version],
        )?;
    }
    validate_store_write_schemas(conn)
}

/// The rebased journal payload stored before captured schema versions existed.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UnversionedRebasedStoreWrite {
    base: crate::StoreWriteBase,
    publication_base: coven_protocol::store_commit::StorePublicationBase,
    changeset_hash: crate::ObjectHash,
    blob_facts: crate::StoreWriteBlobFacts,
}

fn authenticated_write_schema_version<'a>(
    records: StoreRecords<'_>,
    write_id: &str,
    commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    packages: impl ExactSizeIterator<Item = &'a coven_protocol::audience_package::AudiencePackage>,
) -> Result<u32, DbError> {
    if commit.write_id.as_str() != write_id {
        return Err(DbError::Message(
            "authenticated commit belongs to another retained write".into(),
        ));
    }
    let mut versions = commit
        .store_package()
        .map(|package| package.schema_version)
        .into_iter()
        .chain(
            commit
                .circle_packages()
                .iter()
                .map(|package| package.package.schema_version),
        );
    let version = versions
        .next()
        .ok_or_else(|| DbError::Message("authenticated write has no audience package".into()))?;
    if versions.any(|other| other != version) {
        return Err(DbError::Message(
            "authenticated write packages have different schema versions".into(),
        ));
    }
    let partitions = records.store_write_partitions(write_id)?;
    super::preparation::validate_write_partitions(commit, version, &partitions, packages)?;
    Ok(version)
}
