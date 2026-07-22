//! Private accepted-history baselines and deterministic retained replay.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{types::Value, Connection};
use serde::{Deserialize, Serialize};

use super::store_commit::{
    CommitFrontier, ObjectHash, ResolvedStoreDeviceState, RetainedVerifiedActivatedAck,
    RetainedVerifiedRegistration, SnapshotMeta, StoreBatchCommit, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreHistoryCut, StoreRootRef,
    StoreSnapshotRef,
};
use crate::database::{
    DbError, COVEN_INITIALIZED_STATE_KEY, COVEN_SCHEMA_MANIFEST_STATE_KEY,
    STORE_DEVICE_GENESIS_STATE_KEY, SYNC_ROUTING_CONTRACT_STATE_KEY, SYNC_ROUTING_HASH_STATE_KEY,
};
use crate::sync::store::membership::{
    MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX, OWNER_PUBKEY_STATE_KEY,
};

pub(crate) const GENERATION_ZERO: u64 = 0;

const GENESIS_PRESERVED_TABLES: &[&str] = &[
    "protocol_state",
    "store_protocol_root_authority",
    "store_device_registration_activations",
];
const SQLITE_DATABASE_HEADER: &[u8; 16] = b"SQLite format 3\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayTableDisposition {
    Replace,
    ReplaceWhenRouting,
    Preserve,
    ExactTransition,
    MustBeEmpty,
}

const REPLAY_TABLES: &[(&str, ReplayTableDisposition)] = &[
    ("activated_store_acks", ReplayTableDisposition::Replace),
    ("blob_locators", ReplayTableDisposition::Replace),
    ("blob_make_remote_intents", ReplayTableDisposition::Preserve),
    ("circle_access_cache", ReplayTableDisposition::Replace),
    (
        "circle_control_activations",
        ReplayTableDisposition::Replace,
    ),
    ("circle_current_state", ReplayTableDisposition::Replace),
    ("circle_metadata_cache", ReplayTableDisposition::Replace),
    ("circle_operations", ReplayTableDisposition::Preserve),
    ("circle_roster_cache", ReplayTableDisposition::Replace),
    ("cloud_outbox", ReplayTableDisposition::Preserve),
    ("local_blob_refs", ReplayTableDisposition::Preserve),
    ("local_cleanup_intents", ReplayTableDisposition::Preserve),
    (
        "local_store_device_registration",
        ReplayTableDisposition::Preserve,
    ),
    (
        "local_store_founder_graph",
        ReplayTableDisposition::Preserve,
    ),
    (
        "local_store_protocol_root",
        ReplayTableDisposition::Preserve,
    ),
    ("materialized_commits", ReplayTableDisposition::Replace),
    (
        "merge_retraction_cleanups",
        ReplayTableDisposition::Preserve,
    ),
    (
        "outbound_membership_mutation",
        ReplayTableDisposition::Preserve,
    ),
    ("outbound_store_acks", ReplayTableDisposition::Preserve),
    (
        "outbound_store_device_exclusion",
        ReplayTableDisposition::Preserve,
    ),
    ("outbound_store_snapshot", ReplayTableDisposition::Preserve),
    (
        "protocol_inert_objects",
        ReplayTableDisposition::ExactTransition,
    ),
    ("protocol_state", ReplayTableDisposition::Preserve),
    (
        "published_blob_drop_intents",
        ReplayTableDisposition::Preserve,
    ),
    ("published_store_acks", ReplayTableDisposition::Preserve),
    ("published_store_snapshot", ReplayTableDisposition::Preserve),
    ("reclaimed_store_packages", ReplayTableDisposition::Preserve),
    ("remote_objects", ReplayTableDisposition::ExactTransition),
    (
        "retained_merge_materializations",
        ReplayTableDisposition::Preserve,
    ),
    (
        "retained_replay_baselines",
        ReplayTableDisposition::Preserve,
    ),
    ("retained_replay_objects", ReplayTableDisposition::Preserve),
    ("row_blob_locators", ReplayTableDisposition::Replace),
    (
        "snapshot_blob_spool_cleanup",
        ReplayTableDisposition::Preserve,
    ),
    ("snapshot_coverage", ReplayTableDisposition::Preserve),
    (
        "store_author_exclusion_activations",
        ReplayTableDisposition::Replace,
    ),
    (
        "store_device_exclusion_freezes",
        ReplayTableDisposition::ExactTransition,
    ),
    (
        "store_device_registration_activations",
        ReplayTableDisposition::Replace,
    ),
    (
        "store_device_state_snapshots",
        ReplayTableDisposition::Replace,
    ),
    (
        "store_protocol_root_authority",
        ReplayTableDisposition::Preserve,
    ),
    ("store_reclaim_operations", ReplayTableDisposition::Preserve),
    ("store_write_blob_leases", ReplayTableDisposition::Preserve),
    ("store_write_blobs", ReplayTableDisposition::Preserve),
    ("store_write_packages", ReplayTableDisposition::Preserve),
    ("store_write_partitions", ReplayTableDisposition::Preserve),
    ("store_writes", ReplayTableDisposition::Preserve),
    ("stream_activations", ReplayTableDisposition::Replace),
    (
        "_coven_audience",
        ReplayTableDisposition::ReplaceWhenRouting,
    ),
    (
        "_coven_row_routes",
        ReplayTableDisposition::ReplaceWhenRouting,
    ),
];

fn projection_table_names(include_routing: bool) -> Vec<String> {
    REPLAY_TABLES
        .iter()
        .filter_map(|(table, disposition)| match disposition {
            ReplayTableDisposition::Replace => Some((*table).to_string()),
            ReplayTableDisposition::ReplaceWhenRouting if include_routing => {
                Some((*table).to_string())
            }
            ReplayTableDisposition::ReplaceWhenRouting
            | ReplayTableDisposition::Preserve
            | ReplayTableDisposition::ExactTransition
            | ReplayTableDisposition::MustBeEmpty => None,
        })
        .collect()
}

pub(crate) fn validate_merge_generation_zero_preconditions(
    connection: &Connection,
) -> Result<(), DbError> {
    validate_must_be_empty_replay_tables(connection, "generation-zero retained replay")
}

fn validate_must_be_empty_replay_tables(
    connection: &Connection,
    context: &str,
) -> Result<(), DbError> {
    for (table, disposition) in REPLAY_TABLES {
        if *disposition != ReplayTableDisposition::MustBeEmpty {
            continue;
        }
        let count: i64 = connection
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {}",
                    super::session::quote_ident(table)
                ),
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if count != 0 {
            return Err(DbError::Message(format!(
                "{context} requires {table} to be empty"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedReplayGenesisAuthority {
    pub(crate) store_root: StoreRootRef,
    pub(crate) founder_registration: StoreDeviceRegistrationRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedReplaySnapshotAuthority {
    pub(crate) store_root: StoreRootRef,
    pub(crate) founder_registration: StoreDeviceRegistrationRef,
    pub(crate) snapshot: StoreSnapshotRef,
    pub(crate) metadata: SnapshotMeta,
    pub(crate) snapshot_cut: StoreHistoryCut,
    pub(crate) accepted_cut: StoreHistoryCut,
    pub(crate) device_state: ResolvedStoreDeviceState,
    pub(crate) active_registrations:
        BTreeMap<super::store_commit::StoreDeviceId, RetainedVerifiedRegistration>,
    pub(crate) acknowledgements:
        BTreeMap<super::store_commit::StoreDeviceId, RetainedVerifiedActivatedAck>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedReplayAuthority {
    Genesis(RetainedReplayGenesisAuthority),
    StableSnapshot(RetainedReplaySnapshotAuthority),
}

impl RetainedReplaySnapshotAuthority {
    pub(crate) fn validate(&self) -> Result<(), DbError> {
        let metadata_bytes = self.metadata.to_bytes();
        let author = self
            .active_registrations
            .get(&self.metadata.author_registration.device_id)
            .filter(|registration| registration.reference == self.metadata.author_registration)
            .ok_or_else(|| {
                DbError::Message(
                    "retained snapshot author is absent from its active registrations".to_string(),
                )
            })?;
        let parsed = SnapshotMeta::parse_at(
            &metadata_bytes,
            self.store_root.store_root_hash,
            &self.snapshot,
            &author.value,
        )
        .map_err(|error| DbError::Message(format!("retained snapshot metadata: {error}")))?;
        if self.metadata.store_root_hash != self.store_root.store_root_hash
            || self.metadata.generation != self.snapshot.generation
            || self.metadata.snapshot_hash() != self.snapshot.snapshot_hash
            || self.snapshot.object.verify(&metadata_bytes).is_err()
            || self.snapshot_cut.frontier() != self.metadata.coverage
            || !self
                .accepted_cut
                .frontier()
                .covers(&self.snapshot_cut.frontier())
            || parsed != self.metadata
            || self.device_state.state_hash != self.metadata.state.devices.state_hash()
            || self.device_state.recovery != self.metadata.state.devices.recovery()
        {
            return Err(DbError::Message(
                "retained snapshot replay authority differs from its signed snapshot state"
                    .to_string(),
            ));
        }
        let expected_active = self
            .device_state
            .devices
            .iter()
            .filter_map(|(device_id, record)| {
                matches!(
                    record.status,
                    super::store_commit::StoreDeviceStatus::Active
                )
                .then_some((*device_id, &record.registration))
            })
            .collect::<BTreeMap<_, _>>();
        if expected_active.len() != self.active_registrations.len()
            || expected_active.iter().any(|(device_id, reference)| {
                self.active_registrations
                    .get(device_id)
                    .is_none_or(|registration| &registration.reference != *reference)
            })
            || self.acknowledgements.len() != self.active_registrations.len()
        {
            return Err(DbError::Message(
                "retained snapshot replay authority does not exactly cover active devices"
                    .to_string(),
            ));
        }
        for (device_id, registration) in &self.active_registrations {
            let bytes = registration.value.to_bytes();
            registration
                .reference
                .object
                .verify(&bytes)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let parsed = StoreDeviceRegistration::parse_at(&bytes, &self.store_root, *device_id)
                .map_err(|error| DbError::Message(error.to_string()))?;
            if parsed != registration.value {
                return Err(DbError::Message(
                    "retained snapshot registration is not canonical".to_string(),
                ));
            }
            registration
                .reference
                .verify_registration(&registration.value)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let acknowledgement = self.acknowledgements.get(device_id).ok_or_else(|| {
                DbError::Message(
                    "retained snapshot active device has no acknowledgement".to_string(),
                )
            })?;
            acknowledgement
                .validate_chain(&self.store_root, registration)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let (acknowledgement_ref, acknowledgement_value) =
                acknowledgement.latest().ok_or_else(|| {
                    DbError::Message(
                        "retained snapshot acknowledgement proof chain is empty".to_string(),
                    )
                })?;
            let commit_bytes = acknowledgement.activating_commit_value.to_bytes();
            acknowledgement
                .activating_commit
                .object
                .verify(&commit_bytes)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let parsed_commit = StoreBatchCommit::parse_at(
                &commit_bytes,
                self.store_root.store_root_hash,
                &acknowledgement.activating_commit.coord,
                &registration.value,
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            if parsed_commit != acknowledgement.activating_commit_value
                || parsed_commit.commit_hash() != acknowledgement.activating_commit.commit_hash
                || parsed_commit.acknowledgement() != Some(acknowledgement_ref)
                || !history_cut_covers_commit(
                    &self.accepted_cut,
                    &acknowledgement.activating_commit,
                )
                || !acknowledgement_value
                    .snapshot
                    .as_ref()
                    .is_some_and(|acknowledged| {
                        acknowledged.author_registration == self.metadata.author_registration
                            && acknowledged.snapshot == self.snapshot
                    })
                || acknowledgement_value.device_state != self.metadata.state.devices
                || !acknowledgement_value
                    .store_cut
                    .frontier()
                    .covers(&self.metadata.coverage)
            {
                return Err(DbError::Message(
                    "retained snapshot acknowledgement differs from its activated commit"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn history_cut_covers_commit(cut: &StoreHistoryCut, reference: &StoreBatchCommitRef) -> bool {
    let covered = CommitFrontier(BTreeMap::from([(
        reference.coord.stream_id,
        reference.clone(),
    )]));
    cut.frontier().covers(&covered)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedReplayBaseline {
    pub(crate) generation: u64,
    pub(crate) exact_cut: CommitFrontier,
    pub(crate) schema_version: u32,
    pub(crate) routing_hash: ObjectHash,
    pub(crate) image_hash: ObjectHash,
    pub(crate) image_bytes: Vec<u8>,
    pub(crate) authority: RetainedReplayAuthority,
}

impl RetainedReplayBaseline {
    pub(crate) fn generation_zero(
        source: &Connection,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: RetainedReplayGenesisAuthority,
    ) -> Result<Self, DbError> {
        let image_bytes = project_generation_zero_image(source)?;
        let baseline = Self {
            generation: GENERATION_ZERO,
            exact_cut: CommitFrontier(Default::default()),
            schema_version,
            routing_hash,
            image_hash: ObjectHash::digest(&image_bytes),
            image_bytes,
            authority: RetainedReplayAuthority::Genesis(authority),
        };
        baseline.validate_image()?;
        Ok(baseline)
    }

    pub(crate) fn stable_snapshot(
        source: &Connection,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: RetainedReplaySnapshotAuthority,
    ) -> Result<Self, DbError> {
        authority.validate()?;
        let image_bytes = serialized_database(source)?;
        let baseline = Self {
            generation: GENERATION_ZERO,
            exact_cut: authority.metadata.coverage.clone(),
            schema_version,
            routing_hash,
            image_hash: ObjectHash::digest(&image_bytes),
            image_bytes,
            authority: RetainedReplayAuthority::StableSnapshot(authority),
        };
        baseline.validate_image()?;
        Ok(baseline)
    }

    pub(crate) fn canonical_authority_bytes(&self) -> Result<Vec<u8>, DbError> {
        serde_json::to_vec(&self.authority).map_err(|error| {
            DbError::Message(format!("serialize retained replay authority: {error}"))
        })
    }

    pub(crate) fn validate_image(&self) -> Result<(), DbError> {
        if self.generation != GENERATION_ZERO
            || self.image_hash != ObjectHash::digest(&self.image_bytes)
        {
            return Err(DbError::Message(
                "generation-zero retained replay baseline metadata is inconsistent".to_string(),
            ));
        }
        let image = open_image(&self.image_bytes)?;
        match &self.authority {
            RetainedReplayAuthority::Genesis(_) => {
                if !self.exact_cut.0.is_empty() {
                    return Err(DbError::Message(
                        "genesis retained replay baseline has a non-genesis cut".to_string(),
                    ));
                }
                validate_generation_zero_image(&image, self.schema_version, self.routing_hash)
            }
            RetainedReplayAuthority::StableSnapshot(authority) => {
                authority.validate()?;
                if self.exact_cut != authority.metadata.coverage {
                    return Err(DbError::Message(
                        "snapshot retained replay cut differs from its signed metadata".to_string(),
                    ));
                }
                validate_snapshot_replay_image(
                    &image,
                    self.schema_version,
                    self.routing_hash,
                    &self.exact_cut,
                )
            }
        }
    }
}

pub(crate) fn open_image(image: &[u8]) -> Result<Connection, DbError> {
    if image.len() < 20 || &image[..SQLITE_DATABASE_HEADER.len()] != SQLITE_DATABASE_HEADER {
        return Err(DbError::Message(
            "retained replay image is not a SQLite database".to_string(),
        ));
    }
    let mut image = image.to_vec();
    // sqlite3_deserialize cannot use an image whose header requests WAL. File
    // databases carry that mode in bytes 18 and 19; the private in-memory copy
    // has no WAL file, so it must use rollback journaling.
    image[18] = 1;
    image[19] = 1;
    let mut connection = Connection::open_in_memory().map_err(DbError::from)?;
    connection
        .deserialize_read_exact(rusqlite::MAIN_DB, image.as_slice(), image.len(), false)
        .map_err(DbError::from)?;
    Ok(connection)
}

pub(crate) fn replace_live_projection(
    target: &rusqlite::Transaction<'_>,
    replay: &Connection,
    synced_tables: &[crate::sync::session::SyncedTable],
    include_routing: bool,
) -> Result<Vec<u8>, DbError> {
    let mut host_changes = rusqlite::session::Session::new(target).map_err(DbError::from)?;
    for table in synced_tables {
        host_changes
            .attach(Some(table.name()))
            .map_err(DbError::from)?;
    }
    let mut tables = projection_table_names(include_routing);
    tables.extend(synced_tables.iter().map(|table| table.name().to_string()));
    tables.sort();
    tables.dedup();
    target
        .pragma_update(None, "defer_foreign_keys", "ON")
        .map_err(DbError::from)?;
    for table in tables.iter().rev() {
        target
            .execute_batch(&format!(
                "DELETE FROM {}",
                super::session::quote_ident(table)
            ))
            .map_err(DbError::from)?;
    }
    for table in &tables {
        copy_table(replay, target, table)?;
    }
    let violations: bool = target
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if violations {
        return Err(DbError::Message(
            "retained replay projection violates foreign keys".to_string(),
        ));
    }
    let mut changeset = Vec::new();
    host_changes
        .changeset_strm(&mut changeset)
        .map_err(DbError::from)?;
    Ok(changeset)
}

fn copy_table(
    source: &Connection,
    target: &rusqlite::Transaction<'_>,
    table: &str,
) -> Result<(), DbError> {
    let pragma = format!("PRAGMA table_info({})", super::session::quote_ident(table));
    let mut column_statement = source.prepare(&pragma).map_err(DbError::from)?;
    let columns = column_statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(DbError::from)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(DbError::from)?;
    drop(column_statement);
    if columns.is_empty() {
        return Err(DbError::Message(format!(
            "retained replay projection table {table:?} is absent"
        )));
    }
    let quoted_columns = columns
        .iter()
        .map(|column| super::session::quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let select = format!(
        "SELECT {quoted_columns} FROM {}",
        super::session::quote_ident(table)
    );
    let mut source_statement = source.prepare(&select).map_err(DbError::from)?;
    let rows = source_statement
        .query_map([], |row| {
            (0..columns.len())
                .map(|index| row.get::<_, Value>(index))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(DbError::from)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(DbError::from)?;
    drop(source_statement);
    let placeholders = (1..=columns.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let insert = format!(
        "INSERT INTO {} ({quoted_columns}) VALUES ({placeholders})",
        super::session::quote_ident(table)
    );
    for values in rows {
        target
            .execute(&insert, rusqlite::params_from_iter(values))
            .map_err(DbError::from)?;
    }
    Ok(())
}

fn serialized_database(connection: &Connection) -> Result<Vec<u8>, DbError> {
    connection
        .serialize(rusqlite::MAIN_DB)
        .map(|bytes| bytes.to_vec())
        .map_err(DbError::from)
}

fn project_generation_zero_image(source: &Connection) -> Result<Vec<u8>, DbError> {
    let source_bytes = serialized_database(source)?;
    let image = open_image(&source_bytes)?;
    image
        .pragma_update(None, "foreign_keys", "OFF")
        .map_err(DbError::from)?;
    let transaction = image.unchecked_transaction().map_err(DbError::from)?;
    let founder_membership_cursor = founder_membership_cursor_key(&transaction)?;
    for table in crate::db::user_table_names(&transaction).map_err(DbError::from)? {
        if GENESIS_PRESERVED_TABLES.contains(&table.as_str()) {
            continue;
        }
        transaction
            .execute_batch(&format!(
                "DELETE FROM {}",
                super::session::quote_ident(&table)
            ))
            .map_err(DbError::from)?;
    }
    let protocol_keys = protocol_state_keys(&transaction)?;
    for key in protocol_keys {
        if !generation_zero_protocol_key(founder_membership_cursor.as_deref(), &key) {
            transaction
                .execute("DELETE FROM protocol_state WHERE key = ?1", [&key])
                .map_err(DbError::from)?;
        }
    }
    transaction.commit().map_err(DbError::from)?;
    image.execute_batch("VACUUM").map_err(DbError::from)?;
    image
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(DbError::from)?;
    let violations: bool = image
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if violations {
        return Err(DbError::Message(
            "generation-zero retained replay image violates foreign keys".to_string(),
        ));
    }
    serialized_database(&image)
}

fn validate_generation_zero_image(
    image: &Connection,
    schema_version: u32,
    routing_hash: ObjectHash,
) -> Result<(), DbError> {
    validate_replay_image_metadata(image, schema_version, routing_hash)?;
    let protocol_keys = protocol_state_keys(image)?;
    let founder_membership_cursor = founder_membership_cursor_key(image)?;
    if protocol_keys
        .iter()
        .any(|key| !generation_zero_protocol_key(founder_membership_cursor.as_deref(), key))
        || !required_generation_zero_protocol_keys()
            .iter()
            .all(|key| protocol_keys.contains(*key))
        || founder_membership_cursor
            .as_ref()
            .is_none_or(|key| !protocol_keys.contains(key))
    {
        return Err(DbError::Message(
            "retained replay image protocol state is not the generation-zero set".to_string(),
        ));
    }
    for table in crate::db::user_table_names(image).map_err(DbError::from)? {
        let count: i64 = image
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {}",
                    super::session::quote_ident(&table)
                ),
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let expected = match table.as_str() {
            "protocol_state" => None,
            "store_protocol_root_authority" | "store_device_registration_activations" => Some(1),
            _ => Some(0),
        };
        if expected.is_some_and(|expected| count != expected) {
            return Err(DbError::Message(format!(
                "generation-zero retained replay image table {table:?} has {count} rows"
            )));
        }
    }
    validate_replay_image_foreign_keys(image)
}

fn validate_replay_image_metadata(
    image: &Connection,
    schema_version: u32,
    routing_hash: ObjectHash,
) -> Result<(), DbError> {
    let stored_schema_version: u32 = image
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(DbError::from)?;
    if stored_schema_version != schema_version {
        return Err(DbError::Message(format!(
            "retained replay image schema version is {stored_schema_version}, expected {schema_version}"
        )));
    }
    let stored_routing_hash: String = image
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [SYNC_ROUTING_HASH_STATE_KEY],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if stored_routing_hash != routing_hash.to_string() {
        return Err(DbError::Message(
            "retained replay image routing hash differs from its baseline".to_string(),
        ));
    }
    Ok(())
}

fn validate_snapshot_replay_image(
    image: &Connection,
    schema_version: u32,
    routing_hash: ObjectHash,
    exact_cut: &CommitFrontier,
) -> Result<(), DbError> {
    validate_replay_image_metadata(image, schema_version, routing_hash)?;
    let mut actual = BTreeMap::new();
    let mut statement = image
        .prepare("SELECT device_id, seq, commit_ref FROM snapshot_coverage ORDER BY device_id")
        .map_err(DbError::from)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(DbError::from)?;
    for row in rows {
        let (stream_id, sequence, encoded) = row.map_err(DbError::from)?;
        let reference: StoreBatchCommitRef = serde_json::from_str(&encoded).map_err(|error| {
            DbError::Message(format!("snapshot replay coverage reference: {error}"))
        })?;
        if sequence < 0 || u64::try_from(sequence).ok() != Some(reference.coord.sequence()) {
            return Err(DbError::Message(
                "snapshot replay coverage sequence differs from its exact reference".to_string(),
            ));
        }
        if actual.insert(stream_id, reference).is_some() {
            return Err(DbError::Message(
                "snapshot replay coverage repeats a Store stream".to_string(),
            ));
        }
    }
    drop(statement);
    if actual != exact_cut.clone().into_refs() {
        return Err(DbError::Message(
            "snapshot replay image coverage differs from its baseline".to_string(),
        ));
    }
    validate_must_be_empty_replay_tables(image, "snapshot replay baseline")?;
    let materialized_commits: i64 = image
        .query_row("SELECT COUNT(*) FROM materialized_commits", [], |row| {
            row.get(0)
        })
        .map_err(DbError::from)?;
    if materialized_commits != 0 {
        return Err(DbError::Message(
            "snapshot replay baseline contains materialized_commits rows".to_string(),
        ));
    }
    crate::database::Database::validate_snapshot_author_exclusion_activations_on(image)?;
    validate_replay_image_foreign_keys(image)
}

fn validate_replay_image_foreign_keys(image: &Connection) -> Result<(), DbError> {
    let violations: bool = image
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if violations {
        return Err(DbError::Message(
            "retained replay image violates foreign keys".to_string(),
        ));
    }
    Ok(())
}

fn protocol_state_keys(connection: &Connection) -> Result<BTreeSet<String>, DbError> {
    let mut statement = connection
        .prepare("SELECT key FROM protocol_state ORDER BY key")
        .map_err(DbError::from)?;
    let keys = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(DbError::from)?
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .map_err(DbError::from)?;
    Ok(keys)
}

fn required_generation_zero_protocol_keys() -> &'static [&'static str] {
    &[
        COVEN_INITIALIZED_STATE_KEY,
        COVEN_SCHEMA_MANIFEST_STATE_KEY,
        OWNER_PUBKEY_STATE_KEY,
        STORE_DEVICE_GENESIS_STATE_KEY,
        SYNC_ROUTING_CONTRACT_STATE_KEY,
        SYNC_ROUTING_HASH_STATE_KEY,
    ]
}

fn founder_membership_cursor_key(connection: &Connection) -> Result<Option<String>, DbError> {
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT store_protocol_root_bytes
             FROM store_protocol_root_authority WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let root = super::store_commit::StoreProtocolRoot::parse(&bytes)
        .map_err(|error| DbError::Message(format!("retained replay Store root: {error}")))?;
    let stream = super::membership::derive_founder_stream_id(
        &root.descriptor.store_root_id().to_string(),
        &root.descriptor.founder_pubkey,
    );
    Ok(Some(format!(
        "{}{}/{}",
        MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX, root.descriptor.founder_grant, stream
    )))
}

fn generation_zero_protocol_key(founder_membership_cursor: Option<&str>, key: &str) -> bool {
    required_generation_zero_protocol_keys().contains(&key)
        || founder_membership_cursor == Some(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;

    #[test]
    fn every_coven_table_has_one_retained_replay_disposition() {
        let classified = REPLAY_TABLES
            .iter()
            .map(|(table, _)| *table)
            .collect::<BTreeSet<_>>();
        assert_eq!(classified.len(), REPLAY_TABLES.len());
        assert_eq!(classified, crate::db::all_table_names());

        let without_routing = projection_table_names(false)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let with_routing = projection_table_names(true)
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            with_routing
                .difference(&without_routing)
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "_coven_audience".to_string(),
                "_coven_row_routes".to_string(),
            ])
        );
    }

    fn populate_fixture(connection: &Connection) {
        connection
            .execute_batch(
                "CREATE TABLE host_rows (
                     id TEXT PRIMARY KEY,
                     secret TEXT NOT NULL
                 ) STRICT;",
            )
            .expect("create host table");
        crate::db::apply_coven_schema(connection).expect("create Coven tables");
        crate::db::apply_coven_routing_schema(connection).expect("create routing tables");
        connection
            .execute_batch(
                "INSERT INTO host_rows VALUES ('host', 'projection-secret-marker');
                 INSERT INTO remote_objects
                 VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '{}');
                 INSERT INTO store_device_registration_activations
                 VALUES ('founder',
                         'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                         'author', 'device', x'01', '{}', '{}');",
            )
            .expect("insert projection rows");
        let mut keys = required_generation_zero_protocol_keys()
            .iter()
            .map(|key| ((*key).to_string(), "{}".to_string()))
            .collect::<Vec<_>>();
        keys.iter_mut()
            .find(|(key, _)| key == SYNC_ROUTING_HASH_STATE_KEY)
            .expect("routing hash key")
            .1 = ObjectHash::digest(b"routing").to_string();
        keys.push(("local_device_id".to_string(), "excluded-device".to_string()));
        for (key, value) in keys {
            connection
                .execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                    (key, value),
                )
                .expect("insert protocol state");
        }
        crate::sync::test_helpers::install_test_store_root_authority(
            connection,
            "retained-replay-fixture",
        );
        let cursor = founder_membership_cursor_key(connection)
            .expect("derive founder membership cursor")
            .expect("founder membership cursor");
        connection
            .execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, '{}')",
                [cursor],
            )
            .expect("insert founder membership cursor");
    }

    fn fixture() -> Connection {
        let connection = Connection::open_in_memory().expect("open projection fixture");
        populate_fixture(&connection);
        connection
    }

    #[test]
    fn generation_zero_projection_accepts_a_wal_database() {
        let directory = tempfile::tempdir().expect("create projection directory");
        let connection = Connection::open(directory.path().join("store.sqlite3"))
            .expect("open file projection fixture");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .expect("enable WAL");
        assert_eq!(journal_mode, "wal");
        populate_fixture(&connection);

        let bytes = project_generation_zero_image(&connection).expect("project WAL database");
        let image = open_image(&bytes).expect("open projected WAL image");
        assert_eq!(
            image
                .query_row("SELECT COUNT(*) FROM host_rows", [], |row| row
                    .get::<_, i64>(0))
                .expect("count projected host rows"),
            0
        );
    }

    #[test]
    fn generation_zero_projection_reads_uncommitted_founder_state_and_removes_local_bytes() {
        let mut source = fixture();
        source
            .execute("DELETE FROM store_device_registration_activations", [])
            .expect("remove committed founder fixture");
        let transaction = source.transaction().expect("begin founder transaction");
        transaction
            .execute(
                "INSERT INTO store_device_registration_activations
                 VALUES ('founder',
                         'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                         'author', 'device', x'01', '{}', '{}')",
                [],
            )
            .expect("insert uncommitted founder");

        let bytes =
            project_generation_zero_image(&transaction).expect("project uncommitted founder state");
        assert!(!bytes
            .windows(b"projection-secret-marker".len())
            .any(|window| window == b"projection-secret-marker"));
        let image = open_image(&bytes).expect("open projected image");
        assert_eq!(
            image
                .query_row(
                    "SELECT device_id FROM store_device_registration_activations",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("read uncommitted founder from image"),
            "founder"
        );
        assert_eq!(
            image
                .query_row("SELECT COUNT(*) FROM host_rows", [], |row| row
                    .get::<_, i64>(0))
                .expect("count projected host rows"),
            0
        );
        assert_eq!(
            image
                .query_row("SELECT COUNT(*) FROM remote_objects", [], |row| row
                    .get::<_, i64>(0))
                .expect("count projected remote objects"),
            0
        );
        assert!(image
            .query_row(
                "SELECT value FROM protocol_state WHERE key = 'local_device_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .expect("query excluded local state")
            .is_none());
        transaction.rollback().expect("rollback founder fixture");
    }

    #[test]
    fn projection_replacement_returns_the_host_row_delta() {
        let mut live = fixture();
        let replay = fixture();
        replay
            .execute(
                "UPDATE host_rows SET secret = 'replayed-value' WHERE id = 'host'",
                [],
            )
            .expect("change replayed host row");
        let transaction = live.transaction().expect("begin live replacement");
        let changeset = replace_live_projection(
            &transaction,
            &replay,
            &[crate::sync::session::SyncedTable::new(
                "host_rows",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            true,
        )
        .expect("replace live projection");
        let rows = crate::changeset::walk(&changeset).expect("walk replacement changeset");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].table, "host_rows");
        assert_eq!(rows[0].pk(), Some("host"));
        assert_eq!(
            transaction
                .query_row(
                    "SELECT secret FROM host_rows WHERE id = 'host'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("read replaced host row"),
            "replayed-value"
        );
        transaction.rollback().expect("rollback live replacement");
    }
}
