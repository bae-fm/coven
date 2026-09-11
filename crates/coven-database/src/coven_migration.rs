//! Coven's bookkeeping-schema migration ladder.
//!
//! This ladder is separate from the host schema's `PRAGMA user_version`.
//! Coven defines and applies its own table changes; a writer's policy decides
//! whether pending Coven changes may run during open. Readers never authorize
//! writes and therefore refuse pending changes.
//!
//! Every rung is checked against the exact manifest its schema produces, so a
//! table edited in place — a column dropped, a table renamed — with no rung
//! added leaves every existing database matching no rung and refusing to
//! open. A change to any `coven_tables!` entry ships with the rung that
//! carries a database from the previous shape, and the previous shape's
//! columns stay declared in `coven_schema_definitions` for that rung's
//! expected manifest.

use rusqlite::Connection;

use crate::coven_schema::{
    expected_coven_schema_manifest, expected_coven_schema_v0_manifest,
    expected_coven_schema_v1_manifest, live_coven_schema_manifest,
    recreate_current_transition_tables, CovenSchemaManifest,
};
use crate::{
    get_protocol_state_on, set_protocol_state_on, DbError, COVEN_INITIALIZED_STATE_KEY,
    COVEN_SCHEMA_MANIFEST_STATE_KEY,
};

pub(crate) const COVEN_SCHEMA_VERSION_STATE_KEY: &str = "coven_schema_version";

type ApplyCovenMigration = fn(&Connection) -> Result<(), CovenMigrationError>;

const COVEN_MIGRATION_COUNT: usize = 2;
const LATEST_COVEN_SCHEMA_VERSION: u32 = COVEN_MIGRATION_COUNT as u32;

pub(crate) struct CovenMigrationStep<'a> {
    expected_manifest: &'a CovenSchemaManifest,
    apply: ApplyCovenMigration,
}

#[cfg(test)]
impl<'a> CovenMigrationStep<'a> {
    pub(crate) fn new_for_test(
        expected_manifest: &'a CovenSchemaManifest,
        apply: ApplyCovenMigration,
    ) -> Self {
        Self {
            expected_manifest,
            apply,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CovenMigrationPolicy {
    ApplyPending,
    RefusePending,
}

#[derive(Debug, thiserror::Error)]
pub enum CovenMigrationError {
    #[error("Coven schema migration {current} -> {target} is pending")]
    Pending { current: u32, target: u32 },
    #[error("Coven schema version {version} is current, but its version ledger is missing")]
    PendingLedgerInstallation { version: u32 },
    #[error("Store database is missing required Coven schema manifest metadata")]
    MissingManifest,
    #[error("Store Coven schema manifest is invalid: {0}")]
    InvalidManifest(#[source] serde_json::Error),
    #[error("failed to serialize the current Coven schema manifest: {0}")]
    SerializeManifest(#[source] serde_json::Error),
    #[error("stored and live Coven schema manifests differ")]
    StoredLiveManifestMismatch,
    #[error("unversioned Coven schema does not match a known schema version")]
    UnknownUnversionedSchema,
    #[error("uninitialized snapshot contains Coven schema metadata {key:?}")]
    UnexpectedSnapshotMetadata { key: &'static str },
    #[error("Coven schema version {value:?} is not an unsigned integer: {source}")]
    InvalidVersion {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("Coven schema version {found} is unsupported; this binary supports version {latest}")]
    UnsupportedVersion { found: u32, latest: u32 },
    #[error("Coven schema version {version} does not match its exact schema manifest")]
    VersionManifestMismatch { version: u32 },
    #[error("Coven migration to version {version} did not produce its exact schema manifest")]
    MigrationResultMismatch { version: u32 },
    #[error("Coven migration requires empty table {table}")]
    NonEmptyTable { table: String },
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
    #[error(transparent)]
    Db(#[from] DbError),
}

enum CovenSchemaState {
    Current,
    Pending {
        current: u32,
    },
    /// The latest schema with no version ledger: a retained replay image (which
    /// carries none at any version) or a database from before the ledger
    /// existed. Writing the ledger is all that is pending.
    PendingLedgerInstallation {
        version: u32,
    },
}

fn stored_manifest(conn: &Connection) -> Result<CovenSchemaManifest, CovenMigrationError> {
    let json = get_protocol_state_on(conn, COVEN_SCHEMA_MANIFEST_STATE_KEY)?
        .ok_or(CovenMigrationError::MissingManifest)?;
    serde_json::from_str(&json).map_err(CovenMigrationError::InvalidManifest)
}

/// The version whose exact manifest `manifest` is, for a database that carries
/// no version ledger: a retained replay image (its protocol state is the
/// generation-zero set, which has no ledger at any version), an uninitialized
/// snapshot, or a database from before the ledger existed. Matched against
/// every version, and only when exactly one matches.
fn ledgerless_version(
    manifest: &CovenSchemaManifest,
    version_0_manifest: &CovenSchemaManifest,
    migrations: &[CovenMigrationStep<'_>],
) -> Option<u32> {
    let mut matching_versions = std::iter::once((0, version_0_manifest))
        .chain(
            migrations
                .iter()
                .enumerate()
                .map(|(index, migration)| ((index + 1) as u32, migration.expected_manifest)),
        )
        .filter_map(|(version, expected)| (manifest == expected).then_some(version));
    let version = matching_versions.next()?;
    matching_versions.next().is_none().then_some(version)
}

fn classify_schema(
    conn: &Connection,
    version_0_manifest: &CovenSchemaManifest,
    migrations: &[CovenMigrationStep<'_>],
) -> Result<CovenSchemaState, CovenMigrationError> {
    let stored = stored_manifest(conn)?;
    let live = live_coven_schema_manifest(conn)?;
    if stored != live {
        return Err(CovenMigrationError::StoredLiveManifestMismatch);
    }

    let version = get_protocol_state_on(conn, COVEN_SCHEMA_VERSION_STATE_KEY)?;
    match version {
        None => ledgerless_version(&stored, version_0_manifest, migrations)
            .map(|version| {
                if version == migrations.len() as u32 {
                    CovenSchemaState::PendingLedgerInstallation { version }
                } else {
                    CovenSchemaState::Pending { current: version }
                }
            })
            .ok_or(CovenMigrationError::UnknownUnversionedSchema),
        Some(value) => {
            let version = value
                .parse::<u32>()
                .map_err(|source| CovenMigrationError::InvalidVersion { value, source })?;
            let latest = migrations.len() as u32;
            if version > latest {
                return Err(CovenMigrationError::UnsupportedVersion {
                    found: version,
                    latest,
                });
            }
            let expected = if version == 0 {
                version_0_manifest
            } else {
                migrations[(version - 1) as usize].expected_manifest
            };
            if stored != *expected {
                return Err(CovenMigrationError::VersionManifestMismatch { version });
            }
            if version == latest {
                Ok(CovenSchemaState::Current)
            } else {
                Ok(CovenSchemaState::Pending { current: version })
            }
        }
    }
}

fn write_schema_metadata(
    conn: &Connection,
    version: u32,
    manifest: &CovenSchemaManifest,
) -> Result<(), CovenMigrationError> {
    let manifest =
        serde_json::to_string(manifest).map_err(CovenMigrationError::SerializeManifest)?;
    set_protocol_state_on(conn, COVEN_SCHEMA_MANIFEST_STATE_KEY, &manifest)?;
    set_protocol_state_on(conn, COVEN_SCHEMA_VERSION_STATE_KEY, &version.to_string())?;
    Ok(())
}

fn require_empty(conn: &Connection, table: &'static str) -> Result<(), CovenMigrationError> {
    let has_row: bool = conn.query_row(
        &format!("SELECT EXISTS(SELECT 1 FROM {table})"),
        [],
        |row| row.get(0),
    )?;
    if has_row {
        return Err(CovenMigrationError::NonEmptyTable {
            table: table.to_string(),
        });
    }
    Ok(())
}

fn add_root_label_to_transition_tables(conn: &Connection) -> Result<(), CovenMigrationError> {
    require_empty(conn, "cloud_outbox")?;
    require_empty(conn, "blob_make_remote_intents")?;
    recreate_current_transition_tables(conn)?;
    Ok(())
}

/// Version 2: drop the columns whose values live elsewhere. A replay
/// baseline's `generation` was always zero and its `exact_cut` repeated the
/// coverage its authority record carries; an outbound snapshot's `image_ref`
/// and `rollup_ref` are named by its signed metadata. Rows stay: nothing else
/// about them changes, and a Store with a retained replay baseline keeps it.
fn drop_derived_baseline_and_snapshot_columns(
    conn: &Connection,
) -> Result<(), CovenMigrationError> {
    conn.execute_batch(
        "ALTER TABLE retained_replay_baselines DROP COLUMN generation;
         ALTER TABLE retained_replay_baselines DROP COLUMN exact_cut;
         ALTER TABLE outbound_store_snapshot DROP COLUMN image_ref;
         ALTER TABLE outbound_store_snapshot DROP COLUMN rollup_ref;
         ALTER TABLE outbound_circle_snapshot DROP COLUMN image_ref;",
    )?;
    Ok(())
}

fn apply_migration_steps(
    conn: &Connection,
    current_version: u32,
    migrations: &[CovenMigrationStep<'_>],
) -> Result<(), CovenMigrationError> {
    for (index, migration) in migrations.iter().enumerate().skip(current_version as usize) {
        (migration.apply)(conn)?;
        let version = (index + 1) as u32;
        if live_coven_schema_manifest(conn)? != *migration.expected_manifest {
            return Err(CovenMigrationError::MigrationResultMismatch { version });
        }
    }
    Ok(())
}

fn apply_pending_migrations(
    conn: &Connection,
    current_version: u32,
    migrations: &[CovenMigrationStep<'_>],
) -> Result<(), CovenMigrationError> {
    apply_migration_steps(conn, current_version, migrations)?;
    let latest = migrations.len() as u32;
    write_schema_metadata(
        conn,
        latest,
        migrations[(latest - 1) as usize].expected_manifest,
    )?;
    Ok(())
}

fn require_absent_snapshot_metadata(
    conn: &Connection,
    key: &'static str,
) -> Result<(), CovenMigrationError> {
    if get_protocol_state_on(conn, key)?.is_some() {
        Err(CovenMigrationError::UnexpectedSnapshotMetadata { key })
    } else {
        Ok(())
    }
}

fn run_uninitialized_snapshot_migrations_with_ladder(
    conn: &Connection,
    policy: CovenMigrationPolicy,
    version_0_manifest: &CovenSchemaManifest,
    migrations: &[CovenMigrationStep<'_>],
) -> Result<(), CovenMigrationError> {
    require_absent_snapshot_metadata(conn, COVEN_INITIALIZED_STATE_KEY)?;
    require_absent_snapshot_metadata(conn, COVEN_SCHEMA_MANIFEST_STATE_KEY)?;
    require_absent_snapshot_metadata(conn, COVEN_SCHEMA_VERSION_STATE_KEY)?;

    let live = live_coven_schema_manifest(conn)?;
    let current = ledgerless_version(&live, version_0_manifest, migrations)
        .ok_or(CovenMigrationError::UnknownUnversionedSchema)?;
    let latest = migrations.len() as u32;
    if current == latest {
        return Ok(());
    }
    match policy {
        CovenMigrationPolicy::ApplyPending => apply_migration_steps(conn, current, migrations),
        CovenMigrationPolicy::RefusePending => Err(CovenMigrationError::Pending {
            current,
            target: latest,
        }),
    }
}

fn migration_ladder(
    include_routing: bool,
) -> Result<[CovenMigrationStep<'static>; COVEN_MIGRATION_COUNT], CovenMigrationError> {
    Ok([
        CovenMigrationStep {
            expected_manifest: expected_coven_schema_v1_manifest(include_routing)?,
            apply: add_root_label_to_transition_tables,
        },
        CovenMigrationStep {
            expected_manifest: expected_coven_schema_manifest(include_routing)?,
            apply: drop_derived_baseline_and_snapshot_columns,
        },
    ])
}

fn run_coven_migrations_with_ladder(
    conn: &Connection,
    policy: CovenMigrationPolicy,
    version_0_manifest: &CovenSchemaManifest,
    migrations: &[CovenMigrationStep<'_>],
) -> Result<(), CovenMigrationError> {
    match (
        classify_schema(conn, version_0_manifest, migrations)?,
        policy,
    ) {
        (CovenSchemaState::Current, _) => Ok(()),
        (CovenSchemaState::Pending { current }, CovenMigrationPolicy::ApplyPending) => {
            apply_pending_migrations(conn, current, migrations)
        }
        (CovenSchemaState::Pending { current }, CovenMigrationPolicy::RefusePending) => {
            Err(CovenMigrationError::Pending {
                current,
                target: migrations.len() as u32,
            })
        }
        (
            CovenSchemaState::PendingLedgerInstallation { version },
            CovenMigrationPolicy::ApplyPending,
        ) => write_schema_metadata(
            conn,
            version,
            migrations[(version - 1) as usize].expected_manifest,
        ),
        (
            CovenSchemaState::PendingLedgerInstallation { version },
            CovenMigrationPolicy::RefusePending,
        ) => Err(CovenMigrationError::PendingLedgerInstallation { version }),
    }
}

#[cfg(test)]
pub(crate) fn run_coven_migrations_with_ladder_for_test(
    conn: &Connection,
    include_routing: bool,
    policy: CovenMigrationPolicy,
    migrations: &[CovenMigrationStep<'_>],
) -> Result<(), CovenMigrationError> {
    run_coven_migrations_with_ladder(
        conn,
        policy,
        expected_coven_schema_v0_manifest(include_routing)?,
        migrations,
    )
}

#[cfg(test)]
pub(crate) fn run_uninitialized_snapshot_migrations_with_ladder_for_test(
    conn: &Connection,
    include_routing: bool,
    policy: CovenMigrationPolicy,
    migrations: &[CovenMigrationStep<'_>],
) -> Result<(), CovenMigrationError> {
    run_uninitialized_snapshot_migrations_with_ladder(
        conn,
        policy,
        expected_coven_schema_v0_manifest(include_routing)?,
        migrations,
    )
}

pub(crate) fn initialize_coven_schema_version(conn: &Connection) -> Result<(), DbError> {
    set_protocol_state_on(
        conn,
        COVEN_SCHEMA_VERSION_STATE_KEY,
        &LATEST_COVEN_SCHEMA_VERSION.to_string(),
    )
}

pub(crate) fn run_coven_migrations_in_transaction(
    conn: &Connection,
    include_routing: bool,
    policy: CovenMigrationPolicy,
) -> Result<(), CovenMigrationError> {
    let migrations = migration_ladder(include_routing)?;
    run_coven_migrations_with_ladder(
        conn,
        policy,
        expected_coven_schema_v0_manifest(include_routing)?,
        &migrations,
    )
}

pub(crate) fn run_uninitialized_snapshot_coven_migrations_in_transaction(
    conn: &Connection,
    include_routing: bool,
    policy: CovenMigrationPolicy,
) -> Result<(), CovenMigrationError> {
    let migrations = migration_ladder(include_routing)?;
    run_uninitialized_snapshot_migrations_with_ladder(
        conn,
        policy,
        expected_coven_schema_v0_manifest(include_routing)?,
        &migrations,
    )
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn validate_uninitialized_coven_schema_v0_for_test(
    conn: &Connection,
    include_routing: bool,
) -> Result<(), CovenMigrationError> {
    require_absent_snapshot_metadata(conn, COVEN_INITIALIZED_STATE_KEY)?;
    require_absent_snapshot_metadata(conn, COVEN_SCHEMA_MANIFEST_STATE_KEY)?;
    require_absent_snapshot_metadata(conn, COVEN_SCHEMA_VERSION_STATE_KEY)?;
    let live = live_coven_schema_manifest(conn)?;
    if live != *expected_coven_schema_v0_manifest(include_routing)? {
        return Err(CovenMigrationError::UnknownUnversionedSchema);
    }
    Ok(())
}

pub(crate) fn validate_coven_schema_for_reader(
    conn: &Connection,
    include_routing: bool,
) -> Result<(), CovenMigrationError> {
    let migrations = migration_ladder(include_routing)?;
    match classify_schema(
        conn,
        expected_coven_schema_v0_manifest(include_routing)?,
        &migrations,
    )? {
        CovenSchemaState::Current => Ok(()),
        CovenSchemaState::Pending { current } => Err(CovenMigrationError::Pending {
            current,
            target: migrations.len() as u32,
        }),
        CovenSchemaState::PendingLedgerInstallation { version } => {
            Err(CovenMigrationError::PendingLedgerInstallation { version })
        }
    }
}
