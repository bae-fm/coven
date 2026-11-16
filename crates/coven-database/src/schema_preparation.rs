//! Preparing the store's schema as a connection opens: coven's internal
//! tables, then the app's rebuilt indexes and migrations.

use crate::internal_schema;
use crate::migration::validate_versions;
use crate::schema::Schema;
use crate::sqlite::StoreSql;
use crate::store_call::StoreCall;
use crate::{
    CovenMigrationError, DbError, Migration, MigrationError, MigrationOutcome, OpenError,
    SyncedTable,
};

/// Create coven's internal tables on a database that has none, before the
/// app's migrations or any other work reads them. A read-only connection
/// requires the database to hold the current internal version already.
pub(crate) fn prepare_internal_schema(sql: &mut StoreSql) -> Result<(), OpenError> {
    let internal: i32 = sql.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    let internal = internal as u32;
    if internal > internal_schema::VERSION {
        return Err(CovenMigrationError::SchemaTooNew {
            current: internal,
            supported: internal_schema::VERSION,
        }
        .into());
    }
    if internal < internal_schema::VERSION {
        if sql.read_only()? {
            return Err(CovenMigrationError::ReadOnly.into());
        }
        // Coven creates its current internal schema on a database that holds
        // none, which reads as version zero, and has no upgrade from an older
        // version. The check above refuses a newer schema, so coven never
        // rewrites one.
        sql.transaction(|db| {
            db.batch(&internal_schema::initial_schema())?;
            crate::sql_versions::record_library(db)?;
            Ok(db.batch(&format!("PRAGMA application_id = {}", internal_schema::VERSION))?)
        })
        .map_err(|source| CovenMigrationError::Failed { source: Box::new(source) })?;
    }
    Ok(())
}

/// Prepare the app's schema: on a writable connection, rebuild what a
/// new version of SQLite or of the app's collations and functions
/// changes, then migrate, reporting both steps to `progress`; on a
/// read-only one, require the app's versions rebuilt and the migrations
/// done, which reports nothing.
pub(crate) fn prepare_schema(
    sql: &mut StoreSql,
    call: &StoreCall<'_>,
    tables: &[SyncedTable],
    migrations: &[Migration],
    versions: &[crate::sql_versions::SqlVersion],
    author: Option<crate::migration_run::MigrationOrigin>,
    operation: Option<&crate::migration::MigrationOperation>,
    audiences: Option<&crate::audience_database::Stage<'_>>,
    progress: &mut coven_foundation::progress::ProgressReport<crate::OpenStep>,
) -> Result<Vec<MigrationOutcome>, OpenError> {
    let read_only = author.is_none();
    let supported = validate_versions(migrations)?;
    prepare_internal_schema(sql)?;
    // Rebuild the indexes, or on a read-only connection require them rebuilt,
    // before the app's migrations run, since their statements use the indexes.
    if read_only {
        crate::sql_versions::require_built(sql, versions)?;
    } else {
        crate::sql_versions::rebuild(sql, tables, versions, progress)?;
    }
    let current = crate::transaction_constants::schema_version(sql, call)?;
    if current > supported {
        return Err(MigrationError::SchemaTooNew { current, supported }.into());
    }
    // Coven checks the declared kind of each migration it already applied,
    // even when its SQL ran on an earlier open: coven kept the schema of each
    // breaking migration it applied and of no other, so a kind the app now
    // declares differently fails the open.
    for migration in migrations.iter().filter(|m| m.version <= current) {
        let breaking = sql.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_conversion_schemas WHERE version=?1)",
            [migration.version],
            |r| r.get(0),
        )?;
        migration.check_kind(breaking)?;
    }
    if read_only || current == supported {
        let db = &*sql;
        let schema = Schema::read(db, &crate::file_table::declared(tables))?;
        schema.validate(tables)?;
        crate::schema::validate_local_keys(db, &schema, tables)?;
        crate::sqlite::check_followed_triggers(db)?;
        if let Some(table) = crate::row_queries::owner_declaration_changed(db, &schema, tables)? {
            return Err(DbError::Schema(crate::SchemaError::OwnerOnlyChanged { table }).into());
        }
        if let Some(table) = crate::file_table::changed(db, &schema, tables)? {
            return Err(DbError::Schema(crate::SchemaError::FileColumnsChanged { table }).into());
        }
        if !read_only {
            sql.transaction(|db| crate::row_queries::register_schema(db, &schema, tables))?;
        }
        sql.refresh_policy(sqlive::Refresh::Changed)?;
        return Ok(Vec::new());
    }
    let mut at = migrations.iter().find(|m| m.version > current).expect("pending migration");
    let publishes = author.is_some();
    let device = author.as_ref().map(|origin| origin.0);
    progress.begin(
        crate::OpenStep::Migrating,
        migrations.iter().filter(|m| m.version > current).count() as u64,
    );
    // Coven stages each audience's copy of the current rows and checks a run
    // with a breaking migration against each copy (§18.3).
    let pending: Vec<&Migration> = migrations.iter().filter(|m| m.version > current).collect();
    let mut staged = None;
    if let Some(stage) = audiences.filter(|_| {
        pending.iter().any(|m| matches!(m.kind, crate::migration::MigrationKind::Breaking(_)))
    }) {
        let first = pending[0];
        let before = stage(sql, call);
        staged = Some(before.map_err(|source| MigrationError::Failed {
            version: first.version,
            name: first.name,
            source: Box::new(source),
        })?);
    }
    let result = sql.with_profile(|db| {
        db.transaction(|db| {
            let crate::migration_run::MigrationRun { outcomes, marker } =
                crate::migration_run::run(
                    db,
                    call,
                    tables,
                    migrations,
                    current,
                    supported,
                    author.expect("writable migration"),
                    &mut at,
                    progress,
                )?;
            if let Some(staged) = staged.as_mut() {
                crate::audience_database::verify(staged, db, call, &pending, tables)?;
            }
            let breaking = outcomes
                .iter()
                .filter(|outcome| outcome.change == crate::MigrationChange::Breaking)
                .map(|outcome| outcome.version)
                .max();
            let raises = if let Some(minimum) = breaking {
                // Coven captures the raise snapshots through the marker before
                // the app writes again. With no store, coven has no shared
                // version to raise, and an installation that loads its store has
                // no marker.
                let schema = crate::write_schema::WriteSchema::read(db, call, tables.to_vec())?;
                // Coven rebuilds a circle that still waits for its rebuild to
                // this migration's version, in this migration's transaction.
                if let Some(marker) = marker {
                    crate::reload_gate::follow(db, marker)?;
                }
                if let Some(marker) = marker {
                    crate::raise_capture::capture(db, &schema, marker, supported)?;
                }
                crate::raise_capture::require(db, minimum)?;
                true
            } else {
                // When the migrations only add, coven moves a raise that has not
                // published yet to this version and captures it again at this
                // version (§18.5).
                let device = device.expect("writable migration");
                crate::raise_capture::raise_to(db, tables, device, supported)?
            };
            if raises {
                if let Some(operation) = operation.filter(|_| publishes) {
                    crate::raise_capture::journal(db, operation, supported)?;
                }
            }
            crate::row_queries::register_schema(
                db,
                &Schema::read(db, &crate::file_table::declared(tables))?,
                tables,
            )?;
            db.refresh_policy(sqlive::Refresh::Whole)?;
            Ok(outcomes)
        })
    });
    result.map_err(|source| {
        MigrationError::Failed { version: at.version, name: at.name, source: Box::new(source) }
            .into()
    })
}
