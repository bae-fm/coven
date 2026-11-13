//! One atomic migration run and its single local write in the newest schema.

use crate::migration_names::{MigrationEffects, MigrationMatch, MigrationNames};
use crate::migration_snapshot::MigrationCapture;
use crate::schema::Schema;
use crate::sqlite::DatabaseConnection;
use crate::write_rows::AppView;
use crate::{DbError, Migration, MigrationChange, MigrationContext, MigrationOutcome, SyncedTable};
use coven_format::write::WriteDisposition;
use coven_foundation::id_source::DeviceId;
use std::collections::BTreeSet;
use std::time::SystemTime;

/// What one run changed: each step's outcome and, for a breaking batch, its
/// empty migration marker.
pub(crate) struct MigrationRun {
    pub(crate) outcomes: Vec<MigrationOutcome>,
    pub(crate) marker: Option<coven_merge::WriteId>,
}

/// The device and wall clock authoring a migration's local write, and
/// where the installation's state comes from.
#[derive(Clone)]
pub(crate) struct MigrationOrigin(
    pub(crate) DeviceId,
    pub(crate) SystemTime,
    pub(crate) Installation,
);

/// Where an installation's state comes from, which decides whether its
/// breaking migrations make a migration write (§18.4).
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum Installation {
    /// Its own: the store's creator, or a device whose store has loaded.
    Own,
    /// The store it loads from storage next, for a join, restore or reset,
    /// or a joined store whose first load has not registered its device:
    /// the device isn't registered yet, and the load replaces its state.
    Loading,
    /// A rebuild's store (§18.3): it makes the migration write the header
    /// gives, which the device's real database queued for the rebuild, and
    /// queues only its marker in the rebuild's store.
    Rebuilding(coven_format::write::WriteHeader),
}

struct MigrationStep<'a> {
    migration: &'a Migration,
    before: Schema,
    effects: MigrationEffects,
    changed: BTreeSet<String>,
}

pub(crate) fn run<'a>(
    database: &DatabaseConnection,
    tables: &[SyncedTable],
    migrations: &'a [Migration],
    current: u32,
    supported: u32,
    origin: MigrationOrigin,
    at: &mut &'a Migration,
    progress: &mut coven_foundation::progress::ProgressReport<crate::OpenStep>,
) -> Result<MigrationRun, DbError> {
    // Each version's file references: the registered ones, carried through
    // each step's renames, then the final declarations (§18.1).
    if let Installation::Rebuilding(header) = &origin.2 {
        let breaking = migrations
            .iter()
            .filter(|m| {
                m.version > current
                    && matches!(m.kind, crate::migration::MigrationKind::Breaking(_))
            })
            .map(|m| m.version)
            .max();
        check_rebuild_header(database, origin.0, breaking, supported, header)?;
    }
    let mut files = crate::file_table::registered_columns(database)?;
    let mut before = Schema::read(database, &files)?;
    let removed = crate::published::removal_losses_before_migrating(database, &before)?;
    let mut capture = MigrationCapture::new(database, tables)?;
    // Changing an existing table's owner declaration or file references is
    // breaking (§18.1): the raise captures the state before any statement
    // runs.
    let declared = crate::row_queries::owner_declaration_changed(database, &before, tables)?
        .is_some()
        || crate::file_table::changed(database, &before, tables)?.is_some();
    if declared {
        capture.capture_now(&before)?;
    }
    let mut breaking = declared;
    let mut steps = Vec::new();
    for (ran, migration) in migrations.iter().filter(|m| m.version > current).enumerate() {
        *at = migration;
        if matches!(migration.kind, crate::migration::MigrationKind::Breaking(_)) {
            check_starts_from(database, migration, current)?;
        }
        let context = MigrationContext::new(database, before, capture);
        database.migrating(|| migration.apply(&context))?;
        let after = Schema::read(database, &files)?;
        let (effects, step_before, step_capture) = context.finish(&after);
        capture = step_capture;
        let changed = capture.take_targets();
        files = crate::file_table::renamed(&files, &effects.names);
        let after = Schema::read(database, &files)?;
        crate::source_names::record(
            database,
            migration.version - 1,
            migration.version,
            &after,
            &effects.names,
        )?;
        let synced = capture.synced().clone();
        breaking |= changed.iter().any(|t| synced.contains(t))
            || effects.dropped.iter().any(|t| synced.contains(t))
            || step_before.change_to(&after, &synced) == MigrationChange::Breaking;
        if !breaking {
            capture.discard()?;
        }
        steps.push(MigrationStep { migration, before: step_before, effects, changed });
        before = after;
        progress.done(ran as u64 + 1);
    }
    let before = Schema::read(database, &crate::file_table::declared(tables))?;
    before.validate(tables)?;
    crate::schema::validate_local_keys(database, &before, tables)?;
    database.check_followed_triggers()?;
    let final_schema = before;
    let mut synced: BTreeSet<_> = tables.iter().map(|t| t.name.to_ascii_lowercase()).collect();
    let mut outcomes = Vec::new();
    let mut after = &final_schema;
    synced.extend(capture.synced().iter().cloned());
    for step in steps.iter().rev() {
        synced.extend(
            step.effects
                .names
                .tables
                .iter()
                .filter(|(_, new)| synced.contains(&new.to_ascii_lowercase()))
                .map(|(old, _)| old.to_ascii_lowercase())
                .collect::<Vec<_>>(),
        );
        let change = if step.effects.dropped.iter().any(|t| synced.contains(t))
            || step.changed.iter().any(|name| synced.contains(name))
        {
            MigrationChange::Breaking
        } else {
            step.before.change_to(after, &synced)
        };
        outcomes.push(MigrationOutcome {
            version: step.migration.version,
            name: step.migration.name,
            change,
        });
        after = &step.before;
    }
    outcomes.reverse();
    if declared {
        outcomes[0].change = MigrationChange::Breaking;
    }
    // Classification needs the final declarations and every rename in the run.
    // Refuse the first mismatch before retaining conversions or publishing a marker.
    for (step, outcome) in steps.iter().zip(&outcomes) {
        *at = step.migration;
        step.migration.check_kind(outcome.change == MigrationChange::Breaking)?;
    }
    database.batch(&format!("PRAGMA user_version = {}", supported as i32))?;
    for (index, step) in steps.iter().enumerate() {
        if outcomes[index].change == MigrationChange::Breaking {
            *at = step.migration;
            let after = match steps.get(index + 1) {
                Some(next) => &next.before,
                None => &final_schema,
            };
            crate::migration_receive::retain(
                database,
                step.migration.version,
                &step.before,
                after,
                &step.effects.names,
            )?;
        }
    }
    let MigrationOrigin(device, now, installation) = origin;
    // A joined store opened again before its first load has applied this
    // device's registration is as unregistered as the installation that
    // published it.
    let installation = if matches!(installation, Installation::Own)
        && crate::store_state_tables::awaiting_registration(database, device)?
    {
        Installation::Loading
    } else {
        installation
    };
    let author = (device, now);
    *at = steps.last().expect("pending migrations").migration;
    let mut marker = None;
    if let Some(first) = outcomes.iter().position(|o| o.change == MigrationChange::Breaking) {
        let mut names = MigrationNames::through(
            &steps[0].before,
            &final_schema,
            steps.iter().flat_map(|s| s.effects.statements.iter().map(String::as_str)),
        )?;
        let mut baseline_names = MigrationNames::through(
            &steps[first].before,
            &final_schema,
            steps[first..].iter().flat_map(|s| s.effects.statements.iter().map(String::as_str)),
        )?;
        // Every synced table keeps its rows' merge state through the run:
        // the app's, and coven's own files table, which no migration changes.
        let synced = crate::file_table::synced(tables);
        for names in [&mut names, &mut baseline_names] {
            names.tables.retain(|_, new| synced.iter().any(|t| t.name.eq_ignore_ascii_case(new)));
            names.columns.retain(|(table, _), _| names.tables.contains_key(table));
        }
        crate::source_names::record(database, current, supported, &final_schema, &names)?;
        // A write made before the device is registered is one no device, its
        // own included, can accept, and the store this installation loads
        // replaces what the migration changed.
        let given = match installation {
            Installation::Own => Some(None),
            Installation::Rebuilding(header) => Some(Some(header)),
            Installation::Loading => None,
        };
        if let Some(given) = given {
            marker = Some(finish(
                database,
                tables,
                &steps[0].before,
                &names,
                &steps[first].before,
                &baseline_names,
                capture.into_snapshot().expect("breaking statement captured its before state"),
                &removed,
                author,
                given,
            )?);
        }
    }
    if marker.is_none() && !steps.is_empty() {
        // Added columns can change generated values and removal predicates.
        let schema = crate::write_schema::WriteSchema::read(database, tables.to_vec())?;
        let changed = steps[0].before.changed_tables(&final_schema);
        let refresh: BTreeSet<String> = schema
            .declarations
            .iter()
            .filter(|declaration| changed.contains(&declaration.name.to_ascii_lowercase()))
            .map(|declaration| declaration.name.clone())
            .collect();
        if !refresh.is_empty() {
            crate::write_schema::with_evaluators(database, &schema, || {
                crate::migration_state::refresh(database, &schema, &refresh)
            })?;
        }
    }
    // A file a dropped column or table referenced last waits for this
    // device's next write of its own to delete its record (§17.2).
    crate::file_protection::await_own_write(
        database,
        crate::file_protection::migrate(database, tables)?,
    )?;
    Ok(MigrationRun { outcomes, marker })
}

/// Refuse, before any statement runs, a rebuild's migration write header that
/// is not this device's next write in the rebuild's store, a migration at a
/// version the run reaches (its newest breaking migration's, or a later one
/// the device's app has since reached by additions), read through the slot the
/// store has applied: the merge counts a write's changes only over what it read
/// (§18.3).
fn check_rebuild_header(
    database: &DatabaseConnection,
    device: DeviceId,
    breaking: Option<u32>,
    supported: u32,
    header: &coven_format::write::WriteHeader,
) -> Result<(), DbError> {
    let refuse = |reason| Err(DbError::RebuildHeader { reason });
    if header.disposition != WriteDisposition::Migration {
        return refuse("it is not a migration write");
    }
    let Some(breaking) = breaking else {
        return refuse("the run has no breaking migration");
    };
    if header.schema_version < breaking || header.schema_version > supported {
        return refuse(
            "it is not at a version the run reaches after its newest breaking migration",
        );
    }
    if header.position.device != device {
        return refuse("it is another device's write");
    }
    if crate::write_metadata::newest_number(database, device)?
        .is_some_and(|newest| header.position.number <= newest)
    {
        return refuse("it is not newer than this device's writes in the store");
    }
    if header.read_through != crate::sequence::applied(database)? {
        return refuse("it did not read through the slot the store has applied");
    }
    if crate::write_encoding::latest_timestamp(database)?
        .is_some_and(|latest| header.timestamp <= latest)
    {
        return refuse("it is not newer than every write the store holds");
    }
    Ok(())
}

/// A breaking migration's `starts_from` against the schema the run has reached
/// before its statements, by the rules the final declarations meet, and, for the
/// first pending migration, against what this device registered (§18.3).
fn check_starts_from(
    database: &DatabaseConnection,
    migration: &Migration,
    current: u32,
) -> Result<(), DbError> {
    let starts = &migration.starts_from;
    let schema = Schema::read(database, &crate::file_table::declared(starts))?;
    schema.validate(starts)?;
    crate::schema::validate_local_keys(database, &schema, starts)?;
    if migration.version == current + 1 {
        if let Some(table) =
            crate::row_queries::owner_declaration_changed(database, &schema, starts)?
        {
            return Err(DbError::Schema(crate::SchemaError::OwnerOnlyChanged { table }));
        }
        if let Some(table) = crate::file_table::changed(database, &schema, starts)? {
            return Err(DbError::Schema(crate::SchemaError::FileColumnsChanged { table }));
        }
    }
    Ok(())
}

fn finish(
    database: &DatabaseConnection,
    tables: &[SyncedTable],
    initial_schema: &Schema,
    names: &MigrationMatch,
    baseline_schema: &Schema,
    baseline_names: &MigrationMatch,
    mut snapshot: crate::migration_snapshot::MigrationSnapshot<'_>,
    removed: &[coven_format::loss::Loss],
    (device, now): (DeviceId, SystemTime),
    given: Option<coven_format::write::WriteHeader>,
) -> Result<coven_merge::WriteId, DbError> {
    let schema = crate::write_schema::WriteSchema::read(database, tables.to_vec())?;
    crate::write_schema::with_evaluators(database, &schema, || {
        database.batch("CREATE TEMP TABLE _coven_migration_frozen(audience TEXT NOT NULL,record BLOB NOT NULL)")?;
        let counted_frozen = crate::published_migration::carry(database, names, &schema, removed)?;
        let hidden = crate::migration_hidden::Hidden::before(database, initial_schema, names)?;
        let refresh = crate::migration_state::carry(database, initial_schema, names, &schema)?;
        crate::migration_files::carry(database, names)?;
        crate::row_queries::register_schema(database, &schema.schema, tables)?;
        snapshot.rename(baseline_names)?;
        let before = AppView::migration_before(database, &schema, &snapshot);
        let visible = AppView::after(database, &schema);
        let reference_columns = baseline_names.reference_columns(baseline_schema, &schema.schema);
        let mut changed = snapshot.differences(&schema, &reference_columns)?;
        for key in changed.keys() {
            if before.row(key)?.is_none() {
                if let Some(row) = visible.row(key)? {
                    crate::write_capture::validate_key(&schema, schema.table(&key.0), &row.values)?;
                }
            }
        }
        let store = crate::merge_store::MergeStore::new(database, &before);
        crate::migration_references::changes(
            database,
            &schema,
            &before,
            &visible,
            &store,
            &mut changed,
        )?;
        let deleted = crate::store_state_tables::deleted_circles(database)?;
        let mut changes = crate::write_record::changes(
            database,
            &schema,
            &before,
            &visible,
            &store,
            &changed,
            &crate::file_write::Copied::new(),
        )?;
        // A migration is the app writing: a row it writes into a deleted
        // circle fails the open, as a local write there fails (§15.9, §18.4).
        // Its rows never reach `write_commit::check_membership`, since its
        // parts are cleared before it is queued, so this is their check.
        if let Some(circle) = changes.keys().find_map(|row| match row.audience {
            coven_merge::Audience::Circle(circle) if deleted.contains_key(&circle) => Some(circle),
            _ => None,
        }) {
            return Err(DbError::DeletedCircle(circle));
        }
        // Rows removal rules hide independently become ordinary deletes at
        // their generations (§18.7).
        let selected = hidden.select();
        let mut deletes = Vec::new();
        for row in &selected {
            if let Some(change) = hidden.delete(&store, row)? {
                deletes.push(row.clone());
                changes.insert(row.clone(), change);
            }
        }
        let mut record = match given {
            Some(header) => crate::write_record::changed_under(header, changes),
            None => crate::write_record::changed(database, device, now, changes)?,
        };
        for row in &deletes {
            hidden.freeze(
                &schema,
                &store,
                row,
                record.header.position,
                record.header.schema_version,
            )?;
        }
        crate::write_apply::WriteApply::new(database, &schema, &store, &before, &visible, &deleted)
            .apply(Some(&record), BTreeSet::new())?;
        record.header.disposition = WriteDisposition::Migration;
        // Migration row changes belong to the published snapshot, but remain
        // original source evidence for writes authored after the migration.
        let form = crate::write_sources::KeptForm::new(&schema, record.header.schema_version);
        for part in &record.parts {
            crate::ownership::offer_part(database, &record.header, part)?;
            let frames = crate::write_sources::encode_records(&record.header, part)?;
            for (row, bytes) in part.records().zip(&frames) {
                crate::write_sources::validate(database, &record.header, row)?;
                crate::write_sources::retain_authored(database, &form, &record.header, row)?;
                retain_record(database, &record.header, row, bytes)?;
                crate::write_sources::admit(
                    database,
                    &record.header,
                    row.row(),
                    &coven_format::snapshot_rows::ChangeVerdict::Applied,
                )?;
            }
        }
        record.parts.clear();
        crate::write_commit::queue(database, &schema, &record, Default::default())?;
        crate::migration_state::refresh(database, &schema, &refresh)?;
        crate::captured_changes::capture(
            database,
            record.header.position,
            record.header.schema_version,
        )?;
        database.internal_execute("INSERT INTO _coven_migration_losses(write_id,audience,record) SELECT ?1,audience,record FROM temp._coven_migration_frozen",[coven_format::merge_fields::encode_write_id(&record.header.position)?])?;
        // What this run froze of the kept state, over the counted prefix:
        // the migration write's own freezes, over the whole prefix, replace
        // them wherever its losses are put (§18.5).
        for loss in &counted_frozen {
            database.internal_execute(
                "INSERT INTO _coven_migration_counted_losses(write_id,audience,record) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
                rusqlite::params![
                    coven_format::merge_fields::encode_write_id(&record.header.position)?,
                    &loss.row.audience,
                    coven_format::merge_fields::encode_loss(loss)?
                ],
            )?;
        }
        database.batch("DROP TABLE temp._coven_migration_frozen")?;
        snapshot.drop()?;
        Ok(record.header.position)
    })
}

/// Keep a freeze the breaking migration made, which the migration write
/// `migration` puts as its loss once it applies (§18.5).
pub(crate) fn keep_loss(
    db: &DatabaseConnection,
    migration: coven_merge::WriteId,
    loss: &coven_format::loss::Loss,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_migration_losses(write_id,audience,record) VALUES(?1,?2,?3)",
        rusqlite::params![
            coven_format::merge_fields::encode_write_id(&migration)?,
            &loss.row.audience,
            coven_format::merge_fields::encode_loss(loss)?
        ],
    )?;
    Ok(())
}

/// Forget the migration losses kept for `audience`, whose state a loaded
/// snapshot replaces.
pub(crate) fn forget_audience(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
) -> Result<(), DbError> {
    for table in ["_coven_migration_losses", "_coven_migration_counted_losses"] {
        db.internal_execute(&format!("DELETE FROM {table} WHERE audience=?1"), [audience])?;
    }
    Ok(())
}

/// Keep this device's migration write's record whole, for its marker's slot
/// to apply: the marker carries no row changes (§18.4).
fn retain_record(
    database: &DatabaseConnection,
    header: &coven_format::write::WriteHeader,
    record: coven_format::write::PartRecord<'_>,
    bytes: &[u8],
) -> Result<(), DbError> {
    let row = record.row();
    database.internal_execute(
        "INSERT INTO _coven_migration_records VALUES(?1,?2,?3,?4,?5)",
        rusqlite::params![
            coven_format::merge_fields::encode_write_id(&header.position)?,
            &row.audience,
            row.table,
            row.key,
            bytes
        ],
    )?;
    Ok(())
}

/// Forget one of this device's migration write's whole records, with its
/// kept record, once a loaded snapshot replaced it (§18.5).
pub(crate) fn forget_record(
    database: &DatabaseConnection,
    write: &[u8],
    audience: &coven_merge::Audience,
    table: &str,
    key: &[u8],
) -> Result<(), DbError> {
    database.internal_execute(
        "DELETE FROM _coven_migration_records WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
        rusqlite::params![write, audience, table, key],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "migration_run_tests.rs"]
pub(crate) mod tests;
