//! One atomic migration run and its single local write in the newest schema.

use crate::migration_names::{MigrationEffects, MigrationMatch, MigrationNames};
use crate::migration_snapshot::MigrationCapture;
use crate::schema::Schema;
use crate::sqlite::DatabaseConnection;
use crate::write_rows::AppView;
use crate::{DbError, Migration, MigrationChange, MigrationContext, MigrationOutcome, SyncedTable};
use coven_format::write::WriteDisposition;
use coven_foundation::id_source::DeviceId;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::time::SystemTime;

/// The device and wall clock authoring a migration's local write.
#[derive(Clone, Copy)]
pub(crate) struct MigrationOrigin(pub(crate) DeviceId, pub(crate) SystemTime);

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
) -> Result<Vec<MigrationOutcome>, DbError> {
    let mut before = Schema::read(database)?;
    let capture = RefCell::new(MigrationCapture::new(database, tables)?);
    let mut breaking = false;
    let mut steps = Vec::new();
    for migration in migrations.iter().filter(|m| m.version > current) {
        *at = migration;
        let context = MigrationContext::new(database, &before, &capture);
        let ((), changed) = database.migration_changes(|| migration.apply(&context))?;
        let after = Schema::read(database)?;
        let effects = context.finish(&before, &after);
        crate::source_names::record(
            database,
            migration.version - 1,
            migration.version,
            &after,
            &effects.names,
        )?;
        let synced = capture.borrow().synced.clone();
        breaking |= changed.iter().any(|t| synced.contains(t))
            || effects.dropped.iter().any(|t| synced.contains(t))
            || before.change_to(&after, &synced) == MigrationChange::Breaking;
        if !breaking {
            capture.borrow_mut().discard(database)?;
        }
        steps.push(MigrationStep {
            migration,
            before,
            effects,
            changed,
        });
        before = after;
    }
    before.validate(tables)?;
    let final_schema = before;
    let mut synced: BTreeSet<_> = tables.iter().map(|t| t.name.to_ascii_lowercase()).collect();
    let mut outcomes = Vec::new();
    let mut after = &final_schema;
    synced.extend(capture.borrow().synced.iter().cloned());
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
    database.batch(&format!("PRAGMA user_version = {}", supported as i32))?;
    // Waiting writes were made under the schema this database had.
    let waiting_version = current;
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
            if step.migration.version <= waiting_version {
                continue;
            }
            step.migration.convert_waiting(
                database,
                &step.before,
                after,
                &step.effects.names,
                supported,
            )?;
        }
    }
    if let Some(first) = outcomes
        .iter()
        .find(|o| o.change == MigrationChange::Breaking && o.version > waiting_version)
    {
        crate::migration_writes::advance_version(database, first.version, supported)?;
    }
    let MigrationOrigin(device, now) = origin;
    let author = (device, now);
    *at = steps.last().expect("pending migrations").migration;
    if let Some(first) = outcomes
        .iter()
        .position(|o| o.change == MigrationChange::Breaking)
    {
        let mut names = MigrationNames::through(
            &steps[0].before,
            &final_schema,
            steps
                .iter()
                .flat_map(|s| s.effects.statements.iter().map(String::as_str)),
        )?;
        let mut baseline_names = MigrationNames::through(
            &steps[first].before,
            &final_schema,
            steps[first..]
                .iter()
                .flat_map(|s| s.effects.statements.iter().map(String::as_str)),
        )?;
        for names in [&mut names, &mut baseline_names] {
            names
                .tables
                .retain(|_, new| tables.iter().any(|t| t.name.eq_ignore_ascii_case(new)));
            names
                .columns
                .retain(|(table, _), _| names.tables.contains_key(table));
        }
        crate::source_names::record(database, current, supported, &final_schema, &names)?;
        finish(
            database,
            tables,
            &steps[0].before,
            &names,
            &steps[first].before,
            &baseline_names,
            capture
                .into_inner()
                .snapshot
                .expect("breaking statement captured its before state"),
            author,
        )?;
    }
    crate::file_protection::migrate(database, tables)?;
    Ok(outcomes)
}

fn finish(
    database: &DatabaseConnection,
    tables: &[SyncedTable],
    initial_schema: &Schema,
    names: &MigrationMatch,
    baseline_schema: &Schema,
    baseline_names: &MigrationMatch,
    mut snapshot: crate::migration_snapshot::MigrationSnapshot,
    (device, now): (DeviceId, SystemTime),
) -> Result<(), DbError> {
    let schema = crate::write_schema::WriteSchema::read(database, tables.to_vec())?;
    schema.with_evaluators(database, || {
        let refresh = crate::migration_state::carry(database, initial_schema, names, &schema)?;
        crate::row_queries::register_schema(database, &schema.schema, tables)?;
        snapshot.rename(database, baseline_names)?;
        let before = AppView::migration_before(database, &schema, &snapshot);
        let visible = AppView::after(database, &schema);
        let reference_columns = baseline_names.reference_columns(baseline_schema, &schema.schema);
        let mut changed = snapshot.differences(database, &schema, &reference_columns)?;
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
        let deleted = crate::store_log_tables::deleted_circles(database)?;
        let changes = crate::write_record::changes(
            database, &schema, &before, &visible, &store, &changed, &deleted,
        )?;
        let mut record = crate::write_record::record(database, device, now, changes)?;
        crate::write_apply::WriteApply::new(database, &schema, &store, &before, &visible, &deleted)
            .apply(Some(&record), BTreeSet::new())?;
        record.header.disposition = WriteDisposition::Migration;
        // Migration row changes belong to the published snapshot, but remain
        // original source evidence for writes authored after the migration.
        for part in &record.parts {
            for row in &part.rows {
                crate::write_sources::validate(database, &record.header, row)?;
                crate::write_sources::retain(database, &record.header, row)?;
                crate::write_sources::admit(
                    database,
                    &record.header,
                    &row.row,
                    &coven_format::snapshot_rows::ChangeVerdict::Applied,
                )?;
            }
            crate::write_sources::complete(database, &record.header, &part.audience)?;
        }
        record.parts.clear();
        crate::write_commit::queue(database, &record)?;
        crate::migration_state::refresh(database, &schema, &refresh)?;
        snapshot.drop(database)?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "migration_run_tests.rs"]
mod tests;
