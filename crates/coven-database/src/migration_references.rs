//! Recheck references reached from changed parents, using the reverse indexes.

use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_rows::{row_id, AppKey, AppView};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_merge::Parent;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn changes(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    before: &AppView<'_>,
    visible: &AppView<'_>,
    store: &MergeStore<'_>,
    changed: &mut BTreeMap<AppKey, BTreeSet<String>>,
) -> Result<(), DbError> {
    let mut candidates = BTreeMap::<AppKey, BTreeSet<coven_merge::ForeignKey>>::new();
    for (key, columns) in changed.iter() {
        let Some(old) = before.row(key)? else {
            continue;
        };
        let same_audience = visible
            .row(key)?
            .is_some_and(|new| new.audience == old.audience);
        let parent = row_id(key, &old);
        for declaration in &schema.declarations {
            let table = schema.table(&declaration.name);
            for key in table
                .foreign_keys
                .iter()
                .filter(|fk| fk.target.eq_ignore_ascii_case(&parent.table))
            {
                let key = schema.foreign_key(table, key);
                if same_audience
                    && !key
                        .parent_columns
                        .0
                        .iter()
                        .any(|column| columns.contains(column))
                {
                    continue;
                }
                for child in crate::row_queries::children(db, table, &key, &parent)? {
                    candidates
                        .entry((child.table, child.key))
                        .or_default()
                        .insert(key.clone());
                }
            }
        }
    }
    // Schema changes to a foreign key already mark its columns in the SQL diff.
    // Only parent edits can change an otherwise unchanged reference identity.
    for (key, keys) in candidates {
        let Some(old) = before.row(&key)? else {
            continue;
        };
        let Some(new) = visible.row(&key)? else {
            continue;
        };
        let state = store.row(&row_id(&key, &old))?;
        for fk in keys {
            let Some(setter) = fk
                .columns
                .0
                .iter()
                .filter_map(|c| state.state.cells().get(c))
                .max_by_key(|c| store.stamp(c.write))
            else {
                continue;
            };
            let old = setter.value.parents.get(&fk);
            if let Some(parent) = old {
                if crate::write_record::generation(db, &parent.row)? != parent.generation {
                    continue;
                }
            }
            let new = if let Some(key) = new.parents.get(&fk) {
                let parent = row_id(key, &visible.row(key)?.expect("migration reference parent"));
                let generation = crate::write_record::generation(db, &parent)?;
                Some(Parent {
                    row: parent,
                    generation: if generation.is_multiple_of(2) {
                        generation.checked_add(1).expect("generation exhausted")
                    } else {
                        generation
                    },
                })
            } else {
                None
            };
            if old != new.as_ref() {
                changed.entry(key.clone()).or_default().extend(fk.columns.0);
            }
        }
    }
    Ok(())
}
