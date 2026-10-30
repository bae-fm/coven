//! Reapply eligible original queue records once over counted state.

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_merge::RowId;
use std::collections::BTreeSet;

pub(crate) fn stage(db: &DatabaseConnection, schema: &WriteSchema) -> Result<(), DbError> {
    crate::upload::each_plaintext(db, |header, parts| {
        if crate::sequence::write_slot(db, header.header.position)?.is_some() {
            return Ok(());
        }
        let removed:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_devices d JOIN _coven_members m ON m.member=d.member WHERE d.device=?1 AND (d.state<>'active' OR m.removed=1))",
            [header.header.position.device.0.to_be_bytes().as_slice()],|r|r.get(0))?;
        if removed {
            return Ok(());
        }
        let mut opened = Vec::new();
        for (part, mut bytes) in parts {
            if crate::reload_gate::reloading(db, &part.audience)? {
                continue;
            }
            let audience = part.audience.clone();
            opened.push(audience.clone());
            let ignored = crate::download_stage::ignored(db, &header.header, &audience)?;
            crate::download_stream::visit_part::<DbError>(
                part,
                |chunk| {
                    let bytes = bytes
                        .next()
                        .expect("queued plaintext contains the counted part")?;
                    chunk.copy_from_slice(&bytes);
                    Ok(())
                },
                |frame| crate::download_stage::frame(db, &header.header, ignored, frame),
            )?;
        }
        let mut rows = Vec::new();
        crate::judge::visit_write(db, &header.header, |record| {
            let row = record.row();
            if let Some(table) = crate::source_names::table(
                db,
                header.header.schema_version,
                &row.table,
                db.schema_version()?,
            )? {
                rows.push(coven_merge::RowId {
                    table,
                    ..row.clone()
                });
            }
            Ok(())
        })?;
        crate::published::capture_rows(
            db,
            &crate::merge_store::MergeStore::from_staged(db, &schema.schema),
            &rows,
        )?;
        crate::download_stage::stage(db, schema, &header.header, &opened, |audience| {
            Ok(!crate::reload_gate::reloading(db, audience)?)
        })
    })
}

pub(crate) fn present(db: &DatabaseConnection) -> Result<bool, DbError> {
    db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_uploads u JOIN _coven_writes w ON substr(w.timestamp,9,8)=u.device AND w.number=u.number WHERE w.slot IS NULL)",[],|r|r.get(0))
}

/// After an applied slot, fold the unclaimed queue over the counted state
/// again, keeping aside the counted state of only the rows the queue writes:
/// `stage` keeps each queued write's rows before it changes them (§6.2). The
/// rows its removals reach keep their counted records, since folding the
/// queue changes only their app rows, which materializing derives again from
/// the records whenever a slot reaches them. The counted state is counted
/// first only where the slot and the rows put back reach, `touched`; every
/// other row holds it already. Without a queue nothing needs keeping.
pub(crate) fn overlay(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    touched: &BTreeSet<RowId>,
) -> Result<(), DbError> {
    if present(db)? {
        let mut reached = touched.clone();
        reached.extend(crate::download_stage::staged_rows(db)?);
        crate::download_stage::count(db, schema, reached)?;
        stage(db, schema)
    } else {
        crate::published::clear(db)
    }
}
