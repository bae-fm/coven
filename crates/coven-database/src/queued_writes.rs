//! Reapply eligible original queue records once over counted state.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::write::WriteHeader;
use coven_merge::{RowId, WriteId};
use std::collections::{BTreeMap, BTreeSet};

/// Which queued writes fold again after a slot (§6.2).
pub(crate) enum Refold<'a> {
    /// Every one: the slot changed how every queued write applies, or
    /// replaced the state beneath them all, and every kept row was put back.
    Whole,
    /// Only those that change a row the slot reached.
    Reached(&'a Reach),
}

pub(crate) fn stage(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    refold: Refold<'_>,
) -> Result<(), DbError> {
    crate::upload::each_plaintext(db, |header, parts| {
        if let Refold::Reached(reach) = &refold {
            if !reach.reaches(header.header.position) {
                return Ok(());
            }
        }
        if !folded(db, &header.header)? {
            return Ok(());
        }
        let mut opened = Vec::new();
        for (part, mut bytes) in parts {
            if crate::reload_gate::reloading(db, &part.audience)? {
                continue;
            }
            let audience = part.audience.clone();
            let ignored = crate::download_stage::ignored(db, &header.header, &audience)?;
            crate::download_stream::visit_part::<DbError>(
                part,
                |chunk| {
                    let bytes =
                        bytes.next().expect("queued plaintext contains the counted part")?;
                    chunk.copy_from_slice(&bytes);
                    Ok(())
                },
                |frame| crate::download_stage::frame(db, &header.header, ignored, frame),
            )?;
            opened.push(audience);
        }
        let dismissed = crate::upload::dismissed(db, header.header.position)?;
        let rows = rows(db, schema, &header.header, dismissed)?;
        crate::published::capture_rows(
            db,
            &crate::merge_store::MergeStore::from_staged(db, &schema.schema),
            &rows.present,
        )?;
        crate::published::capture_dropped(db, &rows.dropped)?;
        crate::download_stage::stage(db, schema, &header.header, &opened, |audience| {
            Ok(!crate::reload_gate::reloading(db, audience)?)
        })
    })
}

/// Whether the overlay folds the queued write `header` heads: one whose slot
/// is applied is counted already, and a removed device's never applies.
fn folded(db: &DatabaseConnection, header: &WriteHeader) -> Result<bool, DbError> {
    if crate::sequence::write_slot(db, header.position)?.is_some() {
        return Ok(false);
    }
    let removed:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_devices d JOIN _coven_members m ON m.member=d.member WHERE d.device=?1 AND (d.state<>'active' OR m.removed=1))",
        [header.position.device],|r|r.get(0))?;
    Ok(!removed)
}

/// The rows one write changes, by whether the current schema has their table.
pub(crate) struct WriteRows {
    /// Rows of the current schema's tables.
    pub(crate) present: Vec<RowId>,
    /// Rows of tables a migration dropped, whose losses the write changes.
    pub(crate) dropped: Vec<RowId>,
}

/// The rows the write `header` changes: its retained records' rows under the
/// current schema's names, the rows its dismissals name, `dismissed`, and
/// the rows whose losses a captured migration write puts or replaces, those
/// of a row it doesn't change or whose table it dropped among them.
pub(crate) fn rows(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    dismissed: Vec<RowId>,
) -> Result<WriteRows, DbError> {
    let current = db.schema_version()?;
    let mut present = Vec::new();
    for row in db.query(
        "SELECT table_name,key,audience FROM _coven_authored_changes WHERE write_id=?1",
        [coven_format::merge_fields::encode_write_id(&header.position)?],
        crate::row_queries::read_identity,
    )? {
        if let Some(table) =
            crate::source_names::table(db, header.schema_version, &row.table, current)?
        {
            present.push(RowId { table, ..row });
        }
    }
    let mut dropped = Vec::new();
    for row in dismissed.into_iter().chain(crate::captured_changes::loss_rows(db, header)?) {
        if schema.schema.tables.contains_key(&row.table.to_ascii_lowercase()) {
            present.push(row);
        } else {
            dropped.push(row);
        }
    }
    Ok(WriteRows { present, dropped })
}

/// The rows a slot of writes reaches beside the queue: those its writes
/// change, closed over the queue, so that a queued write changing one of
/// them adds all of its rows (Lean `QueueFold.Closed`). Each of the slot's
/// writes puts back the kept rows it newly reaches before it merges, and the
/// overlay folds again only the queued writes that reach them
/// (`refold_reached`). A row no write of the slot reaches keeps the queue's
/// fold it holds.
pub(crate) struct Reach {
    rows: BTreeSet<RowId>,
    /// The kept rows put back so far, whose table the current schema has.
    restored: BTreeSet<RowId>,
    /// The queue, read once a write of the slot changes a row.
    queue: Option<Queue>,
}

/// The queued writes the overlay folds, and the rows each changes.
struct Queue {
    /// Each folded queued write and its rows, in queue order.
    writes: Vec<(WriteId, Vec<RowId>)>,
    /// For each row, the indexes of the queued writes changing it.
    changing: BTreeMap<RowId, Vec<usize>>,
    /// The queued writes the slot has reached.
    reached: BTreeSet<WriteId>,
}

impl Queue {
    fn read(db: &DatabaseConnection, schema: &WriteSchema) -> Result<Self, DbError> {
        let mut writes: Vec<(WriteId, Vec<RowId>)> = Vec::new();
        crate::upload::each_plaintext(db, |header, _| {
            if !folded(db, &header.header)? {
                return Ok(());
            }
            let dismissed = crate::upload::dismissed(db, header.header.position)?;
            let rows = rows(db, schema, &header.header, dismissed)?;
            writes.push((
                header.header.position,
                rows.present.into_iter().chain(rows.dropped).collect(),
            ));
            Ok(())
        })?;
        let mut changing = BTreeMap::<RowId, Vec<usize>>::new();
        for (index, (_, rows)) in writes.iter().enumerate() {
            for row in rows {
                changing.entry(row.clone()).or_default().push(index);
            }
        }
        Ok(Self { writes, changing, reached: BTreeSet::new() })
    }
}

impl Reach {
    /// A slot that has reached no row yet.
    pub(crate) fn new() -> Self {
        Self { rows: BTreeSet::new(), restored: BTreeSet::new(), queue: None }
    }

    /// Whether the slot reached a row the queued write `write` changes.
    fn reaches(&self, write: WriteId) -> bool {
        self.queue.as_ref().is_some_and(|queue| queue.reached.contains(&write))
    }

    /// The kept rows put back, whose table the current schema has.
    pub(crate) fn restored(&self) -> &BTreeSet<RowId> {
        &self.restored
    }
}

/// Before the slot's write `header` merges, `reach` the rows it changes,
/// with `dismissed` those its dismissals name, close them over the
/// queue, and put back the kept rows among those newly reached. A row an
/// earlier write of the slot changed is reached already, so every row
/// put back holds the counted state from before the slot.
pub(crate) fn put_back(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    reach: &mut Reach,
    dismissed: Vec<RowId>,
) -> Result<(), DbError> {
    let rows = rows(db, schema, header, dismissed)?;
    let mut pending: Vec<RowId> = rows
        .present
        .into_iter()
        .chain(rows.dropped)
        .filter(|row| reach.rows.insert(row.clone()))
        .collect();
    if pending.is_empty() {
        return Ok(());
    }
    let queue = match &mut reach.queue {
        Some(queue) => queue,
        None => reach.queue.insert(Queue::read(db, schema)?),
    };
    let mut reached = pending.clone();
    while let Some(row) = pending.pop() {
        for &index in queue.changing.get(&row).into_iter().flatten() {
            let (write, rows) = &queue.writes[index];
            if queue.reached.insert(*write) {
                for row in rows {
                    if reach.rows.insert(row.clone()) {
                        reached.push(row.clone());
                        pending.push(row.clone());
                    }
                }
            }
        }
    }
    reach.restored.extend(crate::published::restore_rows(db, schema, &reached)?);
    Ok(())
}

pub(crate) fn present(db: &DatabaseConnection) -> Result<bool, DbError> {
    db.query_row(&format!("SELECT EXISTS(SELECT 1 FROM _coven_uploads u JOIN _coven_writes w ON {WRITE_DEVICE_SQL}=u.device AND w.number=u.number WHERE w.slot IS NULL)"),[],|r|r.get(0))
}

/// After an applied slot, fold the queued writes `refold` names over the
/// counted state again, keeping aside the counted state of only the rows
/// they write: `stage` keeps each queued write's rows before it changes them
/// (§6.2). The rows its removals reach keep their counted records, since
/// folding the queue changes only their app rows, which materializing
/// derives again from the records whenever a slot reaches them. The counted
/// state is counted first only where the slot and the rows put back reach,
/// `touched`; every other row holds it already. Once the queue is empty,
/// every row still kept is put back into `touched`: no write of the slot
/// reached it, so what it kept is the counted state.
pub(crate) fn overlay(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    touched: &mut BTreeSet<RowId>,
    refold: Refold<'_>,
) -> Result<(), DbError> {
    if present(db)? {
        let mut reached = touched.clone();
        reached.extend(crate::download_stage::staged_rows(db)?);
        crate::download_stage::count(db, schema, reached)?;
        stage(db, schema, refold)
    } else {
        touched.extend(crate::published::restore(db, schema)?);
        Ok(())
    }
}
