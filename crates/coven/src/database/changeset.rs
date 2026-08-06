//! Decode SQLite session changesets into database-independent row changes.

use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::Action;
use rusqlite::session::ChangesetIter;
use rusqlite::types::ValueRef;

use coven_foundation::changeset::{ChangeOp, RowChange};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UpdateValue {
    New,
    Old,
}

enum ColumnCell {
    Absent,
    Present(Option<String>),
}

/// Walk a changeset and return every row change with its column values.
///
/// Returns an empty vec for an empty changeset.
pub(crate) fn walk(changeset_bytes: &[u8]) -> Result<Vec<RowChange>, String> {
    walk_with_update_values(changeset_bytes, UpdateValue::New)
}

pub(crate) fn walk_old(changeset_bytes: &[u8]) -> Result<Vec<RowChange>, String> {
    walk_with_update_values(changeset_bytes, UpdateValue::Old)
}

fn walk_with_update_values(
    changeset_bytes: &[u8],
    update_value: UpdateValue,
) -> Result<Vec<RowChange>, String> {
    if changeset_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let input: &mut dyn std::io::Read = &mut &changeset_bytes[..];
    let mut iter =
        ChangesetIter::start_strm(&input).map_err(|e| format!("changeset start failed: {e}"))?;

    let mut changes = Vec::new();
    while let Some(item) = iter
        .next()
        .map_err(|e| format!("changeset next failed: {e}"))?
    {
        let op = item.op().map_err(|e| format!("changeset op failed: {e}"))?;
        let change_op = match op.code() {
            Action::SQLITE_INSERT => ChangeOp::Insert,
            Action::SQLITE_UPDATE => ChangeOp::Update,
            Action::SQLITE_DELETE => ChangeOp::Delete,
            _ => continue,
        };
        let ncol = op.number_of_columns();

        let cells = (0..ncol)
            .map(|c| extract_col(item, c as usize, change_op, update_value))
            .collect::<Result<Vec<_>, _>>()?;
        let columns = cells
            .iter()
            .map(|cell| match cell {
                ColumnCell::Absent => None,
                ColumnCell::Present(value) => value.clone(),
            })
            .collect();
        changes.push(RowChange {
            table: op.table_name().to_string(),
            op: change_op,
            columns,
        });
    }

    Ok(changes)
}

/// Extract a column value from a changeset item following the op's old/new
/// semantics. An absent column (unchanged in an update) reads as an
/// `InvalidColumnIndex` error from rusqlite, which maps to `None`.
fn extract_col(
    item: &rusqlite::session::ChangesetItem,
    col: usize,
    op: ChangeOp,
    update_value: UpdateValue,
) -> Result<ColumnCell, String> {
    match op {
        ChangeOp::Insert => changeset_value(item, col, UpdateValue::New),
        ChangeOp::Delete => changeset_value(item, col, UpdateValue::Old),
        ChangeOp::Update => {
            let fallback = match update_value {
                UpdateValue::New => UpdateValue::Old,
                UpdateValue::Old => UpdateValue::New,
            };
            match changeset_value(item, col, update_value)? {
                ColumnCell::Absent => changeset_value(item, col, fallback),
                present => Ok(present),
            }
        }
    }
}

fn changeset_value(
    item: &rusqlite::session::ChangesetItem,
    col: usize,
    side: UpdateValue,
) -> Result<ColumnCell, String> {
    let value = match side {
        UpdateValue::New => item.new_value(col),
        UpdateValue::Old => item.old_value(col),
    };
    match value {
        Ok(value) => Ok(ColumnCell::Present(value_ref_to_string(value))),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Ok(ColumnCell::Absent),
        Err(e) => Err(format!(
            "changeset {side:?} value read failed for column {col}: {e}"
        )),
    }
}

/// Render a changeset/column [`ValueRef`] as an owned `String`, or `None` for
/// SQL NULL. Mirrors `sqlite3_value_text`: text and blob bytes become a string
/// (lossy on invalid UTF-8), and integers/reals their decimal text — so the
/// `_updated_at` row-arbitration comparison and blob-plan column reads see the same strings the
/// raw FFI path (gate.rs) produces.
///
/// Synced columns coven reads through here — `_updated_at`, gate columns, FK and
/// blob-plan columns — are expected to be TEXT, INTEGER, or BLOB, never REAL. The
/// REAL arm exists only so a stray float doesn't silently become `None`; it
/// renders a faithful (round-tripping) decimal that always shows it is a float
/// (a trailing `.0` when there is no fractional part or exponent), matching
/// SQLite's REAL→text on whole numbers and simple decimals rather than diverging
/// into Rust's integer-looking `f64::to_string` (`1.0` → `"1"`). It does not
/// reproduce SQLite's exact scientific-notation threshold or 17th-digit rounding
/// — there is no live impact, since no synced column is REAL.
pub(crate) fn value_ref_to_string(v: ValueRef<'_>) -> Option<String> {
    match v {
        ValueRef::Null => None,
        ValueRef::Integer(i) => Some(i.to_string()),
        ValueRef::Real(f) => Some(real_to_sqlite_text(f)),
        ValueRef::Text(t) | ValueRef::Blob(t) => Some(String::from_utf8_lossy(t).into_owned()),
    }
}

/// Render a finite `f64` as a faithful decimal that always reads as a float, the
/// way SQLite's REAL→text does for the common cases: a whole number keeps a
/// trailing `.0` (`1.0` → `"1.0"`, not Rust's `"1"`), everything else is the
/// shortest round-tripping decimal. Non-finite values render as SQLite spells
/// them (`Inf`/`-Inf`); NaN cannot reach a well-formed synced column and renders
/// empty rather than panicking.
fn real_to_sqlite_text(f: f64) -> String {
    if f.is_nan() {
        return String::new();
    }
    if f.is_infinite() {
        return if f < 0.0 {
            "-Inf".to_string()
        } else {
            "Inf".to_string()
        };
    }
    let s = f.to_string();
    // Rust's shortest round-trip prints whole numbers as integers (`1`, `100`);
    // SQLite always marks a float, so append `.0` when there is neither a decimal
    // point nor an exponent.
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s
    } else {
        format!("{s}.0")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The REAL arm must render a faithful float: it round-trips back to the same
    /// `f64`, always reads as a float (has a `.` or exponent), and matches SQLite's
    /// `CAST(real AS TEXT)` on whole numbers and simple decimals — the cases that
    /// would otherwise diverge via Rust's integer-looking `f64::to_string`. SQLite
    /// is the ground truth the gate's raw-FFI `sqlite3_value_text` path uses.
    #[test]
    fn real_renders_as_a_faithful_float() {
        let conn = rusqlite::Connection::open_in_memory().expect("open");
        // Cases SQLite renders identically to our shortest-round-trip-plus-`.0`.
        for &f in &[0.0_f64, 1.0, 1.5, -2.25, 0.1, 123456.789, 1.0e6, 100.0, 0.5] {
            let sqlite_text: String = conn
                .query_row("SELECT CAST(? AS TEXT)", [f], |r| r.get(0))
                .expect("cast");
            let ours = real_to_sqlite_text(f);
            assert_eq!(
                ours, sqlite_text,
                "REAL {f} rendered {ours:?}, SQLite renders {sqlite_text:?}",
            );
        }

        // The general invariant: round-trips and reads as a float, even where the
        // exact spelling differs from SQLite (scientific threshold, 17th digit).
        for &f in &[1.234567890123457_f64, 1.0e-7, 9_999_999_999_999.0, -42.0] {
            let ours = real_to_sqlite_text(f);
            assert!(
                ours.contains('.') || ours.contains('e') || ours.contains('E'),
                "REAL {f} rendered {ours:?} which doesn't read as a float",
            );
            assert_eq!(
                ours.parse::<f64>().expect("parses back"),
                f,
                "REAL {f} rendered {ours:?} which doesn't round-trip",
            );
        }
    }
}
