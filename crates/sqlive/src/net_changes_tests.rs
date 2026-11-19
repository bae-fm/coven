use super::*;

/// The image of a row of `notes(id, title)` holding both columns.
fn image(id: i64, title: &str) -> RowImage {
    RowImage::new(
        Arc::from(["id".to_owned(), "title".to_owned()]),
        vec![Some(SqlValue::Integer(id)), Some(SqlValue::Text(title.into()))],
    )
}

fn text(title: &str) -> Option<SqlValue> {
    Some(SqlValue::Text(title.into()))
}

/// An operation on row `id` of `notes`, with `title` changed as `columns`
/// says.
fn operation(
    id: i64,
    before: Option<&str>,
    after: Option<&str>,
    columns: Vec<ColumnChange>,
    generated: &[&str],
) -> RowOperation {
    RowOperation {
        table: "notes".to_owned(),
        key: vec![SqlValue::Integer(id)],
        before: before.map(|title| image(id, title)),
        after: after.map(|title| image(id, title)),
        columns,
        generated: generated.iter().map(|name| (*name).to_owned()).collect(),
    }
}

fn title(before: Option<SqlValue>, after: Option<SqlValue>) -> Vec<ColumnChange> {
    vec![ColumnChange { column: "title".to_owned(), before, after }]
}

fn finish(operations: Vec<RowOperation>) -> Vec<RowChange> {
    let mut captured = Captured::default();
    for operation in operations {
        captured.record(operation);
    }
    captured.finish()
}

/// Each change as its column and its rows' titles.
fn shown(changes: &[RowChange]) -> Vec<(String, Option<String>, Option<String>)> {
    let title = |image: &Option<RowImage>| {
        image.as_ref().and_then(|image| match image.get("title") {
            Some(SqlValue::Text(title)) => Some(String::from_utf8(title.clone()).unwrap()),
            _ => None,
        })
    };
    changes
        .iter()
        .map(|change| {
            let (before, after) = change.rows.as_ref().expect("a row change names its row");
            (change.column.clone(), title(before), title(after))
        })
        .collect()
}

#[test]
fn an_insert_and_a_delete_of_one_row_change_nothing() {
    let changes = finish(vec![
        operation(1, None, Some("a"), title(None, text("a")), &[]),
        operation(1, Some("a"), None, title(text("a"), None), &[]),
    ]);
    assert!(changes.is_empty());
}

#[test]
fn an_update_and_an_update_back_change_nothing() {
    let changes = finish(vec![
        operation(1, Some("a"), Some("b"), title(text("a"), text("b")), &["folded"]),
        operation(1, Some("b"), Some("a"), title(text("b"), text("a")), &["folded"]),
    ]);
    assert!(changes.is_empty());
}

#[test]
fn a_delete_and_an_insert_of_the_same_row_change_nothing() {
    let changes = finish(vec![
        operation(1, Some("a"), None, title(text("a"), None), &[]),
        operation(1, None, Some("a"), title(None, text("a")), &[]),
    ]);
    assert!(changes.is_empty());
}

#[test]
fn a_delete_and_an_insert_of_other_values_change_the_columns_that_differ() {
    let changes = finish(vec![
        operation(1, Some("a"), None, title(text("a"), None), &[]),
        operation(1, None, Some("b"), title(None, text("b")), &[]),
    ]);
    assert_eq!(shown(&changes), [("title".to_owned(), Some("a".to_owned()), Some("b".to_owned()))]);
}

/// A row changed and changed again is one change, from the row before the
/// first to the row after the last, with its generated columns.
#[test]
fn a_row_changed_twice_is_one_change_from_its_first_row_to_its_last() {
    let changes = finish(vec![
        operation(1, Some("a"), Some("b"), title(text("a"), text("b")), &["folded"]),
        operation(1, Some("b"), Some("c"), title(text("b"), text("c")), &["folded"]),
    ]);
    assert_eq!(
        shown(&changes),
        [
            ("title".to_owned(), Some("a".to_owned()), Some("c".to_owned())),
            ("folded".to_owned(), Some("a".to_owned()), Some("c".to_owned())),
        ]
    );
}

/// A column whose values the transaction didn't keep, as one no live query
/// reads, counts as changed once an operation changed it.
#[test]
fn a_column_not_kept_counts_as_changed() {
    let changes = finish(vec![
        operation(1, Some("a"), Some("a"), title(None, None), &[]),
        operation(1, Some("a"), Some("a"), title(None, None), &[]),
    ]);
    assert_eq!(shown(&changes), [("title".to_owned(), Some("a".to_owned()), Some("a".to_owned()))]);
}

/// An integer and a real that compare equal are different values, which a
/// query can tell apart.
#[test]
fn an_integer_set_to_an_equal_real_changes() {
    let changes = finish(vec![operation(
        1,
        Some("a"),
        Some("a"),
        title(Some(SqlValue::Integer(1)), Some(SqlValue::Real(1.0))),
        &[],
    )]);
    assert_eq!(changes.len(), 1);
}

/// What a later capture records of a row folds onto what an earlier one
/// recorded, as a host's phase onto the app's.
#[test]
fn a_later_capture_folds_onto_an_earlier_ones_rows() {
    let mut app = Captured::default();
    app.record(operation(1, Some("a"), Some("b"), title(text("a"), text("b")), &[]));
    app.record(operation(2, None, Some("x"), title(None, text("x")), &[]));
    let mut host = Captured::default();
    host.record(operation(1, Some("b"), Some("a"), title(text("b"), text("a")), &[]));
    host.record(operation(2, Some("x"), None, title(text("x"), None), &[]));
    app.append(host);
    assert!(app.finish().is_empty());
}

/// Forgetting a table's rows leaves the other tables' rows and the order of
/// those left.
#[test]
fn forgetting_a_table_keeps_the_others_rows() {
    let mut rows = NetRows::default();
    rows.record(operation(1, None, Some("a"), title(None, text("a")), &[]));
    rows.record(RowOperation {
        table: "tags".to_owned(),
        ..operation(1, None, Some("t"), Vec::new(), &[])
    });
    rows.record(operation(2, None, Some("b"), title(None, text("b")), &[]));
    assert_eq!(rows.count("notes"), 2);
    rows.forget("tags");
    assert_eq!(rows.count("tags"), 0);
    rows.record(operation(1, Some("a"), None, title(text("a"), None), &[]));
    let mut changes = Vec::new();
    rows.finish(&mut changes);
    assert_eq!(shown(&changes), [(String::new(), None, Some("b".to_owned()))]);
}
