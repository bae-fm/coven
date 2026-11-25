//! The index finds a change's candidates by its table and by the values its
//! row holds in each key's column, before and after the change.

use super::*;
use crate::read_set::TableRead;
use crate::row_scope::RowScope;
use std::sync::Arc;

fn read(table: &str, scope: RowScope) -> TableRead {
    TableRead {
        table: table.into(),
        columns: ["release".to_owned(), "title".to_owned()].into(),
        scope,
        inserted_only: false,
        host: false,
    }
}

fn text(value: &str) -> Value {
    Value::Text(value.as_bytes().to_vec())
}

fn releases(values: &[Value]) -> RowScope {
    RowScope::equal("release".into(), values.to_vec())
}

/// A row of `tracks` whose `release` holds `release`, or that the write
/// didn't record a release for.
fn row(release: Option<Value>) -> RowImage {
    RowImage::new(Arc::from(["release".to_owned()]), vec![release])
}

fn change(old: Option<RowImage>, new: Option<RowImage>) -> RowChange {
    RowChange { table: "tracks".into(), column: "release".into(), rows: Some((old, new)) }
}

fn found(index: &ReadIndex, changes: &[RowChange]) -> Vec<(SubscriptionId, Vec<usize>)> {
    index.candidates(changes).into_iter().collect()
}

/// A change reaches the subscriptions filed under the value its row holds
/// before or after it, and those whose scope has no key; never one filed
/// under another value or another table.
#[test]
fn a_change_reaches_the_values_its_row_holds() {
    let mut index = ReadIndex::default();
    index.insert(1, &vec![read("tracks", releases(&[text("a"), text("b")]))]);
    index.insert(
        2,
        &vec![read(
            "tracks",
            RowScope::Compare {
                column: "release".into(),
                value: text("c"),
                ordering: Ordering::Equal,
                inclusive: false,
            },
        )],
    );
    index.insert(
        3,
        &vec![read(
            "tracks",
            RowScope::Compare {
                column: "title".into(),
                value: text("m"),
                ordering: Ordering::Greater,
                inclusive: false,
            },
        )],
    );
    index.insert(4, &vec![read("releases", releases(&[text("a")]))]);
    index.insert(5, &vec![read("tracks", releases(&[text("d")]))]);
    let changes = [
        change(Some(row(Some(text("b")))), Some(row(Some(text("c"))))),
        change(None, Some(row(Some(text("e"))))),
    ];
    assert_eq!(found(&index, &changes), [(1, vec![0]), (2, vec![0]), (3, vec![0, 1])]);
}

/// An integer and a real that compare equal are one key, as one equality
/// passes both.
#[test]
fn an_integer_and_an_equal_real_are_one_key() {
    let mut index = ReadIndex::default();
    index.insert(1, &vec![read("tracks", releases(&[Value::Integer(7)]))]);
    index.insert(2, &vec![read("tracks", releases(&[Value::Real(7.5)]))]);
    let changes = [change(None, Some(row(Some(Value::Real(7.0)))))];
    assert_eq!(found(&index, &changes), [(1, vec![0])]);
}

/// A NULL equals no key's value, so it reaches no keyed subscription; a
/// value the write didn't record passes every scope, so it reaches each one
/// keyed on the column; a change no row names reaches every reader of its
/// table.
#[test]
fn null_unrecorded_and_unnamed_rows() {
    let mut index = ReadIndex::default();
    index.insert(1, &vec![read("tracks", releases(&[text("a")]))]);
    index.insert(2, &vec![read("tracks", releases(&[text("b")]))]);
    assert_eq!(found(&index, &[change(None, Some(row(Some(Value::Null))))]), []);
    assert_eq!(found(&index, &[change(None, Some(row(None)))]), [(1, vec![0]), (2, vec![0])]);
    let unnamed = RowChange { table: "tracks".into(), column: "release".into(), rows: None };
    assert_eq!(found(&index, &[unnamed]), [(1, vec![0]), (2, vec![0])]);
}

/// Taking a subscription's reads out leaves the others filed, and taking
/// every one out leaves the index empty.
#[test]
fn removed_reads_reach_nothing() {
    let mut index = ReadIndex::default();
    let first = vec![read("tracks", releases(&[text("a")])), read("tracks", RowScope::All)];
    let second = vec![read("tracks", releases(&[text("a"), text("b")]))];
    index.insert(1, &first);
    index.insert(2, &second);
    let changes = [change(None, Some(row(Some(text("a")))))];
    index.remove(1, &first);
    assert_eq!(found(&index, &changes), [(2, vec![0])]);
    index.remove(2, &second);
    assert_eq!(found(&index, &changes), []);
    assert!(index.tables.is_empty());
}
