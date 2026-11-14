//! The read rule: a read does no work SQL does and keeps no state between
//! reads. This module runs the shapes over the code `read_code.rs` finds
//! (`row_work.rs`) and holds what they find against the policy's two lists.
//!
//! `frozen` lists the violations the workspace has today and `allowed` the
//! findings that are not a read's work. A row is keyed by file, item and
//! shape, and its count is exact: more findings under the key than the count
//! fail as findings, fewer fail with the count to lower it to. A finding no
//! row covers fails as it is.

use std::collections::BTreeMap;

use crate::crate_code::CrateCode;
use crate::finding::Finding;
use crate::kept_state::kept_state;
use crate::policy::{Policy, Shape, FILE_NAME};
use crate::read_code::ReadCode;
use crate::row_work::{row_work, Hit};

/// How every remedy ends.
const ENDING: &str = "Frozen rows only shrink: never add one or raise a count. A use over values that are not a read's rows goes in `allowed` with what they are.";

pub(crate) fn find_read_shape_violations(code: &CrateCode, policy: &Policy) -> Vec<Finding> {
    let Some(shapes) = &policy.read_shapes else {
        return Vec::new();
    };
    let read = ReadCode::collect(&code.expanded, policy, shapes);
    let mut keyed: BTreeMap<(String, String, Shape), Vec<Hit>> = BTreeMap::new();
    let hits = row_work(&read).into_iter().chain(kept_state(&read, &code.expanded, policy, shapes));
    for hit in hits {
        keyed.entry((hit.path.clone(), hit.item.clone(), hit.shape)).or_default().push(hit);
    }
    let rows = shapes.listed();
    let mut findings = Vec::new();
    for (key, hits) in &keyed {
        let row =
            rows.iter().find(|row| (row.path, row.item, row.shape) == (&key.0, &key.1, key.2));
        match row {
            Some(row) if hits.len() < row.count => findings.push(Finding::new(
                FILE_NAME,
                1,
                format!(
                    "read_shapes.{}: {}: {} ({}) lists {} findings but the code has {}: lower the count to {}",
                    row.list.name(),
                    key.0,
                    key.1,
                    key.2.id(),
                    row.count,
                    hits.len(),
                    hits.len()
                ),
                remedy(&shapes.rule, key.2),
            )),
            Some(row) if hits.len() == row.count => {}
            _ => findings.extend(hits.iter().map(|hit| {
                Finding::new(
                    &hit.path,
                    hit.line,
                    format!("{} {} ({})", hit.item, hit.what, hit.shape.id()),
                    remedy(&shapes.rule, hit.shape),
                )
            })),
        }
    }
    for row in &rows {
        if !keyed.contains_key(&(row.path.to_string(), row.item.to_string(), row.shape)) {
            findings.push(Finding::new(
                FILE_NAME,
                1,
                format!(
                    "read_shapes.{}: {}: {} ({}) lists {} findings but the code has none: remove the row",
                    row.list.name(),
                    row.path,
                    row.item,
                    row.shape.id(),
                    row.count
                ),
                remedy(&shapes.rule, row.shape),
            ));
        }
    }
    findings
}

/// What to do instead of `shape`, naming the rule.
fn remedy(rule: &str, shape: Shape) -> String {
    let instead = match shape {
        Shape::QueryPerRow => "join in the statement instead of a query per row.",
        Shape::KeptState => {
            "a read starts from the database every time: drop the state and read it in SQL; a value no table holds that changes many times a second travels on a keyed stream."
        }
        Shape::Sort | Shape::Filter | Shape::Group | Shape::Aggregate | Shape::Pick => {
            "the statement filters, sorts, joins, groups, counts and sums, in the same query or its own live query beside it; a read returns what the screen shows."
        }
    };
    format!("{rule}: {instead} {ENDING}")
}

#[cfg(test)]
#[path = "read_shapes_tests.rs"]
mod tests;
