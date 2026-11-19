//! The read rule: a read does no work SQL does and keeps no state between
//! reads. This module runs the shape checks (`row_work.rs`) over the code
//! `read_code.rs` finds and compares what they find with the policy's two
//! lists.
//!
//! `frozen` lists the violations the workspace has today and `allowed` the
//! findings that are not a read's work: code that works on values that are
//! not rows, and a function that only a live query's `process` runs (a row
//! naming `process`), which may only sort, filter, group, count, pick or
//! join among the rows of the one result that `process` passes it. The check
//! keys a row by file, item and shape, and the row's count is exact: when
//! more findings fall under the key than the count, the check reports them
//! as findings, and when fewer do, it reports the count to lower the row to.
//! The check reports a finding that no row covers as it is.

use std::collections::BTreeMap;

use crate::counters::{counters, request_bumps};
use crate::crate_code::CrateCode;
use crate::cutoffs::cutoffs;
use crate::finding::Finding;
use crate::kept_state::kept_state;
use crate::keyed_statements::keyed_statements;
use crate::read_code::ReadCode;
use crate::row_work::{row_work, Hit};
use owner_construction_policy::{Policy, ReadShapes, Shape, FILE_NAME};

/// How every remedy ends.
const ENDING: &str = "Frozen rows only shrink: never add one or raise a count. A use over values that are not a read's rows goes in `allowed` with what they are.";

pub(crate) fn find_read_shape_violations(code: &CrateCode, policy: &Policy) -> Vec<Finding> {
    let Some(shapes) = &policy.read_shapes else {
        return Vec::new();
    };
    let read = ReadCode::collect(&code.expanded, policy, shapes);
    let mut keyed: BTreeMap<(String, String, Shape), Vec<Hit>> = BTreeMap::new();
    let hits = row_work(&read)
        .into_iter()
        .chain(kept_state(&read, &code.expanded, shapes))
        .chain(counters(code))
        .chain(request_bumps(&read, &code.expanded, shapes))
        .chain(cutoffs(&read, &code.expanded))
        .chain(keyed_statements(&read));
    for hit in hits {
        keyed.entry((hit.path.clone(), hit.item.clone(), hit.shape)).or_default().push(hit);
    }
    let rows = shapes.listed();
    let mut findings = process_rows(&read, shapes);
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
            _ => {
                findings.extend(hits.iter().map(|hit| {
                    Finding::new(
                        &hit.path,
                        hit.line,
                        format!("{} {} ({})", hit.item, hit.what, hit.shape.id()),
                        remedy(&shapes.rule, hit.shape),
                    )
                }));
                // Findings that read alike print once, so the key's count,
                // which a row must state, is its own line.
                let listed = row.map_or(String::new(), |row| {
                    format!(", and its row lists {}", row.count)
                });
                if hits.len() > 1 || row.is_some() {
                    findings.push(Finding::new(
                        &key.0,
                        hits[0].line,
                        format!("{} has {} findings ({}){listed}", key.1, hits.len(), key.2.id()),
                        remedy(&shapes.rule, key.2),
                    ));
                }
            }
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

/// Checks the `allowed` rows that name a `process` function against what the
/// rule allows: only a live query's `process` runs the function, and the
/// finding sorts, filters, groups, counts or picks among one result's rows.
fn process_rows(read: &ReadCode<'_>, shapes: &ReadShapes) -> Vec<Finding> {
    let mut findings = Vec::new();
    for row in shapes.allowed.iter().filter(|row| row.process.is_some()) {
        let named = format!("read_shapes.allowed: {}: {} ({})", row.path, row.item, row.shape.id());
        if !row.shape.shapes_display() {
            findings.push(Finding::new(
                FILE_NAME,
                1,
                format!(
                    "{named} names a `process` function, but a function that `process` runs never makes a finding of this shape: freeze the finding instead"
                ),
                remedy(&shapes.rule, row.shape),
            ));
        }
        if !read.is_process_only(&row.path, &row.item) {
            findings.push(Finding::new(
                FILE_NAME,
                1,
                format!(
                    "{named} names a `process` function, but read code other than a live query's `process` runs it too: drop `process` if its values are not rows, or freeze the finding"
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
        Shape::KeyedStatement => {
            "one statement reads both: join the second statement's tables into the first instead of keying it by what the first returned."
        }
        Shape::Cutoff => {
            "one query, one path: when the one statement is slow, fix it in SQL (the query, an index, a device-local table its triggers keep) and measure."
        }
        Shape::Counter | Shape::ResetCounter | Shape::RequestBump => {
            "a live query reruns because what it read changed: drop the counter and let the query read the rows whose change it follows."
        }
        Shape::KeptState => {
            "a read starts from the database every time: drop the state and read it in SQL; a value no table holds that changes many times a second travels on a keyed stream."
        }
        Shape::Sort
        | Shape::Filter
        | Shape::Group
        | Shape::Aggregate
        | Shape::Pick
        | Shape::Join
        | Shape::LengthBranch
        | Shape::DropRow => {
            "the statement filters, sorts, joins, groups, counts and sums, in the same query or its own live query beside it; a read returns what the screen shows."
        }
    };
    format!("{rule}: {instead} {ENDING}")
}

#[cfg(test)]
#[path = "read_shapes_tests.rs"]
mod tests;
