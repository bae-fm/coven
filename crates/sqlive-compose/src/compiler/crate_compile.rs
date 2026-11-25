//! The compile of a whole crate: every call, in the order the spreads allow.

use std::collections::{BTreeMap, BTreeSet};

use super::error::{Cause, CompileError, Location};
use super::header::Header;
use super::source::{Call, Walked};
use super::spread::{spread_paths, Registry};
use super::unit::{self, fragment_path};
use super::{model, source, Input, Output};

/// Compile the crate `input` describes, or return every error.
pub fn compile_crate(input: Input<'_>) -> Result<Output, Vec<CompileError>> {
    let walked = source::walk(input.source_reader)?;
    let setup_error =
        |cause: Cause| vec![CompileError { location: None, concerns: String::new(), cause }];
    let probe = input.setup.probe().map_err(|error| setup_error(Cause::Setup(error)))?;
    let mut registry = Registry::new(&input.crate_name);
    for text in &input.manifests {
        let manifest: model::Manifest =
            serde_json::from_str(text).map_err(|error| setup_error(Cause::ManifestRead(error)))?;
        manifest.fragments.into_iter().for_each(|record| registry.add(record));
    }
    let mut errors = Vec::new();
    let mut calls = Vec::new();
    let mut names: BTreeMap<String, Location> = BTreeMap::new();
    for call in &walked.calls {
        match &call.header {
            Ok(header) => match names.get(&header.name) {
                Some(other) => errors.push(error_of(
                    call,
                    &header.name,
                    Cause::DuplicateName { name: header.name.clone(), other: other.clone() },
                )),
                None => {
                    names.insert(header.name.clone(), call.location.clone());
                    calls.push((call, header));
                }
            },
            Err(error) => {
                errors.push(error_of(call, &call.name, Cause::Header(error.clone())));
            }
        }
    }
    let order = order(&input.crate_name, &registry, &calls, &mut errors);
    let mut files = Vec::new();
    let mut manifest = model::Manifest::default();
    let mut warnings = Vec::new();
    let mut failed: BTreeSet<usize> = order.failed;
    for &position in &order.compile {
        let (call, header) = calls[position];
        let waits_on_failure =
            order.dependencies[position].iter().any(|dependency| failed.contains(dependency));
        if waits_on_failure {
            failed.insert(position);
            continue;
        }
        let context =
            unit::Context { crate_name: &input.crate_name, probe: &probe, registry: &registry };
        match unit::compile(&context, call, header) {
            Ok(compiled) => {
                files.push((format!("{}.rs", header.name), compiled.code));
                if let Some(record) = compiled.record {
                    registry.add(record.clone());
                    manifest.fragments.push(record);
                }
                warnings.extend(compiled.warnings);
            }
            Err(error) => {
                errors.push(error);
                failed.insert(position);
            }
        }
    }
    if !errors.is_empty() {
        errors.sort_by_key(|error| {
            error.location.as_ref().map(|at| (at.file.clone(), at.line, at.column))
        });
        return Err(errors);
    }
    let manifest = serde_json::to_string_pretty(&manifest)
        .map_err(|error| setup_error(Cause::Manifest(error)))?;
    Ok(Output { files, manifest, read: walked_read(walked), warnings })
}

fn walked_read(walked: Walked) -> Vec<String> {
    walked.read
}

fn error_of(call: &Call, concerns: &str, cause: Cause) -> CompileError {
    CompileError { location: Some(call.location.clone()), concerns: concerns.to_owned(), cause }
}

/// The order to compile the calls in.
struct Order {
    /// The calls' positions: fragments after the fragments they spread, then
    /// reads.
    compile: Vec<usize>,
    /// The positions of the own fragments that each call spreads.
    dependencies: Vec<Vec<usize>>,
    /// The calls that are in a circle of spreads, already reported.
    failed: BTreeSet<usize>,
}

fn order(
    crate_name: &str,
    registry: &Registry,
    calls: &[(&Call, &Header)],
    errors: &mut Vec<CompileError>,
) -> Order {
    let own: BTreeMap<String, usize> = calls
        .iter()
        .enumerate()
        .filter(|(_, (_, header))| !header.is_read())
        .map(|(position, (call, header))| {
            (fragment_path(crate_name, &call.module, &header.name), position)
        })
        .collect();
    let dependencies: Vec<Vec<usize>> = calls
        .iter()
        .map(|(_, header)| {
            spread_paths(&header.sql)
                .iter()
                .filter_map(|path| own.get(&registry.key(path)).copied())
                .collect()
        })
        .collect();
    let mut order = Order { compile: Vec::new(), dependencies, failed: BTreeSet::new() };
    let mut state = vec![0u8; calls.len()];
    for position in 0..calls.len() {
        let mut stack = Vec::new();
        visit(position, calls, crate_name, &mut state, &mut stack, &mut order, errors);
    }
    // A read is compiled after every fragment, in the order it is written.
    let (fragments, reads): (Vec<usize>, Vec<usize>) =
        order.compile.iter().partition(|&&position| !calls[position].1.is_read());
    order.compile = fragments.into_iter().chain(reads).collect();
    order
}

/// Put `position` after the fragments it spreads, reporting a circle.
fn visit(
    position: usize,
    calls: &[(&Call, &Header)],
    crate_name: &str,
    state: &mut [u8],
    stack: &mut Vec<usize>,
    order: &mut Order,
    errors: &mut Vec<CompileError>,
) {
    match state[position] {
        2 => return,
        1 => {
            let start = stack.iter().position(|&member| member == position).unwrap_or(0);
            let path = |member: usize| {
                let (call, header) = calls[member];
                fragment_path(crate_name, &call.module, &header.name)
            };
            let mut chain: Vec<String> =
                stack[start..].iter().map(|&member| path(member)).collect();
            chain.push(path(position));
            let (call, header) = calls[position];
            errors.push(error_of(
                call,
                &header.name,
                Cause::Cycle { first: path(position), chain },
            ));
            order.failed.extend(stack[start..].iter().copied());
            return;
        }
        _ => {}
    }
    state[position] = 1;
    stack.push(position);
    for dependency in order.dependencies[position].clone() {
        visit(dependency, calls, crate_name, state, stack, order, errors);
    }
    stack.pop();
    state[position] = 2;
    order.compile.push(position);
}

#[cfg(test)]
#[path = "crate_compile_tests.rs"]
mod tests;
