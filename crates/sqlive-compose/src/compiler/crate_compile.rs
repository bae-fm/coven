//! The compile of a whole crate: every call, in the order the spreads allow.

use std::collections::BTreeMap;

use super::error::{Cause, CompileError, Location};
use super::{model, source, unit, Input, Output};

/// Compile the crate `input` describes, or return every error.
pub fn compile_crate(input: Input<'_>) -> Result<Output, Vec<CompileError>> {
    let walked = source::walk(input.source_reader)?;
    let probe = input.setup.probe().map_err(|error| {
        vec![CompileError { location: None, concerns: String::new(), cause: Cause::Setup(error) }]
    })?;
    let context = unit::Context { crate_name: &input.crate_name, probe: &probe };
    let mut files = Vec::new();
    let mut manifest = model::Manifest::default();
    let mut warnings = Vec::new();
    let mut errors = Vec::new();
    let mut names: BTreeMap<String, Location> = BTreeMap::new();
    for call in &walked.calls {
        let header = match &call.header {
            Ok(header) => header,
            Err(error) => {
                errors.push(CompileError {
                    location: Some(call.location.clone()),
                    concerns: call.name.clone(),
                    cause: Cause::Header(error.clone()),
                });
                continue;
            }
        };
        if let Some(other) = names.get(&header.name) {
            errors.push(CompileError {
                location: Some(call.location.clone()),
                concerns: header.name.clone(),
                cause: Cause::DuplicateName { name: header.name.clone(), other: other.clone() },
            });
            continue;
        }
        names.insert(header.name.clone(), call.location.clone());
        match unit::compile(&context, call, header) {
            Ok(compiled) => {
                files.push((format!("{}.rs", header.name), compiled.code));
                manifest.fragments.extend(compiled.record);
                warnings.extend(compiled.warnings);
            }
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let manifest = serde_json::to_string_pretty(&manifest).map_err(|error| {
        vec![CompileError {
            location: None,
            concerns: String::new(),
            cause: Cause::Manifest(error),
        }]
    })?;
    Ok(Output { files, manifest, read: walked.read, warnings })
}

#[cfg(test)]
#[path = "crate_compile_tests.rs"]
mod tests;
