//! The workspace's declared types and the types each retains through its
//! fields, as the read rule asks them (`read_code.rs`, `kept_state.rs`):
//! which types hold a live query or a value stream, and which keep state. An
//! alias retains what its target does. The fields only test support compiles
//! are left out.

use std::collections::{BTreeMap, BTreeSet};

use syn::visit_mut::{self, VisitMut};

use crate::policy::{Policy, ReadShapes};
use crate::syntax::{collect_declared_types, is_test_support_only, RustFile, StructInfo};
use crate::type_identity::TypeIdentity;

/// The workspace's declared types and what they retain.
pub(crate) struct Graph<'a> {
    pub(crate) types: BTreeMap<String, StructInfo>,
    pub(crate) shapes: &'a ReadShapes,
}

impl<'a> Graph<'a> {
    pub(crate) fn of(files: &[RustFile], policy: &Policy, shapes: &'a ReadShapes) -> Self {
        let files = &without_test_support_fields(files, policy.test_support_features());
        let identity = TypeIdentity::of(files, &policy.phantom_type_arguments);
        Graph {
            types: collect_declared_types(files, &policy.shared_pointer_types, &identity),
            shapes,
        }
    }

    /// Whether a type is a live query or a value stream, which the graph
    /// names but does not look inside: its locks are request plumbing.
    pub(crate) fn is_request(&self, name: &str) -> bool {
        self.shapes.live_query_types.contains(name) || self.shapes.value_stream_types.contains(name)
    }

    /// The names and the types they retain, through the workspace's types;
    /// a live query or value stream is named but not looked inside.
    pub(crate) fn reach(&self, names: &BTreeSet<String>) -> BTreeSet<String> {
        let mut reached = BTreeSet::new();
        let mut pending = names.iter().cloned().collect::<Vec<_>>();
        while let Some(name) = pending.pop() {
            if !reached.insert(name.clone()) || self.is_request(&name) {
                continue;
            }
            if let Some(info) = self.types.get(&name) {
                pending.extend(info.field_types.iter().cloned());
            }
        }
        reached
    }

    /// The first kept-state type these names retain.
    pub(crate) fn kept(&self, names: &BTreeSet<String>) -> Option<String> {
        self.reach(names).into_iter().find(|name| self.shapes.kept_state_types.contains(name))
    }

    /// Whether these names retain a live query or value stream.
    pub(crate) fn retains_request(&self, names: &BTreeSet<String>) -> bool {
        self.reach(names).iter().any(|name| self.is_request(name))
    }

    pub(crate) fn retains_live_query(&self, names: &BTreeSet<String>) -> bool {
        self.reach(names).iter().any(|name| self.shapes.live_query_types.contains(name))
    }

    /// Whether a type, which is no live query or value stream itself, has a
    /// field that retains a live query: it hands on what the query reads.
    pub(crate) fn holds_live_query(&self, name: &str) -> bool {
        !self.is_request(name)
            && self.types.get(name).is_some_and(|info| {
                info.named_fields.values().any(|names| self.retains_live_query(names))
            })
    }
}

/// The files without the fields only test support compiles.
fn without_test_support_fields(files: &[RustFile], features: &[String]) -> Vec<RustFile> {
    struct Gated<'a>(&'a [String]);
    impl VisitMut for Gated<'_> {
        fn visit_fields_named_mut(&mut self, fields: &mut syn::FieldsNamed) {
            fields.named = std::mem::take(&mut fields.named)
                .into_iter()
                .filter(|field| !is_test_support_only(&field.attrs, self.0))
                .collect();
            visit_mut::visit_fields_named_mut(self, fields);
        }
    }
    files
        .iter()
        .map(|file| {
            let mut file = file.clone();
            Gated(features).visit_file_mut(&mut file.syntax);
            file
        })
        .collect()
}
