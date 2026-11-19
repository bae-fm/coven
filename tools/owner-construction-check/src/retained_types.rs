//! The workspace's declared types and the types each retains through its
//! fields, for the read rule's questions (`read_code.rs`, `kept_state.rs`):
//! which types hold a live query or a value stream, and which keep state. An
//! alias retains what its target does.

use std::collections::{BTreeMap, BTreeSet};

use crate::syntax::{collect_declared_types, RustFile, StructInfo};
use crate::type_identity::TypeIdentity;
use owner_construction_policy::{Policy, ReadShapes};

/// The workspace's declared types and what they retain.
pub(crate) struct Graph<'a> {
    pub(crate) types: BTreeMap<String, StructInfo>,
    pub(crate) shapes: &'a ReadShapes,
}

impl<'a> Graph<'a> {
    pub(crate) fn of(files: &[RustFile], policy: &Policy, shapes: &'a ReadShapes) -> Self {
        let identity = TypeIdentity::of(files, &policy.phantom_type_arguments);
        Graph {
            types: collect_declared_types(files, &policy.shared_pointer_types, &identity),
            shapes,
        }
    }

    /// Whether a type is a live query or a value stream. The graph names such
    /// a type but does not look inside it: its locks belong to how the query
    /// or stream delivers its values.
    pub(crate) fn is_request(&self, name: &str) -> bool {
        self.shapes.live_query_types.contains(name) || self.shapes.value_stream_types.contains(name)
    }

    /// The names and the types they retain, through the workspace's types.
    /// The graph names a live query or value stream but does not look inside
    /// it.
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

    /// Whether a type, which is no live query or value stream itself, has a
    /// field that retains a live query or a value stream: it hands on what
    /// the query reads or the stream delivers.
    pub(crate) fn holds_request(&self, name: &str) -> bool {
        !self.is_request(name)
            && self.types.get(name).is_some_and(|info| {
                info.named_fields.values().any(|names| self.retains_request(names))
            })
    }
}
