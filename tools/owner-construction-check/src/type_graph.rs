//! The field graph of the workspace's declared types and the names its types
//! go by (`type_identity.rs`), which the domain-owner rule and the component
//! bundles read.

use crate::syntax::{collect_declared_types, RustFile, StructInfo};
use crate::type_identity::TypeIdentity;
use owner_construction_policy::Policy;
use std::collections::BTreeMap;

pub(crate) struct TypeGraph {
    pub(crate) identity: TypeIdentity,
    pub(crate) types: BTreeMap<String, StructInfo>,
}

impl TypeGraph {
    pub(crate) fn collect(files: &[RustFile], policy: &Policy) -> Self {
        let identity = TypeIdentity::of(files);
        let types = collect_declared_types(files, &policy.shared_pointer_types, &identity);
        Self { identity, types }
    }
}
