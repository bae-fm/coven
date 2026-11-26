//! Each capability is used directly in one place only (Appendix F.2). Outside a
//! capability's homes, naming one of its crates, writing one of its paths or
//! calling one of its methods is the violation; everything else reaches the
//! capability through the owner that holds it.
//!
//! The check is syntactic: naming the crate or the path is the violation, so a
//! new direct use fails before compilation and review. A gate's `calls` match
//! only a function that code calls or passes as a value, by the path it
//! resolves to through its file's imports (`NameResolver`), so code may name
//! a capability's types and import its modules. The check reads inside macro
//! calls too, so `format!("{}", Uuid::new_v4())` is a direct use. It reads no
//! test code (`test_code.rs`), since fixtures may assemble raw material,
//! except for a capability whose row sets `tests`: then test code outside its
//! homes, their tests and its `test_homes` may not use it directly either.
//! Test code needs no composition root, so background work starts are
//! checked in production code only.
//!
//! A capability's `frozen` rows list the direct uses a workspace has today,
//! by file and gate kind, and its `allowed` rows the matches that use no
//! capability, by the item that holds them: a method gate can't see its
//! receiver, so `metadata` also matches a value that is no path. The uses an
//! allowed row's item holds are that row's, and the rest of a file's uses of
//! a gate are its frozen row's. Each row's count is exact: when the code has
//! more uses than the count, every use under the row prints with a line
//! giving both numbers; when it has fewer, the check says to lower the count,
//! and when it has none, to remove the row. A use no row covers prints as it
//! is.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::crate_code::CrateCode;
use crate::elapsed_only::{measuring_starts, Start};
use crate::finding::Finding;
use crate::macros::{parse_macro_body, token_paths, unparsed_tokens};
use crate::name_resolution::{qualified_path, NameResolver, ResolutionError};
use crate::syntax::{flatten_use_tree, type_name, RustFile};
use owner_construction_policy::{describe_homes, Capability, Gate, Policy, FILE_NAME};

/// The rule over the crates' production code with its macros expanded, and
/// over their test code as written for the capabilities that read it.
pub(crate) fn find_capability_boundary_violations(
    code: &CrateCode,
    policy: &Policy,
) -> Vec<Finding> {
    let mut violations = BTreeSet::new();
    let mut uses = BTreeSet::new();
    let production = NameResolver::collect(&code.expanded);
    for file in &code.expanded {
        let gated = gated(policy, |capability| !capability.holds(&file.relative_path));
        let mut visitor = CapabilityBoundaryVisitor::new(file, &gated, policy, &production);
        visitor.measuring = measuring_starts(&file.syntax, &measured_patterns(&gated));
        visitor.background_starts = &policy.background_starts;
        visitor.read(file, &mut violations, &mut uses);
    }
    if policy.capabilities.iter().any(|capability| capability.tests) {
        let tests = NameResolver::collect(&code.whole);
        for file in &code.tests {
            let gated = gated(policy, |capability| {
                capability.tests && !capability.holds_tests(&file.relative_path)
            });
            CapabilityBoundaryVisitor::new(file, &gated, policy, &tests).read(
                file,
                &mut violations,
                &mut uses,
            );
        }
    }
    for capability in &policy.capabilities {
        let held = uses.iter().filter(|direct| direct.capability == capability.name);
        violations.extend(listed_findings(capability, held.collect()));
    }
    violations.into_iter().collect()
}

/// A gate's match outside its capability's homes: the capability, the file,
/// the gate's kind, the item that holds the match (`item_name`) and its line.
#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct DirectUse<'p> {
    capability: &'p str,
    path: String,
    gate: &'p str,
    item: Option<String>,
    line: usize,
}

/// How the line of a row whose count is off ends.
const ROW_REMEDY: &str = "a capability's frozen and allowed rows state exactly what the code has: lower or remove a row as its line says; a frozen row only shrinks, and an allowed row names a match that uses no capability";

/// The findings of one capability's direct uses against its rows.
fn listed_findings(capability: &Capability, uses: Vec<&DirectUse<'_>>) -> Vec<Finding> {
    let mut allowed: BTreeMap<usize, Vec<&DirectUse<'_>>> = BTreeMap::new();
    let mut unlisted: BTreeMap<(&str, &str), Vec<&DirectUse<'_>>> = BTreeMap::new();
    for direct in uses {
        let row = capability.allowed.iter().position(|row| {
            row.path == direct.path
                && row.gate == direct.gate
                && direct.item.as_ref() == Some(&row.item)
        });
        match row {
            Some(row) => allowed.entry(row).or_default().push(direct),
            None => unlisted.entry((direct.path.as_str(), direct.gate)).or_default().push(direct),
        }
    }
    let mut findings = Vec::new();
    for (index, row) in capability.allowed.iter().enumerate() {
        let named = format!("{}: {} ({})", row.path, row.item, row.gate);
        let uses = allowed.remove(&index).unwrap_or_default();
        findings.extend(counted(capability, "allowed", &named, row.count, &uses));
    }
    for row in &capability.frozen {
        let named = format!("{} ({})", row.path, row.gate);
        let uses = unlisted.remove(&(row.path.as_str(), row.gate.as_str())).unwrap_or_default();
        findings.extend(counted(capability, "frozen", &named, row.count, &uses));
    }
    findings.extend(unlisted.into_values().flatten().map(|direct| use_finding(capability, direct)));
    findings
}

/// The lines of one row, `named` by its key: none when the code has its
/// count of uses.
fn counted(
    capability: &Capability,
    list: &str,
    named: &str,
    count: usize,
    uses: &[&DirectUse<'_>],
) -> Vec<Finding> {
    let row = |has: String| {
        let message = format!(
            "capabilities.{list}: {}: {named} lists {count} findings but the code has {has}",
            capability.name
        );
        vec![Finding::new(FILE_NAME, 1, message, ROW_REMEDY)]
    };
    match uses {
        [] => row("none: remove the row".to_string()),
        _ if uses.len() < count => row(format!("{0}: lower the count to {0}", uses.len())),
        _ if uses.len() == count => Vec::new(),
        [first, ..] => {
            let mut findings =
                uses.iter().map(|direct| use_finding(capability, direct)).collect::<Vec<_>>();
            findings.push(Finding::new(
                &first.path,
                first.line,
                format!("{named} has {} findings, and its {list} row lists {count}", uses.len()),
                ROW_REMEDY,
            ));
            findings
        }
    }
}

fn use_finding(capability: &Capability, direct: &DirectUse<'_>) -> Finding {
    Finding::new(
        &direct.path,
        direct.line,
        format!(
            "{} ({}) is used directly only in {}",
            direct.gate,
            capability.name,
            describe_homes(&capability.homes)
        ),
        "reach a capability through the owner that holds it, given to you when you are built",
    )
}

/// How an allowed row names the function, method, static or const that
/// holds a match: `name`, or `Type::name` inside an `impl` or a trait.
fn item_name(owner: Option<&str>, name: &syn::Ident) -> String {
    match owner {
        Some(owner) => format!("{owner}::{name}"),
        None => name.to_string(),
    }
}

/// Every function, method, static and const these files declare, by file
/// and as `item_name` names it.
pub(crate) fn declared_items(files: &[RustFile]) -> BTreeSet<(String, String)> {
    struct Items<'a> {
        path: &'a str,
        owner: Option<String>,
        items: &'a mut BTreeSet<(String, String)>,
    }

    impl Items<'_> {
        fn declare(&mut self, name: &syn::Ident, owned: bool) {
            let owner = self.owner.as_deref().filter(|_| owned);
            self.items.insert((self.path.to_string(), item_name(owner, name)));
        }

        fn owned_by(&mut self, owner: Option<String>, visit: impl FnOnce(&mut Self)) {
            let previous = std::mem::replace(&mut self.owner, owner);
            visit(self);
            self.owner = previous;
        }
    }

    impl<'ast> Visit<'ast> for Items<'_> {
        fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
            self.declare(&node.sig.ident, false);
            visit::visit_item_fn(self, node);
        }

        fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
            self.declare(&node.ident, false);
            visit::visit_item_static(self, node);
        }

        fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
            self.declare(&node.ident, false);
            visit::visit_item_const(self, node);
        }

        fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
            let owner = type_name(&node.self_ty).unwrap_or_else(|| "<impl>".into());
            self.owned_by(Some(owner), |items| visit::visit_item_impl(items, node));
        }

        fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
            let owner = node.ident.to_string();
            self.owned_by(Some(owner), |items| visit::visit_item_trait(items, node));
        }

        fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
            self.declare(&node.sig.ident, true);
            self.owned_by(None, |items| visit::visit_impl_item_fn(items, node));
        }

        fn visit_impl_item_const(&mut self, node: &'ast syn::ImplItemConst) {
            self.declare(&node.ident, true);
            self.owned_by(None, |items| visit::visit_impl_item_const(items, node));
        }

        fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
            self.declare(&node.sig.ident, true);
            self.owned_by(None, |items| visit::visit_trait_item_fn(items, node));
        }

        fn visit_trait_item_const(&mut self, node: &'ast syn::TraitItemConst) {
            self.declare(&node.ident, true);
            self.owned_by(None, |items| visit::visit_trait_item_const(items, node));
        }

        fn visit_macro(&mut self, node: &'ast syn::Macro) {
            if let Some(body) = parse_macro_body(node) {
                body.visit(self);
            }
        }
    }

    let mut items = BTreeSet::new();
    for file in files {
        Items { path: &file.relative_path, owner: None, items: &mut items }
            .visit_file(&file.syntax);
    }
    items
}

/// The measured paths of these gates (`elapsed_only.rs`).
fn measured_patterns(gated: &[(&Capability, &Gate)]) -> Vec<Vec<String>> {
    gated.iter().flat_map(|(_, gate)| gate.measured_patterns.iter().cloned()).collect()
}

/// The gates of the capabilities `outside` picks, with their capability.
fn gated(policy: &Policy, outside: impl Fn(&Capability) -> bool) -> Vec<(&Capability, &Gate)> {
    policy
        .capabilities
        .iter()
        .filter(|capability| outside(capability))
        .flat_map(|capability| capability.gates.iter().map(move |gate| (capability, gate)))
        .collect()
}

struct CapabilityBoundaryVisitor<'a, 'p> {
    path: &'a str,
    gated: &'a [(&'p Capability, &'p Gate)],
    /// The background work starts the visitor checks: none in test code.
    background_starts: &'p [Gate],
    violations: BTreeSet<Finding>,
    uses: BTreeSet<DirectUse<'p>>,
    policy: &'p Policy,
    resolver: &'a NameResolver,
    /// The type of the `impl` block, or the trait, the visitor is in.
    current_type: String,
    /// The innermost function, method, static or const the visitor is in
    /// (`item_name`).
    item: Option<String>,
    rooted: bool,
    /// Where the calls of measured paths start whose value only measures an
    /// elapsed duration (`elapsed_only.rs`).
    measuring: BTreeSet<Start>,
}

impl<'a, 'p> CapabilityBoundaryVisitor<'a, 'p> {
    fn new(
        file: &'a RustFile,
        gated: &'a [(&'p Capability, &'p Gate)],
        policy: &'p Policy,
        resolver: &'a NameResolver,
    ) -> Self {
        Self {
            path: &file.relative_path,
            gated,
            background_starts: &[],
            violations: BTreeSet::new(),
            uses: BTreeSet::new(),
            policy,
            resolver,
            current_type: "<free>".into(),
            item: None,
            rooted: false,
            measuring: BTreeSet::new(),
        }
    }

    /// Visits `file` and adds what it finds to `violations` and `uses`.
    fn read(
        mut self,
        file: &RustFile,
        violations: &mut BTreeSet<Finding>,
        uses: &mut BTreeSet<DirectUse<'p>>,
    ) {
        self.visit_file(&file.syntax);
        violations.append(&mut self.violations);
        uses.append(&mut self.uses);
    }

    fn record(&mut self, capability: &'p Capability, gate: &'p Gate, span: Span) {
        self.uses.insert(DirectUse {
            capability: &capability.name,
            path: self.path.to_string(),
            gate: &gate.kind,
            item: self.item.clone(),
            line: span.start().line,
        });
    }

    /// Visits what `item` holds as that item's code.
    fn inside(&mut self, item: String, visit: impl FnOnce(&mut Self)) {
        let previous = self.item.replace(item);
        visit(self);
        self.item = previous;
    }

    fn record_background_start(&mut self, gate: &Gate, span: Span) {
        self.violations.insert(Finding::new(
            self.path,
            span.start().line,
            format!("{} outside a composition root", gate.kind),
            "start background work only at a listed root, which must keep and stop it",
        ));
    }

    fn check_method(&mut self, name: &str, span: Span) {
        if !self.rooted {
            for gate in self.background_starts {
                if gate.method_patterns.iter().any(|method| method == name) {
                    self.record_background_start(gate, span);
                }
            }
        }
        for &(capability, gate) in self.gated {
            if gate.method_patterns.iter().any(|method| method == name) {
                self.record(capability, gate, span);
            }
        }
    }

    /// `is_import` distinguishes `use` trees from expression, type, and macro
    /// paths. In an import, a bare crate name (`use open;`) references the
    /// crate; in an expression, a single-segment path (`open(...)`) is a local
    /// item and must not match a gated crate of the same name.
    fn check_segments(&mut self, segments: &[String], is_import: bool, span: Span) {
        if !self.rooted {
            for gate in self.background_starts {
                if gate_matches(gate, segments, is_import) {
                    self.record_background_start(gate, span);
                }
            }
        }
        let start = span.start();
        let measuring = self.measuring.contains(&(start.line, start.column));
        for &(capability, gate) in self.gated {
            let measured = !is_import && !measuring && holds_any(segments, &gate.measured_patterns);
            if measured || gate_matches(gate, segments, is_import) {
                self.record(capability, gate, span);
            }
        }
    }

    /// A function that code calls or passes as a value, by its path as
    /// written (`""` first for a leading `::`), against the gates' `calls`.
    /// A path that resolves to nothing matches as written, unless it is a
    /// single name: that is a local binding, which the resolver doesn't
    /// know, so it may also share a type's or module's name. A path that
    /// resolves ambiguously or through an import cycle is a finding when a
    /// gate here matches calls, since the gate can't tell what it names.
    fn check_call(&mut self, segments: &[String], span: Span) {
        let resolved = self.resolver.resolve(self.path, span, segments);
        let path = match &resolved {
            Ok(path) => path.as_slice(),
            Err(ResolutionError::Missing | ResolutionError::WrongNamespace) => {
                let written = segments.strip_prefix(&[String::new()]).unwrap_or(segments);
                if written.len() < 2 {
                    return;
                }
                written
            }
            Err(error) => {
                if self.gates_calls() {
                    self.violations.insert(Finding::new(
                        self.path,
                        span.start().line,
                        format!("cannot resolve call {}: {error}", segments.join("::")),
                        "use an unambiguous module path or import, so a gate's calls can tell what the call names",
                    ));
                }
                return;
            }
        };
        if !self.rooted {
            for gate in self.background_starts {
                if holds_any(path, &gate.call_patterns) {
                    self.record_background_start(gate, span);
                }
            }
        }
        for &(capability, gate) in self.gated {
            if holds_any(path, &gate.call_patterns) {
                self.record(capability, gate, span);
            }
        }
    }

    /// Whether a gate checked here matches calls.
    fn gates_calls(&self) -> bool {
        let starts = self.background_starts.iter().filter(|_| !self.rooted);
        starts
            .chain(self.gated.iter().map(|&(_, gate)| gate))
            .any(|gate| !gate.call_patterns.is_empty())
    }
}

/// Whether `segments` holds one of `patterns` as adjacent segments.
pub(crate) fn holds_any(segments: &[String], patterns: &[Vec<String>]) -> bool {
    patterns.iter().any(|pattern| {
        segments
            .windows(pattern.len())
            .any(|window| window.iter().zip(pattern.iter()).all(|(a, b)| a == b))
    })
}

/// An expression path's segments as written, a qualified one's from its
/// type: `<File>::open` and `<File as Opener>::open` are `File::open`.
fn expression_path(node: &syn::ExprPath) -> Option<Vec<String>> {
    let Some(qself) = &node.qself else { return Some(qualified_path(&node.path)) };
    let syn::Type::Path(ty) = qself.ty.as_ref() else { return None };
    let rest = node.path.segments.iter().skip(qself.position);
    Some(
        qualified_path(&ty.path)
            .into_iter()
            .chain(rest.map(|segment| segment.ident.to_string()))
            .collect(),
    )
}

fn gate_matches(gate: &Gate, segments: &[String], is_import: bool) -> bool {
    let first_is_gated_crate = (is_import || segments.len() >= 2)
        && segments.first().is_some_and(|first| gate.crates.iter().any(|name| first == name));
    first_is_gated_crate || holds_any(segments, &gate.path_patterns)
}

impl<'ast> Visit<'ast> for CapabilityBoundaryVisitor<'_, '_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let previous = self.rooted;
        self.rooted = false;
        visit::visit_item(self, node);
        self.rooted = previous;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        self.check_method(&node.method.to_string(), node.method.span());
        visit::visit_expr_method_call(self, node);
    }

    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().is_ident("doc") {
            return;
        }
        visit::visit_attribute(self, node);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let previous = std::mem::replace(
            &mut self.current_type,
            type_name(&node.self_ty).unwrap_or_else(|| "<impl>".into()),
        );
        visit::visit_item_impl(self, node);
        self.current_type = previous;
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        let previous = std::mem::replace(&mut self.current_type, node.ident.to_string());
        visit::visit_item_trait(self, node);
        self.current_type = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let previous = self.rooted;
        self.rooted =
            self.policy.is_composition_root(self.path, "<free>", &node.sig.ident.to_string());
        let item = item_name(None, &node.sig.ident);
        self.inside(item, |visitor| visit::visit_item_fn(visitor, node));
        self.rooted = previous;
    }

    fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
        let previous = self.rooted;
        self.rooted = self.policy.is_composition_root(self.path, "<free>", &node.ident.to_string());
        let item = item_name(None, &node.ident);
        self.inside(item, |visitor| visit::visit_item_static(visitor, node));
        self.rooted = previous;
    }

    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        let item = item_name(None, &node.ident);
        self.inside(item, |visitor| visit::visit_item_const(visitor, node));
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let previous = self.rooted;
        self.rooted = self.policy.is_composition_root(
            self.path,
            &self.current_type,
            &node.sig.ident.to_string(),
        );
        let item = item_name(Some(&self.current_type), &node.sig.ident);
        self.inside(item, |visitor| visit::visit_impl_item_fn(visitor, node));
        self.rooted = previous;
    }

    fn visit_impl_item_const(&mut self, node: &'ast syn::ImplItemConst) {
        let item = item_name(Some(&self.current_type), &node.ident);
        self.inside(item, |visitor| visit::visit_impl_item_const(visitor, node));
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        let item = item_name(Some(&self.current_type), &node.sig.ident);
        self.inside(item, |visitor| visit::visit_trait_item_fn(visitor, node));
    }

    fn visit_trait_item_const(&mut self, node: &'ast syn::TraitItemConst) {
        let item = item_name(Some(&self.current_type), &node.ident);
        self.inside(item, |visitor| visit::visit_trait_item_const(visitor, node));
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        let mut paths = Vec::new();
        flatten_use_tree(&node.tree, &mut Vec::new(), &mut paths);
        for segments in paths {
            self.check_segments(&segments, true, node.span());
        }
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if let Some(segments) = expression_path(node) {
            self.check_call(&segments, node.span());
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        let segments =
            node.segments.iter().map(|segment| segment.ident.to_string()).collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        visit::visit_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let segments =
            node.path.segments.iter().map(|segment| segment.ident.to_string()).collect::<Vec<_>>();
        self.check_segments(&segments, false, node.span());
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(unparsed_tokens(node)) {
                    if path.after_dot {
                        self.check_method(&path.segments[0], path.span);
                    } else {
                        self.check_segments(&path.segments, false, path.span);
                        if !path.type_position {
                            let leading = path.leading_colon.then(String::new);
                            let segments =
                                leading.into_iter().chain(path.segments).collect::<Vec<_>>();
                            self.check_call(&segments, path.span);
                        }
                    }
                }
            }
        }
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "capability_boundaries_tests.rs"]
mod tests;
