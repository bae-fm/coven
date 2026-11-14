//! Read code that holds state a read leaves behind (`kept_state.rs`): a
//! closure in read code capturing a binding of the function around it, or
//! the state of `self`, and changing it or holding a kept-state type; and
//! read code naming a `static` that holds one.

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::kept_state::{is_compound_assignment, self_field, takes_mut_self, Graph};
use crate::macros::parse_macro_body;
use crate::policy::Shape;
use crate::read_code::{Body, ReadClosure, ReadCode};
use crate::read_index::names;
use crate::row_work::Hit;
use crate::syntax::RustFile;

pub(crate) fn captured_state(
    read: &ReadCode<'_>,
    graph: &Graph<'_>,
    files: &[RustFile],
) -> Vec<Hit> {
    let mut hits = Vec::new();
    for closure in read.closures() {
        let mut bound = BoundNames(BTreeSet::new());
        for input in &closure.expr.inputs {
            bound.visit_pat(input);
        }
        bound.visit_expr(&closure.expr.body);
        let mut captures = Captures {
            read,
            graph,
            closure,
            bound: bound.0,
            seen: BTreeSet::new(),
            found: Vec::new(),
        };
        captures.visit_expr(&closure.expr.body);
        hits.extend(captures.found.into_iter().map(|(line, what)| Hit {
            path: closure.file.relative_path.clone(),
            line,
            item: closure.item.clone(),
            shape: Shape::KeptState,
            what,
        }));
    }
    let statics = kept_statics(files, graph);
    if !statics.is_empty() {
        for unit in read.units() {
            let mut naming =
                NamedStatics { statics: &statics, seen: BTreeSet::new(), found: Vec::new() };
            match &unit.body {
                Body::Function { block, .. } => naming.visit_block(block),
                Body::Closure(closure) => naming.visit_expr(&closure.body),
            }
            hits.extend(naming.found.into_iter().map(|(line, what)| Hit {
                path: unit.file.relative_path.clone(),
                line,
                item: unit.item.clone(),
                shape: Shape::KeptState,
                what,
            }));
        }
    }
    hits
}

/// Each `static` holding a kept-state type, by name, with that type.
fn kept_statics(files: &[RustFile], graph: &Graph<'_>) -> BTreeMap<String, String> {
    struct Statics<'g, 'a> {
        graph: &'g Graph<'a>,
        found: BTreeMap<String, String>,
    }
    impl<'ast> Visit<'ast> for Statics<'_, '_> {
        fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
            if let Some(kept) = self.graph.kept(&names(&node.ty, false)) {
                self.found.insert(node.ident.to_string(), kept);
            }
        }
    }
    let mut statics = Statics { graph, found: BTreeMap::new() };
    for file in files {
        statics.visit_file(&file.syntax);
    }
    statics.found
}

struct NamedStatics<'a> {
    statics: &'a BTreeMap<String, String>,
    seen: BTreeSet<String>,
    found: Vec<(usize, String)>,
}

impl<'ast> Visit<'ast> for NamedStatics<'_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if let Some(name) = node.path.get_ident().map(ToString::to_string) {
            if let Some(kept) = self.statics.get(&name) {
                if self.seen.insert(name.clone()) {
                    self.found.push((
                        node.span().start().line,
                        format!("reads the static `{name}`, which keeps `{kept}` state"),
                    ));
                }
            }
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

/// The names a closure binds itself: its parameters and every pattern in it.
struct BoundNames(BTreeSet<String>);

impl<'ast> Visit<'ast> for BoundNames {
    fn visit_pat_ident(&mut self, node: &'ast syn::PatIdent) {
        self.0.insert(node.ident.to_string());
        visit::visit_pat_ident(self, node);
    }
}

struct Captures<'a, 'r> {
    read: &'a ReadCode<'r>,
    graph: &'a Graph<'a>,
    closure: &'a ReadClosure<'r>,
    bound: BTreeSet<String>,
    seen: BTreeSet<String>,
    found: Vec<(usize, String)>,
}

/// What a closure captures: a binding of the function around it, or a field
/// of `self`.
enum Captured {
    Binding(String),
    SelfField(String),
}

impl Captured {
    fn name(&self) -> String {
        match self {
            Captured::Binding(name) => name.clone(),
            Captured::SelfField(field) => format!("self.{field}"),
        }
    }
}

impl Captures<'_, '_> {
    /// The captured binding or `self` field an expression is, or is inside.
    fn captured(&self, expr: &syn::Expr) -> Option<Captured> {
        if let Some(field) = self_field(expr) {
            return Some(Captured::SelfField(field));
        }
        match expr {
            syn::Expr::Path(path) if path.qself.is_none() => {
                let name = path.path.get_ident()?.to_string();
                (self.closure.outlives_its_function
                    && !self.bound.contains(&name)
                    && self.closure.captures.contains_key(&name))
                .then_some(Captured::Binding(name))
            }
            syn::Expr::Field(inner) => self.captured(&inner.base),
            syn::Expr::Unary(inner) => self.captured(&inner.expr),
            syn::Expr::Index(inner) => self.captured(&inner.expr),
            syn::Expr::Paren(inner) => self.captured(&inner.expr),
            syn::Expr::Reference(inner) => self.captured(&inner.expr),
            _ => None,
        }
    }

    /// The type of a captured binding or of a field of `self`, by name.
    fn type_of(&self, captured: &Captured) -> Option<String> {
        match captured {
            Captured::Binding(name) => self.closure.captures.get(name).cloned().flatten(),
            Captured::SelfField(field) => {
                self.read.index().fields.get(&(self.closure.owner.clone()?, field.clone())).cloned()
            }
        }
    }

    fn report(&mut self, line: usize, key: String, what: String) {
        if self.seen.insert(key) {
            self.found.push((line, what));
        }
    }

    fn assigned(&mut self, target: &syn::Expr) {
        if let Some(captured) = self.captured(target) {
            let name = captured.name();
            self.report(
                target.span().start().line,
                format!("assigns {name}"),
                format!("assigns the captured `{name}`"),
            );
        }
    }
}

impl<'ast> Visit<'ast> for Captures<'_, '_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_expr_assign(&mut self, node: &'ast syn::ExprAssign) {
        self.assigned(&node.left);
        visit::visit_expr_assign(self, node);
    }

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        if is_compound_assignment(&node.op) {
            self.assigned(&node.left);
        }
        visit::visit_expr_binary(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if let Some(captured) = self.captured(&node.receiver) {
            let method = node.method.to_string();
            let mutating = self
                .type_of(&captured)
                .is_some_and(|ty| takes_mut_self(self.read.index(), &ty, &method));
            if mutating {
                let name = captured.name();
                self.report(
                    node.method.span().start().line,
                    format!("changes {name} with {method}"),
                    format!("changes the captured `{name}` with `{method}`"),
                );
            }
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if let Some(captured @ Captured::Binding(_)) = self.captured_path(node) {
            self.holds(&captured, node.span().start().line);
        }
    }

    fn visit_expr_field(&mut self, node: &'ast syn::ExprField) {
        if let Some(captured @ Captured::SelfField(_)) =
            self.captured(&syn::Expr::Field(node.clone()))
        {
            if matches!(node.base.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self")) {
                self.holds(&captured, node.span().start().line);
                return;
            }
        }
        visit::visit_expr_field(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

impl Captures<'_, '_> {
    fn captured_path(&self, node: &syn::ExprPath) -> Option<Captured> {
        self.captured(&syn::Expr::Path(node.clone()))
    }

    /// Reports a captured binding or `self` field whose type keeps state.
    fn holds(&mut self, captured: &Captured, line: usize) {
        let kept = match captured {
            Captured::Binding(_) => {
                self.type_of(captured).and_then(|ty| self.graph.kept(&BTreeSet::from([ty])))
            }
            Captured::SelfField(field) => {
                let owner = self.closure.owner.clone();
                owner
                    .and_then(|owner| self.graph.types.get(&owner))
                    .and_then(|info| info.named_fields.get(field))
                    .and_then(|names| self.graph.kept(names))
            }
        };
        if let Some(kept) = kept {
            let name = captured.name();
            self.report(
                line,
                format!("holds {name}"),
                format!("captures `{name}`, which keeps `{kept}` state"),
            );
        }
    }
}
