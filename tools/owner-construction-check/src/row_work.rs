//! Work SQL does, written in Rust over a read's rows (the read rule): sorting,
//! filtering, grouping, aggregating and picking rows, and running a statement
//! once for each row, in the code `read_code.rs` finds. The method names are
//! those of the standard library and itertools; the type names, those of the
//! standard collections and `indexmap`.
//!
//! A callback argument is a closure or the path of a function (`Self::keep`,
//! `char::is_numeric`): a single name may be a value, as in `line.find(sep)`.

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::policy::Shape;
use crate::read_code::{call_site, Body, ReadCode};

/// One place read code works on rows.
pub(crate) struct Hit {
    pub(crate) path: String,
    pub(crate) line: usize,
    /// The function or method the work is in.
    pub(crate) item: String,
    pub(crate) shape: Shape,
    /// What the code does, as a finding says it.
    pub(crate) what: String,
}

const SORT_WITHOUT_ARGUMENTS: [&str; 3] = ["sort", "sort_unstable", "sorted"];
const SORT_WITH_ARGUMENTS: [&str; 7] = [
    "sort_by",
    "sort_by_key",
    "sort_by_cached_key",
    "sort_unstable_by",
    "sort_unstable_by_key",
    "sorted_by",
    "sorted_by_key",
];
const FILTER_WITH_CALLBACK: [&str; 9] = [
    "filter",
    "filter_map",
    "retain",
    "retain_mut",
    "partition",
    "skip_while",
    "take_while",
    "dedup_by",
    "dedup_by_key",
];
const FILTER_WITHOUT_ARGUMENTS: [&str; 2] = ["dedup", "unique"];
const GROUP_METHODS: [&str; 3] = ["chunk_by", "group_by", "into_group_map"];
const AGGREGATE_WITHOUT_ARGUMENTS: [&str; 5] = ["count", "sum", "product", "max", "min"];
const AGGREGATE_WITH_ARGUMENTS: [&str; 6] =
    ["max_by", "max_by_key", "min_by", "min_by_key", "fold", "reduce"];
const PICK_WITH_CALLBACK: [&str; 6] = ["any", "all", "find", "find_map", "position", "rposition"];
const COLLECTIONS: [&str; 5] = ["HashMap", "BTreeMap", "HashSet", "BTreeSet", "IndexMap"];
const COLLECTION_CONSTRUCTORS: [&str; 5] = ["new", "with_capacity", "default", "from", "from_iter"];
const SQL_CALLS: [&str; 5] = ["query", "query_row", "prepare", "query_map", "execute"];
/// Methods that run their closure argument once for each row.
const ITERATOR_METHODS: [&str; 12] = [
    "map",
    "for_each",
    "filter_map",
    "flat_map",
    "try_for_each",
    "fold",
    "try_fold",
    "inspect",
    "scan",
    "filter",
    "find_map",
    "map_while",
];
/// The calls whose closure argument runs once for each row they read.
const ROW_CALLS: [&str; 3] = ["query", "query_row", "query_map"];

/// Every place the read code works on rows.
pub(crate) fn row_work(read: &ReadCode<'_>) -> Vec<Hit> {
    let mut hits = Vec::new();
    for unit in read.units() {
        let mut visitor = RowWork {
            read,
            path: &unit.file.relative_path,
            item: &unit.item,
            hits: &mut hits,
            loops: 0,
            rows: 0,
            typed_init: None,
        };
        match &unit.body {
            Body::Function { block, signature } => {
                visitor.returns(&signature.output, signature.ident.span());
                visitor.visit_block(block);
            }
            Body::Closure(closure) => {
                visitor.returns(&closure.output, closure.span());
                visitor.visit_expr(&closure.body);
            }
        }
    }
    hits
}

struct RowWork<'a> {
    read: &'a ReadCode<'a>,
    path: &'a str,
    item: &'a str,
    hits: &'a mut Vec<Hit>,
    /// How many loop bodies the code visited is inside.
    loops: usize,
    /// How many closures run once for each row it is inside.
    rows: usize,
    /// The call whose collection the `let` it initializes already names.
    typed_init: Option<usize>,
}

/// The standard collection a type is, through the wrappers a read returns.
fn collection(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Reference(inner) => collection(&inner.elem),
        syn::Type::Paren(inner) => collection(&inner.elem),
        syn::Type::Group(inner) => collection(&inner.elem),
        syn::Type::Path(path) if path.qself.is_none() => {
            let segment = path.path.segments.last()?;
            let name = segment.ident.to_string();
            if COLLECTIONS.contains(&name.as_str()) {
                return Some(name);
            }
            if matches!(name.as_str(), "Result" | "Option" | "Box" | "Arc" | "Rc") {
                if let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments {
                    return arguments.args.iter().find_map(|argument| match argument {
                        syn::GenericArgument::Type(inner) => collection(inner),
                        _ => None,
                    });
                }
            }
            None
        }
        _ => None,
    }
}

/// Whether an argument is a closure or the path of a function.
fn is_callback(argument: &syn::Expr) -> bool {
    match argument {
        syn::Expr::Closure(_) => true,
        syn::Expr::Reference(inner) => is_callback(&inner.expr),
        syn::Expr::Paren(inner) => is_callback(&inner.expr),
        syn::Expr::Path(path) if path.qself.is_none() => {
            path.path.segments.len() >= 2
                && path.path.segments.last().is_some_and(|last| {
                    last.ident.to_string().starts_with(|first: char| !first.is_uppercase())
                })
        }
        _ => false,
    }
}

impl RowWork<'_> {
    fn hit(&mut self, shape: Shape, line: usize, what: String) {
        self.hits.push(Hit {
            path: self.path.to_string(),
            line,
            item: self.item.to_string(),
            shape,
            what,
        });
    }

    fn per_row(&self) -> bool {
        self.loops > 0 || self.rows > 0
    }

    fn returns(&mut self, output: &syn::ReturnType, span: proc_macro2::Span) {
        if let syn::ReturnType::Type(_, ty) = output {
            if let Some(name) = collection(ty) {
                self.hit(Shape::Group, span.start().line, format!("returns a `{name}`"));
            }
        }
    }

    fn method(&mut self, node: &syn::ExprMethodCall) {
        let name = node.method.to_string();
        let name = name.as_str();
        let line = node.method.span().start().line;
        let arguments = node.args.len();
        let callback = node.args.iter().any(is_callback);
        let (shape, what) = if (SORT_WITHOUT_ARGUMENTS.contains(&name) && arguments == 0)
            || (SORT_WITH_ARGUMENTS.contains(&name) && arguments > 0)
        {
            (Shape::Sort, format!("sorts with `{name}`"))
        } else if (FILTER_WITH_CALLBACK.contains(&name) && callback)
            || (FILTER_WITHOUT_ARGUMENTS.contains(&name) && arguments == 0)
        {
            (Shape::Filter, format!("filters with `{name}`"))
        } else if GROUP_METHODS.contains(&name)
            || (name == "entry" && arguments == 1)
            || (name == "counts" && arguments == 0)
        {
            (Shape::Group, format!("groups with `{name}`"))
        } else if (AGGREGATE_WITHOUT_ARGUMENTS.contains(&name) && arguments == 0)
            || (AGGREGATE_WITH_ARGUMENTS.contains(&name) && arguments > 0)
        {
            (Shape::Aggregate, format!("aggregates with `{name}`"))
        } else if PICK_WITH_CALLBACK.contains(&name) && callback {
            (Shape::Pick, format!("picks with `{name}`"))
        } else {
            return;
        };
        self.hit(shape, line, what);
    }
}

impl<'ast> Visit<'ast> for RowWork<'_> {
    // A nested item is read code only when something reads it.
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_local(&mut self, node: &'ast syn::Local) {
        if let syn::Pat::Type(typed) = &node.pat {
            if let Some(name) = collection(&typed.ty) {
                self.hit(Shape::Group, node.span().start().line, format!("groups into a `{name}`"));
                self.typed_init = node.init.as_ref().and_then(|init| match init.expr.as_ref() {
                    syn::Expr::Call(call) => Some(std::ptr::from_ref(call) as usize),
                    syn::Expr::MethodCall(call) => Some(std::ptr::from_ref(call) as usize),
                    _ => None,
                });
            }
        }
        visit::visit_local(self, node);
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.visit_expr(&node.expr);
        self.loops += 1;
        self.visit_block(&node.body);
        self.loops -= 1;
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.loops += 1;
        visit::visit_expr_while(self, node);
        self.loops -= 1;
    }

    fn visit_expr_loop(&mut self, node: &'ast syn::ExprLoop) {
        self.loops += 1;
        visit::visit_expr_loop(self, node);
        self.loops -= 1;
    }

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        let accumulates = matches!(
            node.op,
            syn::BinOp::AddAssign(_) | syn::BinOp::SubAssign(_) | syn::BinOp::MulAssign(_)
        );
        if accumulates && self.loops > 0 {
            if let syn::Expr::Path(path) = node.left.as_ref() {
                if let Some(local) = path.path.get_ident() {
                    self.hit(
                        Shape::Aggregate,
                        node.span().start().line,
                        format!("adds up `{local}` in a loop"),
                    );
                }
            }
        }
        visit::visit_expr_binary(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        let line = node.span().start().line;
        if let syn::Expr::Path(path) = node.func.as_ref() {
            let segments = path.path.segments.iter().collect::<Vec<_>>();
            let own = self.typed_init == Some(std::ptr::from_ref(node) as usize);
            if let [.., collection, constructor] = segments.as_slice() {
                let collection = collection.ident.to_string();
                let constructor = constructor.ident.to_string();
                if !own
                    && COLLECTIONS.contains(&collection.as_str())
                    && COLLECTION_CONSTRUCTORS.contains(&constructor.as_str())
                {
                    self.hit(
                        Shape::Group,
                        line,
                        format!("groups with `{collection}::{constructor}`"),
                    );
                }
            }
            if self.per_row() && self.read.hands_read_value(self.path, call_site(node.span())) {
                let callee = segments.last().map(|last| last.ident.to_string()).unwrap_or_default();
                self.hit(
                    Shape::QueryPerRow,
                    line,
                    format!("runs `{callee}`, which runs SQL, once for each row"),
                );
            }
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let name = node.method.to_string();
        let line = node.method.span().start().line;
        self.method(node);
        if name == "collect" && self.typed_init != Some(std::ptr::from_ref(node) as usize) {
            for argument in node.turbofish.iter().flat_map(|turbofish| &turbofish.args) {
                if let syn::GenericArgument::Type(ty) = argument {
                    if let Some(collection) = collection(ty) {
                        self.hit(
                            Shape::Group,
                            line,
                            format!("groups with `collect::<{collection}>`"),
                        );
                    }
                }
            }
        }
        if self.per_row() {
            if SQL_CALLS.contains(&name.as_str()) {
                self.hit(
                    Shape::QueryPerRow,
                    line,
                    format!("runs SQL with `{name}` once for each row"),
                );
            } else if self.read.hands_read_value(self.path, call_site(node.method.span())) {
                self.hit(
                    Shape::QueryPerRow,
                    line,
                    format!("runs `{name}`, which runs SQL, once for each row"),
                );
            }
        }
        self.visit_expr(&node.receiver);
        let per_row =
            ITERATOR_METHODS.contains(&name.as_str()) || ROW_CALLS.contains(&name.as_str());
        for argument in &node.args {
            if per_row && matches!(argument, syn::Expr::Closure(_)) {
                self.rows += 1;
                self.visit_expr(argument);
                self.rows -= 1;
            } else {
                self.visit_expr(argument);
            }
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

#[cfg(test)]
#[path = "row_work_tests.rs"]
mod tests;
