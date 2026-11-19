//! Work SQL does, written in Rust over a read's rows (the read rule): sorting,
//! filtering, grouping, aggregating and picking rows, and running a statement
//! once for each row, in the code `read_code.rs` finds. The method names are
//! those of the standard library and itertools; the type names, those of the
//! standard collections and `indexmap`.
//!
//! Three shapes read code takes from a statement's rows without an iterator
//! method: `join` (`.get(place)` with a computed key or place, which matches
//! one statement's rows to another's), `length-branch` (a slice pattern,
//! `.len()` compared, or `.is_empty()`, which counts and picks the case in
//! Rust) and `drop-row` (`and_then(|row| row.column.map(..))`, which drops
//! the row when an optional column is empty). A `.len()` that is not compared
//! counts the rows (`aggregate`), and `.reverse()` orders them (`sort`).
//!
//! A callback argument is a closure or the path of a function (`Self::keep`,
//! `char::is_numeric`): a single name may be a value, as in `line.find(sep)`.

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::read_code::{call_site, Body, ReadCode};
use crate::read_statements::{RowDepth, Statement, VisitRows};
use owner_construction_policy::Shape;

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
/// Every place the read code works on rows.
pub(crate) fn row_work(read: &ReadCode<'_>) -> Vec<Hit> {
    let mut hits = Vec::new();
    for unit in read.units() {
        let mut visitor = RowWork {
            read,
            path: &unit.file.relative_path,
            item: &unit.item,
            hits: &mut hits,
            depth: RowDepth::default(),
            typed_init: None,
            compared_lengths: Vec::new(),
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
    depth: RowDepth,
    /// The call whose collection the `let` it initializes already names.
    typed_init: Option<usize>,
    /// The `len` calls a comparison already found, by address.
    compared_lengths: Vec<usize>,
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

/// The call of `len` with no arguments an expression is, behind parentheses
/// and casts (`x.len() as u32`).
fn len_call(expr: &syn::Expr) -> Option<&syn::ExprMethodCall> {
    match expr {
        syn::Expr::Paren(inner) => len_call(&inner.expr),
        syn::Expr::Cast(inner) => len_call(&inner.expr),
        syn::Expr::MethodCall(call) if call.method == "len" && call.args.is_empty() => Some(call),
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

/// Methods that take the answer of a lookup that may find nothing.
const OPTION_CONSUMERS: [&str; 10] = [
    "ok_or",
    "ok_or_else",
    "copied",
    "cloned",
    "expect",
    "unwrap_or",
    "unwrap_or_default",
    "unwrap_or_else",
    "is_some_and",
    "is_some",
];

/// Whether an expression is `x.get(key)` with a computed key: a lookup of one
/// collection's row by what another's gave. A column read (`row.get(first)?`,
/// `row.get::<_, T>(name)`) answers a `Result`, so no option method follows it.
fn is_lookup(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::Paren(inner) => is_lookup(&inner.expr),
        syn::Expr::MethodCall(call) => {
            call.method == "get"
                && call.turbofish.is_none()
                && call.args.len() == 1
                && !is_literal(&call.args[0])
        }
        _ => false,
    }
}

/// Whether an expression is a literal, a number or a string, possibly negated.
fn is_literal(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::Lit(_) => true,
        syn::Expr::Unary(inner) => is_literal(&inner.expr),
        syn::Expr::Paren(inner) => is_literal(&inner.expr),
        syn::Expr::Reference(inner) => is_literal(&inner.expr),
        _ => false,
    }
}

/// Whether a closure's body maps a value the closure was given: `|row|
/// row.column.map(..)` or `|(a, b)| b.map(..)`.
fn drops_empty_row(argument: &syn::Expr) -> bool {
    let syn::Expr::Closure(closure) = argument else { return false };
    let mut body = closure.body.as_ref();
    while let syn::Expr::Block(block) = body {
        match block.block.stmts.as_slice() {
            [syn::Stmt::Expr(inner, None)] => body = inner,
            _ => return false,
        }
    }
    let syn::Expr::MethodCall(call) = body else { return false };
    if call.method != "map" || !matches!(call.args.first(), Some(syn::Expr::Closure(_))) {
        return false;
    }
    let mut receiver = call.receiver.as_ref();
    while let syn::Expr::Field(field) = receiver {
        receiver = &field.base;
    }
    let syn::Expr::Path(path) = receiver else { return false };
    let Some(name) = path.path.get_ident() else { return false };
    let mut bound = Vec::new();
    for input in &closure.inputs {
        pattern_names(input, &mut bound);
    }
    bound.iter().any(|bound| name == bound)
}

fn pattern_names(pattern: &syn::Pat, names: &mut Vec<syn::Ident>) {
    match pattern {
        syn::Pat::Ident(ident) => names.push(ident.ident.clone()),
        syn::Pat::Tuple(tuple) => tuple.elems.iter().for_each(|inner| pattern_names(inner, names)),
        syn::Pat::Type(typed) => pattern_names(&typed.pat, names),
        syn::Pat::Reference(inner) => pattern_names(&inner.pat, names),
        syn::Pat::Paren(inner) => pattern_names(&inner.pat, names),
        _ => {}
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

    fn returns(&mut self, output: &syn::ReturnType, span: proc_macro2::Span) {
        if let syn::ReturnType::Type(_, ty) = output {
            if let Some(name) = collection(ty) {
                self.hit(Shape::Group, span.start().line, format!("returns a `{name}`"));
            }
        }
    }

    fn method(&mut self, node: &syn::ExprMethodCall) {
        if self.read.is_not_row_work(self.path, call_site(node.method.span())) {
            return;
        }
        let name = node.method.to_string();
        let name = name.as_str();
        let line = node.method.span().start().line;
        let arguments = node.args.len();
        let callback = node.args.iter().any(is_callback);
        let (shape, what) = if (SORT_WITHOUT_ARGUMENTS.contains(&name) && arguments == 0)
            || (SORT_WITH_ARGUMENTS.contains(&name) && arguments > 0)
        {
            (Shape::Sort, format!("sorts with `{name}`"))
        } else if name == "reverse" && arguments == 0 {
            (Shape::Sort, "reverses the order with `reverse`".to_string())
        } else if name == "is_empty" && arguments == 0 {
            (Shape::LengthBranch, "tests whether there are rows with `is_empty`".to_string())
        } else if name == "len"
            && arguments == 0
            && !self.compared_lengths.contains(&(std::ptr::from_ref(node) as usize))
        {
            (Shape::Aggregate, "counts the rows with `len`".to_string())
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
        } else if OPTION_CONSUMERS.contains(&name) && is_lookup(&node.receiver) {
            (Shape::Join, "joins by a key or place with `get`".to_string())
        } else if name == "and_then" && arguments == 1 && drops_empty_row(&node.args[0]) {
            (Shape::DropRow, "drops the row when an optional column is empty".to_string())
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
        self.visit_for_loop_rows(node);
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.visit_while_rows(node);
    }

    fn visit_expr_loop(&mut self, node: &'ast syn::ExprLoop) {
        self.visit_loop_rows(node);
    }

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        let accumulates = matches!(
            node.op,
            syn::BinOp::AddAssign(_) | syn::BinOp::SubAssign(_) | syn::BinOp::MulAssign(_)
        );
        if accumulates && self.depth.in_loop() {
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
        let compares = matches!(
            node.op,
            syn::BinOp::Eq(_)
                | syn::BinOp::Ne(_)
                | syn::BinOp::Lt(_)
                | syn::BinOp::Le(_)
                | syn::BinOp::Gt(_)
                | syn::BinOp::Ge(_)
        );
        let lengths = [&node.left, &node.right].into_iter().filter_map(|side| len_call(side));
        let lengths = lengths.map(|call| std::ptr::from_ref(call) as usize).collect::<Vec<_>>();
        if compares && !lengths.is_empty() {
            self.compared_lengths.extend(lengths);
            self.hit(
                Shape::LengthBranch,
                node.span().start().line,
                "compares how many rows there are with `len`".to_string(),
            );
        }
        visit::visit_expr_binary(self, node);
    }

    fn visit_pat_slice(&mut self, node: &'ast syn::PatSlice) {
        self.hit(
            Shape::LengthBranch,
            node.span().start().line,
            "branches on how many rows a slice holds".to_string(),
        );
        visit::visit_pat_slice(self, node);
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
        }
        if let Some(statement) = Statement::of_call(self.read, self.path, node) {
            if self.depth.per_row() {
                self.hit(
                    Shape::QueryPerRow,
                    line,
                    format!("runs `{}`, which runs SQL, once for each row", statement.name()),
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
        if self.depth.per_row() {
            match Statement::of_method(self.read, self.path, node) {
                Some(Statement::Sql(name)) => self.hit(
                    Shape::QueryPerRow,
                    line,
                    format!("runs SQL with `{name}` once for each row"),
                ),
                Some(Statement::Read(name)) => self.hit(
                    Shape::QueryPerRow,
                    line,
                    format!("runs `{name}`, which runs SQL, once for each row"),
                ),
                None => {}
            }
        }
        self.visit_method_call_rows(node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

impl<'ast> VisitRows<'ast> for RowWork<'_> {
    fn depth(&mut self) -> &mut RowDepth {
        &mut self.depth
    }
}

#[cfg(test)]
#[path = "row_work_tests.rs"]
mod tests;
