//! Work SQL does, written in Rust over a read's rows: sorting, filtering,
//! grouping, aggregating and picking rows, matching one read's rows to
//! another's, branching on how many rows there are and dropping a row whose
//! optional column is empty. Each is a method of the standard library,
//! itertools or indexmap, by the definition the call resolves to, on a value
//! that holds rows (`rows.rs`), or a pattern or loop over one:
//!
//! - `sort`: `sort`, `sort_unstable`, `sorted`, their `_by` and `_by_key`
//!   forms, `sort_by_cached_key` and `reverse`;
//! - `filter`: `filter`, `filter_map`, `retain`, `retain_mut`, `partition`,
//!   `skip_while`, `take_while`, `dedup_by` and `dedup_by_key` with a callback,
//!   and `dedup` and `unique`;
//! - `group`: `chunk_by`, `group_by`, `into_group_map`, `entry` and
//!   `counts`, and a map or set that holds rows, made by a constructor or by
//!   `collect`;
//! - `aggregate`: `count`, `sum`, `product`, `max`, `min`, `max_by`,
//!   `max_by_key`, `min_by`, `min_by_key`, `fold` and `reduce`, a `len` that
//!   is not compared, and adding up a local in a loop over rows;
//! - `pick`: `any`, `all`, `find`, `find_map`, `position` and `rposition`
//!   with a callback;
//! - `join`: `get` with a computed key whose answer an option method takes;
//! - `length-branch`: `is_empty`, a compared `len`, and a slice pattern;
//! - `drop-row`: `Option::and_then` whose closure maps an optional column of
//!   the row it was given.
//!
//! A callback argument is a closure or the path of a function.

use clippy_utils::higher::ForLoop;
use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::Res;
use rustc_hir::def_id::LocalDefId;
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{BinOpKind, Expr, ExprKind, LetStmt, Pat, PatKind};
use rustc_lint::Lint;
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::ty::{self, TyCtxt, TypeckResults, TypingEnv};
use rustc_span::Symbol;

use super::flow::Flow;
use super::rows::{Made, Mappers, RowTypes, is_collection, peel_result};
use super::{
    READ_AGGREGATE, READ_DROP_ROW, READ_FILTER, READ_GROUP, READ_JOIN, READ_LENGTH_BRANCH,
    READ_PICK, READ_SORT, callback, is_library,
};
use crate::finding::Finding;

const SORT_WITHOUT_ARGUMENTS: [&str; 4] = ["sort", "sort_unstable", "sorted", "reverse"];
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
/// The maps and sets that group rows.
const COLLECTIONS: [&str; 6] =
    ["HashMap", "BTreeMap", "HashSet", "BTreeSet", "IndexMap", "IndexSet"];
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
/// Reports the row work in the read code `unit`, with the closures inside
/// it, but those only a live query's `process` runs.
pub(super) fn check<'tcx>(
    tcx: TyCtxt<'tcx>,
    help: &'static str,
    rows: &RowTypes<'tcx>,
    mappers: &Mappers<'_, 'tcx>,
    unit: LocalDefId,
    process_only: &FxHashSet<LocalDefId>,
) {
    let typeck = tcx.typeck(unit);
    let root = tcx.typeck_root_def_id(unit.to_def_id());
    let typing_env = TypingEnv::post_analysis(tcx, root);
    let body = tcx.hir_body_owned_by(unit).value;
    // A read of one column: a call given a mapper of one column's value,
    // which returns a collection of those values.
    let column_read = |expr: &Expr<'tcx>| {
        let arguments = match expr.kind {
            ExprKind::MethodCall(_, _, arguments, _) | ExprKind::Call(_, arguments) => arguments,
            _ => return false,
        };
        arguments.iter().any(|argument| {
            callback(typeck, argument).and_then(|id| mappers.made(id)) == Some(Made::Column)
        }) && is_collection(tcx, typing_env, peel_result(tcx, typeck.expr_ty(expr)))
    };
    let column_rows = Flow::of(tcx, typeck, body, &column_read);
    let mut work = RowWork {
        tcx,
        help,
        rows,
        column_rows: &column_rows,
        typing_env,
        process_only,
        item: tcx.def_path_str(root),
        typeck,
        compared_lengths: FxHashSet::default(),
        row_loops: 0,
        matched_rows: false,
    };
    work.visit_expr(body);
}

struct RowWork<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    /// The help every finding gives: the rule, and what to do instead.
    help: &'static str,
    rows: &'a RowTypes<'tcx>,
    /// What flows from the body's reads of one column.
    column_rows: &'a Flow<'a, 'tcx>,
    typing_env: TypingEnv<'tcx>,
    process_only: &'a FxHashSet<LocalDefId>,
    /// How findings name the unit: its function's path.
    item: String,
    typeck: &'tcx TypeckResults<'tcx>,
    /// The `len` calls a comparison already reported.
    compared_lengths: FxHashSet<rustc_hir::HirId>,
    /// How many loops over rows enclose the code.
    row_loops: usize,
    /// Whether the patterns being visited match a value that holds rows.
    matched_rows: bool,
}

impl<'tcx> RowWork<'_, 'tcx> {
    fn report(
        &self,
        lint: &'static Lint,
        expr_id: rustc_hir::HirId,
        span: rustc_span::Span,
        what: String,
    ) {
        self.tcx.emit_node_span_lint(
            lint,
            expr_id,
            span.source_callsite(),
            Finding { message: format!("`{}` {what}", self.item), help: self.help },
        );
    }

    /// The library method a method call resolves to, by name.
    fn library_method(&self, expr: &Expr<'tcx>) -> Option<Symbol> {
        let callee = self.typeck.type_dependent_def_id(expr.hir_id)?;
        is_library(self.tcx, callee).then(|| self.tcx.item_name(callee))
    }

    /// Whether the expression's value holds rows: its type holds a row
    /// type, or it is a collection that flows from a read of one column.
    fn holds_rows(&self, expr: &Expr<'tcx>) -> bool {
        let ty = self.typeck.expr_ty(expr);
        self.rows.holds_rows(ty)
            || self.column_rows.carries(expr) && is_collection(self.tcx, self.typing_env, ty)
    }

    /// Whether an argument is a closure or the path of a function.
    fn is_callback(&self, argument: &Expr<'tcx>) -> bool {
        callback(self.typeck, argument).is_some()
    }

    /// Visits patterns that match `scrutinee`'s value.
    fn visit_matched(&mut self, scrutinee: &'tcx Expr<'tcx>, visit: impl FnOnce(&mut Self)) {
        let matched = self.holds_rows(scrutinee);
        let outer = std::mem::replace(&mut self.matched_rows, matched);
        visit(self);
        self.matched_rows = outer;
    }

    /// The `len` of rows an expression is, behind casts.
    fn rows_len(&self, expr: &'tcx Expr<'tcx>) -> Option<&'tcx Expr<'tcx>> {
        match expr.kind {
            ExprKind::Cast(inner, _) => self.rows_len(inner),
            ExprKind::MethodCall(_, receiver, [], _)
                if self.library_method(expr).is_some_and(|name| name.as_str() == "len")
                    && self.holds_rows(receiver) =>
            {
                Some(expr)
            }
            _ => None,
        }
    }

    /// Whether an expression is `rows.get(key)` with a computed key.
    fn is_lookup(&self, expr: &Expr<'tcx>) -> bool {
        matches!(expr.kind, ExprKind::MethodCall(_, receiver, [key], _)
            if self.library_method(expr).is_some_and(|name| name.as_str() == "get")
                && self.holds_rows(receiver)
                && !matches!(key.kind, ExprKind::Lit(_)))
    }

    /// Whether a closure argument maps an optional column of the row it was
    /// given: `|row| row.column.map(..)`.
    fn drops_empty_row(&self, argument: &Expr<'tcx>) -> bool {
        let ExprKind::Closure(closure) = argument.kind else { return false };
        let body = self.tcx.hir_body(closure.body);
        let [parameter] = body.params else { return false };
        if !self.rows.is_row(self.typeck.pat_ty(parameter.pat).peel_refs()) {
            return false;
        }
        let mut value = body.value;
        while let ExprKind::Block(block, _) = value.kind
            && block.stmts.is_empty()
            && let Some(inner) = block.expr
        {
            value = inner;
        }
        let ExprKind::MethodCall(segment, mut receiver, [mapper], _) = value.kind else {
            return false;
        };
        if segment.ident.name.as_str() != "map" || !matches!(mapper.kind, ExprKind::Closure(_)) {
            return false;
        }
        while let ExprKind::Field(base, _) = receiver.kind {
            receiver = base;
        }
        matches!(receiver.kind, ExprKind::Path(rustc_hir::QPath::Resolved(None, path))
            if matches!(path.res, Res::Local(binding) if binding == parameter.pat.hir_id))
    }

    fn method(&mut self, expr: &'tcx Expr<'tcx>) {
        let ExprKind::MethodCall(_, receiver, arguments, _) = expr.kind else { return };
        let Some(name) = self.library_method(expr) else { return };
        let name = name.as_str();
        let count = arguments.len();
        let callback = arguments.iter().any(|argument| self.is_callback(argument));
        let rows = self.holds_rows(receiver);
        let (lint, what) = if !rows {
            if OPTION_CONSUMERS.contains(&name) && self.is_lookup(receiver) {
                (
                    READ_JOIN,
                    "joins one read's rows to another's by a key or place with `get`".to_string(),
                )
            } else if name == "and_then" && count == 1 && self.drops_empty_row(&arguments[0]) {
                (READ_DROP_ROW, "drops the row when an optional column is empty".to_string())
            } else {
                return;
            }
        } else if (SORT_WITHOUT_ARGUMENTS.contains(&name) && count == 0)
            || (SORT_WITH_ARGUMENTS.contains(&name) && count > 0)
        {
            (READ_SORT, format!("sorts a read's rows with `{name}`"))
        } else if name == "is_empty" && count == 0 {
            (READ_LENGTH_BRANCH, "tests whether a read returned rows with `is_empty`".to_string())
        } else if name == "len" && count == 0 && !self.compared_lengths.contains(&expr.hir_id) {
            (READ_AGGREGATE, "counts a read's rows with `len`".to_string())
        } else if (FILTER_WITH_CALLBACK.contains(&name) && callback)
            || (FILTER_WITHOUT_ARGUMENTS.contains(&name) && count == 0)
        {
            (READ_FILTER, format!("filters a read's rows with `{name}`"))
        } else if GROUP_METHODS.contains(&name)
            || (name == "entry" && count == 1)
            || (name == "counts" && count == 0)
        {
            (READ_GROUP, format!("groups a read's rows with `{name}`"))
        } else if (AGGREGATE_WITHOUT_ARGUMENTS.contains(&name) && count == 0)
            || (AGGREGATE_WITH_ARGUMENTS.contains(&name) && count > 0)
        {
            (READ_AGGREGATE, format!("aggregates a read's rows with `{name}`"))
        } else if PICK_WITH_CALLBACK.contains(&name) && callback {
            (READ_PICK, format!("picks among a read's rows with `{name}`"))
        } else {
            return;
        };
        self.report(lint, expr.hir_id, expr.span, what);
    }

    /// Reports a map or set of rows that a library constructor or `collect`
    /// makes.
    fn grouped(&self, expr: &'tcx Expr<'tcx>) {
        let callee = match expr.kind {
            ExprKind::Call(function, _) => match function.kind {
                ExprKind::Path(ref qpath) => {
                    self.typeck.qpath_res(qpath, function.hir_id).opt_def_id()
                }
                _ => None,
            },
            ExprKind::MethodCall(..) => self.typeck.type_dependent_def_id(expr.hir_id),
            _ => None,
        };
        if !callee.is_some_and(|callee| is_library(self.tcx, callee)) {
            return;
        }
        let made = self.typeck.expr_ty(expr);
        let ty::Adt(adt, _) = made.kind() else { return };
        let name = self.tcx.item_name(adt.did());
        if is_library(self.tcx, adt.did())
            && COLLECTIONS.contains(&name.as_str())
            && self.holds_rows(expr)
        {
            self.report(
                READ_GROUP,
                expr.hir_id,
                expr.span,
                format!("groups a read's rows into a `{name}`"),
            );
        }
    }
}

impl<'tcx> Visitor<'tcx> for RowWork<'_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.tcx
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        if let ExprKind::Closure(closure) = expr.kind
            && self.process_only.contains(&closure.def_id)
        {
            return;
        }
        if let Some(for_loop) = ForLoop::hir(expr)
            && self.holds_rows(for_loop.arg)
        {
            self.visit_expr(for_loop.arg);
            self.visit_pat(for_loop.pat);
            self.row_loops += 1;
            self.visit_expr(for_loop.body);
            self.row_loops -= 1;
            return;
        }
        match expr.kind {
            ExprKind::Binary(op, left, right) => {
                if matches!(
                    op.node,
                    BinOpKind::Eq
                        | BinOpKind::Ne
                        | BinOpKind::Lt
                        | BinOpKind::Le
                        | BinOpKind::Gt
                        | BinOpKind::Ge
                ) {
                    let lengths = [left, right]
                        .into_iter()
                        .filter_map(|side| self.rows_len(side))
                        .collect::<Vec<_>>();
                    if !lengths.is_empty() {
                        self.compared_lengths.extend(lengths.iter().map(|len| len.hir_id));
                        self.report(
                            READ_LENGTH_BRANCH,
                            expr.hir_id,
                            expr.span,
                            "compares how many rows a read returned with `len`".to_string(),
                        );
                    }
                }
            }
            ExprKind::AssignOp(op, target, _)
                if self.row_loops > 0
                    && matches!(
                        op.node,
                        rustc_hir::AssignOpKind::AddAssign
                            | rustc_hir::AssignOpKind::SubAssign
                            | rustc_hir::AssignOpKind::MulAssign
                    )
                    && matches!(target.kind, ExprKind::Path(_)) =>
            {
                self.report(
                    READ_AGGREGATE,
                    expr.hir_id,
                    expr.span,
                    "adds up a local in a loop over a read's rows".to_string(),
                );
            }
            ExprKind::Let(binding) => {
                self.visit_expr(binding.init);
                self.visit_matched(binding.init, |work| work.visit_pat(binding.pat));
                return;
            }
            ExprKind::Match(scrutinee, arms, _) => {
                self.visit_expr(scrutinee);
                self.visit_matched(scrutinee, |work| {
                    for arm in arms {
                        work.visit_pat(arm.pat);
                    }
                });
                for arm in arms {
                    if let Some(guard) = arm.guard {
                        self.visit_expr(guard);
                    }
                    self.visit_expr(arm.body);
                }
                return;
            }
            ExprKind::MethodCall(..) => {
                self.method(expr);
                self.grouped(expr);
            }
            ExprKind::Call(..) => self.grouped(expr),
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }

    fn visit_local(&mut self, local: &'tcx LetStmt<'tcx>) {
        match local.init {
            Some(init) => self.visit_matched(init, |work| intravisit::walk_local(work, local)),
            None => intravisit::walk_local(self, local),
        }
    }

    fn visit_pat(&mut self, pat: &'tcx Pat<'tcx>) {
        if let PatKind::Slice(..) = pat.kind
            && (self.matched_rows || self.rows.holds_rows(self.typeck.pat_ty(pat)))
        {
            self.report(
                READ_LENGTH_BRANCH,
                pat.hir_id,
                pat.span,
                "branches on how many rows a read returned with a slice pattern".to_string(),
            );
        }
        intravisit::walk_pat(self, pat);
    }
}
