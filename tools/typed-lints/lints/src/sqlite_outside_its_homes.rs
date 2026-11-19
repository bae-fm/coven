//! `clippy::sqlite_outside_its_homes`: production code outside the homes of
//! the database's SQLite capability that reaches SQLite directly.
//!
//! The database's owner keeps SQLite to itself (Appendix F.2): every other
//! piece of code asks it to run the work. Outside the homes of the capability
//! the policy's `[database]` names, production code
//!
//! - names no item through `rusqlite` or `libsqlite3-sys` themselves: no
//!   path whose first part is one of those crates, imported under its own
//!   name or another. SQL value conversions, and the errors, results and
//!   column types they report, reach no database, so code anywhere may name
//!   them. A home may hand on what its own work needs, such as a query's row,
//!   and code anywhere may name that through the home's crate;
//! - names no raw handle, a rusqlite connection, transaction, savepoint or
//!   session, by any path;
//! - holds no raw handle: no value whose type is or holds one, even one whose
//!   type the compiler infers and no code names, such as a closure's
//!   parameter. A type that keeps a handle in a field of its own keeps it for
//!   its owner, and its values hold none.
//!
//! A body that holds a handle makes one finding, at the first place it holds
//! one, unless a path in it already named SQLite. The lint reads code after
//! macros expand, so a macro of rusqlite's makes a finding only where what it
//! writes does, and `params!`, which writes value conversions, makes none.
//! Test code answers to none of this: a test may open its own connection.

use clippy_utils::paths::{PathNS, lookup_path_str};
use owner_construction_policy::describe_homes;
use rustc_data_structures::fx::FxHashSet;
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::{Body, HirId, Path};
use rustc_lint::{LateContext, LateLintPass, LintContext, declare_tool_lint, impl_lint_pass};
use rustc_middle::ty::{self, GenericArgKind, Ty, TyCtxt};
use rustc_span::Span;

use crate::finding::Finding;
use crate::workspace_policy::{WorkspacePolicy, workspace_policy};

declare_tool_lint! {
    /// Production code outside the SQLite capability's homes that names an
    /// item of SQLite's crates or holds a raw SQLite handle.
    pub clippy::SQLITE_OUTSIDE_ITS_HOMES,
    Warn,
    "code outside the SQLite capability's homes reaches SQLite directly",
    report_in_external_macro: true
}

/// The crates that are SQLite, as Rust names them: rusqlite, and the C
/// library's bindings beneath it.
const SQLITE_CRATES: [&str; 2] = ["rusqlite", "libsqlite3_sys"];

/// rusqlite's raw handles, each by its definition's path and as findings name
/// it.
const RAW_HANDLES: [(&str, &str); 4] = [
    ("rusqlite::Connection", "connection"),
    ("rusqlite::Transaction", "transaction"),
    ("rusqlite::Savepoint", "savepoint"),
    ("rusqlite::session::Session", "session"),
];

/// rusqlite's SQL value conversions, and the errors, results and column types
/// they report: no database stands behind them.
const VALUE_CONVERSIONS: [&str; 10] = [
    "rusqlite::Error",
    "rusqlite::Result",
    "rusqlite::types::FromSql",
    "rusqlite::types::FromSqlError",
    "rusqlite::types::FromSqlResult",
    "rusqlite::types::ToSql",
    "rusqlite::types::ToSqlOutput",
    "rusqlite::types::Type",
    "rusqlite::types::Value",
    "rusqlite::types::ValueRef",
];

/// The lint's pass: what the crate being compiled resolves SQLite's items to,
/// the homes, and the bodies that already hold a finding.
pub(crate) struct SqliteOutsideItsHomes {
    workspace: Option<&'static WorkspacePolicy>,
    homes: &'static [String],
    raw_handles: Vec<(DefId, &'static str)>,
    value_conversions: FxHashSet<DefId>,
    /// The bodies, by their owner, that hold a path that named SQLite.
    named: FxHashSet<LocalDefId>,
    /// The spans of the paths already looked at.
    seen_paths: FxHashSet<Span>,
}

impl_lint_pass!(SqliteOutsideItsHomes => [SQLITE_OUTSIDE_ITS_HOMES]);

impl SqliteOutsideItsHomes {
    pub(crate) fn new(tcx: TyCtxt<'_>) -> Self {
        let workspace = workspace_policy(tcx);
        let homes = workspace.map_or(&[][..], |workspace| workspace.policy.database_homes());
        let raw_handles = RAW_HANDLES
            .iter()
            .flat_map(|&(path, kind)| {
                lookup_path_str(tcx, PathNS::Type, path).into_iter().map(move |id| (id, kind))
            })
            .collect();
        let value_conversions = VALUE_CONVERSIONS
            .iter()
            .flat_map(|path| lookup_path_str(tcx, PathNS::Type, path))
            .collect();
        Self {
            workspace,
            homes,
            raw_handles,
            value_conversions,
            named: FxHashSet::default(),
            seen_paths: FxHashSet::default(),
        }
    }

    /// The homes, as findings name them, when the code at `id`, written at
    /// `span`, answers to the lint: production code outside the homes, in a
    /// workspace whose policy names the database's capability.
    fn answers(&self, cx: &LateContext<'_>, id: HirId, span: Span) -> Option<String> {
        let workspace = self.workspace?;
        if self.homes.is_empty() {
            return None;
        }
        let path = workspace.relative_path(cx.tcx, span)?;
        let in_home = self.homes.iter().any(|home| path.starts_with(home.as_str()));
        (!in_home && !workspace.is_test_code(cx.tcx, id)).then(|| describe_homes(self.homes))
    }

    /// Which raw handle `id` is, if it is one, or builds, as a struct's
    /// constructor does.
    fn raw_handle(&self, tcx: TyCtxt<'_>, id: DefId) -> Option<&'static str> {
        let id = match tcx.def_kind(id) {
            DefKind::Ctor(..) => tcx.parent(id),
            _ => id,
        };
        self.raw_handles.iter().find(|(handle, _)| *handle == id).map(|&(_, kind)| kind)
    }

    /// The raw handle a value of type `ty` holds, if any: in its own type or
    /// in the types it is made of, but not in the fields of another type,
    /// which is that type's owner's to keep.
    fn held_handle(&self, tcx: TyCtxt<'_>, ty: Ty<'_>) -> Option<&'static str> {
        ty.walk().find_map(|part| match part.kind() {
            GenericArgKind::Type(part) => match part.kind() {
                ty::Adt(adt, _) => self.raw_handle(tcx, adt.did()),
                _ => None,
            },
            _ => None,
        })
    }

    /// Whether `id`, an item of one of SQLite's crates, is a value conversion
    /// or part of one: a variant, a method or an associated item.
    fn is_value_conversion(&self, tcx: TyCtxt<'_>, id: DefId) -> bool {
        let mut current = Some(id);
        while let Some(id) = current {
            if self.value_conversions.contains(&id) {
                return true;
            }
            current = match tcx.def_kind(id) {
                DefKind::Impl { .. } => tcx
                    .type_of(id)
                    .instantiate_identity()
                    .skip_norm_wip()
                    .ty_adt_def()
                    .map(|adt| adt.did()),
                DefKind::Mod => None,
                _ => tcx.opt_parent(id),
            };
        }
        false
    }
}

/// Whether `path` starts with one of SQLite's crates, under the crate's own
/// name or one an import gives it.
fn names_sqlite_crate(tcx: TyCtxt<'_>, path: &Path<'_>) -> bool {
    // An import's last part resolves in the path's own `res`, once for each
    // namespace the import names.
    let first = match path.segments {
        [_] => path.res,
        [first, ..] => first.res,
        [] => return false,
    };
    first.opt_def_id().is_some_and(|first| {
        first.is_crate_root()
            && SQLITE_CRATES.iter().any(|name| tcx.crate_name(first.krate).as_str() == *name)
    })
}

impl<'tcx> LateLintPass<'tcx> for SqliteOutsideItsHomes {
    fn check_path(&mut self, cx: &LateContext<'tcx>, path: &Path<'tcx>, id: HirId) {
        let Res::Def(_, named) = path.res else {
            return;
        };
        // An import that names an item in two namespaces, as a unit
        // struct's type and constructor, names it once.
        if !self.seen_paths.insert(path.span) {
            return;
        }
        let what = if let Some(kind) = self.raw_handle(cx.tcx, named) {
            format!("a raw SQLite {kind}")
        } else if names_sqlite_crate(cx.tcx, path) && !self.is_value_conversion(cx.tcx, named) {
            "SQLite's own crate".to_string()
        } else {
            return;
        };
        let Some(homes) = self.answers(cx, id, path.span) else {
            return;
        };
        self.named.insert(id.owner.def_id);
        cx.emit_span_lint(
            SQLITE_OUTSIDE_ITS_HOMES,
            path.span,
            Finding {
                message: format!(
                    "this code names `{}`, {what}, outside the SQLite capability's homes, {homes}",
                    cx.tcx.def_path_str(named)
                ),
                help: "ask the database's owner to run the work: only the SQLite capability's \
                       homes reach SQLite directly",
            },
        );
    }

    fn check_body_post(&mut self, cx: &LateContext<'tcx>, body: &Body<'tcx>) {
        let owner = cx.tcx.hir_body_owner_def_id(body.id());
        // A closure's types are its typeck root's, which reports them.
        if cx.tcx.is_typeck_child(owner.to_def_id()) || self.named.contains(&owner) {
            return;
        }
        let typeck = cx.tcx.typeck(owner);
        let mut first: Option<(Span, HirId, Ty<'tcx>, &'static str)> = None;
        for (local_id, &ty) in typeck.node_types().items_in_stable_order() {
            let id = HirId { owner: typeck.hir_owner, local_id };
            let Some(kind) = self.held_handle(cx.tcx, ty) else {
                continue;
            };
            let span = cx.tcx.hir_span(id).source_callsite();
            if first.is_none_or(|(earliest, ..)| span.lo() < earliest.lo()) {
                first = Some((span, id, ty, kind));
            }
        }
        let Some((span, id, ty, kind)) = first else {
            return;
        };
        let Some(homes) = self.answers(cx, id, span) else {
            return;
        };
        cx.tcx.emit_node_span_lint(
            SQLITE_OUTSIDE_ITS_HOMES,
            id,
            span,
            Finding {
                message: format!(
                    "this code holds a raw SQLite {kind}, `{ty}`, outside the SQLite capability's \
                     homes, {homes}"
                ),
                help: "ask the database's owner to run the work: only the SQLite capability's \
                       homes hold a raw handle",
            },
        );
    }
}
