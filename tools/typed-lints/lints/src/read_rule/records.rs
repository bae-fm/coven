//! The read rule across crates. A compile finds the read code of its own
//! crate, but a function that other crates can call may be read code only
//! for them, when a later crate's read code calls it. So each crate's
//! compile writes a record beside its metadata: for each function other
//! crates can call that no read code of the crate reaches, what the read
//! code lints would find if it were read code, in the code it reaches in
//! the crate that no read code reaches, and what the earlier crates'
//! records hold for the functions that code calls. A later crate's read
//! code that calls or passes on such a function gets each of those findings
//! at the call, under the finding's own lint, so a mark there covers it.
//!
//! A record names its crate's stable id, which rustc derives from the
//! crate's name and its build's settings, and each function by the hash of
//! its definition's path, which rustc gives the same in every compile that
//! sees it. A compile that needs an earlier crate's record stops when the
//! record is missing or names another crate's id: the findings would go
//! unchecked or be another build's. Every compile of a crate through the
//! driver writes the record, during analysis and so before its metadata,
//! and cargo compiles a crate again when its last compile went through
//! another wrapper, so the record cargo finds beside a crate's metadata is
//! that compile's.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

use rustc_data_structures::fx::{FxHashMap, FxHashSet};
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{CrateNum, DefId, LOCAL_CRATE, LocalDefId};
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{Expr, ExprKind, HirId};
use rustc_lint::Lint;
use rustc_middle::hir::nested_filter::OnlyBodies;
use rustc_middle::ty::{TyCtxt, TypeckResults, TypingEnv};
use rustc_session::config::{OutFileName, OutputType};
use rustc_span::Span;

use super::{READ_RULE_LINTS, Search, Setup, resolve_callee};
use crate::finding::Finding;

/// One finding a record holds: the lint, its message as the crate's own
/// compile words it, and where it is, as `file:line:column` with the file
/// relative to the workspace's root.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Recorded {
    at: String,
    lint: String,
    message: String,
}

/// Where the read rule's checks put a finding: the compile's diagnostics, for
/// read code, or a record, for code only another crate's read code may run.
pub(super) struct Reporter<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    setup: &'a Setup,
    recorded: Option<RefCell<Vec<Recorded>>>,
}

impl<'a, 'tcx> Reporter<'a, 'tcx> {
    /// A reporter whose findings are the compile's diagnostics.
    pub(super) fn diagnostics(tcx: TyCtxt<'tcx>, setup: &'a Setup) -> Self {
        Self { tcx, setup, recorded: None }
    }

    /// A reporter that records its findings, for `into_recorded`.
    pub(super) fn record(tcx: TyCtxt<'tcx>, setup: &'a Setup) -> Self {
        Self { tcx, setup, recorded: Some(RefCell::default()) }
    }

    /// Reports `message` under `lint` at `span`, in the node `id`.
    pub(super) fn report(&self, lint: &'static Lint, id: HirId, span: Span, message: String) {
        let span = span.source_callsite();
        match &self.recorded {
            None => self.tcx.emit_node_span_lint(
                lint,
                id,
                span,
                Finding { message, help: self.setup.help(lint) },
            ),
            Some(recorded) => recorded.borrow_mut().push(Recorded {
                at: self.place(span),
                lint: lint.name_lower(),
                message,
            }),
        }
    }

    /// Reports what an earlier crate's record holds for `callee`, which the
    /// code of `item` calls or passes on at `span`, in the node `id`.
    fn helper(&self, item: &str, id: HirId, span: Span, callee: DefId, found: &[Recorded]) {
        let span = span.source_callsite();
        match &self.recorded {
            None => {
                let callee = self.tcx.def_path_str(callee);
                for finding in found {
                    let lint = READ_RULE_LINTS
                        .iter()
                        .copied()
                        .find(|lint| lint.name_lower() == finding.lint)
                        .unwrap_or_else(|| {
                            self.tcx.dcx().fatal(format!(
                                "typed lints: the read rule's record of `{callee}` names the \
                                 lint {}, which this driver does not have",
                                finding.lint
                            ))
                        });
                    self.tcx.emit_node_span_lint(
                        lint,
                        id,
                        span,
                        Finding {
                            message: format!(
                                "`{item}` calls `{callee}`, where {} at {}",
                                finding.message, finding.at
                            ),
                            help: self.setup.help(lint),
                        },
                    );
                }
            }
            Some(recorded) => recorded.borrow_mut().extend(found.iter().cloned()),
        }
    }

    /// Where `span` starts, as a record names it: as rustc names it when no
    /// file of the workspace holds it.
    fn place(&self, span: Span) -> String {
        let source_map = self.tcx.sess.source_map();
        let Some(file) = self.setup.workspace.relative_path(self.tcx, span) else {
            return source_map.span_to_diagnostic_string(span.shrink_to_lo());
        };
        let start = source_map.lookup_char_pos(span.lo());
        format!("{file}:{}:{}", start.line, start.col_display + 1)
    }

    /// What the reporter, made by `record`, recorded.
    pub(super) fn into_recorded(self) -> Vec<Recorded> {
        self.recorded.expect("only a reporter made by `record` records").into_inner()
    }
}

/// One crate's record: the findings of each function, by the hash of its
/// definition's path.
type CrateRecord = FxHashMap<String, Vec<Recorded>>;

/// The earlier crates' records, read as the compile needs them.
pub(super) struct Records<'tcx> {
    tcx: TyCtxt<'tcx>,
    /// Each crate's record read so far.
    read: RefCell<FxHashMap<CrateNum, CrateRecord>>,
}

impl<'tcx> Records<'tcx> {
    pub(super) fn new(tcx: TyCtxt<'tcx>) -> Self {
        Self { tcx, read: RefCell::default() }
    }

    /// What the record of `callee`'s crate holds for it.
    fn of(&self, callee: DefId) -> Option<Vec<Recorded>> {
        if !self.read.borrow().contains_key(&callee.krate) {
            let record = self.read_record(callee.krate);
            self.read.borrow_mut().insert(callee.krate, record);
        }
        self.read.borrow()[&callee.krate].get(&path_hash(self.tcx, callee)).cloned()
    }

    /// Reads the record of the earlier crate `krate`, beside the metadata
    /// the compile reads for it, or stops the compile.
    fn read_record(&self, krate: CrateNum) -> CrateRecord {
        let tcx = self.tcx;
        let name = tcx.crate_name(krate);
        let source = tcx.used_crate_source(krate);
        let Some(metadata) = source.rmeta.as_ref().or(source.rlib.as_ref()) else {
            tcx.dcx().fatal(format!(
                "typed lints: the read rule found no metadata file of the crate {name}, beside \
                 which it reads the crate's record"
            ))
        };
        let file = record_file(metadata);
        let stop = |why: &str| -> ! {
            tcx.dcx().fatal(format!(
                "typed lints: the read rule's record of the crate {name} ({}) {why}: lint {name} \
                 again with this driver, as tools/typed-lints/clippy.sh does",
                file.display()
            ))
        };
        let text = std::fs::read_to_string(&file)
            .unwrap_or_else(|error| stop(&format!("could not be read: {error}")));
        let table = text
            .parse::<toml::Table>()
            .unwrap_or_else(|error| stop(&format!("is not TOML: {error}")));
        if table.get("crate").and_then(toml::Value::as_str) != Some(crate_id(tcx, krate).as_str()) {
            stop("names another crate or another build of it");
        }
        let text_of = |value: &toml::Value, key: &str| {
            value
                .get(key)
                .and_then(toml::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| stop(&format!("has a row without its {key}")))
        };
        let list_of = |value: Option<&toml::Value>, key: &str| {
            value
                .and_then(toml::Value::as_array)
                .cloned()
                .unwrap_or_else(|| stop(&format!("has no list of {key}")))
        };
        let mut record = FxHashMap::default();
        for function in list_of(table.get("functions"), "functions") {
            let definition = text_of(&function, "definition");
            let findings = list_of(function.get("findings"), "findings")
                .iter()
                .map(|finding| Recorded {
                    at: text_of(finding, "at"),
                    lint: text_of(finding, "lint"),
                    message: text_of(finding, "message"),
                })
                .collect();
            record.insert(definition, findings);
        }
        record
    }
}

/// How a record names the crate `krate`: its stable id.
fn crate_id(tcx: TyCtxt<'_>, krate: CrateNum) -> String {
    format!("{:016x}", tcx.stable_crate_id(krate).as_u64())
}

/// How a record names the function `id`: the hash of its definition's path.
fn path_hash(tcx: TyCtxt<'_>, id: DefId) -> String {
    tcx.def_path_hash(id).0.to_hex()
}

/// The record file beside the metadata file `metadata`.
fn record_file(metadata: &Path) -> PathBuf {
    metadata.with_extension("read-rule.toml")
}

/// Writes the crate's record beside the metadata the compile writes: each
/// function of `functions` with what read code would find through it.
pub(super) fn write(tcx: TyCtxt<'_>, functions: &[(LocalDefId, Vec<Recorded>)]) {
    let OutFileName::Real(metadata) = tcx.output_filenames(()).path(OutputType::Metadata) else {
        tcx.dcx().fatal(
            "typed lints: the read rule writes the crate's record beside its metadata file, and \
             this compile writes its metadata to the standard output",
        );
    };
    let mut rows = Vec::new();
    for (function, findings) in functions {
        let findings = findings
            .iter()
            .map(|finding| {
                let mut row = toml::Table::new();
                row.insert("lint".into(), finding.lint.clone().into());
                row.insert("message".into(), finding.message.clone().into());
                row.insert("at".into(), finding.at.clone().into());
                toml::Value::Table(row)
            })
            .collect::<Vec<_>>();
        let mut row = toml::Table::new();
        row.insert("definition".into(), path_hash(tcx, function.to_def_id()).into());
        row.insert("findings".into(), toml::Value::Array(findings));
        rows.push(toml::Value::Table(row));
    }
    let mut table = toml::Table::new();
    table.insert("crate".into(), crate_id(tcx, LOCAL_CRATE).into());
    table.insert("functions".into(), toml::Value::Array(rows));
    let file = record_file(&metadata);
    let written = file.with_extension("toml.partial");
    if let Err(error) =
        std::fs::write(&written, table.to_string()).and_then(|()| std::fs::rename(&written, &file))
    {
        tcx.dcx().fatal(format!(
            "typed lints: the read rule could not write the crate's record {}: {error}",
            file.display()
        ));
    }
}

/// Reports, for the code `unit` and the closures inside it, but those only a
/// live query's `process` runs, what the earlier crates' records hold for
/// each function of theirs it calls or passes on.
pub(super) fn check_calls<'tcx>(
    search: &Search<'_, 'tcx>,
    records: &Records<'tcx>,
    unit: LocalDefId,
    process_only: &FxHashSet<LocalDefId>,
    reporter: &Reporter<'_, 'tcx>,
) {
    let tcx = search.tcx;
    let root = tcx.typeck_root_def_id(unit.to_def_id());
    let mut calls = Calls {
        search,
        records,
        reporter,
        typeck: tcx.typeck(unit),
        typing_env: TypingEnv::post_analysis(tcx, root),
        process_only,
        item: tcx.def_path_str(root),
    };
    calls.visit_expr(tcx.hir_body_owned_by(unit).value);
}

struct Calls<'a, 's, 'tcx> {
    search: &'a Search<'s, 'tcx>,
    records: &'a Records<'tcx>,
    reporter: &'a Reporter<'a, 'tcx>,
    typeck: &'tcx TypeckResults<'tcx>,
    typing_env: TypingEnv<'tcx>,
    process_only: &'a FxHashSet<LocalDefId>,
    item: String,
}

impl<'tcx> Calls<'_, '_, 'tcx> {
    /// Reports what the record of `callee`, when an earlier crate of the
    /// workspace defines it, holds for it, at `expr`, which calls or names
    /// it with the arguments of the node `args_of`.
    fn call(&self, expr: &Expr<'tcx>, callee: DefId, args_of: HirId) {
        let tcx = self.search.tcx;
        let callee = resolve_callee(tcx, self.typeck, self.typing_env, callee, args_of);
        if callee.is_local()
            || !self.search.is_workspace(callee)
            || !matches!(tcx.def_kind(callee), DefKind::Fn | DefKind::AssocFn)
        {
            return;
        }
        if let Some(found) = self.records.of(callee) {
            self.reporter.helper(&self.item, expr.hir_id, expr.span, callee, &found);
        }
    }

    /// The function the path `expr` names, if any.
    fn named(&self, expr: &Expr<'tcx>) -> Option<DefId> {
        let ExprKind::Path(ref qpath) = expr.kind else { return None };
        match self.typeck.qpath_res(qpath, expr.hir_id) {
            Res::Def(DefKind::Fn | DefKind::AssocFn, id) => Some(id),
            _ => None,
        }
    }
}

impl<'tcx> Visitor<'tcx> for Calls<'_, '_, 'tcx> {
    type NestedFilter = OnlyBodies;

    fn maybe_tcx(&mut self) -> TyCtxt<'tcx> {
        self.search.tcx
    }

    fn visit_expr(&mut self, expr: &'tcx Expr<'tcx>) {
        match expr.kind {
            ExprKind::Closure(closure) if self.process_only.contains(&closure.def_id) => {}
            ExprKind::Call(function, arguments) => {
                match self.named(function) {
                    Some(callee) => self.call(expr, callee, function.hir_id),
                    None => self.visit_expr(function),
                }
                for argument in arguments {
                    self.visit_expr(argument);
                }
            }
            ExprKind::MethodCall(..) => {
                if let Some(callee) = self.typeck.type_dependent_def_id(expr.hir_id) {
                    self.call(expr, callee, expr.hir_id);
                }
                intravisit::walk_expr(self, expr);
            }
            ExprKind::Path(_) => {
                if let Some(callee) = self.named(expr) {
                    self.call(expr, callee, expr.hir_id);
                }
            }
            _ => intravisit::walk_expr(self, expr),
        }
    }
}
