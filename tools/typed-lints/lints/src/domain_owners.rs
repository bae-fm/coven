//! Each domain of the database names the owner that does its work
//! (Appendix F.2), by its path. `clippy::domain_owner_is_no_owner` finds a
//! domain whose owner, in the crate that declares it, is no owner
//! (`owners.rs`): it holds no capability, shared state, handle or owner, so
//! nothing it holds lets it do the domain's writes.

use rustc_lint::{LateContext, declare_tool_lint};

use crate::finding::Finding;
use crate::owners::{Owners, resolve_type_ids};

declare_tool_lint! {
    /// A database domain whose owner holds nothing an owner holds.
    pub clippy::DOMAIN_OWNER_IS_NO_OWNER,
    Warn,
    "a database domain names an owner that holds no capability, shared state, handle or owner",
    report_in_external_macro: true
}

/// Reports each domain whose owner the crate declares and is no owner.
pub(crate) fn check<'tcx>(owners: &Owners<'tcx>, cx: &LateContext<'tcx>) {
    let tcx = cx.tcx;
    let domains = owners.workspace.policy.database.iter().flat_map(|database| &database.domains);
    for domain in domains {
        for owner in resolve_type_ids(tcx, "database.domains.owner", &domain.owner) {
            let Some(local) = owner.as_local() else { continue };
            if owners.is_owner(tcx.type_of(owner).instantiate_identity().skip_norm_wip()) {
                continue;
            }
            tcx.emit_node_span_lint(
                DOMAIN_OWNER_IS_NO_OWNER,
                tcx.local_def_id_to_hir_id(local),
                tcx.def_span(local),
                Finding {
                    message: format!(
                        "`{}` owns the {} domain but holds no capability, shared state, handle \
                         or owner",
                        domain.owner, domain.name
                    ),
                    help: "a domain's owner does the domain's writes with what it holds: name \
                           the type that holds the database's capability",
                },
            );
        }
    }
}
