use super::*;

pub(crate) fn history_cut_references(cut: &StoreHistoryCut) -> Vec<StoreBatchCommitRef> {
    cut.0.values().cloned().collect()
}

pub(crate) fn commit_predecessor_references(commit: &StoreBatchCommit) -> Vec<StoreBatchCommitRef> {
    commit
        .order
        .predecessor
        .iter()
        .chain(commit.order.dependencies.values())
        .cloned()
        .collect()
}
