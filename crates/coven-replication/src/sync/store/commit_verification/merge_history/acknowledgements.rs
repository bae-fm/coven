use super::*;

impl MergeHistoryVerifier<'_> {
    /// Prove that every advance between two installed cuts only reports an
    /// observation. The cuts come from installed materialization; load their
    /// exact commits through the verifier, including local publications made
    /// during this writer turn. At the installed snapshot boundary, its latest
    /// non-acknowledgement per author proves whether an observation changed.
    /// This classifies already verified cuts; it does not authenticate a new
    /// historical reference by comparing sequence numbers.
    pub(crate) async fn history_has_only_acknowledgements(
        &mut self,
        previous: &StoreHistoryCut,
        current: &StoreHistoryCut,
    ) -> Result<bool, StorePullError> {
        if !current.frontier().covers(&previous.frontier()) {
            return Ok(false);
        }
        for (stream, tip) in &current.0 {
            let known = previous.0.get(stream);
            let mut cursor = tip.clone();
            loop {
                if known == Some(&cursor) {
                    break;
                }
                if known.is_some_and(|known| cursor.coord.sequence() <= known.coord.sequence()) {
                    return Ok(false);
                }
                if self.history.superseded(&cursor) {
                    let baseline = &self.history.baseline;
                    let Some(opened) = baseline.history_summary() else {
                        return Err(StorePullError::InvalidState(
                            "retired acknowledgement interval has no verified summary".into(),
                        ));
                    };
                    // The summary describes this exact boundary. A requested
                    // older end cut cannot be classified using later work.
                    if baseline.coverage().0.get(stream) != Some(&cursor) {
                        return Ok(false);
                    }
                    if let Some(meaningful) =
                        opened.summary.last_non_acknowledgement_commits.get(stream)
                    {
                        let Some(known) = known else {
                            return Ok(false);
                        };
                        if meaningful.coord.sequence() > known.coord.sequence()
                            || (meaningful.coord == known.coord && meaningful != known)
                        {
                            return Ok(false);
                        }
                    }
                    break;
                }
                let verified = self.load_ref(&cursor).await?;
                let commit = verified.value();
                if !commit
                    .operations()
                    .is_some_and(store_commit::StoreCommitOperations::is_acknowledgement_only)
                {
                    return Ok(false);
                }
                match commit.order.predecessor() {
                    Some(predecessor) => cursor = predecessor.clone(),
                    None if known.is_none() => break,
                    None => return Ok(false),
                }
            }
        }
        Ok(true)
    }
}
