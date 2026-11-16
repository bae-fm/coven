use super::*;
use crate::app_policy::CovenPolicy;
use tokio::sync::mpsc::UnboundedReceiver;

impl Database<CovenPolicy> {
    fn checkpointer(&self) -> &WalCheckpointer<CovenPolicy> {
        self.checkpoints.as_ref().expect("the database has a checkpointer")
    }

    fn statistics(&self) -> &UpkeepThread<PlannerStatistics<CovenPolicy>> {
        self.statistics.as_ref().expect("the database keeps its planner's statistics")
    }

    /// Whether a thread of the database's own checkpoints its WAL (§4).
    pub(crate) fn checkpoints(&self) -> bool {
        self.checkpoints.is_some()
    }

    /// Run `fault` as each checkpoint takes the writer.
    pub(crate) fn fault_checkpoints(
        &self,
        fault: impl FnMut() -> Result<(), PolicyError<CovenPolicy>> + Send + 'static,
    ) {
        self.checkpointer().fault(fault);
    }

    /// Run `step` as the copy of each pass a checkpoint makes while writes
    /// go on returns, before its sync.
    pub(crate) fn after_checkpoint_copies(&self, step: impl FnMut() + Send + 'static) {
        self.checkpointer().after_copy(step);
    }

    /// What each checkpoint does at its last step from now on, told while
    /// the checkpoint still holds the writer.
    pub(crate) fn report_checkpoints(&self) -> UnboundedReceiver<crate::WalCheckpoint> {
        self.checkpointer().report_restarts()
    }

    /// Run one wake's checkpoint now, on the checkpointer's connection.
    pub(crate) fn checkpoint_now(&self) -> Result<(), PolicyError<CovenPolicy>> {
        self.checkpointer().run_now()
    }

    /// Each run of the planner's statistics from now on, told once it
    /// committed.
    pub(crate) fn report_statistics_runs(&self) -> UnboundedReceiver<()> {
        self.statistics().report_runs()
    }

    /// Told when the next caller waits for a reader.
    pub(crate) fn observe_next_reader_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        self.readers.observe_next_wait()
    }

    /// Every connection the database holds open, one at a time, each under
    /// its own lock: the readers, the upkeep threads' and the writer.
    pub(crate) fn each_connection(&self, mut visit: impl FnMut(&DatabaseConnection<CovenPolicy>)) {
        self.readers.each(|reader| visit(reader));
        let mut visited = |connection: &DatabaseConnection<CovenPolicy>| {
            visit(connection);
            Ok(())
        };
        if let Some(checkpoints) = &self.checkpoints {
            checkpoints.with_connection(&mut visited).unwrap();
        }
        if let Some(statistics) = &self.statistics {
            statistics.with_connection(&mut visited).unwrap();
        }
        self.host(|writer| visit(writer));
    }
}
