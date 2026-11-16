//! Hooks a test uses to fault or watch a database's upkeep threads and
//! readers, and to reach each of its connections.

use super::*;
use tokio::sync::mpsc::UnboundedReceiver;

impl<P: Policy> Database<P> {
    fn checkpointer(&self) -> &WalCheckpointer<P> {
        self.checkpoints.as_ref().expect("the database has a checkpointer")
    }

    fn statistics(&self) -> &UpkeepThread<PlannerStatistics<P>> {
        self.statistics.as_ref().expect("the database keeps its planner's statistics")
    }

    /// Whether one of the database's own threads checkpoints its WAL.
    pub fn checkpoints(&self) -> bool {
        self.checkpoints.is_some()
    }

    /// Run `fault` as each checkpoint takes the writer.
    pub fn fault_checkpoints(
        &self,
        fault: impl FnMut() -> Result<(), PolicyError<P>> + Send + 'static,
    ) {
        self.checkpointer().fault(fault);
    }

    /// A checkpoint copies the WAL in passes while writes go on. Run `step`
    /// after each pass's copy returns, before the pass syncs.
    pub fn after_checkpoint_copies(&self, step: impl FnMut() + Send + 'static) {
        self.checkpointer().after_copy(step);
    }

    /// Return a receiver that gets what each later checkpoint does at its
    /// last step. The checkpointer sends it while the checkpoint still holds
    /// the writer.
    pub fn report_checkpoints(&self) -> UnboundedReceiver<crate::WalCheckpoint> {
        self.checkpointer().report_restarts()
    }

    /// Run one wake's checkpoint now, on the checkpointer's connection.
    pub fn checkpoint_now(&self) -> Result<(), PolicyError<P>> {
        self.checkpointer().run_now()
    }

    /// Return a receiver that gets a message after each later run of the
    /// planner's statistics commits.
    pub fn report_statistics_runs(&self) -> UnboundedReceiver<()> {
        self.statistics().report_runs()
    }

    /// Return a receiver that gets a message when the next caller waits for
    /// a reader.
    pub fn observe_next_reader_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        self.readers.observe_next_wait()
    }

    /// Run `visit` on each reader in turn, under its own lock.
    pub fn each_reader(&self, visit: impl FnMut(&mut HostSql<P>)) {
        self.readers.each(visit);
    }

    /// Run `visit` on every connection the database holds open, one at a
    /// time, each under its own lock: the readers, the upkeep threads'
    /// connections and the writer.
    pub fn each_connection(&self, mut visit: impl FnMut(&HostSql<P>)) {
        self.readers.each(|reader| visit(reader));
        if let Some(checkpoints) = &self.checkpoints {
            checkpoints.with_connection(&mut visit);
        }
        if let Some(statistics) = &self.statistics {
            statistics.with_connection(&mut visit);
        }
        self.host(|writer| visit(writer));
    }
}
