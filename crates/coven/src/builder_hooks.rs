//! The hooks a test sets on a builder to stop or fail a step of what it opens.
#![cfg(test)]

use super::*;

impl CovenBuilder {
    /// Make the next replacement stop right after its swap, undoing nothing.
    pub(crate) fn interrupt_after_swap(mut self) -> Self {
        self.interrupt_after_swap = true;
        self
    }

    /// Have SQLite refuse to close the store this builder opens.
    pub(crate) fn refuse_database_close(mut self) -> Self {
        self.refuse_database_close = true;
        self
    }

    /// Have the platform refuse this store's network watch with `error`.
    pub(crate) fn refuse_network_watch(mut self, error: NetworkWatchError) -> Self {
        self.network = coven_foundation::network_watch::NetworkSource::Refused(error);
        self
    }

    /// Make the insert of the next reset's device-reset notice fail.
    pub(crate) fn fail_device_reset_notice(mut self) -> Self {
        self.fail_device_reset_notice = true;
        self
    }

    /// Make the next replacement stop right after its fresh device
    /// registered, before its swap, undoing nothing.
    pub(crate) fn interrupt_after_registering(mut self) -> Self {
        self.interrupt_after_registering = true;
        self
    }

    /// Make the next commit of a new installation's custody stop right after
    /// it writes the device id, undoing nothing.
    pub(crate) fn interrupt_commit(mut self) -> Self {
        self.interrupt_commit = true;
        self
    }

    /// Keep time by `recorder`, which records every value a bootstrap
    /// through this builder reports as it loads, values a host reading the
    /// loading watch could miss.
    pub(crate) fn record_loading(
        mut self,
        recorder: Arc<RecordingClock<coven_database::LoadStep>>,
    ) -> Self {
        self.clock = recorder.clone();
        self.loading_recorder = Some(recorder);
        self
    }
}
