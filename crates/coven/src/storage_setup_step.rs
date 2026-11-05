//! The steps of setting up storage, which a host draws instead of a bare
//! spinner (api.md E1, E5).

use coven_database::LoadStep;
use coven_foundation::progress::{Progress, ProgressReport};
use std::future::Future;

/// A step of setting up storage, in the order they run. Which steps run
/// depends on the provider and on whether setup creates the store. A step
/// other than `Loading` is one unit, done when it ends. Setup can be
/// cancelled until `CreatingStore` or `RecordingAccess` begins, its point of
/// no return (§11.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageSetupStep {
    /// S3, creating the store where member keys reach a whole bucket:
    /// listing the bucket's root to check it holds nothing but this store
    /// (§20.7).
    CheckingBucket,
    /// S3: probing the service with fresh objects for the settings the
    /// location needs, and checking them against what it does (§20.8).
    DetectingSettings,
    /// iCloud, creating the store: reading the account and saving the
    /// store's zone.
    PreparingZone,
    /// Dropbox or OneDrive: reading the signed-in account's name.
    ReadingAccount,
    /// Connecting, then checking the provider with a fresh sealed object it
    /// must create once, read back and delete.
    Connecting,
    /// Creating the store, past the point of no return: claiming the bucket
    /// where the location needs it (§20.7), then slot 1, which publishes it.
    CreatingStore,
    /// Reconnecting: opening this member's store keys and loading what other
    /// devices published, each of loading's steps with its count (§13.4).
    Loading(LoadStep),
    /// Reconnecting with changed provider access, past the point of no
    /// return: publishing it as `Set access`.
    RecordingAccess,
}

/// Run `work` as the one unit of `step`, done when it succeeds.
pub(crate) async fn single<T, E>(
    progress: &mut ProgressReport<StorageSetupStep>,
    step: StorageSetupStep,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    progress.begin(step, 1);
    let output = work.await?;
    progress.done(1);
    Ok(output)
}

/// A load's progress, as the part of setup it is.
pub(crate) fn loading(progress: Progress<LoadStep>) -> Progress<StorageSetupStep> {
    Progress {
        step: StorageSetupStep::Loading(progress.step),
        done: progress.done,
        total: progress.total,
    }
}
