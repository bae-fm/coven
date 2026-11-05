//! What a new device tells the app as it opens, connects and loads, so the
//! app can show that instead of a bare spinner (§13.4, E10).

use super::*;

/// Bootstrap progress (E10), before an open handle exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapStatus {
    /// Opening a store's database, with the open's steps as
    /// `CovenBuilder::open` reports them (E1): a new installation's, which
    /// runs every migration, or, when `restore_device` finds the restore
    /// code's keys already installed, this device's own.
    Opening(Progress<coven_database::OpenStep>),
    /// Accepting provider access, connecting to storage and sending a join
    /// request.
    Connecting,
    /// The join request is sent and waits for approval (§13.2).
    WaitingForApproval {
        /// The number the admin of a sent invite compares; `None` in person.
        confirmation: Option<ConfirmationNumber>,
        /// When the join checks for approval next.
        next_check: NextCheck,
    },
    /// Loading the store from storage: the scan for this member's keys when
    /// the floor holds none, the floor's snapshots, then the slots after
    /// them, each step with how much of it is done (§13.4). A waiting join's
    /// scan for its keys is part of its wait; the check in which its keys
    /// open reports that scan, then loads. Registering follows the last.
    Loading(Progress<coven_database::LoadStep>),
    /// Registering this device once the store is loaded: a restore's check
    /// of the access it reaches storage with, the device's registration,
    /// whose claim first reads the slots added since loading, and the
    /// recorded access that follows (§13.1). No count: how many slots the
    /// claim reads isn't known until it has read them.
    Registering,
    /// Keeping the member's keys and the device's credentials in custody and
    /// publishing the store, the last work before the handle opens (§13.1).
    /// A replacement's swap and its reopening, reported as `Opening`, come
    /// before it; `restore_device` also removes the installation it
    /// replaced, a folder of any size, which has no count either.
    Committing,
    /// The join waits on these first unmet conditions, the records
    /// `pending()` returns once the store is open (§19.1).
    Pending(Vec<coven_sync::PendingRecord>),
}

/// Open a new installation's database, reporting the open's steps as
/// `Opening` (E10). Its first open runs every migration from the start, and
/// makes no migration write: the store it loads replaces that state.
pub(super) async fn open_installation(
    database: coven_database::DatabaseBuilder,
    status: &impl Fn(BootstrapStatus),
) -> CovenResult<Database> {
    let database = database.loading_store();
    let progress = database.subscribe_progress();
    progress
        .forward(database.open(), |progress| status(BootstrapStatus::Opening(progress)))
        .await
        .map_err(CovenError::from)
}

/// Run `work`, in which `sync` may load the store into the new installation,
/// reporting loading's steps as `Loading` (E10): a restore's unlock, or the
/// approval check in which a join's keys first open.
pub(super) async fn loading<T>(
    sync: &SequenceSync,
    work: impl std::future::Future<Output = T>,
    status: &impl Fn(BootstrapStatus),
) -> T {
    sync.subscribe_progress()
        .forward(work, |progress| status(BootstrapStatus::Loading(progress)))
        .await
}
