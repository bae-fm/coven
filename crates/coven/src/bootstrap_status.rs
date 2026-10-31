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
    /// checks report their scans too. Keeping keys and credentials follows
    /// the last.
    Loading(Progress<coven_database::LoadStep>),
    /// The join waits on these first unmet conditions, the records
    /// `pending()` returns once the store is open (§19.1).
    Pending(Vec<coven_sync::PendingRecord>),
}

/// Open a new installation's database, reporting the open's steps as
/// `Opening` (E10). Its first open runs every migration from the start.
pub(super) async fn open_installation(
    database: coven_database::DatabaseBuilder,
    status: &impl Fn(BootstrapStatus),
) -> CovenResult<Database> {
    let progress = database.subscribe_progress();
    coven_foundation::progress::forward(progress, database.open(), |progress| {
        status(BootstrapStatus::Opening(progress))
    })
    .await
}

/// Run `work`, in which `sync` may load the store into the new installation,
/// reporting loading's steps as `Loading` (E10): a restore's unlock, or the
/// approval check in which a join's keys first open.
pub(super) async fn loading<T>(
    sync: &SequenceSync,
    work: impl std::future::Future<Output = T>,
    status: &impl Fn(BootstrapStatus),
) -> T {
    coven_foundation::progress::forward(sync.subscribe_progress(), work, |progress| {
        status(BootstrapStatus::Loading(progress))
    })
    .await
}
