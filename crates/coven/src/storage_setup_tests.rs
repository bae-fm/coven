use super::*;
use coven_storage::{test_utils::MemoryStorage, ObjectPath, Storage};
use std::num::NonZeroU64;

fn builder(app: &Coven, storage: &Arc<MemoryStorage>) -> CovenBuilder {
    app.builder()
        .storage_connector(storage.clone())
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)",
        )])
}

/// Ana's phone, setting up the Household store on S3.
struct Phone {
    app: Coven,
    storage: Arc<MemoryStorage>,
    directory: StoreDir,
    handle: CovenHandle,
    _root: tempfile::TempDir,
}

impl Phone {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(root.path().into());
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
        let handle = builder(&app, &storage).open(directory.id(), |_| ()).await.unwrap();
        Self { app, storage, directory, handle, _root: root }
    }

    /// Close the store and open it again, as after the app was killed: what
    /// this session held in memory is gone.
    async fn reopen(&mut self) {
        self.handle.close().await.unwrap();
        self.handle =
            builder(&self.app, &self.storage).open(self.directory.id(), |_| ()).await.unwrap();
    }

    fn setup(&self) -> impl std::future::Future<Output = Result<StorageSetup, StorageSetupError>> {
        let (handle, config) = (self.handle.clone(), self.storage.config());
        async move {
            handle
                .setup_s3_storage(
                    config,
                    "Ana's phone",
                    "member-key".into(),
                    SecretText::new("secret".into()),
                    |_| {},
                )
                .await
        }
    }

    /// The setups `coven_operations` lists, with their outcomes.
    async fn setups(&self) -> Vec<(OperationId, Option<OperationOutcome>)> {
        self.handle
            .read(|sql| {
                Ok(sql.query(
                    "SELECT operation,kind,outcome FROM coven_operations ORDER BY operation",
                    [],
                    |row| {
                        Ok((
                            OperationId(row.get(0)?),
                            row.get::<_, OperationKind>(1)?,
                            row.get::<_, Option<OperationOutcome>>(2)?,
                        ))
                    },
                )?)
            })
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, kind, _)| *kind == OperationKind::StorageSetup)
            .map(|(operation, _, outcome)| (operation, outcome))
            .collect()
    }

    /// The one setup listed, once it has ended.
    async fn ended(&self) -> OperationOutcome {
        let setups = self.setups().await;
        assert_eq!(setups.len(), 1, "one setup is listed: {setups:?}");
        let ending = self.handle.operation_outcome(setups[0].0);
        tokio::time::timeout(coven_foundation::clock::HANG_GUARD, ending)
            .await
            .expect("the setup ends")
            .unwrap()
    }

    /// Whether the setup's own credentials are still kept beside the
    /// committed ones.
    fn candidate_kept(&self) -> bool {
        self.app.store_keychain(self.directory.id()).setup_credentials().unwrap().is_some()
    }

    /// Whether slot 1, which creates the store, is in storage.
    async fn created(&self) -> bool {
        let slot = ObjectPath::slot(self.directory.id(), NonZeroU64::new(1).unwrap());
        self.storage.status(&slot).await.unwrap().is_some()
    }

    /// Stop the next setup at `at`, as the app being killed there would.
    fn crash_at(&self, at: SetupStop) {
        self.handle.test_setup_hook(SetupHook::Crash(at));
    }

    /// Hold the next setup at `at` until the returned sender resumes it,
    /// once the returned receiver hears it got there.
    fn pause_at(
        &self,
        at: SetupStop,
    ) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        self.handle.test_setup_hook(SetupHook::Pause { at, entered, resume: resumed });
        (reached, resume)
    }
}

/// Ana's phone is killed while setup checks Household's provider, before
/// anything is claimed: right after the journal, after detection, after the
/// provider check, or once it kept the store key. Reopening undoes the setup
/// and says so: no location, no store key, none of setup's credentials,
/// nothing created, and `coven_operations` lists it undone. Setting up again
/// then works.
#[tokio::test]
async fn a_setup_cut_before_its_point_of_no_return_is_undone_as_the_store_opens() {
    for at in [SetupStop::Journaled, SetupStop::Detected, SetupStop::Checked, SetupStop::KeyKept] {
        let mut phone = Phone::new().await;
        phone.crash_at(at);
        phone.setup().await.unwrap_err();
        phone.reopen().await;

        assert_eq!(phone.ended().await, OperationOutcome::Undone(UndoCause::Interrupted), "{at:?}");
        assert!(!phone.candidate_kept(), "{at:?}");
        assert_eq!(phone.handle.storage_config().await.unwrap(), None, "{at:?}");
        assert!(!phone.created().await, "{at:?}");
        assert_eq!(phone.handle.store_key_state().unwrap(), StoreKeyState::Locked, "{at:?}");

        phone.setup().await.unwrap();
        assert!(phone.created().await, "{at:?}");
        phone.handle.close().await.unwrap();
    }
}

/// Ana's phone is killed once setup has passed its point of no return:
/// before it claims anything, after slot 1 landed, after it committed the
/// connection, or after it ended its journal row. Reopening shows the setup
/// running, unless it had ended; starting sync finishes it, and Household
/// is set up as if nothing had happened.
#[tokio::test]
async fn a_setup_cut_past_its_point_of_no_return_finishes_once_sync_starts() {
    for at in [SetupStop::Claiming, SetupStop::Created, SetupStop::Committed, SetupStop::Finished] {
        let mut phone = Phone::new().await;
        phone.crash_at(at);
        phone.setup().await.unwrap_err();
        phone.reopen().await;

        let setups = phone.setups().await;
        assert_eq!(setups.len(), 1, "{at:?}");
        if at != SetupStop::Finished {
            assert_eq!(setups[0].1, None, "{at:?}: still running");
        }
        phone.handle.start_sync().await.unwrap();
        assert_eq!(phone.ended().await, OperationOutcome::Landed(Slot(1)), "{at:?}");
        assert!(phone.created().await, "{at:?}");
        assert_eq!(phone.handle.storage_config().await.unwrap(), Some(phone.storage.config()));
        assert!(!phone.candidate_kept(), "{at:?}");
        phone.handle.restore_code().await.unwrap();
        phone.handle.close().await.unwrap();
    }
}

/// Ana taps Cancel while setup checks the provider. Setup undoes what it
/// wrote and fails with `Cancelled`; `coven_operations` lists it cancelled,
/// and nothing was created or kept.
#[tokio::test]
async fn cancelling_before_the_point_of_no_return_undoes_setup() {
    let phone = Phone::new().await;
    let (reached, resume) = phone.pause_at(SetupStop::Checked);
    let setup = tokio::spawn(phone.setup());
    reached.await.unwrap();

    assert_eq!(phone.handle.cancel_storage_setup(), SetupCancellation::Undoing);
    resume.send(()).unwrap();

    assert!(matches!(setup.await.unwrap(), Err(StorageSetupError::Cancelled)));
    assert_eq!(phone.ended().await, OperationOutcome::Cancelled);
    assert!(!phone.created().await);
    assert!(!phone.candidate_kept());
    assert_eq!(phone.handle.storage_config().await.unwrap(), None);
    assert_eq!(phone.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    phone.handle.close().await.unwrap();
}

/// Ana taps Cancel once setup has passed its point of no return. The cancel
/// says setup is finishing, and it does.
#[tokio::test]
async fn cancelling_past_the_point_of_no_return_lets_setup_finish() {
    let phone = Phone::new().await;
    assert_eq!(phone.handle.cancel_storage_setup(), SetupCancellation::NoSetup);
    let (reached, resume) = phone.pause_at(SetupStop::Claiming);
    let setup = tokio::spawn(phone.setup());
    reached.await.unwrap();

    assert_eq!(phone.handle.cancel_storage_setup(), SetupCancellation::Finishing);
    resume.send(()).unwrap();

    setup.await.unwrap().unwrap();
    assert_eq!(phone.ended().await, OperationOutcome::Landed(Slot(1)));
    assert!(phone.created().await);
    phone.handle.close().await.unwrap();
}

/// Ana's app stops waiting for setup once it has passed its point of no
/// return. Setup keeps going to its end.
#[tokio::test]
async fn a_dropped_setup_keeps_going() {
    let phone = Phone::new().await;
    let (reached, resume) = phone.pause_at(SetupStop::Claiming);
    let setup = tokio::spawn(phone.setup());
    reached.await.unwrap();
    setup.abort();
    assert!(setup.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();

    assert_eq!(phone.ended().await, OperationOutcome::Landed(Slot(1)));
    assert!(phone.created().await);
    assert_eq!(phone.handle.storage_config().await.unwrap(), Some(phone.storage.config()));
    phone.handle.close().await.unwrap();
}

/// Ana's phone lost its credentials and reconnects Household, and the app is
/// killed past the point of no return. Starting sync finishes the reconnect
/// with the credentials setup was given.
#[tokio::test]
async fn a_reconnect_cut_past_its_point_of_no_return_finishes_once_sync_starts() {
    let mut phone = Phone::new().await;
    phone.setup().await.unwrap();
    phone.handle.close().await.unwrap();
    phone.app.lose_storage_credentials(phone.directory.id()).unwrap();
    phone.handle =
        builder(&phone.app, &phone.storage).open(phone.directory.id(), |_| ()).await.unwrap();
    let first = phone.setups().await[0].0;
    phone.handle.write(move |sql| Ok(sql.acknowledge_operation(first)?)).await.unwrap();

    phone.crash_at(SetupStop::Claiming);
    phone.setup().await.unwrap_err();
    phone.reopen().await;
    phone.handle.start_sync().await.unwrap();

    assert_eq!(phone.ended().await, OperationOutcome::Satisfied);
    assert!(phone
        .app
        .store_keychain(phone.directory.id())
        .storage_credentials()
        .unwrap()
        .is_some());
    assert!(!phone.candidate_kept());
    phone.handle.close().await.unwrap();
}
