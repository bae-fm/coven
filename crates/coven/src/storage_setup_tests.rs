use super::*;
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    ObjectPath, ObjectPrefix, Storage,
};
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
                Ok::<_, CovenError>(sql.query(
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
        // Setup deletes its own credentials after its row ends, before the
        // connection attempt that finished it parks.
        phone.handle.test_parked().await;
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

/// Ana taps Cancel right after starting setup, before it has journaled
/// itself. The cancel is kept: setup undoes itself as soon as it has
/// journaled, fails with `Cancelled`, and `coven_operations` lists it
/// cancelled, with nothing created or kept.
#[tokio::test]
async fn cancelling_before_setup_journals_itself_undoes_it() {
    let phone = Phone::new().await;
    let (reached, resume) = phone.pause_at(SetupStop::Called);
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

/// Ana's app starts setup twice: the second call is refused while the first
/// runs, so a cancel reaches the setup the app started, and the first
/// finishes.
#[tokio::test]
async fn a_second_setup_call_is_refused_while_one_runs() {
    let phone = Phone::new().await;
    let (reached, resume) = phone.pause_at(SetupStop::Called);
    let setup = tokio::spawn(phone.setup());
    reached.await.unwrap();

    assert!(matches!(phone.setup().await, Err(StorageSetupError::SetupRunning)));
    resume.send(()).unwrap();

    setup.await.unwrap().unwrap();
    assert_eq!(phone.ended().await, OperationOutcome::Landed(Slot(1)));
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
    phone
        .handle
        .write(move |sql| Ok::<_, CovenError>(sql.acknowledge_operation(first)?))
        .await
        .unwrap();

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
    // Setup deletes its own credentials after its row ends, before the
    // connection attempt that finished it parks.
    phone.handle.test_parked().await;
    assert!(!phone.candidate_kept());
    phone.handle.close().await.unwrap();
}

/// Run the phone's setup until it is held at `at`, then `act`, then let it
/// go on, returning its result.
async fn setup_acting_at(
    phone: &Phone,
    at: SetupStop,
    act: impl std::future::Future<Output = ()>,
) -> Result<StorageSetup, StorageSetupError> {
    let (reached, resume) = phone.pause_at(at);
    let setup = phone.setup();
    tokio::pin!(setup);
    tokio::select! {
        result = &mut setup => panic!("setup ended before {at:?}: {result:?}"),
        reached = reached => reached.unwrap(),
    }
    act.await;
    resume.send(()).unwrap();
    setup.await
}

/// The slot numbers storage holds for the phone's store.
async fn stored_slots(phone: &Phone) -> Vec<u64> {
    let objects = phone.storage.list(&ObjectPrefix::all(phone.directory.id())).await.unwrap();
    objects.iter().filter_map(|object| object.path.slot_number()).map(|slot| slot.0).collect()
}

/// Ana's first setup stores slot 1 but loses the reply. Setup keeps that
/// claim's path and bytes and settles it: Household is created once, with
/// Ana its one member, and no other store is said to hold slot 1 (E5).
#[tokio::test]
async fn a_setup_whose_slot_1_reply_is_lost_settles_that_claim() {
    let phone = Phone::new().await;
    let store = phone.directory.id();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (go, held) = tokio::sync::oneshot::channel();
    let losing = async {
        // The first creation in the store's folder past the point of no
        // return is slot 1: its reply is lost once it is stored.
        phone.storage.hold_next_creation(ObjectPrefix::all(store), entered, held).await;
    };
    let result = {
        let setup = setup_acting_at(&phone, SetupStop::Claiming, losing);
        tokio::pin!(setup);
        tokio::select! {
            result = &mut setup => panic!("slot 1 was never created: {result:?}"),
            entered = entering => entered.unwrap(),
        }
        phone.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
        go.send(()).unwrap();
        setup.await
    };
    assert!(
        !matches!(&result, Err(StorageSetupError::Storage(error))
            if error.failure() == StorageFailure::StoreExists),
        "{result:?}"
    );
    if result.is_err() {
        phone.handle.start_sync().await.unwrap();
    }
    assert_eq!(phone.ended().await, OperationOutcome::Landed(coven_merge::Slot(1)));
    assert!(phone.created().await);
    let members = phone.handle.members().await.unwrap();
    assert_eq!(members.len(), 1, "{members:?}");
    assert!(members[0].is_self);
    phone.handle.close().await.unwrap();
}

/// Ana's first setup, past its point of no return, finds the bucket claimed
/// for another store as it claims it: the claim is refused with
/// `OwnLocationRequired`, and setup undoes what it made, keeping no store
/// key, location or credentials, claiming no slot and listing itself undone
/// (E5, §11.6, §20.3).
#[tokio::test]
async fn a_setup_whose_claim_is_refused_undoes_what_it_made() {
    let phone = Phone::new().await;
    let taking = async {
        phone.storage.plant_location_marker(Some(b"another store's id".to_vec())).await;
    };
    let result = setup_acting_at(&phone, SetupStop::Claiming, taking).await;
    assert!(own_location_required(result.as_ref().unwrap_err()), "{result:?}");
    assert_eq!(phone.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(phone.handle.storage_config().await.unwrap().is_none());
    let custody = phone.app.store_keychain(phone.directory.id());
    assert!(custody.storage_credentials().unwrap().is_none());
    assert!(!phone.candidate_kept());
    assert!(matches!(phone.ended().await, OperationOutcome::Undone(_)));
    assert!(stored_slots(&phone).await.is_empty());
    phone.handle.close().await.unwrap();
}

/// The first setup registers the device under the name it was given; a
/// reconnect after this device lost its credentials keeps that name and
/// claims nothing (E5).
#[tokio::test]
async fn setup_names_the_first_device_and_a_reconnect_keeps_its_name() {
    let mut phone = Phone::new().await;
    phone.setup().await.unwrap();
    let names = |members: Vec<MemberInfo>| -> Vec<String> {
        members.into_iter().flat_map(|member| member.devices).map(|device| device.name).collect()
    };
    assert_eq!(names(phone.handle.members().await.unwrap()), ["Ana's phone"]);
    let slots = stored_slots(&phone).await;

    phone.handle.close().await.unwrap();
    phone.app.lose_storage_credentials(phone.directory.id()).unwrap();
    phone.handle =
        builder(&phone.app, &phone.storage).open(phone.directory.id(), |_| ()).await.unwrap();
    phone
        .handle
        .setup_s3_storage(
            phone.storage.config(),
            "Another name",
            "member-key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(names(phone.handle.members().await.unwrap()), ["Ana's phone"]);
    assert_eq!(stored_slots(&phone).await, slots);
    phone.handle.close().await.unwrap();
}

// Setting up a store in a Dropbox shared folder or a OneDrive folder, which
// every member reaches whole, so each holds one store; the Dropbox App
// folder, which nobody else reaches, holds several (§20.3, E5).

fn own_location_required(error: &StorageSetupError) -> bool {
    matches!(
        error,
        StorageSetupError::Storage(source)
            if source.failure() == StorageFailure::OwnLocationRequired
    )
}

struct Opened {
    handle: CovenHandle,
    directory: StoreDir,
    _sign_in: crate::authentication::SignIn,
    _root: tempfile::TempDir,
}

/// A new store on its own device, signed in to `storage`'s provider.
async fn open(storage: Arc<MemoryStorage>) -> Opened {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
    let sign_in = crate::authentication::SignIn::new().await;
    let mut builder = sign_in.configure(builder(&app, &storage));
    builder.authenticate(storage.config().oauth_access().unwrap()).await.unwrap();
    let handle = builder.open(directory.id(), |_| ()).await.unwrap();
    Opened { handle, directory, _sign_in: sign_in, _root: root }
}

async fn setup(
    opened: &Opened,
    location: StorageConfig,
) -> Result<StorageSetup, StorageSetupError> {
    opened.handle.setup_oauth_storage(location, "Laptop", |_| {}).await
}

/// Ana creates Household in her shared folder; Ben's Garden is then refused
/// there before setup writes anything, and Ana's reconnect reads the marker.
#[tokio::test]
async fn a_store_is_created_only_in_a_folder_of_its_own() {
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        let storage = Arc::new(MemoryStorage::builder().provider(provider).build().unwrap());
        let location = storage.config();
        let household = open(storage.clone()).await;
        setup(&household, location.clone()).await.unwrap();
        let garden = open(storage.clone()).await;
        // Setup starts Household's sync; its first pass's requests come first.
        household.handle.test_parked().await;
        let before = storage.request_count();
        let error = setup(&garden, location.clone()).await.unwrap_err();
        assert!(own_location_required(&error), "{provider:?}: {error:?}");
        assert_eq!(storage.request_count(), before, "{provider:?}");
        assert_eq!(coven_storage::recorded_config(&garden.directory).unwrap(), None);
        setup(&household, location).await.unwrap();
        household.handle.close().await.unwrap();
        garden.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn reconnect_refuses_a_folder_marker_naming_another_store() {
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        let storage = Arc::new(MemoryStorage::builder().provider(provider).build().unwrap());
        let location = storage.config();
        let opened = open(storage.clone()).await;
        setup(&opened, location.clone()).await.unwrap();
        let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
        storage.plant_location_marker(Some(other.to_string().into_bytes())).await;
        let error = setup(&opened, location.clone()).await.unwrap_err();
        assert!(own_location_required(&error), "{provider:?}: {error:?}");
        opened.handle.close().await.unwrap();
    }
}

/// Ana keeps Household and Recipes, neither shared, in her Dropbox App folder.
#[tokio::test]
async fn the_app_folder_holds_several_stores() {
    let location = StorageConfig::Dropbox {
        account_label: None,
        location: coven_storage::DropboxLocation::AppFolder,
        folder_label: None,
    };
    let storage = Arc::new(MemoryStorage::builder().location(location.clone()).build().unwrap());
    let household = open(storage.clone()).await;
    let recipes = open(storage.clone()).await;
    setup(&household, location.clone()).await.unwrap();
    setup(&recipes, location.clone()).await.unwrap();
    setup(&household, location).await.unwrap();
    household.handle.close().await.unwrap();
    recipes.handle.close().await.unwrap();
}
