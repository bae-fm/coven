//! A new device asking one of the person's devices to pair it: the request it
//! sends with a pair kept only in memory, the wait for the handover, and the
//! restore it continues as once the handover opens (§13.2).

use super::*;
use coven_format::codes::PairingCode;
use coven_storage::Storage;
use coven_sync::{ApprovalWaits, JoinPoll, PairingAsk, PairingCheck, RetryWaits, SequenceState};

/// Ask one of the person's devices, which shows `code`, to pair this device,
/// and restore from what it hands over, returning the open store (`None` when
/// the pairing ended before its handover opened). A restore's rules hold from
/// its record on; before it, cancelling through `control`, dropping or
/// failing the call removes the installation, whether or not the request was
/// sent, since approval binds nothing in history (§13.2).
///
/// S3 requires a key: the one the QR code carries, or else `s3_credentials`,
/// never both. A store whose unfinished installation holds a join's saved
/// code or a restore's record is refused with `StoreLockError::BootstrapPending`
/// before anything changes (§13.4).
pub async fn join_with_pairing(
    builder: CovenBuilder,
    code: &str,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    control: &JoinControl,
) -> Result<Option<CovenHandle>, BootstrapError> {
    let cancel = control.cancel.subscribe();
    check_cancel(&cancel)?;
    let code = coven_sync::read_pairing_code(code)?;
    let storage = RestoreStorage::decode(code.storage.as_bytes()).map_err(SyncError::from)?;
    let (data, sign_in) = restore_connection(&builder, storage, s3_credentials)?;
    let sign_in = sign_in.map_or(SignInCustody::None, SignInCustody::Session);
    Box::pin(bootstrap_device(
        builder,
        Start::Pair(Pair {
            code,
            data,
            sign_in,
            name: device_name.into(),
            held: control.often.subscribe(),
            parks: control.parks.clone(),
        }),
        on_status,
        &cancel,
    ))
    .await
}

/// How a call begins: with the request it carries out, or a pairing, which
/// first asks for the member's keys and then continues as a restore.
pub(super) enum Start {
    Request(BootstrapRequest),
    Pair(Pair),
}

impl From<BootstrapRequest> for Start {
    fn from(request: BootstrapRequest) -> Self {
        Self::Request(request)
    }
}

impl Start {
    pub(super) fn store(&self) -> StoreId {
        match self {
            Self::Request(BootstrapRequest::Restore { code, .. }) => code.store,
            Self::Request(BootstrapRequest::Join { code, .. }) => code.store,
            Self::Pair(pair) => pair.store(),
        }
    }
}

/// What a pairing sends its request with and waits for the handover on.
pub(super) struct Pair {
    code: PairingCode,
    data: ConnectionCredentials,
    sign_in: SignInCustody,
    name: String,
    /// The intervals the app asks the pairing to check at.
    held: HeldIntervals,
    parks: Parks,
}

impl Pair {
    pub(super) fn store(&self) -> StoreId {
        self.code.store
    }
}

/// Send the pairing's request, wait for the handover and open it: the restore
/// it hands over to, which loads, checks the pair, records itself and
/// registers the device. `None` when the pairing ended first: the showing
/// device declined, closed or lost it, it expired, or it was refused. The
/// installation's database opens for the persisted waits between checks and
/// closes again before the restore opens it.
pub(super) async fn handed_over(
    builder: &mut CovenBuilder,
    pending: &BootstrapStore,
    pair: Pair,
    status: &impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<BootstrapRequest>, BootstrapError> {
    let Pair { code, data, sign_in, name, held, parks } = pair;
    check_cancel(cancel)?;
    let directory = pending.directory();
    let device = directory.settings().map_err(CovenError::from)?.device_id;
    let ask = PairingAsk::new(&code, device, &name)?;
    let clock = builder.clock.clone();
    // No first request goes out once the pairing has expired.
    if !ask.open_at(clock.now()) {
        return Ok(None);
    }
    let provider = data.location.provider();
    let database = open_installation(builder.database(directory)?, status).await?;
    let asking = async {
        status(BootstrapStatus::Connecting);
        let waits = Arc::new(
            RetryWaits::open(database.clone(), clock.clone()).await.map_err(CovenError::from)?,
        );
        let state = Arc::new(SequenceState::new());
        let mut connecting = data.clone();
        let storage: Arc<dyn Storage> =
            connect_storage(builder, &mut connecting, &sign_in, device, &state, &waits).await?;
        if !ask.publish(storage.as_ref()).await? {
            return Ok(None);
        }
        status(BootstrapStatus::WaitingForApproval { confirmation: ask.confirmation() });
        let approval = ApprovalWaits::new(database.clone(), waits, clock.clone());
        let _running = parks.running();
        let mut poll = JoinPoll::new(approval.clone(), held, parks.clone());
        loop {
            poll.due().await?;
            match ask.check(storage.as_ref(), clock.now()).await? {
                PairingCheck::Waiting => approval.waiting().await?,
                PairingCheck::Ended => {
                    approval.settled().await?;
                    return Ok(None);
                }
                PairingCheck::Handover(handover) => {
                    approval.settled().await?;
                    return Ok(Some(ask.open(&handover)?));
                }
            }
        }
    };
    let mut cancellation = cancel.clone();
    let opened = tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => Err(BootstrapError::Cancelled),
        result = asking => result,
    };
    let opened = combine(sign_in.refused(provider, opened), close_database(&database).await)?;
    let Some((member, member_keys)) = opened else {
        return Ok(None);
    };
    let s3_key = match &data.credentials {
        StorageCredentials::S3(key) => Some(key.clone()),
        _ => None,
    };
    let storage = RestoreStorage { location: data.location.clone(), s3_key }
        .encode()
        .map_err(SyncError::from)?;
    Ok(Some(BootstrapRequest::Restore {
        code: RestoreCode { store: code.store, member, member_keys, storage },
        data,
        sign_in,
        name,
    }))
}
