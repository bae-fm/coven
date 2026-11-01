//! The inviting device's one-time invitation, its join request and the
//! approval that binds the request's exact pair (§13.2, §13.3).

use super::{SequenceSync, Session};
use crate::sequence_sync::sequence_operations::SequenceOperation;
use crate::sequence_sync::OperationState;
use crate::{
    Invite, InviteAccess, InviteDelivery, InviteTarget, JoinRequest, OperationKind,
    OperationOutcome, OperationRejection, PendingReason, PendingRecord, PendingSubject,
    ProviderAccessAction, RetryWaits, SyncError, SyncState,
};
use coven_crypto::{
    ConfirmationNumber, InviteSecret, MemberIdentity, ObjectHasher, SecretText, StoreKeyring,
};
use coven_database::{
    Journal, NewOperation, OperationId, OperationRecord, ProviderAccessChange, SqlContext,
};
use coven_format::{
    admin::{MemberRole, StoreChange},
    codes::{InviteCode, InviteDeliveryKind},
    sealed_single::SingleChunkObject,
    MemberAccess, Object,
};
use coven_foundation::id_source::{DeviceId, IdSource, InviteId, MemberId};
use coven_merge::Audience;
use coven_storage::{
    CloudProvider, InviteStorage, ObjectPath, S3Credentials, StorageFailure, StorageInvitation,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc, time::SystemTime};

pub(crate) const INVITE: &str = "invite";

/// How an invited person reaches storage, as the invite's journal keeps it.
#[derive(Serialize, Deserialize)]
pub(crate) enum InviteGrant {
    /// The provider shares the store with this account.
    Account { email: String },
    /// An S3 key the person made in the provider's console for this invite.
    S3Key(S3Credentials),
}

impl InviteGrant {
    fn member_access(&self) -> MemberAccess {
        match self {
            Self::Account { email } => MemberAccess::ProviderAccount(email.clone()),
            Self::S3Key(key) => {
                MemberAccess::S3AccessKey { access_key_id: key.access_key_id.clone() }
            }
        }
    }
}

/// What approving the invite's request binds to its member id.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) enum InviteBinding {
    /// A fresh id with this role and the invite's access.
    Add { role: MemberRole },
    /// An existing active id keeps its role, circles, devices and recorded
    /// access; only its pair changes.
    Replace {
        #[serde(with = "crate::pending_encoding::public_keys")]
        expected: coven_crypto::MemberPublicKeys,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum InviteState {
    /// The app's write made the invite. A pass checks it again against the
    /// history it caught up with before granting anything.
    Requested,
    /// Storage access is being granted.
    Grant,
    /// The code is shown; the request, once read and checked, waits for the app.
    Waiting {
        /// The code as first shown, which every later showing repeats: the
        /// joining device binds its saved request to the exact code (§13.2).
        code: SecretText,
        request: Option<Vec<u8>>,
    },
    /// This checked request is approved: `binding` is the operation that
    /// claims its copies and binds its pair, journaled with this state.
    Approving { code: SecretText, request: Vec<u8>, binding: OperationId },
    /// A kept replacement records the invite's new S3 key for its member.
    Recording,
    /// The invite ended: delete the request object and settle its
    /// account's sharing. An S3 key it carried is the person's to delete in
    /// the provider's console; coven records nothing about it (§14.3).
    Settled,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct InviteWork {
    id: InviteId,
    member: MemberId,
    /// The new person's name; `None` for a replacement.
    invitee: Option<String>,
    delivery: InviteDeliveryKind,
    binding: InviteBinding,
    access: InviteGrant,
    secret: SecretText,
    expires_at: std::time::SystemTime,
    state: InviteState,
}

impl OperationState for InviteWork {}

impl InviteWork {
    fn read(record: &OperationRecord) -> Result<Self, SyncError> {
        Ok(serde_json::from_slice(&record.data)?)
    }
    fn secret(&self) -> Result<InviteSecret, SyncError> {
        Ok(InviteSecret::from_bytes(serde_json::from_str(self.secret.as_str())?))
    }
    fn account(&self) -> Option<&str> {
        match &self.access {
            InviteGrant::Account { email } => Some(email),
            _ => None,
        }
    }
    /// An invite whose account intention is adopted and not yet ended: its
    /// account stays shared (§14.2). One the pass hasn't checked yet holds
    /// no share.
    fn shares(&self) -> bool {
        !matches!(self.state, InviteState::Requested | InviteState::Settled)
    }
    /// Whether the invite ends at its expiry: it waits for its check, grants
    /// access, or waits for its request or the app's decision. Approval,
    /// once begun, finishes.
    fn expires(&self) -> bool {
        matches!(
            self.state,
            InviteState::Requested | InviteState::Grant | InviteState::Waiting { .. }
        )
    }
    fn expired(&self, now: std::time::SystemTime) -> bool {
        self.expires() && now >= self.expires_at
    }

    /// The entry approving `request` claims: adding the invite's member
    /// with the request's pair, or replacing the member's pair with it.
    fn binding_change(&self, request: &[u8]) -> Result<StoreChange, SyncError> {
        let keys = decode_request(request)?.keys;
        Ok(match self.binding {
            InviteBinding::Add { role } => StoreChange::AddMember {
                member: self.member,
                keys,
                role,
                access: self.access.member_access(),
            },
            InviteBinding::Replace { expected } => {
                StoreChange::ReplaceMemberKeys { member: self.member, expected, keys }
            }
        })
    }

    /// The invite as the app sees it, at `stage`, with the operation that
    /// names it.
    fn shown(&self, operation: OperationId, stage: crate::InviteStage) -> Invite {
        Invite {
            id: self.id,
            operation,
            member: self.member,
            invitee: self.invitee.clone(),
            delivery: self.delivery,
            access: self.access.member_access(),
            expires_at: self.expires_at,
            stage,
        }
    }

    fn present_request(&self, bytes: &[u8]) -> Result<JoinRequest, SyncError> {
        let request = decode_request(bytes)?;
        Ok(JoinRequest {
            invite: request.invite,
            member: request.member,
            confirmation: ConfirmationNumber::of(request.invite, &request.keys),
            keys: request.keys,
            device_name: request.device_name,
            provider_account_email: self.account().map(str::to_owned),
            record: bytes.to_vec(),
        })
    }
}

/// The invite a `coven_invites` row lists: its journal entry, and the
/// operation that names it.
fn listed(data: &[u8], operation: OperationId) -> Result<Invite, SyncError> {
    let work: InviteWork = serde_json::from_slice(data)?;
    let shown = |code: &SecretText| code.as_str().to_owned();
    let stage = match &work.state {
        InviteState::Requested | InviteState::Grant => crate::InviteStage::Granting,
        InviteState::Waiting { code, request } => crate::InviteStage::Waiting {
            code: shown(code),
            request: request.as_deref().map(|bytes| work.present_request(bytes)).transpose()?,
        },
        InviteState::Approving { code, request, binding } => crate::InviteStage::Approving {
            code: shown(code),
            request: work.present_request(request)?,
            binding: *binding,
        },
        InviteState::Recording | InviteState::Settled => {
            return Err(coven_database::DbError::Invariant {
                what: "coven_invites lists invites that haven't ended",
            }
            .into())
        }
    };
    Ok(work.shown(operation, stage))
}

impl Invite {
    /// Read `coven_invites`, as an app's live query does:
    /// `handle.subscribe(Invite::list)`. This device's invites that haven't
    /// ended, in the order they were made (§13.2).
    pub fn list(sql: coven_database::SqlReadContext<'_>) -> coven_database::CovenResult<Vec<Self>> {
        Ok(sql.query(
            "SELECT invite,operation FROM coven_invites ORDER BY operation",
            [],
            |row| {
                let data: Vec<u8> = row.get(0)?;
                listed(&data, OperationId(row.get(1)?)).map_err(|error| {
                    coven_database::SqliteError::FromSqlConversionFailure(
                        0,
                        coven_database::types::Type::Blob,
                        Box::new(error),
                    )
                })
            },
        )?)
    }

    /// The code the invite shows.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn shown_code(&self) -> &str {
        match &self.stage {
            crate::InviteStage::Granting => panic!("the invite shows no code before its grant"),
            crate::InviteStage::Waiting { code, .. }
            | crate::InviteStage::Approving { code, .. } => code,
        }
    }

    /// The checked request, waiting for the app or being approved.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn shown_request(&self) -> Option<&JoinRequest> {
        match &self.stage {
            crate::InviteStage::Granting => None,
            crate::InviteStage::Waiting { request, .. } => request.as_ref(),
            crate::InviteStage::Approving { request, .. } => Some(request),
        }
    }

    /// Follow `coven_invites` with a live query, as an app does.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn follow(
        database: &coven_database::Database,
    ) -> coven_database::test_utils::Followed<Vec<Self>> {
        coven_database::test_utils::Followed::new(database.subscribe(Self::list))
    }
}

impl SequenceSync {
    /// Accounts that this device's open invites still need shared.
    pub(crate) async fn invited_accounts(&self) -> Result<BTreeSet<String>, SyncError> {
        let mut accounts = BTreeSet::new();
        for record in self.database.operations().await? {
            if record.kind == INVITE {
                let work = InviteWork::read(&record)?;
                if work.shares() {
                    accounts.extend(work.account().map(str::to_owned));
                }
            }
        }
        Ok(accounts)
    }

    /// Make an invite as the app's write step does, then run a pass, and
    /// return the invite once it shows its code, or its pending record as
    /// the error.
    #[cfg(test)]
    pub(crate) async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
        delivery: InviteDelivery,
    ) -> Result<Invite, SyncError> {
        let (device, ids, now, state) =
            (self.device, self.ids.clone(), self.clock.now(), self.state.clone());
        let held = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let location = self.storage.config().clone();
        let (invite, _, operation) = self
            .database
            .write(move |sql| {
                let made = NewInvite { target, access, delivery };
                create_invite(&sql, device, &held, ids.as_ref(), now, &location, state, made)
                    .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))
            })
            .await?;
        self.pass().await?;
        let shown = self.database.read(Invite::list).await?.into_iter().find(|listed| {
            listed.id == invite && !matches!(listed.stage, crate::InviteStage::Granting)
        });
        if let Some(shown) = shown {
            return Ok(shown);
        }
        match self.database.operation_pending::<PendingRecord>(operation).await? {
            Some(pending) => Err(SyncError::Pending(Box::new(pending))),
            None => Err(SyncError::InvitationChanged),
        }
    }

    /// Approve the request as the app's write step does, then run the pass
    /// that binds it, returning its rejection as the error.
    #[cfg(test)]
    pub(crate) async fn approve_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<(), SyncError> {
        let request = request.clone();
        let (device, now, waits, state) =
            (self.device, self.clock.now(), self.waits.clone(), self.state.clone());
        let held = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let outcome = self
            .journal_and_pass(move |sql| {
                approve_join_request(sql, device, &held, now, waits, state, &request).map(Some)
            })
            .await?
            .expect("an approval always journals its binding");
        crate::operation_outcome::into_slot(outcome).map(drop)
    }

    /// Decline the request as the app's write step does, then run the pass
    /// that deletes it.
    #[cfg(test)]
    pub(crate) async fn decline_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<(), SyncError> {
        let request = request.clone();
        let (now, waits, state) = (self.clock.now(), self.waits.clone(), self.state.clone());
        self.settle_and_pass(move |sql| decline_join_request(sql, now, waits, state, &request))
            .await
    }

    /// Cancel the invite as the app's write step does, then run the pass
    /// that deletes its request.
    #[cfg(test)]
    pub(crate) async fn cancel_invite(&self, invite: InviteId) -> Result<(), SyncError> {
        let (now, waits, state) = (self.clock.now(), self.waits.clone(), self.state.clone());
        self.settle_and_pass(move |sql| cancel_invite(sql, now, waits, state, invite)).await
    }

    #[cfg(test)]
    async fn settle_and_pass(
        &self,
        step: impl FnOnce(&SqlContext<'_, '_>) -> Result<(), SyncError> + Send + 'static,
    ) -> Result<(), SyncError> {
        self.database
            .write(move |sql| {
                step(&sql).map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))
            })
            .await?;
        self.pass().await.map(drop)
    }

    /// Advance this device's invites in a pass: grant, read the request
    /// under backoff, approve an in-person invite's checked request, revoke
    /// on expiry, and finish decided work.
    pub(crate) async fn resume_invites(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != INVITE {
                continue;
            }
            // Expiry takes back the grant even while a request waits for the app.
            let expired = InviteWork::read(&record)?.expired(self.clock.now());
            if !expired && !self.operation_ready(&record, ring).await? {
                continue;
            }
            if let Err(error) = self.run_invite(session, ring, member, record.id).await {
                if matches!(error, SyncError::Rejected(_) | SyncError::Cleanup { .. })
                    || self.database.operation(record.id).await?.is_none()
                {
                    return Err(error);
                }
            }
            self.state.check()?;
        }
        Ok(())
    }

    /// Advance the invite as far as it goes now. `step` follows the step
    /// of the invite's row this attempt last read, which a failure belongs
    /// to; `None` until it has read the row.
    async fn advance_invite(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
        step: &mut Option<u32>,
    ) -> Result<InviteProgress, SyncError> {
        loop {
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(coven_database::DbError::OperationChanged(id))?;
            *step = Some(record.last_step);
            let mut work = InviteWork::read(&record)?;
            if work.expired(self.clock.now()) {
                work.state = InviteState::Settled;
                self.database.advance_operation(work.update(&record)?).await?;
                continue;
            }
            match &work.state {
                InviteState::Requested => {
                    // A refusal waits for the app to cancel the invite: it is
                    // the invite's pending record, and fails no pass.
                    if let Err(refusal) = self.check_invite(session, member, &work) {
                        return Ok(InviteProgress::Waiting(self.error_reason(&refusal)));
                    }
                    work.state = InviteState::Grant;
                    let changes = match work.account() {
                        Some(account) => vec![self.account_change(account, true, true)?],
                        None => Vec::new(),
                    };
                    let update = work.update(&record)?;
                    let granting = update.last_step;
                    self.database
                        .advance_operation_with_access::<PendingRecord>(update, changes)
                        .await?;
                    // A failed grant belongs to the step this attempt took.
                    *step = Some(granting);
                    // The account's grant, unless another open invite's grant
                    // already shared it with an invitation (§14.2). A grant
                    // that fails is the invite's pending record.
                    let grant = match work.account() {
                        Some(account) => self
                            .database
                            .provider_access(account.to_owned())
                            .await?
                            .ok_or(coven_database::DbError::DamagedDatabase)?
                            .request
                            .map(|request| request.operation),
                        None => None,
                    };
                    if let Some(grant) = grant {
                        self.apply_access(session, ring, member, grant).await?;
                    }
                }
                InviteState::Grant => {
                    let invitation = match &work.access {
                        InviteGrant::Account { email } => {
                            let access = self
                                .database
                                .provider_access(email.clone())
                                .await?
                                .ok_or(coven_database::DbError::DamagedDatabase)?;
                            if access.request.is_some() || access.granted != Some(true) {
                                // A grant that failed keeps its cause in the
                                // invite's pending record too.
                                let failed = match &access.request {
                                    Some(request) => {
                                        self.database
                                            .operation_pending::<PendingRecord>(request.operation)
                                            .await?
                                    }
                                    None => None,
                                };
                                return Ok(InviteProgress::Waiting(failed.map_or(
                                    PendingReason::ProviderPending {
                                        action: ProviderAccessAction::Grant,
                                        account: email.clone(),
                                    },
                                    |failed| failed.reason,
                                )));
                            }
                            StorageInvitation::decode(
                                access
                                    .invitation
                                    .as_ref()
                                    .ok_or(coven_database::DbError::DamagedDatabase)?
                                    .as_bytes(),
                            )?
                        }
                        InviteGrant::S3Key(_) => {
                            StorageInvitation::for_account(self.storage.config())?
                        }
                    };
                    let code = self.invite_code(session, &work, invitation)?;
                    work.state = InviteState::Waiting { code, request: None };
                    self.database.advance_operation(work.update(&record)?).await?;
                    // The next pass looks for the request, then backs off.
                    self.state.request_pass();
                    return Ok(InviteProgress::Shown);
                }
                InviteState::Waiting { request: Some(_), .. } => {
                    return Ok(InviteProgress::Waiting(PendingReason::Paused));
                }
                InviteState::Waiting { request: None, code } => {
                    let path = ObjectPath::join_request(self.store(), work.id);
                    let bytes = match self.storage.read(&path).await {
                        Ok(bytes) => bytes,
                        Err(error) if error.failure() == StorageFailure::NotFound => {
                            return Ok(InviteProgress::Waiting(PendingReason::Missing { path }));
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let request = self.open_request(&work, &bytes)?;
                    let code = code.clone();
                    match work.delivery {
                        // Ana watched the phone that scanned her code: an
                        // invite has one request path, so the checked request
                        // is approved in the pass that read it (§13.2).
                        InviteDeliveryKind::InPerson => {
                            let binding = crate::admin_steps::journaled(
                                &SequenceOperation::change(work.binding_change(&request)?),
                                INVITE,
                            )?;
                            self.database
                                .start_operation_advancing(binding, move |binding| {
                                    work.state = InviteState::Approving { code, request, binding };
                                    work.update(&record).map_err(|error| {
                                        coven_database::DbError::SyncStream(Box::new(error))
                                    })
                                })
                                .await?;
                        }
                        InviteDeliveryKind::Sent => {
                            work.state = InviteState::Waiting { code, request: Some(request) };
                            self.database.advance_operation(work.update(&record)?).await?;
                            return Ok(InviteProgress::Waiting(PendingReason::Paused));
                        }
                    }
                }
                InviteState::Approving { request, binding, .. } => {
                    let binding = *binding;
                    let change = work.binding_change(request)?;
                    let ended = match self.database.operation(binding).await? {
                        Some(_) => {
                            match self.run_administration(session, ring, member, binding).await? {
                                OperationOutcome::Rejected(reason) => Approval::Rejected(reason),
                                OperationOutcome::Landed(_) | OperationOutcome::Satisfied => {
                                    Approval::Bound
                                }
                            }
                        }
                        // The binding's operation ended in an attempt that
                        // stopped before the invite moved on: it landed if
                        // the applied history binds the pair.
                        None if self.binds(session, &change) => Approval::Bound,
                        None => Approval::RejectedEarlier,
                    };
                    let reason = match ended {
                        Approval::Bound => {
                            work.state = match (&work.binding, &work.access) {
                                (InviteBinding::Replace { .. }, InviteGrant::S3Key(_)) => {
                                    InviteState::Recording
                                }
                                _ => InviteState::Settled,
                            };
                            self.database.advance_operation(work.update(&record)?).await?;
                            continue;
                        }
                        Approval::Rejected(reason) => reason,
                        Approval::RejectedEarlier => {
                            tracing::warn!(?id, ?binding, "an approval's binding ended unbound");
                            work.state = InviteState::Settled;
                            self.database.advance_operation(work.update(&record)?).await?;
                            continue;
                        }
                    };
                    work.state = InviteState::Settled;
                    self.database.advance_operation(work.update(&record)?).await?;
                    // A sent invite's rejection is its binding's outcome, which
                    // `coven_operations` lists for the app that approved it.
                    // Coven approved an in-person request on its own, so the
                    // rejection fails the pass, once the invite has ended.
                    if work.delivery == InviteDeliveryKind::Sent {
                        continue;
                    }
                    let operation = SyncError::Rejected(reason);
                    return Err(
                        match Box::pin(self.advance_invite(session, ring, member, id, step)).await {
                            Ok(_) => operation,
                            Err(cleanup) => SyncError::Cleanup {
                                operation: Box::new(operation),
                                cleanup: Box::new(cleanup),
                            },
                        },
                    );
                }
                InviteState::Recording => {
                    let change = StoreChange::SetAccess {
                        member: work.member,
                        access: work.access.member_access(),
                    };
                    match self.record_invite_access(session, ring, member, change).await {
                        Ok(()) => (),
                        // The applied history refuses the record, as when the
                        // member was removed meanwhile: nothing would ever
                        // accept it, so the invite ends, as a refused binding
                        // ends it. The person deletes the key in the
                        // provider's console (§14.3).
                        Err(SyncError::Rejected(reason)) => {
                            tracing::warn!(
                                ?id,
                                ?reason,
                                "a kept replacement's key record was refused"
                            );
                        }
                        Err(error) => return Err(error),
                    }
                    work.state = InviteState::Settled;
                    self.database.advance_operation(work.update(&record)?).await?;
                }
                InviteState::Settled => {
                    // Provider deletion is idempotent, including an absent request.
                    match self
                        .storage
                        .delete(&ObjectPath::join_request(self.store(), work.id))
                        .await
                    {
                        Ok(()) => (),
                        Err(error) if error.failure() == StorageFailure::NotFound => (),
                        Err(error) => return Err(error.into()),
                    }
                    let changes = match work.account() {
                        Some(account) => {
                            let others = self.invited_accounts_except(id).await?;
                            let used = session.history.state.members.values().any(|value| {
                                !value.removed
                                    && value.access.same_credential(&MemberAccess::ProviderAccount(
                                        account.to_owned(),
                                    ))
                            });
                            let invited = others.contains(account);
                            vec![self.account_change(account, used || invited, invited)?]
                        }
                        None => Vec::new(),
                    };
                    self.database
                        .finish_operation_with_access::<PendingRecord>(id, changes)
                        .await?;
                    return Ok(InviteProgress::Finished);
                }
            }
        }
    }

    /// The invite checked again against the history this pass caught up
    /// with, before it grants anything: its maker is still a registered
    /// admin whose account holds the store's storage, and a replacement's
    /// member is still active with the pair it expected. The app's write
    /// checked only the slots its device had applied then (§11.1, §13.2).
    fn check_invite(
        &self,
        session: &Session,
        member: &MemberIdentity,
        work: &InviteWork,
    ) -> Result<(), SyncError> {
        let me = member.member();
        let state = &session.history.state;
        if !self.registered(session)
            || !state
                .members
                .get(&me)
                .is_some_and(|value| !value.removed && value.role == MemberRole::Admin)
        {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        if !session.history.owns_storage(me) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        if let InviteBinding::Replace { expected } = work.binding {
            match state.members.get(&work.member).filter(|value| !value.removed) {
                None => {
                    return Err(SyncError::Rejected(OperationRejection::TargetGone(
                        crate::EntryTarget::Member(work.member),
                    )))
                }
                Some(current) if current.keys != expected => {
                    return Err(SyncError::Rejected(OperationRejection::MemberKeysChanged(
                        work.member,
                    )))
                }
                Some(_) => (),
            }
        }
        Ok(())
    }

    async fn invited_accounts_except(
        &self,
        id: OperationId,
    ) -> Result<BTreeSet<String>, SyncError> {
        let mut accounts = BTreeSet::new();
        for record in self.database.operations().await? {
            if record.kind == INVITE && record.id != id {
                let work = InviteWork::read(&record)?;
                if work.shares() {
                    accounts.extend(work.account().map(str::to_owned));
                }
            }
        }
        Ok(accounts)
    }

    fn account_change(
        &self,
        account: &str,
        intended: bool,
        needs_invitation: bool,
    ) -> Result<ProviderAccessChange, SyncError> {
        Ok(ProviderAccessChange {
            operation: NewOperation {
                kind: "provider-access".into(),
                data: serde_json::to_vec(account)?,
                started_by: "coven".into(),
                pauses_writes: false,
            },
            account: account.to_owned(),
            intended,
            initially_granted: false,
            needs_invitation,
            owner: true,
        })
    }

    /// The code to show, fixed once its access is granted: it names the
    /// store as now named and this device's applied slot.
    fn invite_code(
        &self,
        session: &Session,
        work: &InviteWork,
        invitation: StorageInvitation,
    ) -> Result<SecretText, SyncError> {
        let storage = match &work.access {
            InviteGrant::Account { .. } => InviteStorage::Account(invitation),
            InviteGrant::S3Key(credentials) => {
                InviteStorage::S3 { invitation, credentials: credentials.clone() }
            }
        };
        let store =
            session.history.state.store.as_ref().ok_or(coven_database::DbError::DamagedDatabase)?;
        let initial_key = session
            .history
            .introductions
            .iter()
            .find(|intro| intro.audience == Audience::Store)
            .ok_or(coven_database::DbError::DamagedDatabase)?
            .key;
        let code = InviteCode {
            store: store.id,
            name: store.name.clone(),
            invitee: work.invitee.clone(),
            delivery: work.delivery,
            invite: work.id,
            member: work.member,
            secret: work.secret()?,
            initial_key,
            inviting_writer: self.device,
            read_through: session.through,
            storage: storage.encode()?,
        }
        .to_text()?;
        Ok(SecretText::new(code.to_string()))
    }

    /// Open and check a request: it repeats the invite's id and member id,
    /// and its own new signing key verifies it (§13.2).
    fn open_request(&self, work: &InviteWork, bytes: &[u8]) -> Result<Vec<u8>, SyncError> {
        let envelope = SingleChunkObject::decode(bytes)?;
        let SingleChunkObject::JoinRequest { signature, .. } = &envelope;
        let path = ObjectPath::join_request(self.store(), work.id);
        let plain = work.secret()?.join_request_key().open_object_chunk(
            path.as_str(),
            &envelope.prefix().encode()?,
            0,
            0,
            envelope.chunk(),
        )?;
        let request = decode_request(&plain)?;
        if request.invite != work.id || request.member != work.member {
            return Err(SyncError::InvitationChanged);
        }
        let mut hasher = ObjectHasher::new();
        hasher.update(&envelope.signed_bytes()?);
        request.keys.signing.verify_object(path.as_str(), &hasher.finish(), signature)?;
        Ok(plain.to_vec())
    }
}

/// How an approval's binding ended.
enum Approval {
    /// Its entry landed, or the applied history already had it.
    Bound,
    /// The caught-up history refused it.
    Rejected(OperationRejection),
    /// It ended unbound in an earlier attempt, which stopped before the
    /// invite moved on.
    RejectedEarlier,
}

/// How far one call advanced an invite.
pub(crate) enum InviteProgress {
    /// Access is granted and the code can be shown.
    Shown,
    /// The invite waits on this condition.
    Waiting(PendingReason),
    /// The invite's work is done.
    Finished,
}

fn decode_request(bytes: &[u8]) -> Result<coven_format::objects::JoinRequest, SyncError> {
    match Object::decode(bytes)? {
        Object::JoinRequest(request) => Ok(request),
        _ => Err(SyncError::InvitationChanged),
    }
}

#[cfg(test)]
#[path = "sequence_invites_tests.rs"]
mod tests;

#[path = "sequence_invite_approval.rs"]
mod approval;

#[path = "sequence_invite_reset.rs"]
mod reset;

#[path = "sequence_invite_steps.rs"]
mod steps;
pub use steps::{
    approve_join_request, cancel_invite, create_invite, decline_join_request, NewInvite,
};

#[path = "invite_expiry.rs"]
pub(crate) mod expiry;
