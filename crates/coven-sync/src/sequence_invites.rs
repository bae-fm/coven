//! The inviting device's one-time invitation, its join request and the
//! approval that binds the request's exact pair (§13.2, §13.3).

use super::sequence_copies::ApprovedAdmission;
use super::{SequenceSync, Session};
use crate::sequence_sync::sequence_operations::SequenceOperation;
use crate::sequence_sync::OperationState;
use crate::{
    Invite, InviteAccess, InviteLifetime, JoinRequest, OperationKind, OperationOutcome,
    OperationRejection, PendingReason, PendingRecord, PendingSubject, ProviderAccessAction,
    SyncError,
};
use approval::Child;
use coven_crypto::{ConfirmationNumber, InviteSecret, MemberIdentity, SecretText, StoreKeyring};
use coven_database::{
    operation_kind, DbError, Journal, NewOperation, OperationId, OperationRecord, SqlContext,
};
use coven_format::{
    admin::{MemberRole, StoreChange},
    codes::InviteCode,
    invite_end::InviteEnd,
    MemberAccess, Object,
};
use coven_foundation::id_source::{DeviceId, IdSource, InviteId, MemberId};
use coven_merge::Audience;
use coven_storage::{
    CloudProvider, InviteStorage, ObjectPath, S3Credentials, StorageFailure, StorageInvitation,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::SystemTime};

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
    /// The invite's share is being recorded in history, before its grant:
    /// `share` is the operation claiming it, journaled with this state.
    Sharing { share: OperationId },
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
    /// claims its admission, journaled with this state.
    Approving { code: SecretText, request: Vec<u8>, binding: OperationId },
    /// The invite ended: put its marker in the request's place and settle
    /// its account's sharing. `share` is the share's record the invite was
    /// claiming when it ended, whose claim it settles without claiming again;
    /// `end`, once journaled with this state, is the operation claiming the
    /// share's end. Both end with the invite. An S3 key it carried is the
    /// person's to delete in the provider's console; coven records nothing
    /// about it (§14.3). `refusal`, when the invite ended because the provider
    /// refused to share the store for a reason only a person can fix, is what
    /// the invite's operation ends with, for the app to acknowledge (§13.2).
    Settled {
        share: Option<OperationId>,
        end: Option<OperationId>,
        refusal: Option<coven_foundation::sharing::SharingRefusal>,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) struct InviteWork {
    id: InviteId,
    /// The device that made the invite, under which its request lives. A
    /// reset carries the invite to an installation with another id, which
    /// ends the request where it is (§13.2, §19.3).
    device: DeviceId,
    member: MemberId,
    /// The inviting admin's name, which the invite's code carries.
    inviter: String,
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
    /// The refusal that ended the invite, once it has settled.
    fn refusal(&self) -> Option<coven_foundation::sharing::SharingRefusal> {
        match &self.state {
            InviteState::Settled { refusal, .. } => *refusal,
            _ => None,
        }
    }
    fn secret(&self) -> Result<InviteSecret, SyncError> {
        Ok(InviteSecret::from_bytes(serde_json::from_str(self.secret.as_str())?))
    }
    /// Whether the invite admits a new person or a member's new keys: the
    /// request it takes.
    fn admits(&self) -> coven_format::codes::InviteAdmits {
        match self.binding {
            InviteBinding::Add { .. } => coven_format::codes::InviteAdmits::NewPerson,
            InviteBinding::Replace { .. } => coven_format::codes::InviteAdmits::MemberKeys,
        }
    }
    fn account(&self) -> Option<&str> {
        match &self.access {
            InviteGrant::Account { email } => Some(email),
            _ => None,
        }
    }
    /// Whether the invite ends at its expiry: it waits for its check, grants
    /// access, or waits for its request or the app's decision. Approval,
    /// once begun, finishes.
    fn expires(&self) -> bool {
        matches!(
            self.state,
            InviteState::Requested
                | InviteState::Sharing { .. }
                | InviteState::Grant
                | InviteState::Waiting { .. }
        )
    }
    fn expired(&self, now: std::time::SystemTime) -> bool {
        self.expires() && now >= self.expires_at
    }

    /// End the invite, as expiry, a decline or a cancel does: its settled
    /// step carries the share's record it was claiming, which ends with it.
    fn end(&mut self) {
        let share = match self.state {
            InviteState::Sharing { share } => Some(share),
            _ => None,
        };
        self.state = InviteState::Settled { share, end: None, refusal: None };
    }

    /// The invite's step from `record` to `state`, built from the id of the
    /// operation it starts, which `state` names: the two commit together
    /// (`start_child`).
    fn naming(
        record: &OperationRecord,
        state: impl FnOnce(OperationId) -> InviteState + Send + 'static,
    ) -> impl FnOnce(OperationId) -> Result<coven_database::OperationUpdate, SyncError> + Send + 'static
    {
        let record = record.clone();
        move |child| {
            let mut work = InviteWork::read(&record)?;
            work.state = state(child);
            work.update(&record)
        }
    }

    /// The admission approving `request` claims: admitting the invite's
    /// member with the request's pair, or recovering the member with it,
    /// registering the device the request names and making the circle it
    /// carries, with the request's copy of that circle's key (§13.2).
    fn binding_admission(&self, request: &[u8]) -> Result<ApprovedAdmission, SyncError> {
        let request = decode_request(request)?;
        let (keys, device, device_name) = (request.keys, request.device, request.device_name);
        let joining = request
            .joiner
            .circle()
            .cloned()
            .ok_or(coven_format::Error::UnknownTag { field: "invite joiner", tag: 2 })?;
        let circle = joining.circle;
        let change = match self.binding {
            InviteBinding::Add { role } => StoreChange::AdmitMember {
                member: self.member,
                keys,
                role,
                access: self.access.member_access(),
                device,
                device_name,
                person: request
                    .joiner
                    .person()
                    .ok_or(coven_format::Error::UnknownTag { field: "invite joiner", tag: 0 })?
                    .to_owned(),
                circle,
            },
            // A recovery on S3 records the invite's new key as the member's
            // access in the same entry; on another provider the member keeps
            // the account they reach storage with (§13.2, §14.3).
            InviteBinding::Replace { expected } => StoreChange::RecoverMember {
                member: self.member,
                expected,
                keys,
                access: match &self.access {
                    InviteGrant::S3Key(_) => Some(self.access.member_access()),
                    InviteGrant::Account { .. } => None,
                },
                device,
                device_name,
                circle,
            },
        };
        Ok(ApprovedAdmission::new(change, joining.copy))
    }

    /// The invite as the app sees it, at `stage`, with the operation that
    /// names it.
    fn shown(&self, operation: OperationId, stage: crate::InviteStage) -> Invite {
        Invite {
            id: self.id,
            operation,
            member: self.member,
            admits: self.admits(),
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
            joiner: request.joiner.try_into()?,
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
        InviteState::Requested | InviteState::Sharing { .. } | InviteState::Grant => {
            crate::InviteStage::Granting
        }
        InviteState::Waiting { code, request } => crate::InviteStage::Waiting {
            code: shown(code),
            request: request.as_deref().map(|bytes| work.present_request(bytes)).transpose()?,
        },
        InviteState::Approving { code, request, binding } => crate::InviteStage::Approving {
            code: shown(code),
            request: work.present_request(request)?,
            binding: *binding,
        },
        InviteState::Settled { .. } => {
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
    pub fn list(sql: coven_database::SqlReadContext<'_>) -> Result<Vec<Self>, DbError> {
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
    ) -> coven_database::test_utils::Followed<Vec<Self>, DbError> {
        coven_database::test_utils::Followed::new(database.subscribe(Self::list))
    }
}

impl SequenceSync {
    /// Make an invite for a new person as the app's write step does, then
    /// run a pass, and return the invite once it shows its code, or its
    /// pending record as the error.
    #[cfg(test)]
    pub(crate) async fn create_invite(
        &self,
        role: MemberRole,
        access: InviteAccess,
        lifetime: InviteLifetime,
    ) -> Result<Invite, SyncError> {
        self.made_invite(NewInvite::person(role, access, lifetime, "Ana".to_owned())).await
    }

    /// Make an invite for a member's new keys as `add_device_for` does, and
    /// run it as `create_invite` does.
    #[cfg(test)]
    pub(crate) async fn add_device_for(
        &self,
        member: MemberId,
        access: InviteAccess,
        lifetime: InviteLifetime,
    ) -> Result<Invite, SyncError> {
        self.made_invite(NewInvite::member_keys(member, access, lifetime, "Ana".to_owned())).await
    }

    #[cfg(test)]
    async fn made_invite(&self, made: NewInvite) -> Result<Invite, SyncError> {
        let steps = self.operation_steps();
        let location = self.storage.config().clone();
        coven_storage::record_config(&self.directory, &location)?;
        let (invite, _, operation) =
            self.database.write_as(move |sql| steps.create_invite(&sql, made)).await?;
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
        let steps = self.operation_steps();
        let outcome = self
            .journal_and_pass(move |sql| steps.approve_join_request(sql, &request).map(Some))
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
        let steps = self.operation_steps();
        self.settle_and_pass(move |sql| steps.decline_join_request(sql, &request)).await
    }

    /// Cancel the invite as the app's write step does, then run the pass
    /// that deletes its request.
    #[cfg(test)]
    pub(crate) async fn cancel_invite(&self, invite: InviteId) -> Result<(), SyncError> {
        let steps = self.operation_steps();
        self.settle_and_pass(move |sql| steps.cancel_invite(sql, invite)).await
    }

    #[cfg(test)]
    async fn settle_and_pass(
        &self,
        step: impl FnOnce(&SqlContext<'_, '_>) -> Result<(), SyncError> + Send + 'static,
    ) -> Result<(), SyncError> {
        self.database.write_as(move |sql| step(&sql)).await?;
        self.pass().await.map(drop)
    }

    /// Advance this device's invites in a pass: grant, read the request
    /// under backoff, revoke on expiry, and finish decided work.
    pub(crate) async fn resume_invites(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != operation_kind::INVITE {
                continue;
            }
            // Expiry takes back the grant even while a request waits for the app.
            let expired = InviteWork::read(&record)?.expired(self.clock.now());
            if !expired && !self.operation_ready(&record, ring).await? {
                continue;
            }
            if let Err(error) = self.run_invite(session, ring, member, record.id).await {
                if self.database.operation(record.id).await?.is_none() {
                    return Err(error);
                }
            }
            self.state.check()?;
        }
        // Ended invites' markers, once no joining device can send a first
        // request to their paths (`end_request`).
        for path in self.database.due_object_removals(seconds(self.clock.now())?).await? {
            let object = ObjectPath::parse(&path).map_err(|error| {
                StorageFailure::Protocol.with_source(format!("a recorded removal's path: {error}"))
            })?;
            self.storage.delete_if_present(&object).await?;
            self.database.object_removed(path).await?;
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
                work.end();
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
                    // Recorded before the grant, so no share is ever left
                    // that only this device knows of.
                    if let Some(account) = work.account() {
                        let started =
                            self.start_invite_share(session, member, &record, &work, account);
                        match started.await? {
                            Child::Journaled(_) => continue,
                            Child::Decided(OperationOutcome::Rejected(reason)) => {
                                return Ok(InviteProgress::Waiting(
                                    self.error_reason(&SyncError::Rejected(reason)),
                                ))
                            }
                            Child::Decided(_) => (),
                        }
                    }
                    self.grant(session, ring, member, &record, work, None, step).await?;
                }
                InviteState::Sharing { share } => {
                    let share = *share;
                    let outcome = self.run_administration(session, ring, member, share).await?;
                    match outcome {
                        OperationOutcome::Landed(_) | OperationOutcome::Satisfied => {
                            let shared = Some((share, outcome));
                            self.grant(session, ring, member, &record, work, shared, step).await?;
                        }
                        // The invite checks again, and waits on the refusal.
                        OperationOutcome::Rejected(_) => {
                            work.state = InviteState::Requested;
                            let update = work.update(&record)?;
                            self.database
                                .finish_operation_advancing(share, outcome, update)
                                .await?;
                        }
                        OperationOutcome::Stopped(_) => {
                            return Err(coven_database::DbError::Invariant {
                                what: "a stop for good ends the invite with its claims",
                            }
                            .into())
                        }
                        OperationOutcome::Cancelled | OperationOutcome::Undone(_) => {
                            return Err(coven_database::DbError::Invariant {
                                what: "only storage setup is cancelled or undone",
                            }
                            .into())
                        }
                        OperationOutcome::SharingRefused { .. } => {
                            return Err(coven_database::DbError::Invariant {
                                what: "only an invite ends with a sharing refusal",
                            }
                            .into())
                        }
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
                                if let Some(PendingRecord {
                                    reason:
                                        PendingReason::Storage {
                                            failure: StorageFailure::SharingRefused(refusal),
                                            ..
                                        },
                                    ..
                                }) = &failed
                                {
                                    let grant = access
                                        .request
                                        .as_ref()
                                        .ok_or(coven_database::DbError::DamagedDatabase)?
                                        .operation;
                                    self.end_refused(id, grant, *refusal).await?;
                                    continue;
                                }
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
                    let path = ObjectPath::join_request(self.store(), work.device, work.id);
                    let bytes = match self.storage.read(&path).await {
                        Ok(bytes) => bytes,
                        Err(error) if error.failure() == StorageFailure::NotFound => {
                            return Ok(InviteProgress::Waiting(PendingReason::Missing { path }));
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let request = match self.open_request(&work, &session.history.state, &bytes)? {
                        Opened::Checked(request) => request,
                        // No attempt can pass it, and no other request can
                        // reach the invite's one path while it is there: the
                        // invite ends, as a decline does (§13.2).
                        Opened::Refused(refusal) => {
                            tracing::warn!(?id, %refusal, "a join request failed its checks");
                            work.end();
                            self.database.advance_operation(work.update(&record)?).await?;
                            continue;
                        }
                    };
                    // A checked request waits for the app's decision: someone
                    // who saw the code may have sent it first (§13.2).
                    work.state =
                        InviteState::Waiting { code: code.clone(), request: Some(request) };
                    self.database.advance_operation(work.update(&record)?).await?;
                    return Ok(InviteProgress::Waiting(PendingReason::Paused));
                }
                InviteState::Approving { binding, .. } => {
                    let binding = *binding;
                    // The binding ends in the transaction that moves the
                    // invite on, so it is journaled while the invite approves.
                    if self.database.operation(binding).await?.is_none() {
                        return Err(coven_database::DbError::Invariant {
                            what: "an approving invite's binding ends with its step",
                        }
                        .into());
                    }
                    let outcome = self.run_administration(session, ring, member, binding).await?;
                    // Landed or refused, the invite ends. A refusal is the
                    // binding's outcome, which `coven_operations` lists for the
                    // app that approved it; the invite ends as a decline does.
                    work.state = InviteState::Settled { share: None, end: None, refusal: None };
                    self.database
                        .finish_operation_advancing(binding, outcome, work.update(&record)?)
                        .await?;
                }
                InviteState::Settled { share, end, refusal } => {
                    let (share, end, refusal) = (*share, *end, *refusal);
                    let kept_until = self.end_request(&work).await?;
                    // The share's record the invite was claiming when it
                    // ended: its claim may have landed, and is settled
                    // before anything else claims (§5.1).
                    if let Some(share) = share {
                        self.settle_unneeded_claim(session, ring, member, share).await?;
                    }
                    // Applying the share's end records its account's
                    // revoke, unless the account is still used (§14.2).
                    let end = match end {
                        Some(end) => Some(end),
                        None => {
                            self.start_share_end(session, member, &record, &work, share).await?
                        }
                    };
                    if let Some(end) = end {
                        let outcome = self.run_administration(session, ring, member, end).await?;
                        if let OperationOutcome::Rejected(reason) = &outcome {
                            tracing::warn!(?id, ?reason, "an invite share's end was refused");
                        }
                    }
                    // The marker goes once a joining device can no longer
                    // send a first request (`end_request`).
                    let removal = coven_database::ObjectRemoval {
                        path: ObjectPath::join_request(self.store(), work.device, work.id)
                            .as_str()
                            .to_owned(),
                        after: kept_until,
                    };
                    let children = share.into_iter().chain(end).collect();
                    // An invite ended by a refusal only a person can fix keeps
                    // that as its outcome, listed in `coven_operations` until
                    // the app acknowledges it (§13.2).
                    let outcome = match refusal {
                        Some(refusal) => {
                            let account =
                                work.account().ok_or(coven_database::DbError::DamagedDatabase)?;
                            Some((
                                crate::OperationKind::Invite.listed(),
                                OperationOutcome::SharingRefused {
                                    account: account.to_owned(),
                                    refusal,
                                },
                            ))
                        }
                        None => None,
                    };
                    self.database
                        .finish_operation_with_removal(id, children, removal, outcome)
                        .await?;
                    return Ok(InviteProgress::Finished);
                }
            }
        }
    }

    /// Move the checked invite to granting its access, in one transaction
    /// with adopting its account's grant and ending the record of its share,
    /// `shared`, with that record's outcome; then ask for the grant, unless
    /// another open invite's grant already shared the account with an
    /// invitation (§14.2). A grant that fails is the invite's pending record,
    /// and `step` follows the step this takes, which that failure belongs to.
    #[expect(
        clippy::too_many_arguments,
        reason = "a step of the invite's advance takes the pass's session, keys and member"
    )]
    async fn grant(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        record: &OperationRecord,
        mut work: InviteWork,
        shared: Option<(OperationId, OperationOutcome)>,
        step: &mut Option<u32>,
    ) -> Result<(), SyncError> {
        work.state = InviteState::Grant;
        let changes = match work.account() {
            Some(account) => vec![crate::sequence_sync::account_change(account, true, true)?],
            None => Vec::new(),
        };
        let update = work.update(record)?;
        let granting = update.last_step;
        self.database
            .advance_operation_with_access::<PendingRecord>(update, changes, shared)
            .await?;
        *step = Some(granting);
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
            if let Err(error) = self.apply_access(session, ring, member, grant).await {
                return match sharing_refusal(&error) {
                    Some(refusal) => self.end_refused(record.id, grant, refusal).await,
                    None => Err(error),
                };
            }
        }
        Ok(())
    }

    /// End the invite whose grant the provider refused for a reason only a
    /// person can fix, as a failed request check does: it settles, putting
    /// its marker in the request's place and taking its access back, and
    /// ends with the refusal as its outcome (§13.2).
    async fn end_refused(
        &self,
        id: OperationId,
        grant: OperationId,
        refusal: coven_foundation::sharing::SharingRefusal,
    ) -> Result<(), SyncError> {
        let record = self
            .database
            .operation(id)
            .await?
            .ok_or(coven_database::DbError::OperationChanged(id))?;
        let mut work = InviteWork::read(&record)?;
        tracing::warn!(?id, ?refusal, "the provider refused to share the store for an invite");
        let account = work.account().ok_or(coven_database::DbError::DamagedDatabase)?.to_owned();
        work.state = InviteState::Settled { share: None, end: None, refusal: Some(refusal) };
        self.database
            .advance_operation_refusing_grant(work.update(&record)?, account, grant)
            .await?;
        Ok(())
    }

    /// Put the ended invite's marker in its request's place, and return
    /// when the marker may go: a week after the invite's expiry, the
    /// retention grace, so a joining device whose clock lags the inviting
    /// device's by less than that sends no request after it is gone (§13.2).
    /// A request sent in between the delete and the create is taken too.
    async fn end_request(&self, work: &InviteWork) -> Result<u64, SyncError> {
        let kept_until = kept_until(seconds(work.expires_at)?)?;
        let path = ObjectPath::join_request(self.store(), work.device, work.id);
        self.put_marker(&path, kept_until).await?;
        Ok(kept_until)
    }

    /// Put the marker that may go at `kept_until` in the place of whatever
    /// is at `path`, a request or a pairing's handover, and return once it
    /// is there: delete, then create it, which a request sent in between
    /// finds taken, and delete again one that came first.
    pub(crate) async fn put_marker(
        &self,
        path: &ObjectPath,
        kept_until: u64,
    ) -> Result<(), SyncError> {
        let marker = InviteEnd { kept_until }.encode();
        loop {
            self.storage.delete_if_present(path).await?;
            match self.storage.create(path, &marker).await {
                Ok(_) => return Ok(()),
                Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                    match self.storage.read(path).await {
                        // An earlier attempt's marker, or another device's.
                        Ok(found) if InviteEnd::decode(&found).is_ok() => return Ok(()),
                        Ok(_) => (),
                        Err(error) if error.failure() == StorageFailure::NotFound => (),
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => return Err(error.into()),
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
        if !self.registered(session) || !state.active_admin(&me) {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        if !session.history.owns_storage(me) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        if let InviteBinding::Replace { expected } = work.binding {
            match state.active_member(&work.member) {
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
            inviter: work.inviter.clone(),
            admits: work.admits(),
            expires_at: seconds(work.expires_at)?,
            invite: work.id,
            member: work.member,
            secret: work.secret()?,
            initial_key,
            inviting_writer: work.device,
            read_through: session.through,
            storage: storage.encode()?,
        }
        .to_text()?;
        Ok(SecretText::new(code.to_string()))
    }
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

/// The refusal a failed grant carries, when only a person can fix it.
fn sharing_refusal(error: &SyncError) -> Option<coven_foundation::sharing::SharingRefusal> {
    match error {
        SyncError::Storage(error) => match error.failure() {
            StorageFailure::SharingRefused(refusal) => Some(refusal),
            _ => None,
        },
        _ => None,
    }
}

/// When the marker of an invite that expires at `expires`, in seconds since
/// 1970, may go: the retention grace after it (§13.2). A time the marker's
/// seconds can't hold is refused as the expiry the clock can't represent is.
pub(crate) fn kept_until(expires: u64) -> Result<u64, SyncError> {
    expires.checked_add(super::sequence_retention::GRACE.as_secs()).ok_or_else(|| {
        StorageFailure::InvalidConfiguration
            .with_source("the clock cannot represent when an ended invite's marker may go")
            .into()
    })
}

/// A time as whole seconds since the Unix epoch, as codes and markers carry
/// it.
pub(crate) fn seconds(at: SystemTime) -> Result<u64, SyncError> {
    Ok(at
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            StorageFailure::InvalidConfiguration
                .with_source(format!("an invite's time precedes 1970: {error}"))
        })?
        .as_secs())
}

pub(crate) fn decode_request(
    bytes: &[u8],
) -> Result<coven_format::objects::JoinRequest, SyncError> {
    match Object::decode(bytes)? {
        Object::JoinRequest(request) => Ok(request),
        _ => Err(SyncError::InvitationChanged),
    }
}

#[cfg(test)]
#[path = "sequence_invites_tests.rs"]
pub(crate) mod tests;

#[path = "sequence_invite_request.rs"]
mod request;
pub(crate) use request::{Expected, Opened, RequestChecks};

#[path = "sequence_invite_approval.rs"]
mod approval;

#[path = "sequence_invite_reset.rs"]
mod reset;

#[path = "sequence_invite_shares.rs"]
pub(crate) mod shares;

#[path = "sequence_invite_steps.rs"]
mod steps;
pub use steps::NewInvite;
pub(crate) use steps::{approve_request, end_invite, make_invite};

#[path = "invite_expiry.rs"]
pub(crate) mod expiry;
