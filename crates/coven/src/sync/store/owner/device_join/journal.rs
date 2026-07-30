use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinCleanupProgress {
    AwaitingBoth,
    AwaitingAdministrator {
        joiner: JoinerJoinTerminal,
    },
    AwaitingJoiner {
        administrator: ProviderAdminJoinTerminal,
    },
    Ready {
        administrator: ProviderAdminJoinTerminal,
        joiner: JoinerJoinTerminal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinStatus {
    OperationInProgress {
        attempt_id: DeviceJoinAttemptId,
    },
    AwaitingAccessRequest {
        offer: DeviceJoinOffer,
    },
    AwaitingProviderAdmission {
        request: DeviceProviderAccessRequest,
    },
    AwaitingRegistrationRequest {
        approval: DeviceProviderAdmissionApproval,
    },
    AwaitingBootstrap {
        request: DeviceRegistrationRequest,
    },
    AwaitingChallengePublication {
        bootstrap: ProvisionalDeviceBootstrap,
    },
    AwaitingReadiness {
        bootstrap: ProviderReadyDeviceBootstrap,
    },
    AwaitingProviderCompletion {
        readiness: DeviceJoinReadiness,
    },
    AwaitingActivation {
        completion: DeviceProviderAdmissionCompletion,
    },
    AwaitingCompletion {
        activation: DeviceJoinActivation,
    },
    Activated {
        store: JoinedStore,
    },
    Abandoned {
        abandonment: DeviceJoinAbandonment,
    },
    ProviderAccessGrantCreatePending {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
    },
    AbandonmentCreatePending {
        abandonment: DeviceJoinAbandonmentRef,
    },
    CancellationCreatePending {
        cancellation: DeviceJoinOutcomeRef,
    },
    ProviderClosurePending {
        cancellation: DeviceJoinCancellation,
        producer: DeviceJoinProducer,
    },
    ProviderClosed {
        terminal: ProviderAdminJoinTerminal,
    },
    JoinerClosed {
        terminal: JoinerJoinTerminal,
    },
    CleanupReceiptCreatePending {
        cancellation: DeviceJoinCancellation,
        receipt: DeviceJoinCleanupReceiptRef,
    },
    Cancelled {
        cancellation: DeviceJoinCancellation,
    },
    CleanupPending {
        cancellation: DeviceJoinCancellation,
        progress: DeviceJoinCleanupProgress,
    },
    AwaitingCleanupActivation {
        receipt: DeviceJoinCleanupReceipt,
    },
    CleanupActivated {
        activation: DeviceJoinCleanupActivation,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinAction {
    TransferOffer(DeviceJoinOffer),
    TransferProviderAccessRequest(DeviceProviderAccessRequest),
    TransferProviderAdmissionApproval(DeviceProviderAdmissionApproval),
    TransferRegistrationRequest(DeviceRegistrationRequest),
    TransferProvisionalBootstrap(ProvisionalDeviceBootstrap),
    TransferProviderReadyBootstrap(ProviderReadyDeviceBootstrap),
    TransferReadiness(DeviceJoinReadiness),
    TransferProviderAdmissionCompletion(DeviceProviderAdmissionCompletion),
    TransferActivation(DeviceJoinActivation),
    TransferAbandonment(DeviceJoinAbandonment),
    TransferCancellation(DeviceJoinCancellation),
    TransferProviderAdminTerminal(ProviderAdminJoinTerminal),
    TransferJoinerTerminal(JoinerJoinTerminal),
    TransferCleanupReceipt(DeviceJoinCleanupReceipt),
    TransferCleanupActivation(DeviceJoinCleanupActivation),
    CompleteJoin(DeviceJoinActivation),
    CompleteCleanup(DeviceJoinCleanupActivation),
    ResumeOperation {
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OwnerJoinProgress {
    Offered(DeviceJoinOffer),
    RegistrationRequested(DeviceRegistrationRequest),
    AbandonmentCreateIntent {
        offer: DeviceJoinOffer,
        abandonment: DeviceJoinAbandonmentRef,
        prepared: PreparedDeviceJoinObject,
    },
    AttemptActivated(ProvisionalDeviceBootstrap),
    ActivationCreateIntent {
        bootstrap: ProvisionalDeviceBootstrap,
        completion: DeviceProviderAdmissionCompletion,
        outcome: DeviceJoinOutcomeRef,
        prepared: PreparedDeviceJoinObject,
    },
    CancellationCreateIntent {
        attempt: DeviceJoinAttemptRef,
        cancellation: DeviceJoinOutcomeRef,
        prepared: PreparedDeviceJoinObject,
    },
    ActivationPrepared {
        completion: DeviceProviderAdmissionCompletion,
        activation: DeviceJoinActivation,
    },
    Abandoned(DeviceJoinAbandonment),
    Cancelled(DeviceJoinCancellation),
    CleanupReceiptCreateIntent {
        cancellation: DeviceJoinCancellation,
        receipt: DeviceJoinCleanupReceiptRef,
        receipt_bytes: Vec<u8>,
        prepared: PreparedDeviceJoinObject,
    },
    CleanupReceipt(DeviceJoinCleanupReceipt),
    CleanupActivated(DeviceJoinCleanupActivation),
    CancelledComplete(DeviceJoinCleanupActivation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ProviderAdminJoinProgress {
    AccessRequested(DeviceProviderAccessRequest),
    AccessGrantPrepared {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
        prepared: PreparedDeviceJoinObject,
    },
    ApprovalPrepared(DeviceProviderAdmissionApproval),
    AttemptObserved(ProvisionalDeviceBootstrap),
    ChallengeCreateIntent(ProvisionalDeviceBootstrap),
    ProviderReady(ProviderReadyDeviceBootstrap),
    ResponseObserved(DeviceJoinReadiness),
    CleanupIntent {
        cancellation: DeviceJoinCancellation,
        challenge: ProviderChallengeDisposition,
        prior_state_hash: ObjectHash,
    },
    Completed(DeviceProviderAdmissionCompletion),
    Cancelled(ProviderAdminJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedDeviceJoinObject {
    pub object: ExactObjectRef,
    pub stored_bytes: Vec<u8>,
}

impl PreparedDeviceJoinObject {
    pub(super) fn from_prepared(prepared: &crate::storage::PreparedExactObject) -> Self {
        Self {
            object: prepared.reference().clone(),
            stored_bytes: prepared.stored_bytes().to_vec(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum JoinerJoinProgress {
    OfferReceived(DeviceJoinOffer),
    AccessRequested(DeviceProviderAccessRequest),
    ApprovalReceived(DeviceProviderAdmissionApproval),
    RegistrationPrepared(DeviceRegistrationRequest),
    ProviderReady(ProviderReadyDeviceBootstrap),
    RegistrationCreateIntent(ProviderReadyDeviceBootstrap),
    RegistrationCreated(StoreDeviceRegistrationRef),
    AckCreateIntent(StoreDeviceRegistrationRef),
    AckCreated(crate::protocol::store_commit::StoreAckRef),
    ResponseCreateIntent(DeviceJoinReadiness),
    Ready(DeviceJoinReadiness),
    ActivationObserved {
        readiness: DeviceJoinReadiness,
        activation: DeviceJoinActivation,
    },
    Activated(JoinedStore),
    Abandoned(DeviceJoinAbandonment),
    CleanupIntent {
        cancellation: DeviceJoinCancellation,
        registration: SlotDisposition,
        initial_ack: SlotDisposition,
        response: JoinerResponseDisposition,
        prior_state_hash: ObjectHash,
    },
    Cancelled(JoinerJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
    CleanupActivated(DeviceJoinCleanupActivation),
    CancelledComplete(DeviceJoinCleanupActivation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DeviceJoinRoleProgress {
    Owner(OwnerJoinProgress),
    ProviderAdministrator(ProviderAdminJoinProgress),
    Joiner(JoinerJoinProgress),
}

impl DeviceJoinRoleProgress {
    fn role(&self) -> DeviceJoinRole {
        match self {
            Self::Owner(_) => DeviceJoinRole::Owner,
            Self::ProviderAdministrator(_) => DeviceJoinRole::ProviderAdministrator,
            Self::Joiner(_) => DeviceJoinRole::Joiner,
        }
    }

    fn role_name(&self) -> &'static str {
        self.role().as_str()
    }

    fn validate_transition(&self, next: &Self) -> Result<(), DeviceJoinError> {
        let adjacent = match (self, next) {
            (Self::Owner(previous), Self::Owner(next)) => owner_adjacent(previous, next),
            (Self::ProviderAdministrator(previous), Self::ProviderAdministrator(next)) => {
                provider_admin_adjacent(previous, next)
            }
            (Self::Joiner(previous), Self::Joiner(next)) => joiner_adjacent(previous, next),
            _ => false,
        };
        if adjacent {
            Ok(())
        } else {
            Err(DeviceJoinError::NonAdjacentJournalTransition)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinJournalRecord {
    pub(super) attempt_id: DeviceJoinAttemptId,
    pub(super) progress: Box<DeviceJoinRoleProgress>,
}

impl DeviceJoinJournalRecord {
    pub(super) fn owner_offered(offer: DeviceJoinOffer) -> Self {
        Self {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
                offer,
            ))),
        }
    }

    pub(crate) fn require_initial(&self) -> Result<(), DeviceJoinError> {
        validate_initial_progress(&self.progress)
    }

    pub(crate) fn require_replacement_terminal(&self) -> Result<(), DeviceJoinError> {
        if matches!(
            &*self.progress,
            DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
                _
            )) | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(_))
        ) {
            Ok(())
        } else {
            Err(DeviceJoinError::JournalConflict)
        }
    }

    pub(crate) fn store_key(&self) -> String {
        store_journal_key(self.attempt_id, self.progress.role_name())
    }

    pub(crate) fn store_key_for(attempt_id: DeviceJoinAttemptId, role: DeviceJoinRole) -> String {
        store_journal_key(attempt_id, role.as_str())
    }

    pub(crate) fn validate_successor(&self, next: &Self) -> Result<(), DeviceJoinError> {
        if self.attempt_id != next.attempt_id {
            return Err(DeviceJoinError::JournalConflict);
        }
        self.progress.validate_transition(&next.progress)
    }

    pub(crate) fn status(&self) -> DeviceJoinStatus {
        device_join_status(self)
    }

    pub(crate) fn action(&self) -> Option<DeviceJoinAction> {
        device_join_action(self)
    }

    pub(crate) fn sort_key(&self) -> (DeviceJoinAttemptId, DeviceJoinRole) {
        (self.attempt_id, self.progress.role())
    }

    pub(crate) fn attempt_key(&self) -> String {
        attempt_key(self.attempt_id)
    }

    pub(super) fn joiner_abandonment_transition(
        &self,
        abandonment: &DeviceJoinAbandonment,
    ) -> Result<Option<Self>, DeviceJoinError> {
        if self.attempt_id != abandonment.abandonment.attempt_id {
            return Err(DeviceJoinError::JournalConflict);
        }
        match &*self.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(existing)) => {
                if existing == abandonment {
                    Ok(None)
                } else {
                    Err(DeviceJoinError::JournalConflict)
                }
            }
            DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::AccessRequested(_) | JoinerJoinProgress::ApprovalReceived(_),
            ) => Ok(Some(Self {
                attempt_id: self.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::Abandoned(abandonment.clone()),
                )),
            })),
            _ => Err(DeviceJoinError::JournalConflict),
        }
    }
}

/// Durable role journal. Each row stores a closed progress value; SQLite's
/// compare-and-swap update rejects stale or skipped transitions.
#[derive(Clone, Debug)]
pub struct DeviceJoinJournalDatabase {
    path: PathBuf,
}

impl DeviceJoinJournalDatabase {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DeviceJoinError> {
        let path = path.as_ref().to_path_buf();
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS device_join_journals (
                 attempt_id TEXT NOT NULL,
                 role TEXT NOT NULL,
                 payload TEXT NOT NULL,
                 PRIMARY KEY (attempt_id, role)
             ) STRICT, WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS pending_join_transfers (
                 attempt_id TEXT PRIMARY KEY,
                 payload_hash TEXT NOT NULL,
                 payload TEXT NOT NULL
             ) STRICT, WITHOUT ROWID;",
        )?;
        Ok(Self { path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub fn begin(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        validate_initial_progress(&record.progress)?;
        let connection = Connection::open(&self.path)?;
        let tx = connection.unchecked_transaction()?;
        let attempt_id = attempt_key(record.attempt_id);
        let role = record.progress.role_name();
        let payload = serde_json::to_string(&record)?;
        tx.execute(
            "INSERT OR IGNORE INTO device_join_journals (attempt_id, role, payload)
             VALUES (?1, ?2, ?3)",
            (&attempt_id, role, &payload),
        )?;
        let actual: String = tx.query_row(
            "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
            (&attempt_id, role),
            |row| row.get(0),
        )?;
        tx.commit()?;
        let actual = serde_json::from_str::<DeviceJoinJournalRecord>(&actual)?;
        if actual != record {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(actual)
    }

    pub fn load(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        let connection = Connection::open(&self.path)?;
        let raw = connection
            .query_row(
                "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
                (attempt_key(attempt_id), role.as_str()),
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let record = raw
            .map(|value| serde_json::from_str::<DeviceJoinJournalRecord>(&value))
            .transpose()?;
        if record
            .as_ref()
            .is_some_and(|record| record.attempt_id != attempt_id || record.progress.role() != role)
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(record)
    }

    pub fn records(&self) -> Result<Vec<DeviceJoinJournalRecord>, DeviceJoinError> {
        let connection = Connection::open(&self.path)?;
        let mut statement = connection.prepare(
            "SELECT attempt_id, role, payload FROM device_join_journals
             ORDER BY attempt_id, role",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut records = Vec::new();
        for row in rows {
            let (attempt_id, role, payload) = row?;
            let record: DeviceJoinJournalRecord = serde_json::from_str(&payload)?;
            if attempt_key(record.attempt_id) != attempt_id || record.progress.role_name() != role {
                return Err(DeviceJoinError::JournalConflict);
            }
            records.push(record);
        }
        records.sort_by_key(|record| (record.attempt_id, record.progress.role()));
        Ok(records)
    }

    pub fn actions(&self) -> Result<Vec<DeviceJoinAction>, DeviceJoinError> {
        Ok(self
            .records()?
            .iter()
            .filter_map(device_join_action)
            .collect())
    }

    pub fn completed_joiner_readiness(
        &self,
        attempt: &DeviceJoinAttemptRef,
    ) -> Result<Option<DeviceJoinReadiness>, DeviceJoinError> {
        let Some(record) = self.load(attempt.attempt_id, DeviceJoinRole::Joiner)? else {
            return Ok(None);
        };
        match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
                if &readiness.proof.attempt != attempt {
                    return Err(DeviceJoinError::JournalConflict);
                }
                Ok(Some(readiness.clone()))
            }
            _ => Ok(None),
        }
    }

    pub fn observe_joiner_activation_if_pending(
        &self,
        activation: &DeviceJoinActivation,
    ) -> Result<Option<DeviceJoinReadiness>, DeviceJoinError> {
        let attempt_id = activation.outcome.attempt().attempt_id;
        if !matches!(activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        let Some(current) = self.load(attempt_id, DeviceJoinRole::Joiner)? else {
            return Ok(None);
        };
        match &*current.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
                let observed = DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::Joiner(
                        JoinerJoinProgress::ActivationObserved {
                            readiness: readiness.clone(),
                            activation: activation.clone(),
                        },
                    )),
                };
                self.advance(&current, observed)?;
                Ok(Some(readiness.clone()))
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
                readiness,
                activation: existing,
            }) if existing == activation => Ok(Some(readiness.clone())),
            _ => Err(DeviceJoinError::JournalConflict),
        }
    }

    pub fn advance(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        previous.validate_successor(&next)?;
        let previous_payload = serde_json::to_string(previous)?;
        let next_payload = serde_json::to_string(&next)?;
        let connection = Connection::open(&self.path)?;
        let changed = connection.execute(
            "UPDATE device_join_journals SET payload = ?1
             WHERE attempt_id = ?2 AND role = ?3 AND payload = ?4",
            (
                &next_payload,
                attempt_key(previous.attempt_id),
                previous.progress.role_name(),
                &previous_payload,
            ),
        )?;
        if changed != 1 {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(())
    }

    pub fn complete_joiner_cleanup(
        &self,
        activation: DeviceJoinCleanupActivation,
    ) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
        let attempt_id = activation.receipt.attempt_id;
        let current = self
            .load(attempt_id, DeviceJoinRole::Joiner)?
            .ok_or(DeviceJoinError::JournalConflict)?;
        if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(existing)) =
            &*current.progress
        {
            if existing == &activation {
                return Ok(existing.clone());
            }
            return Err(DeviceJoinError::JournalConflict);
        }
        let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(durable)) =
            &*current.progress
        else {
            return Err(DeviceJoinError::JournalConflict);
        };
        if durable != &activation {
            return Err(DeviceJoinError::JournalConflict);
        }
        self.advance(
            &current,
            DeviceJoinJournalRecord {
                attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::CancelledComplete(activation.clone()),
                )),
            },
        )?;
        Ok(activation)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinRole {
    Owner,
    ProviderAdministrator,
    Joiner,
}

impl DeviceJoinRole {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::ProviderAdministrator => "provider_administrator",
            Self::Joiner => "joiner",
        }
    }
}

fn device_join_status(record: &DeviceJoinJournalRecord) -> DeviceJoinStatus {
    match &*record.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(offer))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(offer)) => {
            DeviceJoinStatus::AwaitingAccessRequest {
                offer: offer.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(request),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request)) => {
            DeviceJoinStatus::AwaitingProviderAdmission {
                request: request.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(approval),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(approval)) => {
            DeviceJoinStatus::AwaitingRegistrationRequest {
                approval: approval.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(request))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request)) => {
            DeviceJoinStatus::AwaitingBootstrap {
                request: request.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap))
        | DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AttemptObserved(bootstrap),
        )
        | DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ChallengeCreateIntent(bootstrap),
        ) => DeviceJoinStatus::AwaitingChallengePublication {
            bootstrap: bootstrap.clone(),
        },
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ProviderReady(bootstrap),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ProviderReady(bootstrap))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationCreateIntent(bootstrap)) => {
            DeviceJoinStatus::AwaitingReadiness {
                bootstrap: bootstrap.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ResponseObserved(readiness),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            DeviceJoinStatus::AwaitingProviderCompletion {
                readiness: readiness.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationCreateIntent {
            completion,
            ..
        })
        | DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => DeviceJoinStatus::AwaitingActivation {
            completion: completion.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            activation, ..
        })
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
            activation,
            ..
        }) => DeviceJoinStatus::AwaitingCompletion {
            activation: activation.clone(),
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Activated(store)) => {
            DeviceJoinStatus::Activated {
                store: store.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(abandonment))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(abandonment)) => {
            DeviceJoinStatus::Abandoned {
                abandonment: abandonment.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessGrantPrepared { request, grant, .. },
        ) => DeviceJoinStatus::ProviderAccessGrantCreatePending {
            request: request.clone(),
            grant: grant.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AbandonmentCreateIntent {
            abandonment,
            ..
        }) => DeviceJoinStatus::AbandonmentCreatePending {
            abandonment: abandonment.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancellationCreateIntent {
            cancellation,
            ..
        }) => DeviceJoinStatus::CancellationCreatePending {
            cancellation: cancellation.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(cancellation)) => {
            DeviceJoinStatus::CleanupPending {
                cancellation: cancellation.clone(),
                progress: DeviceJoinCleanupProgress::AwaitingBoth,
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::CleanupIntent { cancellation, .. },
        ) => DeviceJoinStatus::ProviderClosurePending {
            cancellation: cancellation.clone(),
            producer: DeviceJoinProducer::ProviderAdministrator,
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupIntent {
            cancellation, ..
        }) => DeviceJoinStatus::ProviderClosurePending {
            cancellation: cancellation.clone(),
            producer: DeviceJoinProducer::Joiner,
        },
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => DeviceJoinStatus::ProviderClosed {
            terminal: ProviderAdminJoinTerminal::Cancelled(closure.clone()),
        },
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => DeviceJoinStatus::ProviderClosed {
            terminal: ProviderAdminJoinTerminal::WriteRevoked(revocation.clone()),
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            DeviceJoinStatus::JoinerClosed {
                terminal: JoinerJoinTerminal::Cancelled(closure.clone()),
            }
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            DeviceJoinStatus::JoinerClosed {
                terminal: JoinerJoinTerminal::WriteRevoked(revocation.clone()),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceiptCreateIntent {
            cancellation,
            receipt,
            ..
        }) => DeviceJoinStatus::CleanupReceiptCreatePending {
            cancellation: cancellation.clone(),
            receipt: receipt.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(receipt)) => {
            DeviceJoinStatus::AwaitingCleanupActivation {
                receipt: receipt.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupActivated(activation))
        | DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancelledComplete(activation))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(activation))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(activation)) => {
            DeviceJoinStatus::CleanupActivated {
                activation: activation.clone(),
            }
        }
        _ => DeviceJoinStatus::OperationInProgress {
            attempt_id: record.attempt_id,
        },
    }
}

fn device_join_action(record: &DeviceJoinJournalRecord) -> Option<DeviceJoinAction> {
    let resume = || DeviceJoinAction::ResumeOperation {
        attempt_id: record.attempt_id,
        role: record.progress.role(),
    };
    match &*record.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(offer)) => {
            Some(DeviceJoinAction::TransferOffer(offer.clone()))
        }
        DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::RegistrationRequested(_)
            | OwnerJoinProgress::AbandonmentCreateIntent { .. }
            | OwnerJoinProgress::ActivationCreateIntent { .. }
            | OwnerJoinProgress::CancellationCreateIntent { .. }
            | OwnerJoinProgress::CleanupReceiptCreateIntent { .. },
        ) => Some(resume()),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => Some(
            DeviceJoinAction::TransferProvisionalBootstrap(bootstrap.clone()),
        ),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared {
            activation, ..
        }) => Some(DeviceJoinAction::TransferActivation(activation.clone())),
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(abandonment)) => {
            Some(DeviceJoinAction::TransferAbandonment(abandonment.clone()))
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(cancellation)) => {
            Some(DeviceJoinAction::TransferCancellation(cancellation.clone()))
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(receipt)) => {
            Some(DeviceJoinAction::TransferCleanupReceipt(receipt.clone()))
        }
        DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::CleanupActivated(activation)
            | OwnerJoinProgress::CancelledComplete(activation),
        ) => Some(DeviceJoinAction::TransferCleanupActivation(
            activation.clone(),
        )),

        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(_)
            | ProviderAdminJoinProgress::AccessGrantPrepared { .. }
            | ProviderAdminJoinProgress::AttemptObserved(_)
            | ProviderAdminJoinProgress::ChallengeCreateIntent(_)
            | ProviderAdminJoinProgress::ResponseObserved(_)
            | ProviderAdminJoinProgress::CleanupIntent { .. },
        ) => Some(resume()),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(approval),
        ) => Some(DeviceJoinAction::TransferProviderAdmissionApproval(
            approval.clone(),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ProviderReady(bootstrap),
        ) => Some(DeviceJoinAction::TransferProviderReadyBootstrap(
            bootstrap.clone(),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => Some(DeviceJoinAction::TransferProviderAdmissionCompletion(
            completion.clone(),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => Some(DeviceJoinAction::TransferProviderAdminTerminal(
            ProviderAdminJoinTerminal::Cancelled(closure.clone()),
        )),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => Some(DeviceJoinAction::TransferProviderAdminTerminal(
            ProviderAdminJoinTerminal::WriteRevoked(revocation.clone()),
        )),

        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::OfferReceived(_)
            | JoinerJoinProgress::ApprovalReceived(_)
            | JoinerJoinProgress::ProviderReady(_)
            | JoinerJoinProgress::RegistrationCreateIntent(_)
            | JoinerJoinProgress::RegistrationCreated(_)
            | JoinerJoinProgress::AckCreateIntent(_)
            | JoinerJoinProgress::AckCreated(_)
            | JoinerJoinProgress::ResponseCreateIntent(_)
            | JoinerJoinProgress::CleanupIntent { .. },
        ) => Some(resume()),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request)) => Some(
            DeviceJoinAction::TransferProviderAccessRequest(request.clone()),
        ),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request)) => Some(
            DeviceJoinAction::TransferRegistrationRequest(request.clone()),
        ),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            Some(DeviceJoinAction::TransferReadiness(readiness.clone()))
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved {
            activation,
            ..
        }) => Some(DeviceJoinAction::CompleteJoin(activation.clone())),
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            Some(DeviceJoinAction::TransferJoinerTerminal(
                JoinerJoinTerminal::Cancelled(closure.clone()),
            ))
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            Some(DeviceJoinAction::TransferJoinerTerminal(
                JoinerJoinTerminal::WriteRevoked(revocation.clone()),
            ))
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(activation)) => {
            Some(DeviceJoinAction::CompleteCleanup(activation.clone()))
        }
        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::Activated(_)
            | JoinerJoinProgress::Abandoned(_)
            | JoinerJoinProgress::CancelledComplete(_),
        ) => None,
    }
}

pub(crate) fn load_pending_device_join_actions(
    pending: &DeviceJoinJournalDatabase,
) -> Result<Vec<DeviceJoinAction>, DeviceJoinError> {
    pending.actions()
}

pub(crate) fn load_pending_device_join_status(
    pending: &DeviceJoinJournalDatabase,
    attempt_id: DeviceJoinAttemptId,
) -> Result<Option<DeviceJoinStatus>, DeviceJoinError> {
    pending
        .load(attempt_id, DeviceJoinRole::Joiner)
        .map(|record| record.as_ref().map(DeviceJoinJournalRecord::status))
}

pub(super) fn store_journal_key(attempt_id: DeviceJoinAttemptId, role: &str) -> String {
    format!("device_join/{}/{role}", attempt_key(attempt_id))
}

pub(super) fn database_error(error: crate::database::DbError) -> DeviceJoinError {
    DeviceJoinError::Store(error.into_message())
}

pub(super) fn provider_error(error: impl std::fmt::Display) -> DeviceJoinError {
    DeviceJoinError::Provider(error.to_string())
}

fn owner_adjacent(previous: &OwnerJoinProgress, next: &OwnerJoinProgress) -> bool {
    matches!(
        (previous, next),
        (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::RegistrationRequested(_)
        ) | (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AttemptActivated(_)
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AbandonmentCreateIntent { .. },
            OwnerJoinProgress::Abandoned(_)
        ) | (
            OwnerJoinProgress::AttemptActivated(_),
            OwnerJoinProgress::ActivationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AttemptActivated(_),
            OwnerJoinProgress::CancellationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::ActivationCreateIntent { .. },
            OwnerJoinProgress::ActivationPrepared { .. }
        ) | (
            OwnerJoinProgress::CancellationCreateIntent { .. },
            OwnerJoinProgress::Cancelled(_)
        ) | (
            OwnerJoinProgress::Cancelled(_),
            OwnerJoinProgress::CleanupReceiptCreateIntent { .. }
        ) | (
            OwnerJoinProgress::CleanupReceiptCreateIntent { .. },
            OwnerJoinProgress::CleanupReceipt(_)
        ) | (
            OwnerJoinProgress::CleanupReceipt(_),
            OwnerJoinProgress::CleanupActivated(_)
        ) | (
            OwnerJoinProgress::CleanupActivated(_),
            OwnerJoinProgress::CancelledComplete(_)
        )
    )
}

fn provider_admin_adjacent(
    previous: &ProviderAdminJoinProgress,
    next: &ProviderAdminJoinProgress,
) -> bool {
    matches!(
        (previous, next),
        (
            ProviderAdminJoinProgress::AccessRequested(_),
            ProviderAdminJoinProgress::AccessGrantPrepared { .. }
        ) | (
            ProviderAdminJoinProgress::AccessGrantPrepared { .. },
            ProviderAdminJoinProgress::ApprovalPrepared(_)
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::AttemptObserved(_)
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::ChallengeCreateIntent(_)
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::ProviderReady(_)
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::ResponseObserved(_)
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::Completed(_)
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::CleanupIntent { .. },
            ProviderAdminJoinProgress::Cancelled(_)
        ) | (
            ProviderAdminJoinProgress::CleanupIntent { .. },
            ProviderAdminJoinProgress::WriteRevoked(_)
        )
    )
}

fn joiner_adjacent(previous: &JoinerJoinProgress, next: &JoinerJoinProgress) -> bool {
    matches!(
        (previous, next),
        (
            JoinerJoinProgress::OfferReceived(_),
            JoinerJoinProgress::AccessRequested(_)
        ) | (
            JoinerJoinProgress::AccessRequested(_),
            JoinerJoinProgress::ApprovalReceived(_)
        ) | (
            JoinerJoinProgress::AccessRequested(_),
            JoinerJoinProgress::Abandoned(_)
        ) | (
            JoinerJoinProgress::ApprovalReceived(_),
            JoinerJoinProgress::RegistrationPrepared(_)
        ) | (
            JoinerJoinProgress::ApprovalReceived(_),
            JoinerJoinProgress::Abandoned(_)
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::ProviderReady(_)
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::ProviderReady(_),
            JoinerJoinProgress::RegistrationCreateIntent(_)
        ) | (
            JoinerJoinProgress::ProviderReady(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::RegistrationCreateIntent(_),
            JoinerJoinProgress::RegistrationCreated(_)
        ) | (
            JoinerJoinProgress::RegistrationCreateIntent(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::RegistrationCreated(_),
            JoinerJoinProgress::AckCreateIntent(_)
        ) | (
            JoinerJoinProgress::RegistrationCreated(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::AckCreateIntent(_),
            JoinerJoinProgress::AckCreated(_)
        ) | (
            JoinerJoinProgress::AckCreateIntent(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::ResponseCreateIntent(_)
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::Ready(_)
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::ResponseCreateIntent(_),
            JoinerJoinProgress::Ready(_)
        ) | (
            JoinerJoinProgress::ResponseCreateIntent(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::ActivationObserved { .. }
        ) | (
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::ProviderReady(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::RegistrationCreateIntent(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::RegistrationCreated(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::AckCreateIntent(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::ResponseCreateIntent(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::CleanupIntent { .. },
            JoinerJoinProgress::Cancelled(_)
        ) | (
            JoinerJoinProgress::ActivationObserved { .. },
            JoinerJoinProgress::Activated(_)
        ) | (
            JoinerJoinProgress::Cancelled(_),
            JoinerJoinProgress::CleanupActivated(_)
        ) | (
            JoinerJoinProgress::WriteRevoked(_),
            JoinerJoinProgress::CleanupActivated(_)
        ) | (
            JoinerJoinProgress::CleanupActivated(_),
            JoinerJoinProgress::CancelledComplete(_)
        )
    )
}

fn validate_initial_progress(progress: &DeviceJoinRoleProgress) -> Result<(), DeviceJoinError> {
    if matches!(
        progress,
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(_))
            | DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::AccessRequested(_)
            )
            | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(_))
    ) {
        Ok(())
    } else {
        Err(DeviceJoinError::NonAdjacentJournalTransition)
    }
}

pub(super) fn require_distinct_slots(slots: &[ObjectSlot]) -> Result<(), DeviceJoinError> {
    let unique = slots.iter().collect::<BTreeSet<_>>();
    if unique.len() == slots.len() {
        Ok(())
    } else {
        Err(DeviceJoinError::DuplicateReservedSlot)
    }
}

pub(super) fn attempt_key(attempt_id: DeviceJoinAttemptId) -> String {
    serde_json::to_value(attempt_id)
        .expect("device join attempt id serialization cannot fail")
        .as_str()
        .expect("device join attempt id serializes as a string")
        .to_string()
}
