use super::*;
use coven_protocol::store_commit::VerifiedStoreBatchCommit;

pub struct PreparedStoreWrite {
    pub write_id: WriteId,
    pub changeset: Vec<u8>,
    pub partitions: PreparedStoreWritePartitions,
    pub inverse_changeset: Vec<u8>,
    pub base: StoreWriteBase,
    pub blob_facts: StoreWriteBlobFacts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStoreWritePartitions {
    pub store: Option<gate::AudiencePartition>,
    pub circles: Vec<gate::AudiencePartition>,
    pub local: Option<gate::AudiencePartition>,
}

pub struct MergeReplayWriteOverlay {
    pub write_id: WriteId,
    pub partitions: PreparedStoreWritePartitions,
}

#[derive(Clone, Copy)]
pub enum StoreWriteRouting<'a> {
    Unscoped,
    MergeScoped(&'a EncryptionService),
}

impl PreparedStoreWritePartitions {
    #[cfg(any(test, feature = "test-utils"))]
    pub fn iter(&self) -> impl Iterator<Item = &gate::AudiencePartition> {
        self.store
            .iter()
            .chain(self.circles.iter())
            .chain(self.local.iter())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreWriteBase {
    pub dependencies: BTreeMap<String, StoreBatchCommitRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreWriteBlobFacts {
    pub blobs: Vec<StoreWriteBlobFact>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreWriteBlobFact {
    pub table: String,
    pub row_id: String,
    pub row_stamp: String,
    pub column: String,
    pub blob: BlobRef,
    pub plaintext_size: u64,
    pub plaintext_hash: ObjectHash,
    pub external_path: Option<PathBuf>,
    pub previous: Option<StoreWriteRemoteBlob>,
    pub audience_move: Option<StoreWriteBlobMoveDestination>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreWriteRemoteBlob {
    pub authority: coven_protocol::audience_package::PackageAudience,
    pub stored: StoredBlobRef,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreWriteBlobMoveDestination {
    Local,
    Remote {
        audience: coven_protocol::blob::locator::RemoteAudience,
        locator: coven_protocol::blob::locator::BlobLocator,
        spool_path: PathBuf,
    },
}

impl StoreWriteBlobFact {
    pub fn identity_key(&self) -> (String, String, String, String) {
        (
            self.table.clone(),
            self.row_id.clone(),
            self.column.clone(),
            self.row_stamp.clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct PreparedStoreWriteCommit {
    pub audiences: PreparedAudienceObjects,
    pub commit: ExactProtocolObject<VerifiedStoreBatchCommit>,
    pub head: ExactProtocolObject<StoreDeviceHead>,
}

/// A candidate whose activation is blocked: the commit and head it would have
/// activated, each named by the reference that identifies it.
///
/// The upload bytes are deliberately absent. A blocked candidate is only ever
/// examined and cleaned up — its objects are deleted from storage by reference,
/// never written again — so carrying them would be carrying what no reader
/// reads.
#[derive(Debug, Clone)]
pub struct BlockedMergeCandidate {
    pub commit: VerifiedStoreBatchCommit,
    pub commit_bytes: Vec<u8>,
    pub commit_object: ExactObjectRef,
    pub head: StoreDeviceHead,
    pub head_object: ExactObjectRef,
}

#[derive(Debug, Clone)]
pub struct PreparedMergeAbandonmentCandidates {
    pub candidate: BlockedMergeCandidate,
    pub authority: BlockedMergeCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletePreparedStoreWriteOutcome {
    Published,
    AuthorExcluded {
        device_id: coven_protocol::store_commit::StoreDeviceId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorExclusionActivationLocator {
    exclusion: coven_protocol::store_commit::StoreDeviceExclusionRef,
    accepted_cut: BTreeMap<coven_protocol::causal_grants::AuthorStreamId, StoreBatchCommitRef>,
    activation_commit: StoreBatchCommitRef,
    activation_head: coven_protocol::store_commit::StoreDeviceHeadRef,
}

impl AuthorExclusionActivationLocator {
    pub fn verified(
        exclusion: coven_protocol::store_commit::StoreDeviceExclusionRef,
        accepted_cut: BTreeMap<coven_protocol::causal_grants::AuthorStreamId, StoreBatchCommitRef>,
        activation_commit: StoreBatchCommitRef,
        activation_head: coven_protocol::store_commit::StoreDeviceHeadRef,
    ) -> Self {
        Self {
            exclusion,
            accepted_cut,
            activation_commit,
            activation_head,
        }
    }

    pub fn exclusion(&self) -> &coven_protocol::store_commit::StoreDeviceExclusionRef {
        &self.exclusion
    }

    pub fn accepted_cut(
        &self,
    ) -> &BTreeMap<coven_protocol::causal_grants::AuthorStreamId, StoreBatchCommitRef> {
        &self.accepted_cut
    }

    pub fn activation_head(&self) -> &coven_protocol::store_commit::StoreDeviceHeadRef {
        &self.activation_head
    }

    pub fn activation_commit(&self) -> &StoreBatchCommitRef {
        &self.activation_commit
    }
}

#[derive(Debug, Clone)]
pub enum TerminalCandidateAuthority {
    AuthorExclusion(AuthorExclusionActivationLocator),
    MembershipGrantRevocation {
        grant_id: coven_protocol::membership::MembershipGrantId,
        membership: coven_protocol::circle_control::StoreMembershipStateRef,
        activation_commit: StoreBatchCommitRef,
        activation_head: coven_protocol::store_commit::StoreDeviceHeadRef,
    },
    DependencyRetraction(coven_protocol::remote_object::VerifiedDependencyRetractionAuthority),
}

#[derive(Debug, Clone)]
pub struct TerminalCandidateCleanupVerification {
    pub authority: TerminalCandidateAuthority,
    pub candidate: BlockedMergeCandidate,
}

#[derive(Debug)]
pub struct InitialStoreMembershipAuthority {
    pub head_refs: Vec<coven_protocol::membership::MembershipHeadRef>,
}

impl InitialStoreMembershipAuthority {
    const CURSOR_STATE_KEY_PREFIX: &'static str = "membership_head_cursor/";

    pub fn cursor_state_key_for_stream(
        owner_grant: &coven_protocol::membership::MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
    ) -> String {
        format!("{}{owner_grant}/{stream_id}", Self::CURSOR_STATE_KEY_PREFIX)
    }

    fn cursor_state_key(reference: &coven_protocol::membership::MembershipHeadRef) -> String {
        Self::cursor_state_key_for_stream(
            &reference.coord.author_owner_grant,
            reference.coord.stream_id,
        )
    }

    pub fn load_on(conn: &Connection) -> Result<Self, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT value FROM protocol_state \
                 WHERE substr(key, 1, length(?1)) = ?1 ORDER BY key",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([Self::CURSOR_STATE_KEY_PREFIX], |row| {
                row.get::<_, String>(0)
            })
            .map_err(DbError::from)?;
        let mut head_refs = Vec::new();
        for row in rows {
            let value = row.map_err(DbError::from)?;
            let reference: coven_protocol::membership::MembershipHeadRef =
                serde_json::from_str(&value).map_err(|error| {
                    DbError::context("membership head cursor is malformed", error)
                })?;
            if reference.coord.seq == 0 {
                return Err(DbError::Message(
                    "membership head cursor has sequence zero".to_string(),
                ));
            }
            head_refs.push(reference);
        }
        Ok(Self { head_refs })
    }

    pub fn install_on(&self, conn: &Connection) -> Result<(), DbError> {
        for reference in &self.head_refs {
            let key = Self::cursor_state_key(reference);
            if let Some(existing) = get_protocol_state_on(conn, &key)? {
                let existing: coven_protocol::membership::MembershipHeadRef =
                    serde_json::from_str(&existing).map_err(|error| {
                        DbError::context("membership head cursor is malformed", error)
                    })?;
                if existing.coord.stream_key() != reference.coord.stream_key() {
                    return Err(DbError::Message(
                        "membership head cursor key names a different stream".to_string(),
                    ));
                }
                if existing.coord.seq > reference.coord.seq {
                    continue;
                }
                if existing.coord.seq == reference.coord.seq {
                    if existing == *reference {
                        continue;
                    }
                    return Err(DbError::Message(
                        "membership head cursor forks at the same sequence".to_string(),
                    ));
                }
            }
            let value = serde_json::to_string(reference)
                .map_err(|error| DbError::context("serialize membership head cursor", error))?;
            set_protocol_state_on(conn, &key, &value)?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn cursor_state_key_for_test(
        reference: &coven_protocol::membership::MembershipHeadRef,
    ) -> String {
        Self::cursor_state_key(reference)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeAbandonmentState {
    None,
    Prepared,
    Accepted,
    CandidateWon,
    OtherWon,
    AuthorExcluded,
}

#[derive(Debug, Clone)]
pub struct OutboundStoreAck {
    pub reference: StoreAckRef,
    pub ack: ExactProtocolObject<StoreAck>,
    pub circle_acknowledgements: Vec<coven_protocol::prepared_commit::CircleAckActivation>,
    pub activation: OutboundStoreAckActivation,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OutboundStoreAckActivation {
    AwaitingCandidate,
    Prepared(coven_protocol::prepared_commit::PreparedStoreOperationCommit),
    Nonactivating(coven_protocol::prepared_commit::PreparedStoreOperationCommit),
}

#[derive(Debug, Clone)]
pub struct PublishedStoreAck {
    pub reference: StoreAckRef,
    pub successor_slot: coven_protocol::objects::ObjectSlot,
}
