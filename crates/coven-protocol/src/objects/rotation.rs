use super::*;

/// Store-key work is in flight or committed but not fully adopted. Every cloud
/// seal refuses while this holds, including while a local removal candidate may
/// still publish and after a committed rotation whose key is not locally
/// adopted or whose exact operation journal remains open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "store-key rotation is pending ({state:?}) while this device is sealing under generation \
     {live_generation}; refusing to seal for the cloud until the pending state is completed"
)]
pub struct RotationPending {
    pub state: RotationPendingState,
    pub live_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationPendingState {
    Candidate {
        generation: u64,
    },
    LocalCommitted {
        generation: u64,
    },
    PeerCommitted {
        generation: u64,
    },
    CandidateAndPeer {
        candidate_generation: u64,
        peer_generation: u64,
    },
    LocalCommittedAndPeer {
        local_generation: u64,
        peer_generation: u64,
    },
}

/// The exact store-key work that blocks sealing: a local candidate, an activated
/// local removal awaiting adoption, a peer's committed generation awaiting
/// adoption, or a local fact together with a peer fact. Durable database
/// transitions and this in-memory copy move together at operation boundaries.
///
/// Shared (behind one `Arc`, via `CloudSyncStorage::shared_pending_rotation`)
/// across every path that seals data for the cloud — changesets, heads, blobs,
/// tombstones, snapshots — so a rotation this device can't adopt blocks all of
/// them the same way, not just the removal call that discovered it. This is the
/// structural half of the invariant: this device must never seal under a
/// generation the store has already superseded.
/// The protocol-state key that persists the serialized [`RotationGate`].
/// Restored before the first sync cycle so a restart cannot forget an
/// unfinished candidate or an unadopted committed rotation and resume sealing
/// under an unauthorized key.
pub const ROTATION_GATE_STATE_KEY: &str = "rotation_gate";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RotationGate {
    /// This device's own rotation, with no unadopted peer generation.
    Local(LocalRotation),
    /// A generation the store committed that this device has not adopted, with
    /// no local rotation of its own.
    Peer { generation: NonZeroU64 },
    /// Both facts at once: this device's rotation, and a peer generation it has
    /// not adopted.
    LocalAndPeer {
        local: LocalRotation,
        peer_generation: NonZeroU64,
    },
}

/// This device's own rotation: a candidate it may still publish or lose, or its
/// committed rotation awaiting local adoption. The commit consumes the candidate,
/// so the two are the same fact at different points of its life — a device holds
/// one or the other, never both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LocalRotation {
    Candidate {
        generation: NonZeroU64,
        mutation: crate::store_commit::ObjectHash,
    },
    Committed {
        generation: NonZeroU64,
        mutation: crate::store_commit::ObjectHash,
    },
}

impl LocalRotation {
    /// Reported through the replication layer's `PendingRotation::pending_generation`,
    /// which exists for status reporting in tests and for hosts built with
    /// `test-utils`.
    #[cfg(any(test, feature = "test-utils"))]
    fn generation(&self) -> NonZeroU64 {
        match self {
            Self::Candidate { generation, .. } | Self::Committed { generation, .. } => *generation,
        }
    }
}

impl RotationGate {
    /// This device's own rotation, if the gate holds one.
    fn local(&self) -> Option<LocalRotation> {
        match self {
            Self::Local(local) | Self::LocalAndPeer { local, .. } => Some(*local),
            Self::Peer { .. } => None,
        }
    }

    /// The unadopted peer generation, if the gate holds one.
    fn peer(&self) -> Option<NonZeroU64> {
        match self {
            Self::Peer { generation }
            | Self::LocalAndPeer {
                peer_generation: generation,
                ..
            } => Some(*generation),
            Self::Local(_) => None,
        }
    }

    /// The gate holding both facts — `None` when neither is left, which is the
    /// absence of a gate rather than an empty one.
    fn from_parts(local: Option<LocalRotation>, peer: Option<NonZeroU64>) -> Option<Self> {
        match (local, peer) {
            (Some(local), Some(peer_generation)) => Some(Self::LocalAndPeer {
                local,
                peer_generation,
            }),
            (Some(local), None) => Some(Self::Local(local)),
            (None, Some(generation)) => Some(Self::Peer { generation }),
            (None, None) => None,
        }
    }

    /// The gate `local` owns, keeping whatever peer fact came with it.
    fn with_local(local: LocalRotation, peer: Option<NonZeroU64>) -> Self {
        match peer {
            Some(peer_generation) => Self::LocalAndPeer {
                local,
                peer_generation,
            },
            None => Self::Local(local),
        }
    }

    pub fn pending_state(&self) -> RotationPendingState {
        match self {
            Self::Local(LocalRotation::Candidate { generation, .. }) => {
                RotationPendingState::Candidate {
                    generation: generation.get(),
                }
            }
            Self::Local(LocalRotation::Committed { generation, .. }) => {
                RotationPendingState::LocalCommitted {
                    generation: generation.get(),
                }
            }
            Self::Peer { generation } => RotationPendingState::PeerCommitted {
                generation: generation.get(),
            },
            Self::LocalAndPeer {
                local: LocalRotation::Candidate { generation, .. },
                peer_generation,
            } => RotationPendingState::CandidateAndPeer {
                candidate_generation: generation.get(),
                peer_generation: peer_generation.get(),
            },
            Self::LocalAndPeer {
                local: LocalRotation::Committed { generation, .. },
                peer_generation,
            } => RotationPendingState::LocalCommittedAndPeer {
                local_generation: generation.get(),
                peer_generation: peer_generation.get(),
            },
        }
    }

    /// Stage `mutation` as this device's rotation candidate, on whatever gate is
    /// already open (`None` when none is).
    pub fn with_candidate(
        gate: Option<Self>,
        generation: u64,
        mutation: crate::store_commit::ObjectHash,
    ) -> Result<Self, String> {
        let Some(generation) = NonZeroU64::new(generation) else {
            return Err("rotation candidate names generation zero".to_string());
        };
        let candidate = LocalRotation::Candidate {
            generation,
            mutation,
        };
        match gate.as_ref().and_then(Self::local) {
            Some(LocalRotation::Committed { .. }) => {
                Err("a committed local rotation already owns the gate".to_string())
            }
            Some(existing) if existing != candidate => {
                Err("another rotation candidate already owns the gate".to_string())
            }
            _ => Ok(Self::with_local(
                candidate,
                gate.as_ref().and_then(Self::peer),
            )),
        }
    }

    /// Promote this device's staged candidate to its committed rotation.
    pub fn commit_candidate(
        gate: Option<Self>,
        generation: u64,
        mutation: crate::store_commit::ObjectHash,
    ) -> Result<Self, String> {
        let refusal = "rotation commit does not own the pending candidate gate";
        let Some(generation) = NonZeroU64::new(generation) else {
            return Err(refusal.to_string());
        };
        let committed = LocalRotation::Committed {
            generation,
            mutation,
        };
        let local = gate.as_ref().and_then(Self::local);
        // The gate must hold this exact candidate — or already hold the commit,
        // which is the same fact arriving twice rather than a second rotation.
        if local
            != Some(LocalRotation::Candidate {
                generation,
                mutation,
            })
            && local != Some(committed)
        {
            return Err(refusal.to_string());
        }
        Ok(Self::with_local(
            committed,
            gate.as_ref().and_then(Self::peer),
        ))
    }

    /// Record that the store committed `generation`. Forward-only: an older
    /// generation never displaces a newer one already recorded.
    pub fn merge_peer_commit(gate: Option<Self>, generation: u64) -> Result<Self, String> {
        let Some(generation) = NonZeroU64::new(generation) else {
            return Err("committed rotation names generation zero".to_string());
        };
        let peer_generation = gate
            .as_ref()
            .and_then(Self::peer)
            .map_or(generation, |recorded| recorded.max(generation));
        Ok(match gate.as_ref().and_then(Self::local) {
            Some(local) => Self::LocalAndPeer {
                local,
                peer_generation,
            },
            None => Self::Peer {
                generation: peer_generation,
            },
        })
    }

    pub fn remove_candidate(
        self,
        generation: u64,
        mutation: crate::store_commit::ObjectHash,
    ) -> Result<Option<Self>, String> {
        let lost = NonZeroU64::new(generation).map(|generation| LocalRotation::Candidate {
            generation,
            mutation,
        });
        if lost.is_none() || self.local() != lost {
            return Err("rotation loss does not own the pending candidate gate".to_string());
        }
        Ok(Self::from_parts(None, self.peer()))
    }

    pub fn replace_candidate_mutation(
        self,
        generation: u64,
        previous: crate::store_commit::ObjectHash,
        replacement: crate::store_commit::ObjectHash,
    ) -> Result<Self, String> {
        let refusal = "rotation candidate replacement lost its exact owner";
        let Some(generation) = NonZeroU64::new(generation) else {
            return Err(refusal.to_string());
        };
        if self.local()
            != Some(LocalRotation::Candidate {
                generation,
                mutation: previous,
            })
        {
            return Err(refusal.to_string());
        }
        Ok(Self::with_local(
            LocalRotation::Candidate {
                generation,
                mutation: replacement,
            },
            self.peer(),
        ))
    }

    pub fn complete_local_adoption(
        self,
        generation: u64,
        mutation: crate::store_commit::ObjectHash,
    ) -> Result<Option<Self>, String> {
        match self.local() {
            Some(LocalRotation::Candidate { .. }) => {
                return Err(
                    "rotation adoption cannot close while a candidate is pending".to_string(),
                )
            }
            Some(LocalRotation::Committed {
                generation: committed,
                mutation: committed_mutation,
            }) if committed.get() == generation && committed_mutation == mutation => {}
            _ => return Err("rotation adoption does not own the committed gate".to_string()),
        }
        // Adopting the local rotation adopts every peer generation it covers; a
        // newer peer generation is a separate fact and stays.
        Ok(Self::from_parts(
            None,
            self.peer().filter(|peer| peer.get() > generation),
        ))
    }

    pub fn complete_peer_adoption(self, adopted_generation: u64) -> Result<Option<Self>, String> {
        if adopted_generation == 0 {
            return Err("adopted rotation names generation zero".to_string());
        }
        Ok(Self::from_parts(
            self.local(),
            self.peer().filter(|peer| peer.get() > adopted_generation),
        ))
    }

    /// The newest generation the gate names. Reported through the replication
    /// layer's `PendingRotation::pending_generation`.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn generation(&self) -> NonZeroU64 {
        match self {
            Self::Local(local) => local.generation(),
            Self::Peer { generation } => *generation,
            Self::LocalAndPeer {
                local,
                peer_generation,
            } => local.generation().max(*peer_generation),
        }
    }
}
