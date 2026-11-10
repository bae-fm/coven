//! Why sharing a store was refused for good (§13.2).

/// Why the provider refused to share the store, in a way only a person can
/// fix: the invited address's side or the inviting account's. Retrying is
/// refused the same way until an address or an account changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SharingRefusal {
    /// The provider won't take the invited address: it names no account the
    /// provider can share with.
    InvalidAddress,
    /// The inviting account's email is unverified.
    EmailUnverified,
    /// The inviting account's team doesn't share outside the team.
    TeamSharingOff,
    /// The inviting account's plan has no sharing.
    PlanWithoutSharing,
    /// The folder holds as many members or pending invitations as the
    /// inviting account allows.
    TooManyMembers,
}
