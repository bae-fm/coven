use super::*;

pub enum Readiness {
    Ready,
    AlreadyMaterialized,
    Held(HeldStorePosition),
}

pub(crate) fn held_object_error(error: StoreObjectError) -> HeldStorePositionReason {
    match error {
        StoreObjectError::Storage(source) => HeldStorePositionReason::ObjectUnreadableStorage {
            key: "exact Store object".to_string(),
            source: source.into(),
        },
        StoreObjectError::InvalidObject { key, source, .. } => match *source {
            StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
            StoreProtocolError::RelocatedSlot { .. }
            | StoreProtocolError::RelocatedPackage { .. }
            | StoreProtocolError::StoreRootMismatch { .. }
            | StoreProtocolError::StoreMismatch { .. }
            | StoreProtocolError::FounderMismatch { .. } => {
                HeldStorePositionReason::WrongSlotProtocol(source.into())
            }
            source => HeldStorePositionReason::ObjectUnreadableProtocol {
                key,
                source: source.into(),
            },
        },
    }
}

pub(super) fn historical_local_store_membership(
    latest: LocalStoreMembership,
    candidate: LocalStoreMembership,
) -> LocalStoreMembership {
    if matches!(latest, LocalStoreMembership::Removed)
        || matches!(candidate, LocalStoreMembership::Removed)
    {
        LocalStoreMembership::Removed
    } else if matches!(latest, LocalStoreMembership::Current)
        && matches!(candidate, LocalStoreMembership::Current)
    {
        LocalStoreMembership::Current
    } else if matches!(latest, LocalStoreMembership::IdentityNotSupplied)
        || matches!(candidate, LocalStoreMembership::IdentityNotSupplied)
    {
        LocalStoreMembership::IdentityNotSupplied
    } else {
        LocalStoreMembership::NotYetMember
    }
}
