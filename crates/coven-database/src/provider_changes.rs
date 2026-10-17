//! Provider work committed with the membership state that requires it.

use crate::{sqlite::DatabaseConnection, AccessKeyDeletion, DbError, ProviderAccessChange};

/// Account intentions and credential deletions derived from authenticated history.
#[derive(Default)]
pub struct ProviderChanges {
    /// Provider accounts needed by active members and open invitations.
    pub accounts: Vec<ProviderAccessChange>,
    /// Credentials superseded or removed from the active membership.
    pub keys: Vec<AccessKeyDeletion>,
}

impl ProviderChanges {
    pub(crate) fn record<P: crate::PendingValue>(
        &self,
        db: &DatabaseConnection,
    ) -> Result<(), DbError> {
        let device = crate::pending::own_device(db)?;
        crate::provider_access::adopt::<P>(db, device, &self.accounts)?;
        crate::operation::record_access_keys::<P>(db, device, &self.keys)
    }
}
