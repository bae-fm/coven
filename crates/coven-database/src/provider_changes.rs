//! Provider work committed with the membership state that requires it.

use crate::{sqlite::DatabaseConnection, DbError, ProviderAccessChange};

/// Account intentions derived from authenticated history.
#[derive(Default)]
pub struct ProviderChanges {
    /// Provider accounts needed by active members and open invitations.
    pub accounts: Vec<ProviderAccessChange>,
}

impl ProviderChanges {
    pub(crate) fn record<P: crate::PendingValue>(
        &self,
        db: &DatabaseConnection,
    ) -> Result<(), DbError> {
        crate::provider_access::adopt::<P>(db, &self.accounts)
    }
}
