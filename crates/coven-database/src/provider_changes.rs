//! Provider work committed with the membership state that requires it.

use crate::ProviderAccessChange;

/// Account intentions derived from authenticated history.
#[derive(Default)]
pub struct ProviderChanges {
    /// Provider accounts needed by active members and open invitations.
    pub accounts: Vec<ProviderAccessChange>,
}
