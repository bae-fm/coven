//! Circle control authority resolved against retained Store state.
//!
//! The module owns three reads the rest of the store session takes: a control's
//! retained activation, whether one control covers another, and the key a
//! transaction opens a Circle blob with. Everything beneath them lives in the
//! submodule that owns that question.

use crate::store::store_session::verified_store_authority::VerifiedStoreLookup;
use crate::store::store_session::StoreRecords;
use crate::*;

mod activation;
mod lineage;
mod package_access;

pub(crate) use activation::{
    circle_activation_commit_ref_on, retained_circle_activation_commit_ref_on,
};
use lineage::verified_circle_control_covers_with_prefix_on;
use package_access::circle_blob_opening_protection_on;
pub use package_access::CirclePackageAccess;

impl StoreDatabase {
    pub(super) fn verified_circle_activation_on(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        let Some(activation_commit) = records.circle_activation_commit_ref(circle_id, control)?
        else {
            return Ok(None);
        };
        let retained = authority.retained_materialization_by_ref_on(records, &activation_commit)?;
        if retained.root() != root {
            return Err(DbError::Message(
                "Circle activation belongs to another Store root".to_string(),
            ));
        }
        retained.circle_activation(circle_id, control).map(Some)
    }

    pub(super) fn verified_circle_control_covers_on(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        current: &coven_protocol::circle::PreparedCircleControl,
        prior: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        verified_circle_control_covers_with_prefix_on(
            records,
            authority,
            root,
            circle_id,
            current,
            prior,
            &[],
        )
    }
}

impl crate::store::store_session::StoreTransaction<'_, '_> {
    pub(super) fn circle_blob_opening_protection(
        self,
        verified_store: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        expected_control: &coven_protocol::circle::CircleControlCoord,
        expected_key_fingerprint: coven_keys::encryption::KeyFingerprint,
    ) -> Result<coven_protocol::objects::BlobSpoolProtection, DbError> {
        circle_blob_opening_protection_on(
            self.records(),
            verified_store,
            root,
            circle_id,
            expected_control,
            expected_key_fingerprint,
        )
    }
}
