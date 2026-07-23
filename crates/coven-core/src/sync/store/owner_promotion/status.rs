use crate::sync::store::Store;
use crate::sync::store_commit::{OwnerPromotionId, OwnerPromotionStatus};

use super::OwnerPromotionError;

impl Store {
    #[doc(hidden)]
    pub async fn owner_promotion_status(
        &self,
        promotion_id: OwnerPromotionId,
    ) -> Result<OwnerPromotionStatus, OwnerPromotionError> {
        self.database()
            .load_owner_promotion_journal(promotion_id)
            .await?
            .map(|journal| journal.status())
            .ok_or(OwnerPromotionError::NotFound(promotion_id))
    }
}
