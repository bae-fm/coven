use crate::database::Database;
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{OwnerPromotionId, OwnerPromotionStatus};

use super::OwnerPromotionError;

pub async fn owner_promotion_status(
    db: &Database,
    promotion_id: OwnerPromotionId,
) -> Result<OwnerPromotionStatus, OwnerPromotionError> {
    StoreDatabase::new(db)
        .load_owner_promotion_journal(promotion_id)
        .await?
        .map(|journal| journal.status())
        .ok_or(OwnerPromotionError::NotFound(promotion_id))
}
