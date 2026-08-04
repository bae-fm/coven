//! Store-owned Owner promotion workflow.

use super::AuthorizedWriterOperation;

mod authority;
mod error;
pub(crate) use crate::protocol::owner_promotion_journal as journal;
mod operation;

pub(crate) use error::OwnerPromotionError;
pub(crate) use operation::AuthorizedOwnerPromotion;

#[cfg(test)]
mod tests;
