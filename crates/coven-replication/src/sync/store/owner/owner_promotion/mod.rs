//! Store-owned Owner promotion workflow.

use super::AuthorizedWriterOperation;

mod error;
pub(crate) use coven_protocol::owner_promotion_journal as journal;
mod operation;

pub use error::OwnerPromotionError;
pub(crate) use operation::AuthorizedOwnerPromotion;

#[cfg(test)]
mod tests;
