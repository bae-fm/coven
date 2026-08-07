//! Promotion of a Store member to the Owner membership role.

mod error;
mod history;
pub(crate) use coven_protocol::owner_promotion_journal as journal;
mod operation;

pub use error::OwnerPromotionError;
pub(crate) use history::OwnerPromotionHistory;
pub(crate) use operation::AuthorizedOwnerPromotion;

#[cfg(test)]
mod tests;
