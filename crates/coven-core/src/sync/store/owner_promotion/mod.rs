//! Store-owned Owner promotion workflow.

mod acceptance;
mod authority;
mod error;
mod finalization;
mod journal;
mod request;
mod status;

pub use acceptance::accept_owner_promotion;
pub use error::OwnerPromotionError;
pub use finalization::finalize_owner_promotion;
pub(crate) use journal::{OwnerPromotionJournal, OwnerPromotionJournalTransition};
pub use request::begin_owner_promotion;
pub use status::owner_promotion_status;

#[cfg(test)]
mod tests;
