//! Store-owned Owner promotion workflow.

mod acceptance;
mod authority;
mod error;
mod finalization;
mod journal;
mod request;
mod status;

pub use error::OwnerPromotionError;
pub(crate) use journal::{OwnerPromotionJournal, OwnerPromotionJournalTransition};

#[cfg(test)]
mod tests;
