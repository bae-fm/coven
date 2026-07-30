//! Store-owned Owner promotion workflow.

mod acceptance;
mod authority;
mod error;
mod finalization;
mod journal;
mod request;

pub(super) use acceptance::accept;
pub(crate) use error::OwnerPromotionError;
pub(super) use finalization::finalize;
pub(crate) use journal::{OwnerPromotionJournal, OwnerPromotionJournalTransition};
pub(super) use request::begin;

#[cfg(test)]
mod tests;
