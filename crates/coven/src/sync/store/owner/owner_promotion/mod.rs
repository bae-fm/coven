//! Store-owned Owner promotion workflow.

use super::AuthorizedWriterOperation;

mod authority;
mod error;
mod journal;
mod operation;

pub(crate) use error::OwnerPromotionError;
pub(crate) use journal::{OwnerPromotionJournal, OwnerPromotionJournalTransition};
pub(crate) use operation::AuthorizedOwnerPromotion;

#[cfg(test)]
mod tests;
