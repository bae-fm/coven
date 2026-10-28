//! How far a request's object bytes have gone, for a caller that shows a
//! transfer as it goes (§6.7): the bytes of a request body handed to the
//! connection, or the bytes of a response received. Bytes sent are never
//! confirmation: the provider may still refuse or lose what it was handed.

use std::sync::Arc;

/// Told how far through an object a request's bytes reach, counted from the
/// object's start, each time more are handed over or received. A request
/// sent again, such as after a refreshed sign-in, counts again from where
/// it starts. Clones tell the same place.
#[derive(Clone)]
pub struct ByteProgress {
    report: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    /// Where the request's body starts in its object.
    start: u64,
}

impl std::fmt::Debug for ByteProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteProgress")
            .field("read", &self.report.is_some())
            .field("start", &self.start)
            .finish()
    }
}

impl ByteProgress {
    /// Tell `report` each new count.
    pub fn new(report: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self {
            report: Some(Arc::new(report)),
            start: 0,
        }
    }

    /// Tell no one, for a request whose progress nothing shows.
    pub fn unread() -> Self {
        Self {
            report: None,
            start: 0,
        }
    }

    /// The same place told about bytes that start `start` bytes into the
    /// object, as a part or a ranged read's do.
    pub(crate) fn starting_at(&self, start: u64) -> Self {
        Self {
            report: self.report.clone(),
            start: self.start + start,
        }
    }

    /// `moved` bytes of this request's have gone: handed to the connection,
    /// or received.
    pub(crate) fn moved(&self, moved: u64) {
        if let Some(report) = &self.report {
            report(self.start + moved);
        }
    }
}

#[cfg(test)]
#[path = "byte_progress_tests.rs"]
mod tests;
