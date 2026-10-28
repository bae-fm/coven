//! How far a request body has been handed to the connection, for a caller
//! that shows an upload as it goes (§20.1). It is never confirmation: the
//! provider may still refuse or lose what it was handed.

use std::sync::Arc;

/// Told the bytes of an object handed to the connection so far, counted from
/// the object's start, each time more are handed over. A request sent again,
/// such as after a refreshed sign-in, counts again from where it starts.
/// Clones tell the same place.
#[derive(Clone)]
pub struct SendProgress {
    report: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    /// Where the request's body starts in its object.
    start: u64,
}

impl std::fmt::Debug for SendProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendProgress")
            .field("read", &self.report.is_some())
            .field("start", &self.start)
            .finish()
    }
}

impl SendProgress {
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

    /// The same place told about a body that starts `start` bytes into the
    /// object, as a part does.
    pub(crate) fn starting_at(&self, start: u64) -> Self {
        Self {
            report: self.report.clone(),
            start: self.start + start,
        }
    }

    /// The connection has taken `taken` bytes of this request's body.
    pub(crate) fn taken(&self, taken: u64) {
        if let Some(report) = &self.report {
            report(self.start + taken);
        }
    }
}

#[cfg(test)]
#[path = "send_progress_tests.rs"]
mod tests;
