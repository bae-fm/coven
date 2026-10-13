use super::*;

/// Each attempted native call in the in-memory provider, before fault injection.
/// This provider has one page per listing and no implicit metadata lookups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryRequest {
    /// Read a whole object or an explicit byte range (including missing paths).
    Read(ObjectPath, Option<ByteRange>),
    /// Enumerate one prefix in one native call.
    List(ObjectPrefix),
    /// Publish an immutable object.
    Create(ObjectPath),
    /// Replace an object.
    Replace(ObjectPath),
    /// Query exact metadata.
    Status(ObjectPath),
    /// Delete an object.
    Delete(ObjectPath),
    /// Account, permission or multipart control/part call.
    Other(&'static str),
}

impl MemoryStorage {
    /// Attempted calls on this connection; `for_device` starts a separate trace.
    pub fn request_trace(&self) -> Vec<MemoryRequest> {
        self.provider
            .request_trace
            .lock()
            .expect("request trace poisoned")
            .clone()
    }
}
