use super::*;

impl MemoryProvider {
    /// The provider's time for a publication or listing: its clock's reading.
    pub(super) fn storage_time(&self) -> Result<coven_format::value::StorageTime, StorageError> {
        coven_format::value::StorageTime::from_system_time(self.clock.now())
            .map_err(|error| StorageFailure::Protocol.with_source(error))
    }

    /// Request every page after the first of a listing with `entries`
    /// entries; each is traced and can fail like any request.
    pub(super) async fn further_pages(
        &self,
        request: MemoryRequest,
        entries: usize,
    ) -> Result<(), StorageError> {
        for _ in 1..entries.div_ceil(self.listing_page).max(1) {
            self.before(request.clone()).await?;
        }
        Ok(())
    }
}
