use coven_crypto::SecretBytes;
use serde::Serialize;
use std::io::{self, Cursor, Write};
use zeroize::Zeroizing;

// Count without retaining any secret bytes, then serialize into a fixed slice.
// A slice writer cannot grow or abandon an allocation containing secret data.
pub(crate) fn encode(value: &impl Serialize) -> Result<SecretBytes, crate::StorageError> {
    let mut count = CountingWriter(0);
    serde_json::to_writer(&mut count, value)
        .map_err(|error| crate::StorageFailure::Encoding.with_source(error))?;
    let mut bytes = Zeroizing::new(vec![0; count.0]);
    let mut output = Cursor::new(bytes.as_mut_slice());
    serde_json::to_writer(&mut output, value)
        .map_err(|error| crate::StorageFailure::Encoding.with_source(error))?;
    if output.position() != count.0 as u64 {
        return Err(crate::StorageFailure::Protocol.with_source("secret encoding changed length"));
    }
    Ok(SecretBytes::new(std::mem::take(&mut *bytes)))
}

struct CountingWriter(usize);
impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("secret encoding length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "secret_json_tests.rs"]
mod tests;
