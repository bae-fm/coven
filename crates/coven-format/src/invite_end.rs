//! The marker an ended invite leaves at its join request's path (D9, D10).

use crate::error::Error;

/// What an ended invite leaves at `<store>/join-requests/<invite>`, created
/// once in place of the request: a joining device that finds it there ends
/// its join as declined, and a request sent after the invite ended finds the
/// path taken. It holds no secret, only the time after which any device may
/// delete it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InviteEnd {
    /// Seconds since the Unix epoch after which the marker may go: a
    /// joining device sends no first request by then.
    pub kept_until: u64,
}

const KIND: u8 = 37;
const LENGTH: usize = 3 + 8;

impl InviteEnd {
    /// The marker's complete bytes: its kind, format and time.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LENGTH);
        bytes.push(KIND);
        bytes.extend_from_slice(&crate::FormatVersion::CURRENT.number().to_be_bytes());
        bytes.extend_from_slice(&self.kept_until.to_be_bytes());
        bytes
    }

    /// Read a marker. Bytes of another kind, such as a join request, fail
    /// with `UnknownTag`.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, KIND)?;
        let time: [u8; 8] =
            bytes.get(3..LENGTH).ok_or(Error::Truncated)?.try_into().expect("eight bytes");
        if bytes.len() > LENGTH {
            return Err(Error::TrailingBytes);
        }
        Ok(Self { kept_until: u64::from_be_bytes(time) })
    }
}

#[cfg(test)]
#[path = "invite_end_tests.rs"]
mod tests;
