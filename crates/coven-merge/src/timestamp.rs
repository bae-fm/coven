use crate::MergeError;
use coven_foundation::id_source::DeviceId;

/// §7.2: 48-bit milliseconds, a 16-bit counter, and a 64-bit device id,
/// ordered in that order. No clock is read here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    milliseconds: u64,
    counter: u16,
    device: DeviceId,
}

impl Timestamp {
    /// The greatest millisecond the timestamp can represent.
    pub const MAX_MILLISECONDS: u64 = (1_u64 << 48) - 1;

    /// Construct a timestamp, refusing milliseconds outside its 48-bit range.
    pub fn new(milliseconds: u64, counter: u16, device: DeviceId) -> Result<Self, MergeError> {
        if milliseconds > Self::MAX_MILLISECONDS {
            return Err(MergeError::MillisecondsOutOfRange(milliseconds));
        }
        Ok(Self {
            milliseconds,
            counter,
            device,
        })
    }

    /// Stamp a local write or entry after the latest timestamp from local work
    /// and applied incoming work. The caller persists that latest timestamp.
    /// A counter overflow advances the millisecond; exhaustion is an error.
    pub fn next(latest: Option<Self>, clock_ms: u64, device: DeviceId) -> Result<Self, MergeError> {
        if clock_ms > Self::MAX_MILLISECONDS {
            return Err(MergeError::MillisecondsOutOfRange(clock_ms));
        }
        match latest {
            None => Self::new(clock_ms, 0, device),
            Some(last) if clock_ms > last.milliseconds => Self::new(clock_ms, 0, device),
            Some(last) => match last.counter.checked_add(1) {
                Some(counter) => Self::new(last.milliseconds, counter, device),
                None if last.milliseconds < Self::MAX_MILLISECONDS => {
                    Self::new(last.milliseconds + 1, 0, device)
                }
                None => Err(MergeError::TimestampExhausted),
            },
        }
    }

    /// The wall-clock part, in milliseconds.
    pub fn milliseconds(self) -> u64 {
        self.milliseconds
    }

    /// The logical counter within the millisecond.
    pub fn counter(self) -> u16 {
        self.counter
    }

    /// The writing device's id.
    pub fn device(self) -> DeviceId {
        self.device
    }
}

#[cfg(test)]
#[path = "timestamp_tests.rs"]
mod tests;
