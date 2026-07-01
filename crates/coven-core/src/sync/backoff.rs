//! Shared backoff math. One formula serves both the cycle-level wait (sync_loop,
//! when consecutive whole-cycle failures pile up) and the blob engine's per-upload
//! wait ([`crate::blob::upload`], when an individual upload keeps failing).

/// The natural cycle interval — both backoff users count attempts in multiples
/// of this so a fresh failure rides the next natural sync cycle.
const BASE_SECS: u64 = 30;

/// `BASE_SECS · 2^n` seconds, capped at `cap` seconds. The exponent is clamped
/// before the shift so a runaway counter never overflows. Callers wrap the
/// result in whatever `Duration` type they need (`std::time::Duration`,
/// `chrono::Duration`, …).
pub fn backoff_secs(n: u32, cap: u64) -> u64 {
    BASE_SECS.saturating_mul(1u64 << n.min(20)).min(cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_per_step_until_cap() {
        assert_eq!(backoff_secs(0, 300), 30);
        assert_eq!(backoff_secs(1, 300), 60);
        assert_eq!(backoff_secs(2, 300), 120);
        assert_eq!(backoff_secs(3, 300), 240);
    }

    #[test]
    fn cap_clamps_the_growth() {
        assert_eq!(backoff_secs(4, 300), 300); // 480 → 300
        assert_eq!(backoff_secs(100, 300), 300); // long past the cap
    }

    #[test]
    fn huge_n_does_not_overflow() {
        // Without the exponent clamp, `1u64 << u32::MAX` is UB.
        assert_eq!(backoff_secs(u32::MAX, 600), 600);
    }
}
