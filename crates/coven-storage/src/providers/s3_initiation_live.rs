#![cfg(test)]

//! Whether the service lists an unfinished upload's start, and never dates it
//! before a publication that finished first, nor after one that came later
//! (suite 7). Ending a floor session begun in an earlier second than the
//! current floor's publication (Appendix E.10) is sound only then: a session
//! begun after the floor was published must never look older than it. Only
//! fixed labels, times and comparison outcomes enter output.
use super::*;
use coven_foundation::clock::Clock;

/// Trials, spread across seconds by `SPACING`.
const TRIALS: u64 = 20;
/// Between trials, so the publications' times cover the whole second.
const SPACING: std::time::Duration = std::time::Duration::from_millis(50);

/// Observe in a fresh store and print the verdict. A copy profile stages a
/// floor session in its device's folder, never at the floor's key, so it has
/// nothing to observe here.
pub(super) async fn report(settings: &live::LiveS3, profile: S3Settings) -> bool {
    if profile.copy_publication.multipart().is_some() {
        println!("upload_initiation publication=copy");
        return false;
    }
    let held = in_namespace(settings, "upload_initiation", profile, observe).await;
    println!(
        "upload_initiation_ordered {} cleanup=PASS",
        match held {
            Some(true) => "PASS",
            Some(false) => "FAIL",
            None => "FAIL (not observed)",
        }
    );
    held == Some(true)
}

/// Each trial reads the floor's time, begins a floor session after it and
/// lists that session's start, then aborts it and replaces the floor, whose
/// new time must not be earlier than the session's start.
async fn observe(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
) -> Result<bool, StorageError> {
    let floor = ObjectPath::floor(store);
    let key = floor.as_str().to_owned();
    s.create(&floor, b"upload initiation trial=0").await?;
    let clock = coven_foundation::clock::SystemClock;
    let mut held = true;
    for trial in 1..=TRIALS {
        let current = s.status(&floor).await?.ok_or(StorageFailure::NotFound)?;
        let mut session = s.begin_replace(&current, 4).await?;
        s.upload_part(&mut session, b"data").await?;
        let listed: Vec<_> = s
            .provider()
            .unfinished_uploads(&key, None)
            .await?
            .into_iter()
            .filter(|(listed, _, _)| *listed == key)
            .collect();
        s.abort_upload(&session).await?;
        let begun = match listed.as_slice() {
            [(_, _, Some(initiated))] => u64::try_from(initiated.secs()).ok(),
            _ => None,
        };
        s.replace(&current, format!("upload initiation trial={trial}").as_bytes()).await?;
        let replaced = s.status(&floor).await?.ok_or(StorageFailure::NotFound)?;
        let (after, before) = match begun {
            Some(begun) => {
                (begun >= current.stored_at.seconds(), replaced.stored_at.seconds() >= begun)
            }
            None => (false, false),
        };
        println!(
            "upload_initiation trial={trial} floor={} begun={} next_floor={} listed={} after_floor={after} before_next={before}",
            current.stored_at.seconds(),
            begun.map_or("none".into(), |begun| begun.to_string()),
            replaced.stored_at.seconds(),
            listed.len(),
        );
        held &= after && before;
        clock.wait_out(SPACING).await;
    }
    Ok(held)
}
