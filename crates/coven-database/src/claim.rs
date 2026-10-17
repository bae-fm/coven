//! Durable identity and exact attempted bytes for one sequence claim (§6).

use crate::{sqlite::DatabaseConnection, write_encoding::counter, DbError, OperationId};
use coven_format::path::ObjectPath;
use coven_foundation::id_source::DeviceId;
use rusqlite::params;
use std::io::Read;

/// The original work carried by a claim. Every slot kind shares one counter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimWork {
    /// The queued originals through this write number, inclusive.
    Writes(u64),
    /// An admin, key-copy or snapshot-announcement step of an operation.
    Operation(OperationId),
}

/// A create-once request, persisted before the provider is called.
pub struct ClaimAttempt<R> {
    /// The exact path bound by the sealed envelope.
    pub path: ObjectPath,
    /// The complete sealed bytes, reused for an uncertain request.
    pub bytes: R,
}

/// Settlement of the most recent attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimSettlement {
    /// No authoritative read has settled the request.
    Unknown,
    /// The slot is still absent above the floor; resend its exact bytes.
    Retry,
    /// Other bytes occupy the slot, or the floor snapshot excludes this claim.
    NotLanded,
    /// Exact bytes or the floor snapshot establish publication.
    Landed,
    /// Authenticated claim evidence belongs to another copy of this device id.
    DeviceIdInUse,
}

/// A claim retains every attempt, including attempts that lost contention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    /// The installation that reserved this number.
    pub device: DeviceId,
    /// One monotonically increasing number across every slot kind.
    pub number: u64,
    /// The queue batch or journal operation which owns it.
    pub work: ClaimWork,
    /// Attempts in their durable order; only the last can be outstanding.
    pub attempts: Vec<ObjectPath>,
    /// What authoritative storage reads established about the last attempt.
    pub settlement: ClaimSettlement,
}

/// Authenticated observations supplied by the sync reader.
pub enum ClaimRead {
    /// A slot at or above the checked floor. Absence permits exact retry.
    Kept(Option<Box<dyn Read + Send>>),
    /// A snapshot whose coverage includes the attempted slot below the floor.
    Compacted {
        /// The floor's first kept slot.
        first: u64,
        /// The authenticated snapshot's coverage.
        covers: u64,
        /// This device's last included claim in that snapshot.
        last_claim: u64,
        /// Digest paired with that number; absent exactly when it is zero.
        digest: Option<[u8; 32]>,
    },
}

fn settlement(text: String) -> rusqlite::Result<ClaimSettlement> {
    match text.as_str() {
        "unknown" => Ok(ClaimSettlement::Unknown),
        "retry" => Ok(ClaimSettlement::Retry),
        "not-landed" => Ok(ClaimSettlement::NotLanded),
        "landed" => Ok(ClaimSettlement::Landed),
        "device-id-in-use" => Ok(ClaimSettlement::DeviceIdInUse),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn metadata(db: &DatabaseConnection) -> Result<Option<Claim>, DbError> {
    db.query("SELECT device,number,through,operation,settlement FROM _coven_claim_journal WHERE singleton=1", [], |r| {
        let work = match (r.get::<_,Option<Vec<u8>>>(2)?, r.get::<_,Option<i64>>(3)?) {
            (Some(n),None) => ClaimWork::Writes(counter(n)),
            (None,Some(id)) => ClaimWork::Operation(OperationId(id)),
            _ => return Err(rusqlite::Error::InvalidQuery),
        };
        Ok(Claim { device:DeviceId(counter(r.get(0)?)), number:counter(r.get(1)?), work,
            attempts:Vec::new(), settlement:settlement(r.get(4)?)? })
    }).map(|mut rows| rows.pop())
}

pub(crate) fn current(db: &DatabaseConnection) -> Result<Option<Claim>, DbError> {
    metadata(db)?.map(|mut claim| {
        claim.attempts = db.query("SELECT path FROM _coven_claim_attempts WHERE device=?1 AND number=?2 ORDER BY attempt",
            params![claim.device.0.to_be_bytes().as_slice(),claim.number.to_be_bytes().as_slice()], |r| {
                let path:String=r.get(0)?;
                ObjectPath::parse(&path).map_err(|e|crate::write_encoding::decode_failure(0,rusqlite::types::Type::Text,e))
            })?;
        Ok(claim)
    }).transpose()
}

pub(crate) fn begin<E: From<DbError>, R: Read>(
    db: &DatabaseConnection,
    device: DeviceId,
    work: ClaimWork,
    seal: impl FnOnce(u64) -> Result<ClaimAttempt<R>, E>,
) -> Result<Claim, E> {
    if metadata(db)?.is_some() {
        return Err(DbError::ClaimPending.into());
    }
    let previous: Option<Vec<u8>> = db.query_row(
        "SELECT (SELECT number FROM _coven_claim_counters WHERE device=?1)",
        [device.0.to_be_bytes().as_slice()],
        |r| r.get(0),
    )?;
    let number = previous
        .map(counter)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(DbError::ClaimNumberExhausted)?;
    let attempt = seal(number)?;
    let (through, operation) = match work {
        ClaimWork::Writes(through) => (Some(through.to_be_bytes().to_vec()), None),
        ClaimWork::Operation(operation) => (None, Some(operation.0)),
    };
    db.internal_execute("INSERT INTO _coven_claim_counters(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=excluded.number",
        params![device.0.to_be_bytes().as_slice(),number.to_be_bytes().as_slice()])?;
    db.internal_execute("INSERT INTO _coven_claim_journal(device,number,through,operation,settlement,singleton) VALUES(?1,?2,?3,?4,'unknown',1)",
        params![device.0.to_be_bytes().as_slice(),number.to_be_bytes().as_slice(),through,operation])?;
    let path = attempt.path.clone();
    append(db, device, number, attempt)?;
    if let ClaimWork::Writes(through) = work {
        db.internal_execute(
            "UPDATE _coven_uploads SET claim=?1 WHERE device=?2 AND number<=?3",
            params![
                number.to_be_bytes().as_slice(),
                device.0.to_be_bytes().as_slice(),
                through.to_be_bytes().as_slice()
            ],
        )?;
    }
    Ok(Claim {
        device,
        number,
        work,
        attempts: vec![path],
        settlement: ClaimSettlement::Unknown,
    })
}

fn append(
    db: &DatabaseConnection,
    device: DeviceId,
    number: u64,
    mut attempt: ClaimAttempt<impl Read>,
) -> Result<(), DbError> {
    let ordinal: i64 = db.query_row("SELECT coalesce(max(attempt),0)+1 FROM _coven_claim_attempts WHERE device=?1 AND number=?2",
        params![device.0.to_be_bytes(),number.to_be_bytes()], |r|r.get(0))?;
    db.internal_execute(
        "INSERT INTO _coven_claim_attempts(device,number,attempt,path) VALUES(?1,?2,?3,?4)",
        params![
            device.0.to_be_bytes(),
            number.to_be_bytes(),
            ordinal,
            attempt.path.as_str()
        ],
    )?;
    let mut hash = coven_crypto::ObjectHasher::new();
    for chunk in 0i64.. {
        let bytes = read_chunk(&mut attempt.bytes)?;
        if bytes.is_empty() {
            break;
        }
        hash.update(&bytes);
        db.internal_execute("INSERT INTO _coven_claim_chunks(device,number,attempt,chunk,bytes) VALUES(?1,?2,?3,?4,?5)",
            params![device.0.to_be_bytes(),number.to_be_bytes(),ordinal,chunk,bytes])?;
    }
    db.internal_execute(
        "UPDATE _coven_claim_attempts SET digest=?4 WHERE device=?1 AND number=?2 AND attempt=?3",
        params![
            device.0.to_be_bytes(),
            number.to_be_bytes(),
            ordinal,
            hash.finish().as_bytes()
        ],
    )?;
    Ok(())
}

fn read_chunk(bytes: &mut dyn Read) -> Result<Vec<u8>, DbError> {
    let mut chunk = Vec::new();
    bytes
        .take(coven_format::chunks::CHUNK_SIZE as u64)
        .read_to_end(&mut chunk)
        .map_err(|error| DbError::SyncStream(Box::new(error)))?;
    Ok(chunk)
}

/// Bounded reads of the current attempt's durable ciphertext.
pub struct ClaimBytes<'a> {
    db: &'a DatabaseConnection,
    device: DeviceId,
    number: u64,
    attempt: i64,
    chunk: i64,
    ended: bool,
}

impl Iterator for ClaimBytes<'_> {
    type Item = Result<Vec<u8>, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        let bytes = self.db.query_row("SELECT (SELECT bytes FROM _coven_claim_chunks WHERE device=?1 AND number=?2 AND attempt=?3 AND chunk=?4)",
            params![self.device.0.to_be_bytes(),self.number.to_be_bytes(),self.attempt,self.chunk], |r|r.get::<_,Option<Vec<u8>>>(0));
        match bytes {
            Ok(Some(bytes)) => {
                self.chunk += 1;
                Some(Ok(bytes))
            }
            Ok(None) => {
                self.ended = true;
                None
            }
            Err(error) => {
                self.ended = true;
                Some(Err(error))
            }
        }
    }
}

pub(crate) fn read_attempt<T, E: From<DbError>>(
    db: &DatabaseConnection,
    consume: impl FnOnce(ClaimBytes<'_>) -> Result<T, E>,
) -> Result<T, E> {
    let claim = metadata(db)?.ok_or(DbError::NoClaim)?;
    let attempt = db.query_row(
        "SELECT max(attempt) FROM _coven_claim_attempts WHERE device=?1 AND number=?2",
        params![claim.device.0.to_be_bytes(), claim.number.to_be_bytes()],
        |r| r.get(0),
    )?;
    consume(ClaimBytes {
        db,
        device: claim.device,
        number: claim.number,
        attempt,
        chunk: 0,
        ended: false,
    })
}

fn equal(db: &DatabaseConnection, mut received: Box<dyn Read + Send>) -> Result<bool, DbError> {
    read_attempt(db, |mut saved| {
        let mut equal = true;
        loop {
            let incoming = read_chunk(&mut received)?;
            let stored = saved.next().transpose()?;
            if incoming.is_empty() && stored.is_none() {
                return Ok(equal);
            }
            equal &= stored.as_deref() == Some(incoming.as_slice());
        }
    })
}

pub(crate) fn retry(
    db: &DatabaseConnection,
    attempt: ClaimAttempt<impl Read>,
) -> Result<(), DbError> {
    let claim = metadata(db)?.ok_or(DbError::NoClaim)?;
    if claim.settlement != ClaimSettlement::NotLanded {
        return Err(DbError::ClaimUnsettled);
    }
    append(db, claim.device, claim.number, attempt)?;
    db.internal_execute(
        "UPDATE _coven_claim_journal SET settlement='unknown' WHERE singleton=1",
        [],
    )?;
    Ok(())
}

pub(crate) fn settle(db: &DatabaseConnection, read: ClaimRead) -> Result<ClaimSettlement, DbError> {
    let claim = current(db)?.ok_or(DbError::NoClaim)?;
    if matches!(
        claim.settlement,
        ClaimSettlement::Landed | ClaimSettlement::NotLanded | ClaimSettlement::DeviceIdInUse
    ) {
        return Ok(claim.settlement);
    }
    let attempt = claim
        .attempts
        .last()
        .expect("a journal starts with its first attempt");
    let (result, name) = match read {
        ClaimRead::Kept(Some(bytes)) => {
            if equal(db, bytes)? {
                (ClaimSettlement::Landed, "landed")
            } else {
                (ClaimSettlement::NotLanded, "not-landed")
            }
        }
        ClaimRead::Kept(None) => (ClaimSettlement::Retry, "retry"),
        ClaimRead::Compacted {
            first,
            covers,
            last_claim,
            digest,
        } => {
            let slot = attempt.slot_number().ok_or(DbError::InvalidClaimCoverage)?;
            if first <= slot.0 || covers < first - 1 {
                return Err(DbError::InvalidClaimCoverage);
            }
            if last_claim < claim.number {
                (ClaimSettlement::NotLanded, "not-landed")
            } else if last_claim > claim.number {
                (ClaimSettlement::DeviceIdInUse, "device-id-in-use")
            } else {
                let attempted_digest: Vec<u8> = db.query_row(
                    "SELECT digest FROM _coven_claim_attempts WHERE device=?1 AND number=?2 ORDER BY attempt DESC LIMIT 1",
                    params![claim.device.0.to_be_bytes(), claim.number.to_be_bytes()],
                    |r| r.get(0),
                )?;
                if digest.as_ref().map(|value| value.as_slice())
                    == Some(attempted_digest.as_slice())
                {
                    (ClaimSettlement::Landed, "landed")
                } else {
                    (ClaimSettlement::DeviceIdInUse, "device-id-in-use")
                }
            }
        }
    };
    db.internal_execute(
        "UPDATE _coven_claim_journal SET settlement=?1 WHERE singleton=1",
        [name],
    )?;
    Ok(result)
}

/// Finish only a settled attempt. The owning operation controls whether a lost
/// claim is abandoned or retried; a landed batch removes its original queue rows.
pub(crate) fn finish(db: &DatabaseConnection) -> Result<(), DbError> {
    let claim = metadata(db)?.ok_or(DbError::NoClaim)?;
    match claim.settlement {
        ClaimSettlement::Unknown | ClaimSettlement::Retry => return Err(DbError::ClaimUnsettled),
        ClaimSettlement::Landed if matches!(claim.work, ClaimWork::Writes(_)) => {
            let unconsumed: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_uploads u LEFT JOIN _coven_writes w ON substr(w.timestamp,9,8)=u.device AND w.number=u.number WHERE u.device=?1 AND u.claim=?2 AND w.slot IS NULL)",
                params![claim.device.0.to_be_bytes(), claim.number.to_be_bytes()],
                |r| r.get(0),
            )?;
            if unconsumed {
                return Err(DbError::ClaimUnsettled);
            }
            db.internal_execute(
                "DELETE FROM _coven_uploads WHERE device=?1 AND claim=?2",
                params![
                    claim.device.0.to_be_bytes().as_slice(),
                    claim.number.to_be_bytes().as_slice()
                ],
            )?;
        }
        _ => (),
    }
    db.internal_execute("DELETE FROM _coven_claim_journal WHERE singleton=1", [])?;
    Ok(())
}

/// Path evidence distinguishes contention from an unattempted use of this id.
pub(crate) fn attempted(db: &DatabaseConnection, path: &ObjectPath) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_claim_attempts WHERE path=?1)",
        [path.as_str()],
        |r| r.get(0),
    )
}
