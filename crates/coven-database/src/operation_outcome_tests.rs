//! Each listed kind's outcome type reads the outcomes coven writes for it,
//! and refuses the ones it never writes.

use super::*;
use sqlive::rusqlite::types::{FromSql, FromSqlResult, ValueRef};

fn read<T: FromSql>(bytes: &[u8]) -> FromSqlResult<T> {
    T::column_result(ValueRef::Blob(bytes))
}

/// A stop for good ends a listed operation of any kind landed or stopped,
/// without knowing its kind, so every kind's type reads both.
#[test]
fn every_kind_reads_the_outcomes_a_stop_writes() {
    let slot = coven_merge::Slot::new(7).unwrap();
    let stop = DeviceStop::Removed(Removal::Device);
    let landed = StopOutcome::Landed(slot).encode();
    let stopped = StopOutcome::Stopped(stop).encode();
    assert_eq!(read::<ChangeOutcome>(&landed).unwrap(), ChangeOutcome::Landed(slot));
    assert_eq!(read::<ChangeOutcome>(&stopped).unwrap(), ChangeOutcome::Stopped(stop));
    assert_eq!(read::<FreshChangeOutcome>(&landed).unwrap(), FreshChangeOutcome::Landed(slot));
    assert_eq!(read::<FreshChangeOutcome>(&stopped).unwrap(), FreshChangeOutcome::Stopped(stop));
    assert_eq!(read::<InviteOutcome>(&landed).unwrap(), InviteOutcome::Landed(slot));
    assert_eq!(read::<InviteOutcome>(&stopped).unwrap(), InviteOutcome::Stopped(stop));
    assert_eq!(read::<StorageSetupOutcome>(&landed).unwrap(), StorageSetupOutcome::Landed(slot));
    assert_eq!(read::<StorageSetupOutcome>(&stopped).unwrap(), StorageSetupOutcome::Stopped(stop));
}

/// A kind's type has no outcome that coven never gives its kinds, so an
/// outcome of another kind's type fails to read.
#[test]
fn a_kind_refuses_the_outcomes_it_never_ends_with() {
    let refused = ListedOutcome::Invite(InviteOutcome::SharingRefused {
        account: "dan@example.com".to_owned(),
        refusal: coven_foundation::sharing::SharingRefusal::TooManyMembers,
    })
    .encode();
    assert!(read::<ChangeOutcome>(&refused).is_err());
    assert!(read::<StorageSetupOutcome>(&refused).is_err());
    let satisfied = ListedOutcome::Change(ChangeOutcome::Satisfied).encode();
    assert!(read::<FreshChangeOutcome>(&satisfied).is_err());
    let cancelled = ListedOutcome::StorageSetup(StorageSetupOutcome::Cancelled).encode();
    assert!(read::<ChangeOutcome>(&cancelled).is_err());
    assert!(read::<InviteOutcome>(&cancelled).is_err());
}
