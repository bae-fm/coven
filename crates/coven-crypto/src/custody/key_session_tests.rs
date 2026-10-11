use super::*;
use crate::{custody::InMemoryCustody, StoreKey};
use coven_foundation::id_source::KeyId;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct RecordingCustody {
    persisted: Mutex<Option<StoreKeyring>>,
    unlocks: AtomicUsize,
    saves: AtomicUsize,
    closes: AtomicUsize,
    refuse: AtomicBool,
}

impl RecordingCustody {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            persisted: Mutex::new(Some(ring(1))),
            unlocks: AtomicUsize::new(0),
            saves: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            refuse: AtomicBool::new(false),
        })
    }
    fn check(&self) -> Result<(), KeyError> {
        if self.refuse.load(Ordering::SeqCst) {
            Err(KeyError::PassphraseAuthentication)
        } else {
            Ok(())
        }
    }
}

impl StoreKeyCustody for RecordingCustody {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        Ok(self.persisted.lock().unwrap().clone())
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        *self.persisted.lock().unwrap() = Some(keys.clone());
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.check()?;
        self.persisted.lock().unwrap().take();
        Ok(())
    }
    fn close(&self) {
        self.closes.fetch_add(1, Ordering::SeqCst);
    }
}

fn ring(n: u8) -> StoreKeyring {
    StoreKeyring::new(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_u128(n.into())),
        [n; 32],
    ))
}

fn assert_ring(actual: Option<StoreKeyring>, expected: &StoreKeyring) {
    assert_eq!(
        actual.unwrap().to_secret_bytes().as_bytes(),
        expected.to_secret_bytes().as_bytes()
    );
}

#[test]
fn workers_share_one_unlock_and_only_successful_saves_change_held_keys() {
    let custody = RecordingCustody::new();
    let session = Arc::new(KeySession::store(custody.clone()).unwrap());
    let worker = session.clone();
    for _ in 0..10 {
        assert_ring(worker.read().unwrap(), &ring(1));
    }
    custody.refuse.store(true, Ordering::SeqCst);
    assert!(session.persist(&ring(2)).is_err());
    assert_ring(worker.read().unwrap(), &ring(1));
    assert!(session.forget().is_err());
    assert_ring(worker.read().unwrap(), &ring(1));
    custody.refuse.store(false, Ordering::SeqCst);
    session.persist(&ring(2)).unwrap();
    assert_ring(worker.read().unwrap(), &ring(2));
    assert_ring(custody.persisted.lock().unwrap().clone(), &ring(2));
    session.forget().unwrap();
    assert!(worker.read().unwrap().is_none());
    assert!(custody.persisted.lock().unwrap().is_none());
    session.persist(&ring(3)).unwrap();
    assert_ring(worker.read().unwrap(), &ring(3));
    assert_eq!(custody.unlocks.load(Ordering::SeqCst), 1);
    session.close();
    assert!(session.held.lock().unwrap().is_none());
    assert!(matches!(worker.read(), Err(KeyError::StoreClosed)));
    assert!(matches!(
        worker.persist(&ring(4)),
        Err(KeyError::StoreClosed)
    ));
    assert!(matches!(worker.forget(), Err(KeyError::StoreClosed)));
    assert_eq!(custody.saves.load(Ordering::SeqCst), 3);
    worker.close();
    drop(session);
    drop(worker);
    assert_eq!(custody.closes.load(Ordering::SeqCst), 1);
    assert_ring(custody.persisted.lock().unwrap().clone(), &ring(3));
}

#[test]
fn dropping_the_last_session_and_failed_open_both_close_custom_custody() {
    let custody = RecordingCustody::new();
    let session = Arc::new(KeySession::store(custody.clone()).unwrap());
    let worker = session.clone();
    drop(session);
    assert_eq!(custody.closes.load(Ordering::SeqCst), 0);
    drop(worker);
    assert_eq!(custody.closes.load(Ordering::SeqCst), 1);
    custody.refuse.store(true, Ordering::SeqCst);
    assert!(KeySession::store(custody.clone()).is_err());
    assert_eq!(custody.closes.load(Ordering::SeqCst), 2);
}

#[test]
fn closing_erases_in_memory_identity_and_store_material() {
    let keys = Arc::new(InMemoryCustody::new(ring(1)));
    let identity = Arc::new(InMemoryCustody::new(MemberKeys::generate().unwrap()));
    let store_session = KeySession::store(keys.clone()).unwrap();
    let member_session = KeySession::member(identity.clone()).unwrap();
    member_session.close();
    store_session.close();
    assert!(keys.read().is_none());
    assert!(identity.read().is_none());
    assert!(matches!(member_session.read(), Err(KeyError::StoreClosed)));
}
