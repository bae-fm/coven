// The shared state rule of the policy of this folder: each line that holds
// a finding of `clippy::shared_state_outside_handle` holds a `finding`
// comment, once for each finding it holds.

use std::sync::{Arc, Mutex};

use crate::Clock;

// Shared state leaves its owner only inside a handle, except at a root;
// private state leaves it when a method returns it shared.
pub struct Sender<T>(Arc<Mutex<Option<T>>>);

pub struct Receiver<T>(Arc<Mutex<Option<T>>>);

fn channel() -> (Sender<u8>, Receiver<u8>) {
    let shared = Arc::new(Mutex::new(None));
    (Sender(shared.clone()), Receiver(shared))
}

pub struct Watch(Receiver<u8>);

pub struct Status {
    sender: Sender<u8>,
}

impl Status {
    pub fn subscribe(/* finding */ &self) -> Receiver<u8> {
        Receiver(self.sender.0.clone())
    }

    pub fn sender(/* finding */ &self) -> Sender<u8> {
        Sender(self.sender.0.clone())
    }

    pub async fn watch(&self) -> Watch {
        Watch(Receiver(self.sender.0.clone()))
    }

    fn inner(&self) -> Receiver<u8> {
        Receiver(self.sender.0.clone())
    }
}

pub struct Value;

impl Value {
    pub fn channel(/* finding */ /* finding */) -> (Sender<u8>, Receiver<u8>) {
        channel()
    }
}

pub struct Broadcast {
    clock: Clock,
}

impl Broadcast {
    pub fn open(&self) -> Receiver<u8> {
        channel().1
    }
}

pub struct Keychain {
    entries: Mutex<u8>,
}

impl Keychain {
    pub fn backend(/* finding */ &self) -> &Mutex<u8> {
        &self.entries
    }

    pub fn counts(/* finding */ &self) -> Arc<Mutex<u8>> {
        Arc::new(Mutex::new(0))
    }

    pub fn fresh() -> Mutex<u8> {
        Mutex::new(0)
    }

    fn inner(&self) -> &Mutex<u8> {
        &self.entries
    }
}
