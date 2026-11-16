use super::*;

impl<P: Policy> ReadPool<P> {
    pub(crate) fn observe_next_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        assert!(self.waiting.lock().unwrap().replace(sender).is_none());
        receiver
    }
}
