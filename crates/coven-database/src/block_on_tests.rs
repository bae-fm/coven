use super::*;

#[test]
fn block_on_needs_no_runtime() {
    let (send, receive) = tokio::sync::oneshot::channel();
    let sender = std::thread::spawn(move || send.send(7).unwrap());
    assert_eq!(block_on(receive).unwrap(), 7);
    sender.join().unwrap();
}
