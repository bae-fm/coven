use super::*;

#[tokio::test]
async fn desktop_callback_returns_the_url_and_releases_the_listener() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let send = async {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"GET /callback?code=code&state=state HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        assert!(String::from_utf8(response)
            .unwrap()
            .contains("Cache-Control: no-store"));
    };
    let (redirect, ()) = tokio::join!(receive_redirect(listener), send);
    assert_eq!(
        redirect.unwrap().as_str(),
        "http://localhost:19284/callback?code=code&state=state"
    );
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn dropping_callback_wait_closes_the_listener() {
    use std::{future::Future, task::Poll};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut pending = Box::pin(receive_redirect(listener));
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(pending);
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}
