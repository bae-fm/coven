use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn http_delivers_the_first_body_bytes_before_the_response_finishes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabc")
            .await
            .unwrap();
        released.await.unwrap();
        socket.write_all(b"def").await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let response = crate::providers::http::client()
        .unwrap()
        .get(format!("http://{address}"))
        .send()
        .await
        .unwrap();
    let metadata = StoredObject {
        path: crate::ObjectPath::positions(
            crate::test_utils::STORE_ID,
            coven_foundation::id_source::DeviceId(1),
        ),
        size: 6,
        stored_at: coven_format::value::StorageTime::new(0, 0).unwrap(),
        time_resolution: std::time::Duration::from_secs(1),
        object_id: "object".into(),
        revision: "1".into(),
        checksum: None,
    };
    let receive = async {
        let mut body = stream(CloudProvider::Dropbox, response, metadata, None)
            .await
            .unwrap();
        let mut first = [0; 3];
        assert_eq!(body.read(&mut first).await.unwrap(), 3);
        assert_eq!(&first, b"abc");
        body
    };
    let mut body = tokio::time::timeout(std::time::Duration::from_secs(2), receive)
        .await
        .unwrap();
    release.send(()).unwrap();
    let mut last = [0; 3];
    assert_eq!(body.read(&mut last).await.unwrap(), 3);
    assert_eq!(&last, b"def");
    assert_eq!(body.read(&mut last).await.unwrap(), 0);
    server.await.unwrap();
}
