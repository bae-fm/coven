use super::*;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn s3_delivers_bytes_before_eof_and_keeps_body_failures() {
    for interrupted in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (release, released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nLast-Modified: Thu, 01 Jan 1970 00:00:00 GMT\r\nETag: \"object\"\r\nConnection: close\r\n\r\nabc").await.unwrap();
            released.await.unwrap();
            if !interrupted {
                socket.write_all(b"def").await.unwrap();
            }
            socket.shutdown().await.unwrap();
        });
        let storage = crate::providers::s3::tests::profile_provider(
            &format!("http://{address}"),
            profile::PLAIN,
        );
        let path = ObjectPath::device_log(
            crate::test_utils::STORE_ID,
            coven_foundation::id_source::DeviceId(1),
            std::num::NonZeroU64::MIN,
        );
        let receive = async {
            let mut body = storage.stream_object(&path, None).await.unwrap();
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
        let result = body.read(&mut last).await;
        if interrupted {
            assert_eq!(result.unwrap_err().failure(), StorageFailure::Network);
        } else {
            assert_eq!(result.unwrap(), 3);
            assert_eq!(&last, b"def");
            assert_eq!(body.read(&mut last).await.unwrap(), 0);
        }
        server.await.unwrap();
    }
}
