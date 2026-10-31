use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Send one request with an empty body and return the request head the
/// server read.
async fn head_sent(method: &str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut chunk = [0; 1024];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before the request head ended");
            head.extend_from_slice(&chunk[..read]);
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        String::from_utf8(head).unwrap().to_ascii_lowercase()
    });
    let request = HttpRequest::try_from(
        ::http::Request::builder()
            .method(method)
            .uri(format!("http://{address}/bucket/object?uploads"))
            .body(SdkBody::empty())
            .unwrap(),
    )
    .unwrap();
    let clock = std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
        std::time::SystemTime::UNIX_EPOCH,
    ));
    let connector = S3HttpConnector(HttpClient::new(clock).unwrap());
    let response = connector.call(request).await.unwrap();
    assert_eq!(response.status().as_u16(), 200);
    server.await.unwrap()
}

/// GCS refuses a POST or PUT without `Content-Length` with HTTP 411, and an
/// empty body gives the HTTP library nothing to state: CreateMultipartUpload
/// and CopyObject send none. A GET states no length.
#[tokio::test]
async fn an_empty_post_or_put_states_its_zero_length() {
    for method in ["POST", "PUT"] {
        let head = head_sent(method).await;
        assert!(head.contains("\r\ncontent-length: 0\r\n"), "{method}: {head}");
    }
    let head = head_sent("GET").await;
    assert!(!head.contains("content-length"), "GET: {head}");
}

/// S3 shares the stall window: a GET whose body stops arriving fails once 61
/// seconds pass without a byte.
#[tokio::test]
#[expect(
    clippy::let_underscore_must_use,
    reason = "the test server only waits for the client to close"
)]
async fn an_s3_download_that_stops_fails_after_the_stall_window() {
    use http_body::Body as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = [0; 4096];
        assert!(socket.read(&mut head).await.unwrap() > 0);
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\na").await.unwrap();
        let _ = socket.read(&mut head).await;
    });
    let start = std::time::SystemTime::UNIX_EPOCH;
    let clock = std::sync::Arc::new(coven_foundation::clock::FixedClock::new(start));
    let connector = S3HttpConnector(HttpClient::new(clock.clone()).unwrap());
    let request = HttpRequest::try_from(
        ::http::Request::builder()
            .method("GET")
            .uri(format!("http://{address}/bucket/object"))
            .body(SdkBody::empty())
            .unwrap(),
    )
    .unwrap();
    let mut body = connector.call(request).await.unwrap().into_body();
    let first = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await;
    assert_eq!(&first.unwrap().unwrap().into_data().unwrap()[..], b"a");
    let read = tokio::spawn(async move {
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
    });
    clock.set(start + std::time::Duration::from_secs(61));
    let error = read.await.unwrap().unwrap().unwrap_err();
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
    let mut stalled = false;
    while let Some(error) = cause {
        stalled |= error.is::<crate::providers::http_client::Stalled>();
        cause = error.source();
    }
    assert!(stalled, "{error:?}");
}

/// An operation that names who hears its body go is heard through the
/// connector, as Dropbox's and OneDrive's requests are: the S3 SDK's request
/// carries the `ByteProgress` to coven's client.
#[tokio::test]
async fn an_s3_body_is_heard_as_the_connection_takes_it() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let length = 100 * 1024;
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut read = Vec::new();
        let mut chunk = [0; 64 * 1024];
        while read.len() < length || !read.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0, "connection closed before the body ended");
            read.extend_from_slice(&chunk[..count]);
            let head = read.windows(4).position(|window| window == b"\r\n\r\n");
            if head.is_some_and(|end| read.len() - end - 4 >= length) {
                break;
            }
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    let heard = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut request = HttpRequest::try_from(
        ::http::Request::builder()
            .method("PUT")
            .uri(format!("http://{address}/bucket/object"))
            .body(SdkBody::from(vec![1u8; length]))
            .unwrap(),
    )
    .unwrap();
    request.add_extension(ByteProgress::new({
        let heard = heard.clone();
        move |count| heard.lock().unwrap().push(count)
    }));
    let clock = std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
        std::time::SystemTime::UNIX_EPOCH,
    ));
    let connector = S3HttpConnector(HttpClient::new(clock).unwrap());
    let response = connector.call(request).await.unwrap();
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(*heard.lock().unwrap(), [64 * 1024, length as u64]);
}
