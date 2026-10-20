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
    let connector = S3HttpConnector(http::client().unwrap());
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
        assert!(
            head.contains("\r\ncontent-length: 0\r\n"),
            "{method}: {head}"
        );
    }
    let head = head_sent("GET").await;
    assert!(!head.contains("content-length"), "GET: {head}");
}
