use super::*;
use coven_foundation::clock::FixedClock;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// The test's clock, moved forward by hand.
struct Time {
    clock: Arc<FixedClock>,
    now: SystemTime,
}
impl Time {
    fn new() -> Self {
        Self {
            clock: Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            now: SystemTime::UNIX_EPOCH,
        }
    }
    fn client(&self) -> HttpClient {
        HttpClient::new(self.clock.clone()).unwrap()
    }
    fn advance(&mut self, seconds: u64) {
        self.now += Duration::from_secs(seconds);
        self.clock.set(self.now);
    }
}

async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/object", listener.local_addr().unwrap());
    (listener, url)
}

/// Read a request head, returning any body bytes read past it.
async fn read_head(socket: &mut TcpStream) -> Vec<u8> {
    let mut read = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        if let Some(end) = read.windows(4).position(|window| window == b"\r\n\r\n") {
            return read.split_off(end + 4);
        }
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0, "connection closed before the request head ended");
        read.extend_from_slice(&chunk[..count]);
    }
}

/// A server that answers with a four-byte body, sending each byte when told.
#[expect(
    clippy::let_underscore_must_use,
    reason = "the test server only waits for the client to close"
)]
async fn dribbling_download() -> (String, mpsc::UnboundedSender<()>) {
    let (listener, url) = listener().await;
    let (send, mut sends) = mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_head(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n")
            .await
            .unwrap();
        for byte in *b"abcd" {
            if sends.recv().await.is_none() {
                return;
            }
            socket.write_all(&[byte]).await.unwrap();
        }
        // Hold the connection open until the client is done with it.
        let _ = socket.read(&mut [0; 1]).await;
    });
    (url, send)
}

/// A download that keeps moving finishes however long it takes in all: four
/// bytes, 50 seconds apart, take 200 seconds.
#[tokio::test]
async fn a_slow_download_that_keeps_moving_completes() {
    let mut time = Time::new();
    let client = time.client();
    let (url, send) = dribbling_download().await;
    let mut response = client
        .send(CloudProvider::Dropbox, client.request(Method::GET, &url))
        .await
        .unwrap();
    let mut body = Vec::new();
    for _ in 0..4 {
        time.advance(50);
        send.send(()).unwrap();
        body.extend_from_slice(&response.chunk().await.unwrap().unwrap());
    }
    assert_eq!(body, b"abcd");
    assert!(response.chunk().await.unwrap().is_none());
}

/// 59 seconds without a byte is not a stall; 61 is, and the read fails then.
#[tokio::test]
async fn a_download_that_stops_fails_after_the_stall_window() {
    let mut time = Time::new();
    let client = time.client();
    let (url, send) = dribbling_download().await;
    let mut response = client
        .send(CloudProvider::Dropbox, client.request(Method::GET, &url))
        .await
        .unwrap();
    time.advance(59);
    send.send(()).unwrap();
    assert_eq!(&response.chunk().await.unwrap().unwrap()[..], b"a");
    let read = tokio::spawn(async move { response.chunk().await });
    time.advance(61);
    let error = read.await.unwrap().unwrap_err();
    let error = transport(CloudProvider::Dropbox, error);
    assert_eq!(error.failure(), StorageFailure::Network);
    assert!(stalled(&error), "{error:?}");
}

/// A server that reads the request body and reports each part it receives.
/// It answers once the body ends.
#[expect(
    clippy::let_underscore_must_use,
    reason = "the test server only waits for the client to close"
)]
async fn counting_upload() -> (String, mpsc::UnboundedReceiver<usize>) {
    let (listener, url) = listener().await;
    let (report, reports) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut received = read_head(&mut socket).await.len();
        if received > 0 {
            report.send(received).unwrap();
        }
        let mut chunk = [0; 4096];
        while received < 4 {
            let count = socket.read(&mut chunk).await.unwrap();
            if count == 0 {
                return;
            }
            received += count;
            report.send(received).unwrap();
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let _ = socket.read(&mut [0; 1]).await;
    });
    (url, reports)
}

/// A request body the test produces one byte at a time.
struct Fed(mpsc::UnboundedReceiver<Bytes>);
impl http_body::Body for Fed {
    type Data = Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|bytes| bytes.map(|bytes| Ok(http_body::Frame::data(bytes))))
    }
    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(4)
    }
}

async fn upload(
    client: &HttpClient,
    url: &str,
) -> (
    mpsc::UnboundedSender<Bytes>,
    tokio::task::JoinHandle<Result<reqwest::Response, StorageError>>,
) {
    let (feed, fed) = mpsc::unbounded_channel();
    let request = client
        .request(Method::PUT, url)
        .body(reqwest::Body::wrap(Fed(fed)));
    let client = client.clone();
    let sent = tokio::spawn(async move { client.send(CloudProvider::OneDrive, request).await });
    (feed, sent)
}

/// An upload that keeps moving finishes: four bytes, 50 seconds apart.
#[tokio::test]
async fn a_slow_upload_that_keeps_moving_completes() {
    let mut time = Time::new();
    let client = time.client();
    let (url, mut received) = counting_upload().await;
    let (feed, sent) = upload(&client, &url).await;
    for (index, byte) in b"abcd".iter().enumerate() {
        time.advance(50);
        feed.send(Bytes::copy_from_slice(&[*byte])).unwrap();
        while received.recv().await.unwrap() < index + 1 {}
    }
    assert_eq!(sent.await.unwrap().unwrap().status(), 200);
}

/// 59 seconds without sending a byte is not a stall; 61 is.
#[tokio::test]
async fn an_upload_that_stops_fails_after_the_stall_window() {
    let mut time = Time::new();
    let client = time.client();
    let (url, mut received) = counting_upload().await;
    let (feed, sent) = upload(&client, &url).await;
    time.advance(59);
    feed.send(Bytes::from_static(b"a")).unwrap();
    assert_eq!(received.recv().await.unwrap(), 1);
    time.advance(61);
    let error = sent.await.unwrap().unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    assert!(stalled(&error), "{error:?}");
    drop(feed);
}

/// A server that reads a request body of `length` bytes, then answers.
#[expect(
    clippy::let_underscore_must_use,
    reason = "the test server only waits for the client to close"
)]
async fn reading_upload(length: usize) -> String {
    let (listener, url) = listener().await;
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut received = read_head(&mut socket).await.len();
        let mut chunk = [0; 64 * 1024];
        while received < length {
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0, "connection closed before the body ended");
            received += count;
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let _ = socket.read(&mut [0; 1]).await;
    });
    url
}

/// A caller hears its body's bytes as the connection takes them, a step at a
/// time, never more than a step at once, up to the whole body: a 200-KiB
/// body is heard at 64, 128, 192 and 200 KiB.
#[tokio::test]
async fn a_body_is_heard_a_step_at_a_time_as_the_connection_takes_it() {
    let time = Time::new();
    let client = time.client();
    let length = 200 * 1024;
    let url = reading_upload(length).await;
    let heard = Arc::new(Mutex::new(Vec::new()));
    let sent = ByteProgress::new({
        let heard = heard.clone();
        move |count| heard.lock().unwrap().push(count)
    });
    let request = client
        .request(Method::PUT, &url)
        .body(vec![7u8; length])
        .build()
        .unwrap();
    let response = client.execute(request, &sent).await.unwrap();
    assert_eq!(response.status(), 200);
    let step = UPLOAD_STEP as u64;
    assert_eq!(
        *heard.lock().unwrap(),
        [step, 2 * step, 3 * step, length as u64]
    );
}

/// Connecting and waiting for the reply count too: a server that never
/// answers fails the request after the window.
#[tokio::test]
#[expect(
    clippy::let_underscore_must_use,
    reason = "the test server only waits for the client to close"
)]
async fn a_request_with_no_reply_fails_after_the_stall_window() {
    let mut time = Time::new();
    let client = time.client();
    let (listener, url) = listener().await;
    let (accepted, accepting) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_head(&mut socket).await;
        accepted.send(()).unwrap();
        let _ = socket.read(&mut [0; 1]).await;
    });
    let request = client.request(Method::GET, &url);
    let sent = {
        let client = client.clone();
        tokio::spawn(async move { client.send(CloudProvider::Dropbox, request).await })
    };
    accepting.await.unwrap();
    time.advance(61);
    let error = sent.await.unwrap().unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    assert!(stalled(&error), "{error:?}");
}

/// Whether the cause chain holds the stall.
fn stalled(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if error.is::<Stalled>() {
            return true;
        }
        cause = error.source();
    }
    false
}

/// A request that reaches no server names the failure behind reqwest's own
/// "error sending request": a host that doesn't resolve, a refused
/// connection. The URL keeps only its scheme and host, in the text, in
/// `Debug`, and in every cause below it.
#[tokio::test]
async fn an_unreachable_server_names_every_layer_of_its_cause() {
    let client = HttpClient::new(Arc::new(coven_foundation::clock::SystemClock)).unwrap();
    for (url, origin, cause) in [
        (
            "https://ana:hunter2@coven-test.invalid/2/files/Gifts.txt?token=deadbeef",
            "https://coven-test.invalid/…",
            "dns error: ",
        ),
        (
            "http://127.0.0.1:1/2/files/Gifts.txt?token=deadbeef",
            "http://127.0.0.1:1/…",
            // hyper's own words; the OS's message after them differs by
            // platform ("Connection refused (os error 61)" on macOS).
            "tcp connect error: ",
        ),
    ] {
        let error = client
            .send(CloudProvider::Dropbox, client.request(Method::GET, url))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Network);
        assert!(!error.answered());
        let text = error.to_string();
        assert!(text.starts_with("Dropbox: Network: "), "{text}");
        assert!(text.contains(origin), "{text}");
        assert!(text.contains(cause), "{text}");
        let mut shown = vec![text, format!("{error:?}")];
        let mut layer = std::error::Error::source(&error);
        while let Some(cause) = layer {
            shown.push(format!("{cause} {cause:?}"));
            layer = cause.source();
        }
        for text in shown {
            for hidden in ["hunter2", "ana", "Gifts", "deadbeef", "token"] {
                assert!(!text.contains(hidden), "{hidden} in {text}");
            }
        }
    }
}
