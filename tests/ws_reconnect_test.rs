use std::time::Duration;

use feed_mux::ws_reconnect::{
    classify_ws_error, close_ws_write, is_expected_disconnect, should_reset_backoff,
    ReconnectBackoff, WsDisconnectKind,
};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn reconnect_backoff_waits_and_grows() {
    let mut backoff = ReconnectBackoff::new(Duration::from_millis(10), Duration::from_millis(40));
    assert_eq!(backoff.next_delay(), Duration::from_millis(10));

    let start = std::time::Instant::now();
    backoff.wait().await;
    assert!(start.elapsed() >= Duration::from_millis(10));
    assert_eq!(backoff.next_delay(), Duration::from_millis(20));
}

#[tokio::test]
async fn repeated_abrupt_close_grows_backoff_without_reset() {
    let mut backoff = ReconnectBackoff::new(Duration::from_millis(10), Duration::from_millis(80));

    for expected in [
        Duration::from_millis(10),
        Duration::from_millis(20),
        Duration::from_millis(40),
        Duration::from_millis(80),
    ] {
        assert_eq!(
            backoff.next_delay(),
            expected,
            "delay before unhealthy abrupt-close wait"
        );
        assert!(
            !should_reset_backoff(false, Duration::from_millis(100)),
            "short session without data must not reset backoff"
        );
        backoff.wait().await;
    }

    assert_eq!(backoff.next_delay(), Duration::from_millis(80), "stays capped");
}

#[tokio::test]
async fn healthy_session_resets_backoff_after_growth() {
    let mut backoff = ReconnectBackoff::new(Duration::from_millis(10), Duration::from_millis(80));
    backoff.wait().await;
    backoff.wait().await;
    assert_eq!(backoff.next_delay(), Duration::from_millis(40));

    assert!(should_reset_backoff(true, Duration::from_millis(1)));
    backoff.reset();
    assert_eq!(backoff.next_delay(), Duration::from_millis(10));
}

#[tokio::test]
async fn abrupt_server_drop_classifies_as_expected_disconnect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ws = accept_async(stream).await.unwrap();
        // Drop without close frame — mirrors "no close frame received or sent".
    });

    let url = format!("ws://{addr}");
    let (ws, _) = connect_async(&url).await.unwrap();
    let (_write, mut read) = ws.split();

    server.await.unwrap();
    let err = read.next().await.unwrap().unwrap_err();
    assert_eq!(classify_ws_error(&err), WsDisconnectKind::AbruptClose);
    assert!(is_expected_disconnect(&err));
}

#[tokio::test]
async fn graceful_close_sends_close_frame_before_drop() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let ws = accept_async(stream).await.unwrap();
        let (mut write, mut read) = ws.split();
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(payload)) => {
                    write.send(Message::Pong(payload)).await.unwrap();
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        close_ws_write(&mut write).await;
    });

    let url = format!("ws://{addr}");
    let (ws, _) = connect_async(&url).await.unwrap();
    let (mut write, mut read) = ws.split();
    write.send(Message::Close(None)).await.unwrap();

    while read.next().await.transpose().unwrap().is_some() {}

    close_ws_write(&mut write).await;
    server.await.unwrap();
}

#[test]
fn reset_without_handshake_maps_to_abrupt_close() {
    let err = WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake);
    assert_eq!(classify_ws_error(&err), WsDisconnectKind::AbruptClose);
}
