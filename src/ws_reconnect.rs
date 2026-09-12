use std::time::Duration;

use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::error::{Error as WsError, ProtocolError};
use tokio_tungstenite::tungstenite::Message;
use tracing::Level;

/// Exponential backoff for WebSocket reconnect loops.
#[derive(Debug, Clone)]
pub struct ReconnectBackoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl ReconnectBackoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        let initial = initial.max(Duration::from_millis(1));
        let max = max.max(initial);
        Self {
            initial,
            max,
            current: initial,
        }
    }

    pub fn reset(&mut self) {
        self.current = self.initial;
    }

    pub fn next_delay(&self) -> Duration {
        self.current
    }

    pub async fn wait(&mut self) {
        tokio::time::sleep(self.current).await;
        self.current = self.current.saturating_mul(2).min(self.max);
    }
}

/// Why a WebSocket session ended — drives single-line reconnect logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsDisconnectKind {
    /// Peer sent a Close frame or the session ended after a graceful close.
    CleanClose,
    /// TCP dropped or reset without a WebSocket close handshake.
    AbruptClose,
    /// Other read/write/protocol failure.
    TransportError,
}

impl WsDisconnectKind {
    pub fn log_level(self) -> Level {
        match self {
            Self::CleanClose => Level::INFO,
            Self::AbruptClose | Self::TransportError => Level::WARN,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::CleanClose => "WebSocket closed cleanly",
            Self::AbruptClose => "WebSocket disconnected abruptly (no close frame)",
            Self::TransportError => "WebSocket session error",
        }
    }
}

/// Classify tungstenite errors without logging secrets or full debug chains.
pub fn classify_ws_error(err: &WsError) -> WsDisconnectKind {
    match err {
        WsError::ConnectionClosed | WsError::AlreadyClosed => WsDisconnectKind::AbruptClose,
        WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
            WsDisconnectKind::AbruptClose
        }
        WsError::Protocol(ProtocolError::SendAfterClosing) => WsDisconnectKind::CleanClose,
        WsError::Io(e) if is_connection_reset(e) => WsDisconnectKind::AbruptClose,
        _ => WsDisconnectKind::TransportError,
    }
}

/// Short, safe error summary for logs — never includes URLs or auth tokens.
pub fn ws_error_summary(err: &WsError) -> String {
    match err {
        WsError::ConnectionClosed => "connection closed".to_string(),
        WsError::AlreadyClosed => "already closed".to_string(),
        WsError::Protocol(p) => format!("protocol: {p}"),
        WsError::Io(e) => format!("io: {e}"),
        WsError::Tls(e) => format!("tls: {e}"),
        WsError::Url(_) => "invalid url".to_string(),
        WsError::Capacity(e) => format!("capacity: {e}"),
        WsError::WriteBufferFull(_) => "write buffer full".to_string(),
        WsError::AttackAttempt => "attack attempt".to_string(),
        #[allow(unreachable_patterns)]
        other => format!("{other}"),
    }
}

pub fn is_expected_disconnect(err: &WsError) -> bool {
    matches!(
        classify_ws_error(err),
        WsDisconnectKind::CleanClose | WsDisconnectKind::AbruptClose
    )
}

fn is_connection_reset(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        err.kind(),
        ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::UnexpectedEof
    )
}

/// Best-effort graceful close of the write half before dropping the session.
pub async fn close_ws_write<S>(write: &mut S)
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let _ = write
        .send(Message::Close(None))
        .await
        .map_err(|e| tracing::trace!(error = %e, "failed to send WebSocket Close frame"));
    let _ = write
        .close()
        .await
        .map_err(|e| tracing::trace!(error = %e, "failed to close WebSocket write half"));
}

/// Emit exactly one reconnect log line for a disconnect.
pub fn log_ws_reconnect(
    upstream: &'static str,
    kind: WsDisconnectKind,
    delay: Duration,
    detail: Option<&str>,
) {
    let delay_secs = delay.as_secs();
    match kind.log_level() {
        Level::INFO => {
            if let Some(detail) = detail {
                tracing::info!(
                    upstream,
                    reason = detail,
                    delay_secs,
                    "{}; reconnecting in {}s",
                    kind.message(),
                    delay_secs
                );
            } else {
                tracing::info!(
                    upstream,
                    delay_secs,
                    "{}; reconnecting in {}s",
                    kind.message(),
                    delay_secs
                );
            }
        }
        Level::WARN => {
            if let Some(detail) = detail {
                tracing::warn!(
                    upstream,
                    reason = detail,
                    delay_secs,
                    "{}; reconnecting in {}s",
                    kind.message(),
                    delay_secs
                );
            } else {
                tracing::warn!(
                    upstream,
                    delay_secs,
                    "{}; reconnecting in {}s",
                    kind.message(),
                    delay_secs
                );
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_until_cap() {
        let mut b = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(8));
        assert_eq!(b.next_delay(), Duration::from_secs(1));
        b.current = b.current.saturating_mul(2).min(b.max);
        assert_eq!(b.next_delay(), Duration::from_secs(2));
        b.current = b.current.saturating_mul(2).min(b.max);
        assert_eq!(b.next_delay(), Duration::from_secs(4));
        b.current = b.current.saturating_mul(2).min(b.max);
        assert_eq!(b.next_delay(), Duration::from_secs(8));
        b.current = b.current.saturating_mul(2).min(b.max);
        assert_eq!(b.next_delay(), Duration::from_secs(8));
    }

    #[test]
    fn backoff_resets_after_success() {
        let mut b = ReconnectBackoff::new(Duration::from_secs(2), Duration::from_secs(30));
        b.current = Duration::from_secs(16);
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_secs(2));
    }

    #[test]
    fn classifies_connection_closed_as_abrupt() {
        let err = WsError::ConnectionClosed;
        assert_eq!(classify_ws_error(&err), WsDisconnectKind::AbruptClose);
        assert!(is_expected_disconnect(&err));
    }

    #[test]
    fn classifies_reset_without_handshake_as_abrupt() {
        let err = WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake);
        assert_eq!(classify_ws_error(&err), WsDisconnectKind::AbruptClose);
    }

    #[test]
    fn error_summary_is_short_and_non_secret() {
        let err = WsError::ConnectionClosed;
        let summary = ws_error_summary(&err);
        assert_eq!(summary, "connection closed");
        assert!(!summary.contains("token"));
        assert!(!summary.contains("password"));
        assert!(!summary.contains("wss://"));
    }
}
