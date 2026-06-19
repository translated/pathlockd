//! HTTP/3 (QUIC) front end. Serves the *same* axum router as the TCP side by
//! bridging each HTTP/3 request stream to a tower `oneshot` call and streaming
//! the response body back — so SSE works over HTTP/3 unchanged.
//!
//! 0-RTT: QUIC early data is replayable, so any request dispatched before the
//! TLS handshake completes is restricted to read-only RPCs (see
//! [`rest::is_read_only_path`]). Mutating RPCs received as early data get
//! `425 Too Early` and must be retried on the established 1-RTT connection.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::Router;
use bytes::{Buf, Bytes, BytesMut};
use http_body_util::BodyExt;
use tower::ServiceExt;
use tracing::{debug, warn};

use super::rest;

const MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_REQUESTS_PER_CONNECTION: usize = 256;

type H3Stream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

/// Bind the QUIC endpoint (surfacing bind errors synchronously) and spawn the
/// accept loop.
pub fn spawn(
    addr: SocketAddr,
    server_config: quinn::ServerConfig,
    app: Router,
    zero_rtt: bool,
) -> anyhow::Result<()> {
    let endpoint = quinn::Endpoint::server(server_config, addr)?;
    tokio::spawn(accept_loop(endpoint, app, zero_rtt));
    Ok(())
}

async fn accept_loop(endpoint: quinn::Endpoint, app: Router, zero_rtt: bool) {
    while let Some(incoming) = endpoint.accept().await {
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(incoming, app, zero_rtt).await {
                debug!(error = %e, "h3 connection ended");
            }
        });
    }
}

async fn handle_connection(
    incoming: quinn::Incoming,
    app: Router,
    zero_rtt: bool,
) -> anyhow::Result<()> {
    let connecting = incoming.accept()?;

    // `handshake_done` is false only while a 0-RTT connection is still in its
    // replayable early-data phase; it flips true once the handshake confirms.
    let handshake_done = Arc::new(AtomicBool::new(true));
    let conn = if zero_rtt {
        match connecting.into_0rtt() {
            Ok((conn, accepted)) => {
                handshake_done.store(false, Ordering::SeqCst);
                let flag = handshake_done.clone();
                tokio::spawn(async move {
                    let _ = accepted.await;
                    flag.store(true, Ordering::SeqCst);
                });
                conn
            }
            Err(connecting) => connecting.await?,
        }
    } else {
        connecting.await?
    };

    let mut h3_conn: h3::server::Connection<h3_quinn::Connection, Bytes> =
        h3::server::Connection::new(h3_quinn::Connection::new(conn)).await?;
    let requests = Arc::new(tokio::sync::Semaphore::new(MAX_REQUESTS_PER_CONNECTION));

    loop {
        match h3_conn.accept().await {
            Ok(Some(resolver)) => {
                let (req, stream) = match resolver.resolve_request().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        debug!(error = %e, "h3 request resolve failed");
                        continue;
                    }
                };
                let app = app.clone();
                let flag = handshake_done.clone();
                let permit = requests.clone().acquire_owned().await?;
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = handle_request(req, stream, app, flag).await {
                        debug!(error = %e, "h3 request failed");
                    }
                });
            }
            Ok(None) => break,
            Err(e) => {
                debug!(error = %e, "h3 accept failed");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_request(
    req: http::Request<()>,
    mut stream: H3Stream,
    app: Router,
    handshake_done: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let (parts, _) = req.into_parts();

    // 0-RTT safety gate: never run a mutation from replayable early data.
    if !handshake_done.load(Ordering::SeqCst)
        && !rest::is_read_only_path(&parts.method, parts.uri.path())
    {
        let resp = http::Response::builder()
            .status(http::StatusCode::TOO_EARLY)
            .body(())
            .expect("static 425 response");
        stream.send_response(resp).await?;
        stream.finish().await?;
        return Ok(());
    }

    // Collect the request body (JSON bodies are small).
    let mut body = BytesMut::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        while chunk.has_remaining() {
            let slice = chunk.chunk();
            if body.len().saturating_add(slice.len()) > MAX_REQUEST_BODY_BYTES {
                let resp = http::Response::builder()
                    .status(http::StatusCode::PAYLOAD_TOO_LARGE)
                    .body(())?;
                stream.send_response(resp).await?;
                stream.finish().await?;
                return Ok(());
            }
            body.extend_from_slice(slice);
            let n = slice.len();
            chunk.advance(n);
        }
    }

    // Rebuild as an axum request and run it through the shared router.
    let mut builder = http::Request::builder().method(parts.method).uri(parts.uri);
    if let Some(headers) = builder.headers_mut() {
        *headers = parts.headers;
    }
    let axum_req = builder.body(Body::from(body.freeze()))?;

    let response = match app.oneshot(axum_req).await {
        Ok(resp) => resp,
        Err(infallible) => match infallible {},
    };

    let (resp_parts, mut resp_body) = response.into_parts();
    let head = http::Response::from_parts(resp_parts, ());
    stream.send_response(head).await?;

    // Stream the response body frame-by-frame. For SSE this never completes
    // until the client disconnects, which surfaces here as a send error.
    loop {
        match resp_body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if data.has_remaining() {
                        stream.send_data(data).await?;
                    }
                }
            }
            Some(Err(e)) => {
                warn!(error = %e, "h3 response body error");
                break;
            }
            None => break,
        }
    }
    stream.finish().await?;
    Ok(())
}
