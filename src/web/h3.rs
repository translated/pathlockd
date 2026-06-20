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
use std::time::Duration;

use axum::body::Body;
use axum::Router;
use bytes::{Buf, Bytes, BytesMut};
use http_body_util::BodyExt;
use tokio::sync::Semaphore;
use tower::ServiceExt;
use tracing::{debug, warn};

use super::rest;

const MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_REQUESTS_PER_CONNECTION: usize = 256;

type H3Stream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

/// Global budgets shared with the TCP facade so HTTP/3 cannot consume
/// unbounded connections, streams, or body bytes while the gated router is
/// unreachable. The connection permit is held for the QUIC connection's
/// lifetime; the stream permit is held for the whole request (including a
/// streaming SSE response), so active SSE-over-h3 streams count against the
/// global active-stream budget.
#[derive(Clone)]
pub struct H3Limits {
    pub connections: Arc<Semaphore>,
    pub streams: Arc<Semaphore>,
    pub body_timeout: Duration,
}

/// Bind the QUIC endpoint (surfacing bind errors synchronously) and spawn the
/// accept loop.
pub fn spawn(
    addr: SocketAddr,
    server_config: quinn::ServerConfig,
    app: Router,
    zero_rtt: bool,
    limits: H3Limits,
) -> anyhow::Result<()> {
    let endpoint = quinn::Endpoint::server(server_config, addr)?;
    tokio::spawn(accept_loop(endpoint, app, zero_rtt, limits));
    Ok(())
}

async fn accept_loop(endpoint: quinn::Endpoint, app: Router, zero_rtt: bool, limits: H3Limits) {
    loop {
        // Global connection budget: a permit per QUIC connection, held until
        // the connection ends. Without this, an unbounded number of HTTP/3
        // connections could be accepted.
        let Ok(conn_permit) = limits.connections.clone().acquire_owned().await else {
            continue;
        };
        let Some(incoming) = endpoint.accept().await else {
            break;
        };
        let app = app.clone();
        let limits = limits.clone();
        tokio::spawn(async move {
            let _conn_permit = conn_permit;
            if let Err(e) = handle_connection(incoming, app, zero_rtt, limits).await {
                debug!(error = %e, "h3 connection ended");
            }
        });
    }
}

async fn handle_connection(
    incoming: quinn::Incoming,
    app: Router,
    zero_rtt: bool,
    limits: H3Limits,
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
                let (req, mut stream) = match resolver.resolve_request().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        debug!(error = %e, "h3 request resolve failed");
                        continue;
                    }
                };
                // Tag replayability at the moment the stream was accepted
                // (i.e. when its early data arrived), not when the request
                // task finally runs. A request delayed behind the
                // per-connection semaphore would otherwise see
                // `handshake_done` flip true mid-flight and run a replayed
                // mutation. Once the handshake completes no further streams
                // are early-data, so a stream accepted before that point is
                // reliably replayable; a stream accepted after is reliably
                // not (a delayed flag flip can only cause a safe false
                // positive — a 425 the client retries on the established
                // connection).
                let early_data = !handshake_done.load(Ordering::SeqCst);
                let app = app.clone();
                let permit = requests.clone().acquire_owned().await?;
                // Global active-stream budget: held for the whole request,
                // including a streaming SSE response, so HTTP/3 SSE streams
                // count against the same budget as TCP requests.
                let stream_permit = limits.streams.clone().acquire_owned().await;
                if stream_permit.is_err() {
                    let resp = http::Response::builder()
                        .status(http::StatusCode::SERVICE_UNAVAILABLE)
                        .body(())?;
                    stream.send_response(resp).await?;
                    stream.finish().await?;
                    continue;
                }
                let body_timeout = limits.body_timeout;
                tokio::spawn(async move {
                    let _permit = permit;
                    let _stream_permit = stream_permit;
                    if let Err(e) = handle_request(req, stream, app, early_data, body_timeout).await
                    {
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
    early_data: bool,
    body_timeout: Duration,
) -> anyhow::Result<()> {
    let (parts, _) = req.into_parts();

    // 0-RTT safety gate: never run a mutation from replayable early data.
    // `early_data` was captured when the stream was accepted (above), so a
    // replayed mutation cannot sneak through after the handshake completes.
    if early_data && !rest::is_read_only_path(&parts.method, parts.uri.path()) {
        let resp = http::Response::builder()
            .status(http::StatusCode::TOO_EARLY)
            .body(())
            .expect("static 425 response");
        stream.send_response(resp).await?;
        stream.finish().await?;
        return Ok(());
    }

    // Collect the request body (JSON bodies are small), bounded by a
    // wall-clock timeout so a trickle client cannot hold a stream slot open.
    let mut body = BytesMut::new();
    let body_read = async {
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
        Ok::<(), anyhow::Error>(())
    };
    if tokio::time::timeout(body_timeout, body_read).await.is_err() {
        let resp = http::Response::builder()
            .status(http::StatusCode::REQUEST_TIMEOUT)
            .body(())?;
        stream.send_response(resp).await?;
        stream.finish().await?;
        return Ok(());
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
