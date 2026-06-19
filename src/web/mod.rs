//! Web facade: the `PathLock` API and its event streams exposed over HTTPS
//! (HTTP/1.1 + HTTP/2) and HTTP/3, alongside — not replacing — the gRPC server.
//!
//! Design:
//!   * One axum [`Router`] of JSON endpoints ([`rest`]) plus SSE/long-poll event
//!     routes ([`sse`]). Handlers call the same `PathLock` impl the gRPC server
//!     uses, so there is exactly one code path through the engine.
//!   * The TCP side terminates TLS with tokio-rustls and serves the router with
//!     hyper's HTTP/1.1+HTTP/2 auto-negotiation.
//!   * The QUIC side ([`h3`]) serves the *same* router over HTTP/3, with 0-RTT
//!     early data restricted to read-only RPCs.
//!
//! The facade is opt-in (`web_listen`); when unset, only gRPC runs.

pub mod eventlog;
mod h3;
mod rest;
mod sse;
mod state;
mod tls;

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::service::PathLockService;

use self::eventlog::EventLog;
use self::state::AppState;

/// Build the facade and spawn its listeners. Binding happens before returning so
/// address/cert errors surface at startup; the accept loops then run detached
/// (like the raft transport server) until process exit.
pub async fn spawn(cfg: &Config, svc: PathLockService) -> anyhow::Result<()> {
    tls::install_crypto_provider();
    let web_tls = tls::build(cfg).context("building web TLS")?;

    // Retain a short window after the last SSE client detaches so a reconnecting
    // EventSource can replay from its Last-Event-ID across a brief drop.
    let retention = Duration::from_secs(30);
    let log = EventLog::new(
        svc.broadcaster.clone(),
        cfg.web_event_log_capacity,
        retention,
    );
    let state = AppState { svc, log };
    let app: Router = rest::routes().merge(sse::routes()).with_state(state);

    // --- TCP: HTTP/1.1 + HTTP/2 over TLS ---
    let tcp_addr: SocketAddr = cfg.web_listen.parse()?;
    let listener = TcpListener::bind(tcp_addr)
        .await
        .with_context(|| format!("binding web_listen {tcp_addr}"))?;
    let acceptor = TlsAcceptor::from(web_tls.tcp.clone());
    info!(%tcp_addr, "web facade listening (HTTP/1.1 + HTTP/2)");
    tokio::spawn(serve_tcp(listener, acceptor, app.clone()));

    // --- QUIC: HTTP/3 ---
    if cfg.h3_enabled() {
        let h3_addr: SocketAddr = cfg.h3_listen.parse()?;
        h3::spawn(h3_addr, web_tls.quic, app, cfg.web_zero_rtt)
            .with_context(|| format!("binding h3_listen {h3_addr}"))?;
        info!(%h3_addr, "web facade listening (HTTP/3)");
    }
    Ok(())
}

async fn serve_tcp(listener: TcpListener, acceptor: TlsAcceptor, app: Router) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                warn!(error = %e, "web TCP accept failed");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, "web TLS handshake failed");
                    return;
                }
            };
            let io = TokioIo::new(tls);
            let service = TowerToHyperService::new(app);
            if let Err(e) = ConnBuilder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, service)
                .await
            {
                // Client-side resets are routine; log at debug-ish level.
                error!(error = %e, "web connection error");
            }
        });
    }
}
