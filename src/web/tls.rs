//! TLS material for the web facade.
//!
//! One certificate/key pair backs two configurations built from it:
//!   * a rustls `ServerConfig` for the TCP side (HTTP/1.1 + HTTP/2), ALPN
//!     `h2`,`http/1.1`;
//!   * a `quinn::ServerConfig` for the QUIC side (HTTP/3), ALPN `h3`, with
//!     0-RTT early data optionally enabled.
//!
//! If no cert/key is configured, an ephemeral self-signed cert is generated for
//! local development — clients must then disable verification.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::config::Config;

/// The two server configs the web facade serves from.
pub struct WebTls {
    pub tcp: Arc<rustls::ServerConfig>,
    pub quic: quinn::ServerConfig,
}

/// Install the process-wide rustls crypto provider (ring). Idempotent; safe to
/// call once before building any config.
pub fn install_crypto_provider() {
    // Errors only if a provider is already installed, which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn build(cfg: &Config) -> anyhow::Result<WebTls> {
    let (certs, key) = match (&cfg.tls_cert_path, &cfg.tls_key_path) {
        (Some(cert), Some(key)) => load_pem(cert, key)?,
        _ => self_signed()?,
    };

    let tcp = tcp_config(certs.clone(), key.clone_key())?;
    let quic = quic_config(certs, key, cfg.web_zero_rtt)?;
    Ok(WebTls {
        tcp: Arc::new(tcp),
        quic,
    })
}

fn tcp_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> anyhow::Result<rustls::ServerConfig> {
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building TCP TLS config")?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn quic_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    zero_rtt: bool,
) -> anyhow::Result<quinn::ServerConfig> {
    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building QUIC TLS config")?;
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    if zero_rtt {
        // Advertise that the server accepts replayable early data. The HTTP/3
        // layer still refuses to *dispatch* mutating RPCs received before the
        // handshake completes (see web::h3).
        crypto.max_early_data_size = u32::MAX;
        crypto.send_half_rtt_data = true;
    }
    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .context("converting rustls config for QUIC")?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(quic_crypto)))
}

fn load_pem(
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_pem = std::fs::read(cert_path)
        .with_context(|| format!("reading tls_cert_path {}", cert_path.display()))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(&cert_pem[..]))
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parsing certs from {}", cert_path.display()))?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in {}", cert_path.display());
    }
    let key_pem = std::fs::read(key_path)
        .with_context(|| format!("reading tls_key_path {}", key_path.display()))?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(&key_pem[..]))
        .with_context(|| format!("parsing key from {}", key_path.display()))?
        .with_context(|| format!("no private key found in {}", key_path.display()))?;
    Ok((certs, key))
}

fn self_signed() -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let sans = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let generated =
        rcgen::generate_simple_self_signed(sans).context("generating self-signed dev cert")?;
    let cert = generated.cert.der().clone();
    let key = PrivateKeyDer::try_from(generated.signing_key.serialize_der())
        .map_err(|e| anyhow::anyhow!("encoding self-signed key: {e}"))?;
    Ok((vec![cert], key))
}
