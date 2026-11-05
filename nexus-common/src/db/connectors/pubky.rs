use pubky::{BuildError, Pubky, PubkyHttpClient};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::OnceCell;
use tracing::debug;

/// Deadline for any single request of the shared Pubky client, body included.
/// The client has none of its own, so a homeserver that stops answering would
/// hold the request, and the processor waiting on it, for the processor-level
/// timeout. Fetches with a tighter budget wrap their own deadline around this.
pub const PUBKY_REQUEST_TIMEOUT: Duration = Duration::from_secs(180);

static PUBKY_SINGLETON: OnceCell<Arc<Pubky>> = OnceCell::const_new();

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum PubkyClientError {
    #[error("PubkyClient not initialized")]
    NotInitialized,

    #[error("Client initialization error: {0}")]
    ClientError(String),
}

pub struct PubkyConnector;

fn build_client(
    testnet_host: Option<&str>,
    request_timeout: Duration,
) -> Result<PubkyHttpClient, BuildError> {
    let mut builder = PubkyHttpClient::builder();
    builder.request_timeout(request_timeout);
    if let Some(host) = testnet_host {
        builder
            .testnet_with_host(host)
            // Force pkarr/mainline DHT to bind an ephemeral local port instead of default behavior
            // We do this to prevent the client DHT from competing with `StaticTestnet` for port 6881
            .pkarr(|p| p.dht(|d| d.port(0)));
    }
    builder.build()
}

impl PubkyConnector {
    /// Initializes the `Pubky` singleton.
    ///
    /// - For mainnet, pass `None`.
    /// - For testnet, pass `Some(hostname)` (e.g., "localhost" or "homeserver").
    pub async fn initialise(testnet_host: Option<&str>) -> Result<(), PubkyClientError> {
        PUBKY_SINGLETON
            .get_or_try_init(|| async {
                let mode = testnet_host
                    .map(|host| format!("testnet with host '{host}'"))
                    .unwrap_or_else(|| "mainnet".to_string());
                debug!("Initialising Pubky singleton in {mode} mode");

                let client = build_client(testnet_host, PUBKY_REQUEST_TIMEOUT)
                    .map_err(|e| PubkyClientError::ClientError(e.to_string()))?;
                Ok(Arc::new(Pubky::with_client(client)))
            })
            .await
            .map(|_| ())
    }
    /// Retrieves the instance of `Pubky`
    pub fn get() -> Result<Arc<Pubky>, PubkyClientError> {
        PUBKY_SINGLETON
            .get()
            .cloned()
            .ok_or(PubkyClientError::NotInitialized)
    }

    /// Initializes `PUBKY_SINGLETON` with a provided `Pubky` instance.
    ///
    /// # Usage:
    /// - This function is primarily intended for **watcher tests** where a controlled `Pubky` instance
    ///   needs to be injected instead of relying on environment-based initialization
    pub async fn init_from(sdk: Pubky) -> Result<(), PubkyClientError> {
        PUBKY_SINGLETON
            .get_or_try_init(|| async { Ok(Arc::new(sdk)) })
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky::Method;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn the_shared_client_has_a_request_deadline_above_every_fetch_budget() {
        assert!(PUBKY_REQUEST_TIMEOUT >= Duration::from_secs(120));
        assert!(PUBKY_REQUEST_TIMEOUT <= Duration::from_secs(600));
    }

    #[tokio::test]
    async fn a_client_built_with_a_deadline_gives_up_on_a_silent_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let client = build_client(Some("localhost"), Duration::from_millis(400)).unwrap();
        let started = std::time::Instant::now();
        let result = client
            .request(Method::GET, &format!("http://{addr}/"))
            .send()
            .await;
        let error = result.expect_err("a server that never answers must time out");
        assert!(error.is_timeout(), "{error:?}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }
}
