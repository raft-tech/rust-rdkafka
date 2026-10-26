use std::sync::mpsc;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::ClientContext;
use rdkafka::error::KafkaError;
use rdkafka::ClientConfig;
use rdkafka_sys::RDKafkaErrorCode;

struct EventContext {
    errors: mpsc::Sender<()>,
    stats: mpsc::Sender<()>,
}

impl ClientContext for EventContext {
    fn error(&self, _error: KafkaError, _reason: &str) {
        let _ = self.errors.send(());
    }

    fn stats_raw(&self, _statistics: &[u8]) {
        let _ = self.stats.send(());
    }
}

#[tokio::test]
async fn client_events_do_not_interrupt_admin_responses() {
    // Choose an unused local port so that connection attempts fail immediately.
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = port.local_addr().unwrap();
    drop(port);
    let (errors_tx, errors_rx) = mpsc::channel();
    let (stats_tx, stats_rx) = mpsc::channel();
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", address.to_string())
        .set("statistics.interval.ms", "100")
        .create_with_context(EventContext {
            errors: errors_tx,
            stats: stats_tx,
        })
        .unwrap();

    errors_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    stats_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let options = AdminOptions::new().request_timeout(Some(Duration::from_millis(100)));
    let result = tokio::time::timeout(Duration::from_secs(5), admin.create_topics(&[], &options))
        .await
        .expect("admin polling thread stopped processing responses");
    assert_eq!(
        result,
        Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut))
    );
}

#[cfg(feature = "ssl")]
#[test]
fn oauth_token_callback_runs_without_manual_polling() {
    use rdkafka::client::OAuthToken;
    use std::error::Error;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct OAuthContext(mpsc::Sender<Option<String>>, bool);

    impl ClientContext for OAuthContext {
        const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

        fn generate_oauth_token(&self, config: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
            self.0.send(config.map(str::to_owned)).unwrap();
            if self.1 {
                return Err("test token provider failure".into());
            }
            Ok(OAuthToken {
                token: "test-token".into(),
                principal_name: "test-principal".into(),
                lifetime_ms: (SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() + 60_000)
                    as i64,
            })
        }
    }

    for fail in [false, true] {
        let (tx, rx) = mpsc::channel();
        let admin: AdminClient<_> = ClientConfig::new()
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "OAUTHBEARER")
            .set("sasl.oauthbearer.config", "test-config")
            .create_with_context(OAuthContext(tx, fail))
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap().as_deref(),
            Some("test-config")
        );
        drop(admin);
    }
}
