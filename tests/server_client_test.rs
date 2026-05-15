use rain_engine::{Client, ServerBuilder};
use rain_engine_runtime::{EventIngressRequest, WebhookIngressRequest};
use serde_json::json;
use std::collections::BTreeSet;
use tokio::net::TcpListener;

/// Helper: boot a mock server on a free port and return (addr, join handle).
async fn start_mock_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = ServerBuilder::new()
        .with_bind_address(addr)
        .with_in_memory_store();

    let handle = tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Give the server time to bind
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    (addr, handle)
}

#[tokio::test]
async fn test_human_input_round_trip() {
    let (addr, _handle) = start_mock_server().await;
    let client = Client::new(&format!("http://{}", addr)).unwrap();

    let result = client
        .send_human_input("user1", "session_human", "Hello World!")
        .await;

    assert!(result.is_ok(), "Human input should succeed");
    let run_result = result.unwrap();
    assert!(
        !run_result.advances.is_empty(),
        "Should have at least one advance"
    );
    assert_eq!(
        run_result.outcome.response.as_deref(),
        Some("Mock Response"),
        "Mock provider should respond"
    );
}

#[tokio::test]
async fn test_webhook_round_trip() {
    let (addr, _handle) = start_mock_server().await;
    let client = Client::new(&format!("http://{}", addr)).unwrap();

    let request = WebhookIngressRequest {
        session_id: "session_webhook".to_string(),
        payload: json!({"event": "push", "repo": "rain-engine"}),
        attachments: vec![],
        granted_scopes: BTreeSet::new(),
        idempotency_key: Some("webhook-1".to_string()),
        provider: None,
        policy_override: None,
    };

    let result = client.send_webhook("github", &request).await;
    assert!(
        result.is_ok(),
        "Webhook trigger should succeed: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_external_event_round_trip() {
    let (addr, _handle) = start_mock_server().await;
    let client = Client::new(&format!("http://{}", addr)).unwrap();

    let request = EventIngressRequest {
        session_id: "session_external".to_string(),
        payload: json!({"sensor": "temp", "value": 42}),
        attachments: vec![],
        granted_scopes: BTreeSet::new(),
        idempotency_key: None,
        provider: None,
        policy_override: None,
    };

    let result = client.send_external_event("iot-sensor", &request).await;
    assert!(
        result.is_ok(),
        "External event should succeed: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_system_observation_round_trip() {
    let (addr, _handle) = start_mock_server().await;
    let client = Client::new(&format!("http://{}", addr)).unwrap();

    let request = EventIngressRequest {
        session_id: "session_sysobs".to_string(),
        payload: json!({"cpu_usage": 0.85}),
        attachments: vec![],
        granted_scopes: BTreeSet::new(),
        idempotency_key: None,
        provider: None,
        policy_override: None,
    };

    let result = client
        .send_system_observation("metrics-agent", &request)
        .await;
    assert!(
        result.is_ok(),
        "System observation should succeed: {:?}",
        result.err()
    );
}
