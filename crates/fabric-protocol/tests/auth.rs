//! Proves `facet-protocol`'s HTTP listener actually enforces the bearer
//! token — a real bound server, real HTTP requests, not just the pure
//! `authorized()` comparison `transport.rs`'s own unit tests already cover.
//! This is the concrete evidence the listener is no longer the
//! unauthenticated port `ProtocolServer::handle`'s doc comment used to warn
//! about.

use std::net::SocketAddr;

use fabric_core::DbmsId;
use fabric_protocol::{
    FabricMessage,
    NodeHeartbeat,
    ProtocolAppState,
    ProtocolServer,
};

/// Binds the real router on an ephemeral loopback port and returns its base
/// URL, keeping the server alive for the test's duration via a detached
/// task (each test gets its own port, so tests can run concurrently).
async fn spawn_server(token: &str) -> String {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let bound = listener.local_addr().unwrap();

    let state = ProtocolAppState::new(ProtocolServer::new(bound), token.to_string());
    let app = fabric_protocol::router(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    format!("http://{bound}")
}

fn heartbeat_message() -> FabricMessage {
    FabricMessage::Heartbeat(NodeHeartbeat {
        node_id: DbmsId::new("db-a"),
        timestamp_ms: 1,
        healthy: true,
    })
}

#[tokio::test]
async fn a_request_with_no_token_is_refused() {
    let base = spawn_server("real-token").await;

    let response = reqwest::Client::new()
        .post(format!("{base}/v1/message"))
        .json(&heartbeat_message())
        .send()
        .await
        .expect("request should complete");

    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_request_with_the_wrong_token_is_refused() {
    let base = spawn_server("real-token").await;

    let response = reqwest::Client::new()
        .post(format!("{base}/v1/message"))
        .header("x-api-key", "not-the-real-token")
        .json(&heartbeat_message())
        .send()
        .await
        .expect("request should complete");

    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_request_with_the_right_token_is_handled() {
    let base = spawn_server("real-token").await;

    let response = reqwest::Client::new()
        .post(format!("{base}/v1/message"))
        .header("x-api-key", "real-token")
        .json(&heartbeat_message())
        .send()
        .await
        .expect("request should complete");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let body: serde_json::Value = response.json().await.expect("valid JSON body");
    assert_eq!(body, serde_json::json!("Acknowledged"));
}

/// The token check happens before the body is even parsed as a
/// `FabricMessage` — a caller with no credential should never learn
/// anything about the schema it declined to reveal, including via a 400
/// for a malformed body it never should have been allowed to submit.
#[tokio::test]
async fn a_malformed_body_with_no_token_is_still_refused_as_unauthorized_first() {
    let base = spawn_server("real-token").await;

    let response = reqwest::Client::new()
        .post(format!("{base}/v1/message"))
        .body("not json")
        .send()
        .await
        .expect("request should complete");

    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
}
