use std::net::SocketAddr;

use fabric_protocol::{ProtocolAppState, ProtocolServer, PROTOCOL_TOKEN_ENV};

#[tokio::main]
async fn main() {
    let address: SocketAddr = "127.0.0.1:7700"
        .parse()
        .expect("valid protocol address");

    // Fail closed, the same posture `fabric-daemon::config::Settings::resolve`
    // already enforces for its own `FABRIC_ADMIN_TOKEN`: this listener
    // accepts node registrations, heartbeats, topology and telemetry
    // reports, so it is never served without a credential a caller must
    // present.
    let token = match std::env::var(PROTOCOL_TOKEN_ENV) {
        Ok(token) if !token.is_empty() => token,
        _ => {
            eprintln!(
                "facet-protocol: environment variable {PROTOCOL_TOKEN_ENV} is not set: \
                 this listener accepts node registrations, heartbeats, topology and \
                 telemetry reports, so it is never served unauthenticated"
            );
            std::process::exit(1);
        }
    };

    let protocol = ProtocolServer::new(address);
    let state = ProtocolAppState::new(protocol.clone(), token);
    let app = fabric_protocol::router(state);

    println!("Facet Protocol listening on {}", protocol.address());

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind protocol listener");

    axum::serve(listener, app)
        .await
        .expect("protocol server");
}
