use std::net::SocketAddr;

use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json,
    Router,
};

use fabric_protocol::{
    FabricMessage,
    FabricResponse,
    ProtocolServer,
};

#[derive(Clone)]
struct AppState {
    protocol: ProtocolServer,
}

#[tokio::main]
async fn main() {
    let address: SocketAddr = "127.0.0.1:7700"
        .parse()
        .expect("valid protocol address");

    let protocol = ProtocolServer::new(address);

    let state = AppState {
        protocol: protocol.clone(),
    };

    let app = Router::new()
        .route("/v1/message", post(message))
        .with_state(state);

    println!(
        "Facet Protocol listening on {}",
        protocol.address()
    );

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind protocol listener");

    axum::serve(listener, app)
        .await
        .expect("protocol server");
}

async fn message(
    State(state): State<AppState>,
    Json(message): Json<FabricMessage>,
) -> Result<Json<FabricResponse>, (StatusCode, String)> {
    let response = state.protocol.handle(message);

    Ok(Json(response))
}