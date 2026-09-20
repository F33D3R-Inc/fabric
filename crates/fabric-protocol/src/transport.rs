use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};

use crate::{
    FabricMessage,
    FabricResponse,
};

#[derive(Debug)]
pub enum ProtocolError {
    InvalidMessage(String),
    Serialization(String),
    Io(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Self::InvalidMessage(message) => {
                write!(f, "invalid message: {message}")
            }

            Self::Serialization(message) => {
                write!(f, "serialization error: {message}")
            }

            Self::Io(message) => {
                write!(f, "I/O error: {message}")
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug, Clone)]
pub struct ProtocolServer {
    address: SocketAddr,
}

impl ProtocolServer {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn decode(
        payload: &[u8],
    ) -> Result<FabricMessage, ProtocolError> {
        serde_json::from_slice(payload)
            .map_err(|error| {
                ProtocolError::Serialization(error.to_string())
            })
    }

    pub fn encode(
        response: &FabricResponse,
    ) -> Result<Vec<u8>, ProtocolError> {
        serde_json::to_vec(response)
            .map_err(|error| {
                ProtocolError::Serialization(error.to_string())
            })
    }

    /// Decode-and-acknowledge only. **This is not the authoritative message
    /// handler** — [`fabric_runtime::FabricRuntime::handle`] is, and it is the
    /// one that retains anything (node registry, topology, telemetry).
    ///
    /// This exists so the protocol crate can be exercised without depending on
    /// the runtime (which depends on *it*, so the reverse edge would be a
    /// cycle). It must stay stateless regardless of authentication: making
    /// this retain fleet state is a separate, later step (a stateful daemon
    /// needing a real inventory here is its own design question, same as
    /// `fabric-daemon`'s admin surface already had to answer for its own
    /// stateful reports/aborts — see that crate's `admin.rs`), not something
    /// this fix does as a side effect.
    ///
    /// The `facet-protocol` binary that serves this over HTTP is no longer
    /// the unauthenticated listener this comment used to warn about (see
    /// [`router`] and [`authorized`]): every request must present the bearer
    /// token configured via [`PROTOCOL_TOKEN_ENV`] before a message ever
    /// reaches this function, the same shared-secret scheme
    /// `fabric-daemon::admin` already uses for its own stateful surface.
    /// `handle` itself takes no part in that check — it is a pure function
    /// over an already-authenticated, already-decoded message, exactly as
    /// before.
    pub fn handle(
        &self,
        message: FabricMessage,
    ) -> FabricResponse {
        match message {
            FabricMessage::RegisterNode(registration) => {
                FabricResponse::Registered {
                    node_id: registration.node_id.0,
                }
            }

            FabricMessage::Heartbeat(_) => {
                FabricResponse::Acknowledged
            }

            FabricMessage::Topology(_) => {
                FabricResponse::Acknowledged
            }

            FabricMessage::Telemetry(_) => {
                FabricResponse::Acknowledged
            }

            FabricMessage::Workload(_) => {
                FabricResponse::Acknowledged
            }
        }
    }
}

/// Environment variable holding the bearer token every request to
/// `facet-protocol`'s HTTP listener must present. Named separately from
/// `fabric-daemon`'s `FABRIC_ADMIN_TOKEN` — a different socket, a different
/// credential, a different job, exactly as that crate's `admin` module
/// already documents for its own surface.
pub const PROTOCOL_TOKEN_ENV: &str = "FABRIC_PROTOCOL_TOKEN";

/// Shared state for the authenticated HTTP listener: the (stateless)
/// protocol handler plus the one secret every request must present.
#[derive(Clone)]
pub struct ProtocolAppState {
    protocol: ProtocolServer,
    token: Arc<String>,
}

impl ProtocolAppState {
    /// `token` must be non-empty — callers resolve it from
    /// [`PROTOCOL_TOKEN_ENV`] and refuse to start otherwise, the same
    /// fail-closed posture `fabric-daemon::config::Settings::resolve` already
    /// enforces for `FABRIC_ADMIN_TOKEN`. Not re-checked here: emptiness is a
    /// start-up configuration error, not a per-request condition, and
    /// `authorized` already refuses an empty presented value against any
    /// expected one (see its own doc).
    pub fn new(protocol: ProtocolServer, token: String) -> Self {
        Self {
            protocol,
            token: Arc::new(token),
        }
    }
}

/// The authenticated router: `POST /v1/message`, gated by [`require_token`].
/// The one route `facet-protocol`'s binary used to build inline — moved into
/// the library so the auth behavior is exercised by this crate's own tests
/// against a real bound server, not just asserted by the binary.
pub fn router(state: ProtocolAppState) -> Router {
    Router::new()
        .route("/v1/message", post(message))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token))
        .with_state(state)
}

async fn message(
    State(state): State<ProtocolAppState>,
    Json(message): Json<FabricMessage>,
) -> Json<FabricResponse> {
    Json(state.protocol.handle(message))
}

/// Axum middleware: refuses any request that does not present the
/// configured bearer token via `x-api-key`, mirroring
/// `facetql::auth::auth_middleware`'s header and status code exactly, so an
/// operator who already knows FacetQL's convention needs to learn nothing
/// new for this listener.
async fn require_token(
    State(state): State<ProtocolAppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let presented = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if authorized(presented, &state.token) {
        return next.run(req).await;
    }

    (StatusCode::UNAUTHORIZED, "missing or invalid x-api-key\n").into_response()
}

/// Compare in time that does not depend on how many leading bytes matched.
///
/// Identical technique to `fabric_daemon::admin`'s own `authorized`: this is
/// a bearer secret on a socket a node or another daemon reaches over the
/// network, and a `==` on strings would answer a little sooner for a wrong
/// guess that shares a prefix — a way to learn the token one byte at a time.
/// Kept as its own copy here rather than a shared crate: it is five lines,
/// self-contained, and has no dependency either crate should take on the
/// other just to share it.
pub fn authorized(presented: &str, expected: &str) -> bool {
    let presented = presented.as_bytes();
    let expected = expected.as_bytes();

    let mut difference = (presented.len() ^ expected.len()) as u8;

    for index in 0..presented.len().max(expected.len()) {
        let left = presented.get(index).copied().unwrap_or(0);
        let right = expected.get(index).copied().unwrap_or(0);
        difference |= left ^ right;
    }

    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wrong_token_is_refused_however_close_it_is() {
        assert!(authorized("secret", "secret"));
        assert!(!authorized("secre", "secret"));
        assert!(!authorized("secrets", "secret"));
        assert!(!authorized("", "secret"));
        assert!(!authorized("Secret", "secret"));
    }

    /// An empty expected token would make every request authorized,
    /// including one that presents no header at all. `ProtocolAppState::new`
    /// documents that callers must never construct one with an empty token;
    /// this is the second lock on the same door.
    #[test]
    fn an_absent_credential_never_matches_a_real_one() {
        assert!(!authorized("", "t"));
    }
}