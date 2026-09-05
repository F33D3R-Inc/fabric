//! The operator port: a different socket, a different credential, a different
//! job.
//!
//! It is deliberately not a path on the data port. The data port speaks
//! FacetQL's wire protocol and nothing else — every path it serves is one
//! `facetql/src/api/routes.rs` serves, which is the property that lets
//! `fqStore` point at it without knowing — so an `/admin/...` of Fabric's own
//! invention there would be a path FacetQL does not have, on the port every
//! application client can reach, describing the fleet's shape.
//!
//! # It is authenticated, and it fails closed
//!
//! `NOTES` FAB-SEC-001 is explicit that a stateful control listener needs
//! authentication before it may exist: the reason `fabric-protocol`'s daemon
//! is still a stateless echo is that making it stateful would hand an
//! unauthenticated port a writable fleet inventory. This surface *is*
//! stateful — it accepts transfer reports and aborts — so it carries a token
//! from the environment, the daemon refuses to start without one, and it binds
//! to loopback by default.
//!
//! # What it will not do
//!
//! It will not abort an action that has cut over. That refusal lives in the
//! control loop, where the decision is made, and it is reported here as a
//! `409` with the reason in FacetQL's own style: plain text an operator can
//! act on.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use fabric_controller::ActionId;
use serde::Deserialize;

use crate::control::ControlRequest;
use crate::status::StatusHandle;

#[derive(Clone)]
pub struct AdminState {
    status: Arc<StatusHandle>,
    token: Arc<String>,
    requests: tokio::sync::mpsc::UnboundedSender<ControlRequest>,
}

impl AdminState {
    pub fn new(
        status: Arc<StatusHandle>,
        token: String,
        requests: tokio::sync::mpsc::UnboundedSender<ControlRequest>,
    ) -> Self {
        Self {
            status,
            token: Arc::new(token),
            requests,
        }
    }
}

/// What a data mover reports when it has copied something.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferReport {
    /// A coordinate is exactly one atom of its shard's grid, so this is 0 or
    /// 1: 1 means the cell's data is fully copied.
    #[serde(default)]
    pub atoms_copied: usize,

    pub bytes_copied: u64,

    /// The cell's resident size, if the mover knows it. Only ever an advisory
    /// progress fraction; nothing decides a phase is done because of it.
    #[serde(default)]
    pub resident_bytes: Option<u64>,
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status))
        .route("/fleet", get(fleet))
        .route("/placements", get(placements))
        .route("/routing", get(routing))
        .route("/actions", get(actions))
        .route("/actions/history", get(history))
        .route("/actions/{id}/transfer", post(transfer))
        .route("/actions/{id}/abort", post(abort))
        .fallback(not_found)
        .with_state(state)
}

/// The daemon's own liveness, for a container runtime. Unauthenticated and
/// deliberately empty of fleet detail: it answers "is this process serving",
/// which is not a secret, and nothing else.
async fn healthz() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

async fn status(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    guard(&state, &headers, |state| {
        Json(&*state.status.get()).into_response()
    })
}

async fn fleet(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    guard(&state, &headers, |state| {
        Json(state.status.get().backends.clone()).into_response()
    })
}

async fn placements(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    guard(&state, &headers, |state| {
        Json(state.status.get().placements.clone()).into_response()
    })
}

/// The one number that says whether a control-loop decision has reached live
/// traffic, next to the map it describes.
async fn routing(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    guard(&state, &headers, |state| {
        let status = state.status.get();

        Json(serde_json::json!({
            "routing_generation": status.routing_generation,
            "placement_generation": status.placement_generation,
            "snapshot_at_ms": status.snapshot_at_ms,
            "placements": status.placements,
        }))
        .into_response()
    })
}

async fn actions(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    guard(&state, &headers, |state| {
        Json(state.status.get().in_flight.clone()).into_response()
    })
}

async fn history(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    guard(&state, &headers, |state| {
        Json(state.status.get().history.clone()).into_response()
    })
}

async fn transfer(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
    body: axum::body::Bytes,
) -> Response {
    if let Some(refusal) = refuse(&state, &headers) {
        return refusal;
    }

    let report: TransferReport = match serde_json::from_slice(&body) {
        Ok(report) => report,

        Err(error) => {
            return (StatusCode::BAD_REQUEST, format!("fabricd: {error}\n")).into_response();
        }
    };

    ask(&state, |reply| ControlRequest::Transfer {
        id: ActionId(id),
        atoms_copied: report.atoms_copied.min(1),
        bytes_copied: report.bytes_copied,
        resident_bytes: report.resident_bytes,
        reply,
    })
    .await
}

async fn abort(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Response {
    if let Some(refusal) = refuse(&state, &headers) {
        return refusal;
    }

    ask(&state, |reply| ControlRequest::Abort {
        id: ActionId(id),
        reply,
    })
    .await
}

async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "fabricd: no such admin route\n").into_response()
}

/// Put a question to the control loop and wait for its answer.
///
/// The loop is the only thing that may touch the runtime, so every write goes
/// through here. A refusal comes back as a `409`: it is the control plane
/// declining to do something unsafe, not an error in the request's shape.
async fn ask<F>(state: &AdminState, build: F) -> Response
where
    F: FnOnce(tokio::sync::oneshot::Sender<Result<String, String>>) -> ControlRequest,
{
    let (reply, answer) = tokio::sync::oneshot::channel();

    if state.requests.send(build(reply)).is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "fabricd: the control loop has stopped\n",
        )
            .into_response();
    }

    match answer.await {
        Ok(Ok(message)) => (StatusCode::OK, format!("{message}\n")).into_response(),

        Ok(Err(reason)) => (StatusCode::CONFLICT, format!("fabricd: {reason}\n")).into_response(),

        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "fabricd: the control loop stopped before answering\n",
        )
            .into_response(),
    }
}

fn guard<F>(state: &AdminState, headers: &HeaderMap, answer: F) -> Response
where
    F: FnOnce(&AdminState) -> Response,
{
    match refuse(state, headers) {
        Some(refusal) => refusal,
        None => answer(state),
    }
}

fn refuse(state: &AdminState, headers: &HeaderMap) -> Option<Response> {
    let presented = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if authorized(presented, &state.token) {
        return None;
    }

    Some(
        (
            StatusCode::UNAUTHORIZED,
            "fabricd: the operator surface requires x-api-key\n",
        )
            .into_response(),
    )
}

/// Compare in time that does not depend on how many leading bytes matched.
///
/// The admin token is a bearer secret on a socket an operator may well expose
/// to a bastion host; a `==` on strings answers a little sooner for a wrong
/// guess that shares a prefix, and that difference is a way to learn the token
/// one byte at a time.
fn authorized(presented: &str, expected: &str) -> bool {
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

    /// An empty expected token would make every request authorized, including
    /// one that presents no header at all. The configuration refuses to start
    /// without one; this is the second lock on the same door.
    #[test]
    fn an_absent_credential_never_matches_a_real_one() {
        assert!(!authorized("", "t"));
    }

    #[test]
    fn a_transfer_report_refuses_a_field_it_does_not_know() {
        let error = serde_json::from_str::<TransferReport>(
            r#"{ "bytes_copied": 1, "atoms_coped": 1 }"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("atoms_coped"), "{error}");
    }
}
