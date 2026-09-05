//! What can go wrong on the wire, in the shape the control plane has to make
//! decisions about.
//!
//! The variants are split by *what the caller must do*, not by where the
//! failure happened. [`FacetqlError::Unauthorized`] and
//! [`FacetqlError::Transport`] are separate from
//! [`FacetqlError::PreconditionFailed`] precisely because the first two mean
//! "this instance is not answering for us — treat it as unhealthy" and the
//! third means "the instance is fine, you lost a race".
//!
//! No variant carries a token. Error text comes from FacetQL's own response
//! body or from reqwest, neither of which is ever handed the caller's
//! credential in a loggable position (the token travels only in the
//! `x-api-key` header).

/// A failed FacetQL request.
#[derive(Debug)]
pub enum FacetqlError {
    /// The endpoint was built wrong (missing token, bad base URL). Never a
    /// runtime condition: it is caught before any request is sent.
    Configuration(String),

    /// The request never got an answer: connection refused, DNS, TLS,
    /// timeout. **Fail closed** — an instance that cannot be reached is not a
    /// healthy instance.
    Transport(String),

    /// 401/403. The token is wrong, revoked, or lacks the role the endpoint
    /// requires (`GET /stats` is admin-gated). Also fails closed: an instance
    /// we cannot authenticate to is one we cannot manage.
    Unauthorized { status: u16, body: String },

    /// 412 from `POST /transaction`: a `set_if` precondition did not hold, so
    /// **nothing in the batch was applied**. The compare-and-set lost; the
    /// caller should re-read and decide, not retry blindly.
    PreconditionFailed(String),

    /// 409: the address already exists (`if_absent`) or the node is already
    /// claimed.
    Conflict(String),

    /// 404.
    NotFound(String),

    /// Any other non-success status, with FacetQL's own words.
    Status { status: u16, body: String },

    /// The response was not the JSON this contract says it is. A decode
    /// failure is a contract mismatch, not a data problem — it is worth
    /// surfacing loudly rather than defaulting a field.
    Decode { context: String, message: String },
}

impl FacetqlError {
    /// Whether this failure means the instance itself must be treated as not
    /// serviceable.
    ///
    /// A lost compare-and-set, a 409 or a 404 are perfectly healthy answers
    /// to a request that asked for something that was not true. Everything
    /// else — unreachable, unauthenticated, malformed, 5xx — means the
    /// control plane does not currently have a working relationship with that
    /// instance and must not report it as alive.
    pub fn implies_unhealthy(&self) -> bool {
        match self {
            Self::PreconditionFailed(_) | Self::Conflict(_) | Self::NotFound(_) => false,
            Self::Configuration(_)
            | Self::Transport(_)
            | Self::Unauthorized { .. }
            | Self::Status { .. }
            | Self::Decode { .. } => true,
        }
    }
}

impl std::fmt::Display for FacetqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(message) => {
                write!(f, "facetql endpoint misconfigured: {message}")
            }
            Self::Transport(message) => {
                write!(f, "facetql unreachable: {message}")
            }
            Self::Unauthorized { status, body } => {
                write!(f, "facetql refused the token ({status}): {body}")
            }
            Self::PreconditionFailed(body) => {
                write!(f, "facetql compare-and-set refused (412): {body}")
            }
            Self::Conflict(body) => write!(f, "facetql conflict (409): {body}"),
            Self::NotFound(body) => write!(f, "facetql not found (404): {body}"),
            Self::Status { status, body } => {
                write!(f, "facetql returned {status}: {body}")
            }
            Self::Decode { context, message } => {
                write!(f, "facetql response for {context} did not decode: {message}")
            }
        }
    }
}

impl std::error::Error for FacetqlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lost_race_does_not_condemn_the_instance() {
        assert!(!FacetqlError::PreconditionFailed("stale".into()).implies_unhealthy());
        assert!(!FacetqlError::Conflict("exists".into()).implies_unhealthy());
        assert!(!FacetqlError::NotFound("gone".into()).implies_unhealthy());
    }

    #[test]
    fn unreachable_and_unauthenticated_both_fail_closed() {
        assert!(FacetqlError::Transport("refused".into()).implies_unhealthy());
        assert!(
            FacetqlError::Unauthorized {
                status: 403,
                body: "admin only".into()
            }
            .implies_unhealthy()
        );
        assert!(
            FacetqlError::Status {
                status: 500,
                body: "disk".into()
            }
            .implies_unhealthy()
        );
    }
}
