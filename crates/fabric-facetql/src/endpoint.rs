//! One FacetQL instance as Fabric addresses it: an identity, a base URL and
//! the token that opens it.
//!
//! The control plane holds a credential for every database instance in the
//! fleet, so this type's job is as much containment as construction:
//!
//! * the token is **never** in a URL — FacetQL's `?key=` is an SSE-only
//!   fallback and a query string lands in access logs, proxy logs and
//!   `ps` output. It goes in the `x-api-key` header, nowhere else;
//! * the [`Debug`] impl is hand-written to redact it, because a derived one
//!   would print the token into every log line, panic message and
//!   `dbg!` that ever touches a struct containing an endpoint; and
//! * an empty token is refused at construction. Sending an empty
//!   `x-api-key` would ask FacetQL to decide, and the answer to "am I
//!   authenticated?" must be settled before the request leaves.

use fabric_core::DbmsId;

use crate::error::FacetqlError;

/// An authenticated address for one FacetQL instance.
///
/// Clone is cheap-ish and intentional: a poller holds one per target.
#[derive(Clone)]
pub struct FacetqlEndpoint {
    dbms_id: DbmsId,
    base_url: String,
    token: String,
}

impl FacetqlEndpoint {
    /// Build an endpoint. `base_url` may carry a trailing slash; it is
    /// trimmed so paths concatenate cleanly.
    ///
    /// Fails when the token is empty or the URL is not `http`/`https` —
    /// both are configuration errors, and a control plane that starts up
    /// with a misconfigured credential and discovers it one request at a
    /// time is a control plane that reports a live instance as dead.
    pub fn new(
        dbms_id: DbmsId,
        base_url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, FacetqlError> {
        let base_url = base_url.into();
        let base_url = base_url.trim().trim_end_matches('/').to_string();
        let token = token.into();

        if token.is_empty() {
            return Err(FacetqlError::Configuration(format!(
                "no API token for FacetQL instance '{}'",
                dbms_id.0
            )));
        }

        if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
            return Err(FacetqlError::Configuration(format!(
                "FacetQL instance '{}' has a base URL that is not http(s): {base_url}",
                dbms_id.0
            )));
        }

        Ok(Self {
            dbms_id,
            base_url,
            token,
        })
    }

    /// Read the endpoint's token from an environment variable.
    ///
    /// The token is read straight into the struct and never echoed, so a
    /// misconfiguration reports the *variable name*, not its contents.
    pub fn from_env(
        dbms_id: DbmsId,
        base_url: impl Into<String>,
        token_var: &str,
    ) -> Result<Self, FacetqlError> {
        let token = std::env::var(token_var).map_err(|_| {
            FacetqlError::Configuration(format!(
                "environment variable {token_var} is not set: no API token for \
                 FacetQL instance '{}'",
                dbms_id.0
            ))
        })?;

        Self::new(dbms_id, base_url, token)
    }

    pub fn dbms_id(&self) -> &DbmsId {
        &self.dbms_id
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The token, for the one place that is allowed to read it: the
    /// `x-api-key` header builder in [`crate::client`].
    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    /// Absolute URL for a path such as `/stats`.
    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

/// Hand-written so the token cannot reach a log. See the module docs.
impl std::fmt::Debug for FacetqlEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FacetqlEndpoint")
            .field("dbms_id", &self.dbms_id)
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint() -> FacetqlEndpoint {
        FacetqlEndpoint::new(DbmsId::new("db-a"), "http://127.0.0.1:8892/", "s3cret").unwrap()
    }

    #[test]
    fn a_trailing_slash_does_not_double_up() {
        assert_eq!(endpoint().url("/stats"), "http://127.0.0.1:8892/stats");
    }

    #[test]
    fn debug_never_prints_the_token() {
        let rendered = format!("{:?}", endpoint());
        assert!(rendered.contains("<redacted>"));
        assert!(
            !rendered.contains("s3cret"),
            "the token leaked into Debug: {rendered}"
        );
    }

    #[test]
    fn an_empty_token_is_refused_at_construction() {
        let err = FacetqlEndpoint::new(DbmsId::new("db-a"), "http://127.0.0.1:8892", "")
            .unwrap_err();
        assert!(matches!(err, FacetqlError::Configuration(_)));
        assert!(err.to_string().contains("no API token"));
    }

    #[test]
    fn a_non_http_base_url_is_refused() {
        let err =
            FacetqlEndpoint::new(DbmsId::new("db-a"), "127.0.0.1:8892", "t").unwrap_err();
        assert!(err.to_string().contains("not http(s)"));
    }

    #[test]
    fn a_missing_env_var_names_the_variable_not_a_value() {
        let err = FacetqlEndpoint::from_env(
            DbmsId::new("db-a"),
            "http://127.0.0.1:8892",
            "FABRIC_TEST_TOKEN_THAT_IS_NOT_SET",
        )
        .unwrap_err();
        assert!(err.to_string().contains("FABRIC_TEST_TOKEN_THAT_IS_NOT_SET"));
    }
}
