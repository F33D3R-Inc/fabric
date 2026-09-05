//! The wire: an HTTP client speaking FacetQL's canonical contract.
//!
//! This is the seam that did not exist. Before it, `fabric` appeared zero
//! times in the rest of the stack and Fabric's pipeline was fed by replaying
//! a JSON file from disk. Everything else in this crate — the telemetry
//! sampler, the durable placement store — is built on the six calls below.
//!
//! Requests are built by hand (`content-type` header plus a string body) and
//! responses are read with `.text()` and parsed with `serde_json`, because
//! this crate depends on reqwest with `default-features = false` and cannot
//! assume the `json` feature is compiled in. That mirrors facetql's own CLI
//! client exactly.
//!
//! **Authentication is a header, always.** `x-api-key` on every request; the
//! `?key=` query parameter FacetQL also accepts is an SSE-only fallback and
//! putting a credential in a URL leaks it into every log between here and
//! there.

use crate::endpoint::FacetqlEndpoint;
use crate::error::FacetqlError;
use crate::wire::{
    CreateNodeRequest, EngineStats, Node, QueryPage, QueryRequest, TransactionRequest,
    TxOperation,
};

/// Largest page `POST /nodes/query` will return (FacetQL clamps `limit` to
/// 500). Paging asks for exactly this and follows the cursor.
const PAGE_LIMIT: usize = 500;

/// Safety valve on cursor-following. A server that kept handing back a
/// non-empty `next` would otherwise spin here forever; 10,000 pages of 500 is
/// five million placements, which is far past anything a control plane holds.
const MAX_PAGES: usize = 10_000;

/// Largest batch `POST /nodes/multiget` accepts (FacetQL's engine bounds it at
/// 1000 addresses, and refuses rather than truncating).
pub const MULTIGET_LIMIT: usize = 1_000;

/// An authenticated client for one FacetQL instance.
#[derive(Debug, Clone)]
pub struct FacetqlClient {
    http: reqwest::Client,
    endpoint: FacetqlEndpoint,
}

impl FacetqlClient {
    /// Build a client with a default reqwest client.
    pub fn new(endpoint: FacetqlEndpoint) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint,
        }
    }

    /// Build a client over a caller-supplied reqwest client — the way to set
    /// connect/read timeouts, which a poller wants so one wedged instance
    /// cannot stall a sweep of the fleet.
    pub fn with_http(endpoint: FacetqlEndpoint, http: reqwest::Client) -> Self {
        Self { http, endpoint }
    }

    pub fn endpoint(&self) -> &FacetqlEndpoint {
        &self.endpoint
    }

    // ── the wire ───────────────────────────────────────────────────────

    /// `GET /stats` — the engine's own storage/operation statistics.
    ///
    /// Admin-gated: a non-admin token gets 403, which arrives here as
    /// [`FacetqlError::Unauthorized`] and therefore as "unhealthy", not as
    /// "zero traffic".
    pub async fn stats(&self) -> Result<EngineStats, FacetqlError> {
        let body = self.send(self.request(reqwest::Method::GET, "/stats"), "GET /stats").await?;
        decode(&body, "GET /stats")
    }

    /// `GET /nodes?kind=&owner=&limit=&offset=` — a bare JSON array.
    ///
    /// Kept because it is part of the contract, but it is the *offset* API:
    /// FacetQL caps a deep offset at 10,000 rows. Anything that walks a whole
    /// kind must use [`FacetqlClient::query_all`], which follows the keyset
    /// cursor instead.
    pub async fn list_nodes(
        &self,
        kind: Option<&str>,
        owner: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Node>, FacetqlError> {
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(kind) = kind {
            query.push(("kind", kind.to_string()));
        }
        if let Some(owner) = owner {
            query.push(("owner", owner.to_string()));
        }
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(offset) = offset {
            query.push(("offset", offset.to_string()));
        }

        let request = self.request(reqwest::Method::GET, "/nodes").query(&query);
        let body = self.send(request, "GET /nodes").await?;
        decode(&body, "GET /nodes")
    }

    /// `POST /nodes/query` — one page of a predicate query, plus the opaque
    /// cursor for the next one.
    pub async fn query(&self, request: &QueryRequest) -> Result<QueryPage, FacetqlError> {
        let body = self.post_json("/nodes/query", request, "POST /nodes/query").await?;
        decode(&body, "POST /nodes/query")
    }

    /// Every node matching `request`, following the keyset cursor to the end.
    ///
    /// The caller's `after` and `limit` are overridden — this method owns the
    /// pagination. Using the cursor rather than an incrementing offset is not
    /// a preference: an offset walk over a kind being written to concurrently
    /// silently skips and repeats rows, and past 10,000 it is refused
    /// outright.
    pub async fn query_all(&self, request: &QueryRequest) -> Result<Vec<Node>, FacetqlError> {
        let mut request = request.clone();
        request.limit = Some(PAGE_LIMIT);
        request.after = None;

        let mut nodes = Vec::new();

        for _ in 0..MAX_PAGES {
            let page = self.query(&request).await?;
            let last_page = !page.has_more();
            nodes.extend(page.nodes);

            if last_page {
                return Ok(nodes);
            }

            request.after = Some(page.next);
        }

        Err(FacetqlError::Decode {
            context: "POST /nodes/query".to_string(),
            message: format!("cursor did not terminate within {MAX_PAGES} pages"),
        })
    }

    /// `GET /node/:address` — one node, or `None` when it is not there.
    ///
    /// `None` rather than an error for 404 because "the node is gone" is a
    /// fact a caller reconciling two instances has to act on, not a failure:
    /// the mover turns it into a delete on the destination.
    pub async fn get_node(&self, address: &str) -> Result<Option<Node>, FacetqlError> {
        let path = format!("/node/{}", percent_encoding::utf8_percent_encode(
            address,
            percent_encoding::NON_ALPHANUMERIC,
        ));

        let request = self.request(reqwest::Method::GET, &path);

        match self.send(request, "GET /node/:address").await {
            Ok(body) => decode(&body, "GET /node/:address").map(Some),
            Err(FacetqlError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// `POST /nodes/multiget` — many point reads in one request.
    ///
    /// **Absent addresses are simply missing from the reply**, which is
    /// FacetQL's own behaviour (`StorageEngine::multi_get` skips what it
    /// cannot read) and is exactly what a reconciler needs: what came back is
    /// what still exists, what did not is what has been deleted or is no
    /// longer readable by this identity. The engine bounds the batch at 1000
    /// addresses, so [`MULTIGET_LIMIT`] is the caller's chunk size.
    pub async fn multiget(&self, addresses: &[String]) -> Result<Vec<Node>, FacetqlError> {
        let body = self
            .post_json(
                "/nodes/multiget",
                &serde_json::json!({ "addresses": addresses }),
                "POST /nodes/multiget",
            )
            .await?;

        decode(&body, "POST /nodes/multiget")
    }

    /// `POST /nodes/count` — how many nodes match, as a number.
    ///
    /// The selection half of a query with none of its paging: the same rows
    /// `query` would return for the same `kind`/`owner`/`where`, counted in
    /// the engine. Walking a cursor to the end to learn one integer is a round
    /// trip per page; this is one.
    pub async fn count(&self, request: &QueryRequest) -> Result<u64, FacetqlError> {
        #[derive(serde::Serialize)]
        struct CountBody<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            kind: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            owner: Option<&'a str>,
            #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
            where_: Option<&'a serde_json::Value>,
        }

        #[derive(serde::Deserialize)]
        struct CountReply {
            count: u64,
        }

        let body = self
            .post_json(
                "/nodes/count",
                &CountBody {
                    kind: request.kind.as_deref(),
                    owner: request.owner.as_deref(),
                    where_: request.where_.as_ref(),
                },
                "POST /nodes/count",
            )
            .await?;

        let reply: CountReply = decode(&body, "POST /nodes/count")?;

        Ok(reply.count)
    }

    /// `GET /events` (SSE) — the live notification stream, opened but not
    /// read.
    ///
    /// Returns the response with its body still streaming, so the caller
    /// consumes frames as they arrive. Sent on `streaming`, a client the
    /// caller supplies without a request timeout: a subscription is the one
    /// request that is *supposed* never to finish, and the shared client's
    /// timeout would sever it on a schedule.
    ///
    /// # What this stream does and does not promise
    ///
    /// FacetQL's `/events` is an in-memory `tokio::sync::broadcast` of fixed
    /// capacity whose SSE handler *silently discards* a lagged receiver's
    /// messages (`subscribe_events` in `facetql/src/api/routes.rs` maps both
    /// "not for you" and "you fell behind" to the same `None`), and no frame
    /// carries a sequence number. So a subscriber can neither detect nor
    /// recover a gap, and there is no resume-from-position. Callers that need
    /// completeness must treat this as a *hint* and confirm against the data
    /// itself — which is what [`crate::mover`] does.
    pub async fn open_events(
        &self,
        streaming: &reqwest::Client,
    ) -> Result<reqwest::Response, FacetqlError> {
        let response = streaming
            .get(self.endpoint.url("/events"))
            .header("x-api-key", self.endpoint.token())
            .header("accept", "text/event-stream")
            .send()
            .await
            .map_err(|error| {
                FacetqlError::Transport(format!(
                    "GET /events on {}: {error}",
                    self.endpoint.base_url()
                ))
            })?;

        let status = response.status();

        if status.is_success() {
            return Ok(response);
        }

        let body = response.text().await.unwrap_or_default();

        Err(match status.as_u16() {
            401 | 403 => FacetqlError::Unauthorized {
                status: status.as_u16(),
                body,
            },
            status => FacetqlError::Status { status, body },
        })
    }

    /// `POST /node` — write one node at a client-supplied address.
    ///
    /// With `if_absent` set on the request this is create-once: an address
    /// that already exists answers 409, which arrives as
    /// [`FacetqlError::Conflict`]. Without it, it is an upsert.
    pub async fn create_node(
        &self,
        request: &CreateNodeRequest,
    ) -> Result<(), FacetqlError> {
        self.post_json("/node", request, "POST /node").await?;
        Ok(())
    }

    /// `POST /transaction` — an all-or-nothing batch.
    ///
    /// A `set_if` whose precondition did not hold comes back as 412 and
    /// therefore as [`FacetqlError::PreconditionFailed`], and **nothing in
    /// the batch was applied**.
    pub async fn transaction(&self, operations: Vec<TxOperation>) -> Result<(), FacetqlError> {
        let request = TransactionRequest { operations };
        self.post_json("/transaction", &request, "POST /transaction").await?;
        Ok(())
    }

    /// `POST /node/:address/claim` — the atomic claim primitive.
    ///
    /// `Ok(true)` won the claim, `Ok(false)` means somebody else already
    /// holds it (409). Anything else is a real failure.
    pub async fn claim(&self, address: &str) -> Result<bool, FacetqlError> {
        let path = format!("/node/{address}/claim");
        let request = self.request(reqwest::Method::POST, &path);

        match self.send(request, "POST /node/:address/claim").await {
            Ok(_) => Ok(true),
            Err(FacetqlError::Conflict(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    // ── plumbing ───────────────────────────────────────────────────────

    /// Every request goes through here, so every request carries the header
    /// and nothing carries the token anywhere else.
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, self.endpoint.url(path))
            .header("x-api-key", self.endpoint.token())
    }

    async fn post_json<T: serde::Serialize>(
        &self,
        path: &str,
        payload: &T,
        context: &str,
    ) -> Result<String, FacetqlError> {
        let body = serde_json::to_string(payload).map_err(|error| FacetqlError::Decode {
            context: context.to_string(),
            message: format!("request body could not be encoded: {error}"),
        })?;

        let request = self
            .request(reqwest::Method::POST, path)
            .header("content-type", "application/json")
            .body(body);

        self.send(request, context).await
    }

    /// Send, then map the status onto the variant that says what the caller
    /// must do about it. The body is FacetQL's own words; the token is not in
    /// it, because the token was only ever in a request header.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        context: &str,
    ) -> Result<String, FacetqlError> {
        let response = request.send().await.map_err(|error| {
            FacetqlError::Transport(format!(
                "{context} on {}: {error}",
                self.endpoint.base_url()
            ))
        })?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if status.is_success() {
            return Ok(body);
        }

        Err(match status.as_u16() {
            401 | 403 => FacetqlError::Unauthorized {
                status: status.as_u16(),
                body,
            },
            404 => FacetqlError::NotFound(body),
            409 => FacetqlError::Conflict(body),
            412 => FacetqlError::PreconditionFailed(body),
            status => FacetqlError::Status { status, body },
        })
    }
}

fn decode<T: serde::de::DeserializeOwned>(body: &str, context: &str) -> Result<T, FacetqlError> {
    serde_json::from_str(body).map_err(|error| FacetqlError::Decode {
        context: context.to_string(),
        message: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabric_core::DbmsId;

    fn client() -> FacetqlClient {
        FacetqlClient::new(
            FacetqlEndpoint::new(DbmsId::new("db-a"), "http://127.0.0.1:8892", "s3cret")
                .unwrap(),
        )
    }

    #[test]
    fn the_token_travels_in_the_header_and_never_in_the_url() {
        let client = client();
        let request = client
            .request(reqwest::Method::GET, "/stats")
            .build()
            .expect("request builds");

        assert_eq!(request.url().as_str(), "http://127.0.0.1:8892/stats");
        assert!(request.url().query().is_none());
        assert_eq!(
            request
                .headers()
                .get("x-api-key")
                .and_then(|value| value.to_str().ok()),
            Some("s3cret")
        );
    }

    #[test]
    fn debug_of_a_client_does_not_print_the_token() {
        let rendered = format!("{:?}", client());
        assert!(!rendered.contains("s3cret"), "token leaked: {rendered}");
    }

    #[test]
    fn a_query_request_encodes_to_the_contract_body() {
        let request = QueryRequest {
            kind: Some("__fabric_placement".into()),
            limit: Some(PAGE_LIMIT),
            ..QueryRequest::default()
        };
        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"kind":"__fabric_placement","limit":500}"#
        );
    }
}
