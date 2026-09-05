//! Moving the bytes: forwarding one request, broadcasting one request, and
//! merging many event streams into one.
//!
//! Nothing in this module decides anything. It is the transport half of the
//! front door, and its single obligation is **fidelity** — the request that
//! reaches a backend is the request the client sent, and the response the
//! client reads is the response the backend gave. `fqStore` must not be able
//! to tell the front door from a FacetQL by reading either one.
//!
//! Concretely that means the method, the raw request target (path *and* query,
//! percent-encoding untouched), the body bytes and every end-to-end header are
//! passed through unchanged, and the status, body bytes and headers come back
//! unchanged. Only hop-by-hop headers are dropped, because they describe the
//! single connection they arrived on and re-sending them on a different
//! connection is a protocol error.
//!
//! # The credential
//!
//! `x-api-key` is an ordinary end-to-end header here and is forwarded like any
//! other. The front door holds no token of its own on the data path and never
//! reads, rewrites, caches or logs the caller's — it cannot become a way to
//! act as an identity FacetQL did not authenticate, because it never has one.

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};

/// Headers that describe one hop and must not be copied onto the next one.
/// `host` is dropped separately: the outbound one belongs to the backend.
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// What a backend answered.
#[derive(Debug, Clone)]
pub struct Upstream {
    pub node: String,
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Upstream {
    /// Relay it to the client exactly as it arrived.
    pub fn into_response(self) -> Response {
        let mut response = Response::builder().status(self.status);

        if let Some(headers) = response.headers_mut() {
            copy_headers(&self.headers, headers);
        }

        response
            .body(Body::from(self.body))
            .expect("a status and copied headers always build a response")
    }

    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }
}

/// A backend that could not be reached at all — distinct from one that
/// answered with a refusal, which is a perfectly good response to relay.
#[derive(Debug, Clone)]
pub struct Unreachable {
    pub node: String,
    pub message: String,
}

/// Send one request to one backend and read its whole answer.
pub async fn forward(
    http: &reqwest::Client,
    node: &str,
    base_url: &str,
    method: &Method,
    target: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Upstream, Unreachable> {
    let response = send(http, node, base_url, method, target, headers, body).await?;

    let status = StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(StatusCode::BAD_GATEWAY);

    let mut out = HeaderMap::new();
    for (name, value) in response.headers() {
        /*
         * `content-length` is deliberately not copied: the body is re-framed
         * onto a different connection and hyper computes the length of what it
         * actually sends. Copying a stale one produces a response the client
         * cannot parse.
         */
        if is_hop_by_hop(name.as_str()) || name.as_str() == "content-length" {
            continue;
        }

        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            out.append(name, value);
        }
    }

    let body = response.bytes().await.map_err(|error| Unreachable {
        node: node.to_string(),
        message: format!("the response body from '{node}' could not be read: {error}"),
    })?;

    Ok(Upstream {
        node: node.to_string(),
        status,
        headers: out,
        body,
    })
}

/// Open a streaming request — the SSE path, which must not buffer.
pub async fn open_stream(
    http: &reqwest::Client,
    node: &str,
    base_url: &str,
    method: &Method,
    target: &str,
    headers: &HeaderMap,
) -> Result<reqwest::Response, Unreachable> {
    let response = send(http, node, base_url, method, target, headers, Bytes::new()).await?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();

        return Err(Unreachable {
            node: node.to_string(),
            message: format!("'{node}' refused the subscription ({status}): {body}"),
        });
    }

    Ok(response)
}

async fn send(
    http: &reqwest::Client,
    node: &str,
    base_url: &str,
    method: &Method,
    target: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<reqwest::Response, Unreachable> {
    let method = reqwest::Method::from_bytes(method.as_str().as_bytes()).map_err(|error| {
        Unreachable {
            node: node.to_string(),
            message: format!("unsupported method: {error}"),
        }
    })?;

    let mut request = http.request(method, format!("{base_url}{target}"));

    for (name, value) in headers {
        if is_hop_by_hop(name.as_str()) || name.as_str() == "host" {
            continue;
        }

        request = request.header(name.as_str(), value.as_bytes());
    }

    if !body.is_empty() {
        request = request.body(body);
    }

    request.send().await.map_err(|error| Unreachable {
        node: node.to_string(),
        message: format!("'{node}' at {base_url} did not answer: {error}"),
    })
}

/// Whether two backends gave the same answer.
///
/// Byte equality would be too strict for a JSON array whose order is not part
/// of its meaning — two instances holding the same declared indexes must not
/// be reported as disagreeing because they listed them differently — so arrays
/// are compared as multisets and everything else structurally. A body that is
/// not JSON falls back to byte equality, which is the only comparison
/// available for it.
pub fn answers_agree(left: &Upstream, right: &Upstream) -> bool {
    if left.status != right.status {
        return false;
    }

    match (
        serde_json::from_slice::<serde_json::Value>(&left.body),
        serde_json::from_slice::<serde_json::Value>(&right.body),
    ) {
        (Ok(serde_json::Value::Array(left)), Ok(serde_json::Value::Array(right))) => {
            let mut left: Vec<String> = left.iter().map(|item| item.to_string()).collect();
            let mut right: Vec<String> = right.iter().map(|item| item.to_string()).collect();

            left.sort();
            right.sort();

            left == right
        }

        (Ok(left), Ok(right)) => left == right,

        _ => left.body == right.body,
    }
}

/// Merge several upstream event streams into one SSE body.
///
/// FacetQL's event bus is per instance, so "the fleet's events" is the union
/// of every instance's stream and nothing less. Two properties are load
/// bearing:
///
/// * **frames are never interleaved mid-frame.** Each upstream is re-framed on
///   the blank line that terminates an SSE event before anything is written to
///   the client, so two backends emitting at once cannot produce a spliced
///   event that parses as neither.
/// * **a stream that stops does not go quiet.** If an upstream ends or errors,
///   the merged body ends with an error rather than continuing to look
///   complete while silently missing one backend's events. A truncated stream
///   is something a client reconnects from; a silently partial one is not.
pub fn merge_events(streams: Vec<(String, reqwest::Response)>) -> Body {
    let framed: Vec<_> = streams
        .into_iter()
        .map(|(node, response)| Box::pin(frames(response.bytes_stream(), node)))
        .collect();

    Body::from_stream(futures_util::stream::select_all(framed))
}

/// Re-frame one upstream byte stream into whole SSE events.
fn frames<S>(stream: S, node: String) -> impl Stream<Item = Result<Bytes, String>> + Send
where
    S: Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
{
    futures_util::stream::unfold(
        (Box::pin(stream), BytesMut::new(), node, false),
        |(mut stream, mut buffer, node, ended)| async move {
            if ended {
                return None;
            }

            loop {
                if let Some(end) = frame_end(&buffer) {
                    let frame = buffer.split_to(end).freeze();

                    return Some((Ok(frame), (stream, buffer, node, false)));
                }

                match stream.next().await {
                    Some(Ok(chunk)) => buffer.extend_from_slice(&chunk),

                    Some(Err(error)) => {
                        let message = format!(
                            "fabric front door: the event stream from '{node}' failed \
                             ({error}); this merged stream is no longer complete"
                        );

                        return Some((Err(message), (stream, buffer, node, true)));
                    }

                    None => {
                        let message = format!(
                            "fabric front door: the event stream from '{node}' ended; \
                             this merged stream is no longer complete"
                        );

                        return Some((Err(message), (stream, buffer, node, true)));
                    }
                }
            }
        },
    )
}

/// Index just past the blank line that terminates the first SSE event in the
/// buffer, if there is one.
fn frame_end(buffer: &[u8]) -> Option<usize> {
    let lf = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| index + 2);

    let crlf = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4);

    match (lf, crlf) {
        (Some(lf), Some(crlf)) => Some(lf.min(crlf)),
        (Some(lf), None) => Some(lf),
        (None, Some(crlf)) => Some(crlf),
        (None, None) => None,
    }
}

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|header| header.eq_ignore_ascii_case(name))
}

fn copy_headers(from: &HeaderMap, into: &mut HeaderMap) {
    for (name, value) in from {
        into.append(name.clone(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(node: &str, status: u16, body: &str) -> Upstream {
        Upstream {
            node: node.to_string(),
            status: StatusCode::from_u16(status).unwrap(),
            headers: HeaderMap::new(),
            body: Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    #[test]
    fn a_frame_is_only_emitted_once_its_blank_line_has_arrived() {
        assert_eq!(frame_end(b"event: x\ndata: 1\n"), None);
        assert_eq!(frame_end(b"event: x\ndata: 1\n\n"), Some(18));
        assert_eq!(frame_end(b"data: 1\r\n\r\ndata: 2"), Some(11));
    }

    /// Two instances holding the same declared indexes in a different order
    /// agree; one that is missing an index does not.
    #[test]
    fn agreement_is_about_content_not_byte_order() {
        assert!(answers_agree(
            &upstream("a", 200, r#"[{"name":"x"},{"name":"y"}]"#),
            &upstream("b", 200, r#"[{"name":"y"},{"name":"x"}]"#),
        ));

        assert!(!answers_agree(
            &upstream("a", 200, r#"[{"name":"x"},{"name":"y"}]"#),
            &upstream("b", 200, r#"[{"name":"x"}]"#),
        ));

        assert!(!answers_agree(
            &upstream("a", 200, "FacetQL Online"),
            &upstream("b", 503, "FacetQL Online"),
        ));

        // Not JSON: byte equality is the only available comparison.
        assert!(answers_agree(
            &upstream("a", 200, "FacetQL Online"),
            &upstream("b", 200, "FacetQL Online"),
        ));
    }

    /// An upstream that ends must end the merged stream with an error. The
    /// alternative — dropping that upstream and carrying on — is a stream that
    /// looks like the fleet's and silently is not, for as long as the client
    /// keeps it open.
    #[tokio::test]
    async fn a_stream_that_ends_ends_the_merge_rather_than_going_quiet() {
        let upstream = futures_util::stream::iter(vec![
            Ok(Bytes::from_static(b"event: a\ndata: 1\n")),
            Ok(Bytes::from_static(b"\nevent: b\ndata: 2\n\n")),
        ]);

        let collected: Vec<Result<Bytes, String>> =
            frames(upstream, "db-a".to_string()).collect().await;

        // Two whole frames, re-framed across the chunk boundary they arrived
        // split on, then the end-of-stream error.
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].as_ref().unwrap(), "event: a\ndata: 1\n\n");
        assert_eq!(collected[1].as_ref().unwrap(), "event: b\ndata: 2\n\n");

        let ending = collected[2].as_ref().unwrap_err();
        assert!(ending.contains("db-a"), "{ending}");
        assert!(ending.contains("no longer complete"), "{ending}");
    }

    #[test]
    fn hop_by_hop_headers_are_recognised_case_insensitively() {
        assert!(is_hop_by_hop("Transfer-Encoding"));
        assert!(is_hop_by_hop("connection"));
        assert!(!is_hop_by_hop("x-api-key"));
        assert!(!is_hop_by_hop("content-type"));
    }
}
