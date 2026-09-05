//! Deciding what a FacetQL request *is*, before anything is forwarded.
//!
//! This module is a pure function of the request — method, path, query string
//! and body bytes — plus the declared [`Keyspace`]. It performs no I/O, holds
//! no state and knows nothing about which backends exist. That is deliberate:
//! the classification is the part that has to be right, so it is the part that
//! is exhaustively testable without a server, a socket or a fleet.
//!
//! Every route in `facetql/src/api/routes.rs` is accounted for below. A path
//! that is not one of them is a 404 — the same answer FacetQL gives — because
//! a proxy that forwarded unknown paths would be guessing at a contract it
//! does not have.
//!
//! # The four things a request can be
//!
//! * **[`Plan::Colocated`]** — every key it touches must live on one backend,
//!   and then it is forwarded there byte-for-byte. A single-key read is the
//!   degenerate case; a `POST /transaction` over eleven addresses is the
//!   interesting one.
//! * **[`Plan::Broadcast`]** — it is a fleet-wide declaration or probe
//!   (`GET /`, `/admin/indexes`, `/admin/references`) and every backend must
//!   answer. Reads additionally require the answers to *agree*, because a
//!   fleet whose instances disagree about their declared indexes is a fact the
//!   caller needs rather than one to pick a winner from.
//! * **[`Plan::Subscribe`]** — `GET /events`, which is a fan-*in*: FacetQL's
//!   event bus is per instance, so the only faithful presentation of "the
//!   fleet's events" is every backend's stream merged into one.
//! * **[`Plan::Refuse`]** — the request has no correct answer through a
//!   router, and saying so is better than any of the ways to fake one.
//!
//! # Why some things are refused
//!
//! The refusals are not gaps in this file; they are the honest edge of what a
//! proxy can do to FacetQL's contract, and each one is refused *whole* rather
//! than served partially:
//!
//! * A **transaction spanning two backends** cannot be made atomic by a
//!   proxy. Splitting the batch would apply half of it and report success —
//!   the single worst outcome available here, because
//!   `POST /transaction`'s entire value is that a refused precondition means
//!   *nothing* was applied. Refused.
//! * A **`/nodes/multiget` spanning two backends** would be served by two
//!   requests whose union is the answer — but FacetQL's contract says an
//!   address that is absent or unreadable is simply missing from the reply.
//!   So a backend that failed would be indistinguishable from rows that do not
//!   exist, and the caller would read a partial answer as a complete one.
//!   Refused. (This is the same invariant `RoutingTable::resolve_range`
//!   already enforces one layer down: a scan fails whole or not at all.)
//! * A **kind-less listing or query** (`GET /nodes` with no `kind`,
//!   `POST /nodes/query` with no `kind`) spans the namespace; and its keyset
//!   cursor is opaque and backend-local, so pages from two backends cannot be
//!   merged into one resumable sequence at all. Refused unless the whole
//!   keyspace is one place ([`Keyspace::spanning_key`]).
//! * **`GET /stats`** counts one engine's nodes, edges, users and
//!   process-lifetime operations. There is no fleet-wide sum that is not a
//!   lie (`storage.page_size` cannot be added; `user_count` counts the same
//!   identity once per instance), and Fabric's own poller reads `/stats` from
//!   each instance directly and differences it per instance. Refused unless
//!   the keyspace is one place.
//! * **`POST /admin/users`** mints a token inside the instance that serves it
//!   and returns it once. Broadcasting it would create one identity per
//!   backend with a different secret each and hand the caller one that works
//!   on exactly one of them. There is no FacetQL primitive for creating a user
//!   with a caller-supplied token or hash, so this cannot be made correct from
//!   outside. Refused unless the keyspace is one place — and reported upward
//!   as a missing FacetQL primitive rather than worked around here.

use axum::http::Method;
use fabric_routing::{ReadPreference, RouteIntent, RoutingKey};

use crate::frontdoor::keyspace::{Keyspace, KeyspaceMiss};

/// One key a request touches, and what it wants to do there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIntent {
    pub key: RoutingKey,
    pub intent: RouteIntent,

    /// The part of the request this key came from, for the message a refusal
    /// carries: `operations[3].set_if 'p:1'` is worth far more to whoever has
    /// to fix it than "a key".
    pub origin: String,
}

/// Whether a broadcast's answers have to match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agreement {
    /// A read: every backend must answer identically, or the fleet disagrees
    /// with itself and the caller is told so.
    Required,

    /// A declaration: every backend must accept it, but the responses are
    /// not required to be byte-equal (a 200 and a 201 both mean "declared").
    NotRequired,
}

/// What the front door is going to do with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Forward verbatim to the single backend every one of these keys
    /// resolves to. An empty key list is impossible by construction.
    Colocated { keys: Vec<KeyIntent> },

    /// Send verbatim to every backend the keyspace spans.
    Broadcast {
        agreement: Agreement,
        what: &'static str,
    },

    /// Merge every backend's SSE stream into one.
    Subscribe,

    Refuse(Refusal),
}

/// Why the front door will not serve a request.
///
/// Deliberately one enum shared by the planning stage and the dispatch stage:
/// there is one place ([`Refusal::status`]) where a refusal becomes an HTTP
/// status, so a new refusal cannot be added without deciding what a client
/// sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Not a path FacetQL serves. Answered exactly as FacetQL answers it.
    UnknownRoute { method: String, path: String },

    /// The path is not valid UTF-8 once percent-decoded.
    MalformedPath { reason: String },

    /// The body is not the JSON this endpoint's contract says it is. FacetQL
    /// would answer 400 (an axum `Json` rejection) and so does this.
    MalformedBody { context: String, message: String },

    /// The keyspace cannot place this request.
    Keyspace(KeyspaceMiss),

    /// The request is well formed and every part of it is routable — to more
    /// than one backend. See the module docs for why each of these is refused
    /// whole rather than split.
    SpansBackends {
        what: String,
        nodes: Vec<String>,
    },

    /// A request whose semantics a proxy cannot reproduce over a split fleet.
    NotProxyable {
        what: &'static str,
        reason: &'static str,
    },

    /// No credential at all. Refused at the door, in FacetQL's own words, so
    /// the front door never resolves a route for an unauthenticated caller.
    MissingCredential,
}

impl Refusal {
    /// The status a client sees. The whole mapping, in one place.
    pub fn status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;

        match self {
            Self::UnknownRoute { .. } => StatusCode::NOT_FOUND,

            Self::MalformedPath { .. } | Self::MalformedBody { .. } => {
                StatusCode::BAD_REQUEST
            }

            Self::MissingCredential => StatusCode::UNAUTHORIZED,

            /*
             * 421 Misdirected Request: "the request was directed at a server
             * that is unable to produce a response". That is precisely true of
             * all three — the front door is not the server that can answer
             * this, and no amount of retrying will change that. It is
             * deliberately not 400 (the request is not malformed), not 500
             * (nothing failed), and not 503 (retrying is pointless): each of
             * those would send the caller looking in the wrong place.
             */
            Self::Keyspace(_) | Self::SpansBackends { .. } | Self::NotProxyable { .. } => {
                StatusCode::MISDIRECTED_REQUEST
            }
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownRoute { method, path } => {
                write!(f, "no route for {method} {path}")
            }

            Self::MalformedPath { reason } => write!(f, "malformed path: {reason}"),

            Self::MalformedBody { context, message } => {
                write!(f, "{context}: could not decode the request body: {message}")
            }

            Self::Keyspace(miss) => write!(f, "fabric front door: {miss}"),

            Self::SpansBackends { what, nodes } => write!(
                f,
                "fabric front door: {what} spans {} backends ({}), and this \
                 request cannot be split without losing the guarantee it was \
                 made for; nothing was forwarded",
                nodes.len(),
                nodes.join(", ")
            ),

            Self::NotProxyable { what, reason } => write!(
                f,
                "fabric front door: {what} cannot be served across a split \
                 fleet: {reason}; address one FacetQL instance directly"
            ),

            /*
             * Byte-identical to `facetql::auth::auth_middleware`, because a
             * client must not be able to tell the front door from the database
             * by reading an error string.
             */
            Self::MissingCredential => {
                write!(f, "missing x-api-key header (or ?key= for SSE)")
            }
        }
    }
}

/// Classify one request.
///
/// `path` and `query` are the raw request target; `body` is the raw request
/// body, which is parsed here only to *read* the fields that decide routing
/// and is forwarded unchanged either way.
pub fn plan(
    method: &Method,
    path: &str,
    query: Option<&str>,
    body: &[u8],
    keyspace: &Keyspace,
    read_preference: &ReadPreference,
) -> Plan {
    let segments = match decode_segments(path) {
        Ok(segments) => segments,
        Err(reason) => return Plan::Refuse(Refusal::MalformedPath { reason }),
    };

    let read = || RouteIntent::Read(read_preference.clone());
    let parts: Vec<&str> = segments.iter().map(String::as_str).collect();

    match (method, parts.as_slice()) {
        // ── liveness ────────────────────────────────────────────────────
        //
        // `GET /` is FacetQL's only unauthenticated route, and through the
        // front door it means "can the fleet serve?", not "is one machine up".
        // So every backend is asked and one that is down fails the probe.
        (&Method::GET, []) => Plan::Broadcast {
            agreement: Agreement::Required,
            what: "GET /",
        },

        // ── one node, addressed ─────────────────────────────────────────
        (&Method::POST, ["node"]) => {
            // The only request that names both halves of a keyspace rule, and
            // therefore the only one that can contradict it. `create_node`
            // requires both fields, so a body missing either is a 400 here
            // exactly as it would be at FacetQL.
            let body = match json(body, "POST /node") {
                Ok(body) => body,
                Err(refusal) => return Plan::Refuse(refusal),
            };

            colocated(
                keyspace,
                "POST /node",
                RouteIntent::Write,
                field(&body, "kind"),
                field(&body, "address"),
            )
        }

        (&Method::GET, ["node", address])
        | (&Method::GET, ["node", address, "history"])
        | (&Method::GET, ["node", address, "owned"])
        | (&Method::GET, ["node", address, "edges", "out"])
        | (&Method::GET, ["node", address, "edges", "in"]) => {
            colocated(keyspace, path, read(), None, Some(address.to_string()))
        }

        (&Method::PUT, ["node", address])
        | (&Method::DELETE, ["node", address])
        | (&Method::POST, ["node", address, "claim"]) => {
            colocated(
                keyspace,
                path,
                RouteIntent::Write,
                None,
                Some(address.to_string()),
            )
        }

        /*
         * A sequence's identity is its name, which is the address of the node
         * the engine keeps it in (`StorageEngine::sequence_next`). It is a
         * write: taking a block of ids mutates that node.
         */
        (&Method::POST, ["sequence", name, "next"]) => colocated(
            keyspace,
            path,
            RouteIntent::Write,
            None,
            Some(name.to_string()),
        ),

        // ── many nodes ──────────────────────────────────────────────────
        (&Method::GET, ["nodes"]) => match query_param(query, "kind") {
            Some(kind) => colocated(keyspace, "GET /nodes", read(), Some(kind), None),

            None => spanning(
                keyspace,
                read(),
                "GET /nodes without a kind",
                "it lists every kind, and pages from two backends cannot be \
                 merged into one offset sequence",
            ),
        },

        (&Method::POST, ["nodes", "query"])
        | (&Method::POST, ["nodes", "count"])
        | (&Method::POST, ["nodes", "count_by"]) => {
            let context = path.to_string();

            let parsed = match json(body, &context) {
                Ok(parsed) => parsed,
                Err(refusal) => return Plan::Refuse(refusal),
            };

            match field(&parsed, "kind") {
                Some(kind) => colocated(keyspace, &context, read(), Some(kind), None),

                None => spanning(
                    keyspace,
                    read(),
                    "a kind-less predicate query",
                    "it selects across every kind, and the keyset cursor it \
                     pages with is opaque and backend-local, so two backends' \
                     pages cannot be merged into one resumable sequence",
                ),
            }
        }

        (&Method::POST, ["nodes", "multiget"]) => {
            let parsed = match json(body, "POST /nodes/multiget") {
                Ok(parsed) => parsed,
                Err(refusal) => return Plan::Refuse(refusal),
            };

            let Some(addresses) = parsed.get("addresses").and_then(|v| v.as_array()) else {
                return Plan::Refuse(Refusal::MalformedBody {
                    context: "POST /nodes/multiget".to_string(),
                    message: "no `addresses` array".to_string(),
                });
            };

            let mut keys = Vec::with_capacity(addresses.len());

            for (index, address) in addresses.iter().enumerate() {
                let Some(address) = address.as_str() else {
                    return Plan::Refuse(Refusal::MalformedBody {
                        context: "POST /nodes/multiget".to_string(),
                        message: format!("addresses[{index}] is not a string"),
                    });
                };

                match keyspace.resolve(None, Some(address)) {
                    Ok(key) => keys.push(KeyIntent {
                        key,
                        intent: read(),
                        origin: format!("addresses[{index}] '{address}'"),
                    }),
                    Err(miss) => return Plan::Refuse(Refusal::Keyspace(miss)),
                }
            }

            /*
             * An empty multiget touches nothing, so nothing constrains where
             * it goes — but it still has to go somewhere to get FacetQL's own
             * answer for it. The whole namespace is as good as any part of it.
             */
            if keys.is_empty() {
                return spanning(
                    keyspace,
                    read(),
                    "an empty POST /nodes/multiget",
                    "it names no address, so nothing decides which backend \
                     should answer it",
                );
            }

            Plan::Colocated { keys }
        }

        // ── edges ───────────────────────────────────────────────────────
        //
        // An edge lives in exactly one engine, next to its endpoints. If the
        // keyspace puts `from` and `to` on different backends there is no
        // instance that can hold it, and FacetQL has no cross-instance edge.
        (&Method::POST, ["edge"]) | (&Method::DELETE, ["edge"]) => {
            let context = format!("{method} /edge");

            let parsed = match json(body, &context) {
                Ok(parsed) => parsed,
                Err(refusal) => return Plan::Refuse(refusal),
            };

            let mut keys = Vec::with_capacity(2);

            for end in ["from", "to"] {
                let Some(address) = field(&parsed, end) else {
                    return Plan::Refuse(Refusal::MalformedBody {
                        context: context.clone(),
                        message: format!("no `{end}` address"),
                    });
                };

                match keyspace.resolve(None, Some(&address)) {
                    Ok(key) => keys.push(KeyIntent {
                        key,
                        intent: RouteIntent::Write,
                        origin: format!("{end} '{address}'"),
                    }),
                    Err(miss) => return Plan::Refuse(Refusal::Keyspace(miss)),
                }
            }

            Plan::Colocated { keys }
        }

        // ── the batch ───────────────────────────────────────────────────
        (&Method::POST, ["transaction"]) => transaction_plan(body, keyspace),

        // ── events ──────────────────────────────────────────────────────
        //
        // Fan-in, and its partner `POST /publish` is deliberately NOT a
        // fan-out: a publish sent to every backend would reach a subscriber
        // once per backend, because the subscriber is merging all of them.
        // Routing the publish to exactly one backend by its channel makes
        // delivery exactly-once through the merged stream, and reuses the same
        // declared keyspace rather than inventing an "event home".
        (&Method::GET, ["events"]) => Plan::Subscribe,

        (&Method::POST, ["publish"]) => {
            let parsed = match json(body, "POST /publish") {
                Ok(parsed) => parsed,
                Err(refusal) => return Plan::Refuse(refusal),
            };

            let Some(channel) = field(&parsed, "channel") else {
                return Plan::Refuse(Refusal::MalformedBody {
                    context: "POST /publish".to_string(),
                    message: "no `channel`".to_string(),
                });
            };

            colocated(
                keyspace,
                "POST /publish",
                RouteIntent::Write,
                Some(channel),
                None,
            )
        }

        // ── per-instance state ──────────────────────────────────────────
        (&Method::GET, ["stats"]) => spanning(
            keyspace,
            read(),
            "GET /stats",
            "its counts and its process-lifetime operation counters describe \
             one engine, and no fleet-wide sum of them is true — Fabric's own \
             telemetry poller reads /stats from each instance directly",
        ),

        (&Method::POST, ["admin", "users"])
        | (&Method::GET, ["admin", "users"])
        | (&Method::DELETE, ["admin", "users", _]) => spanning(
            keyspace,
            RouteIntent::Write,
            "the /admin/users endpoints",
            "creating a user mints a secret inside the instance that serves \
             it and returns it once, so broadcasting would create one identity \
             per backend with a different token each; FacetQL exposes no way \
             to create a user from a caller-supplied token or hash",
        ),

        // ── fleet-wide declarations ─────────────────────────────────────
        //
        // Indexes and references are idempotent declarations — re-declaring an
        // identical one succeeds, which is exactly what makes fct's boot-time
        // reconcile safe — so every backend must carry the same set, and the
        // front door declares to all of them. A listing must agree across the
        // fleet: an instance missing an index is a real fact, not a tie to
        // break by picking a winner.
        (&Method::GET, ["admin", "indexes"]) | (&Method::GET, ["admin", "references"]) => {
            Plan::Broadcast {
                agreement: Agreement::Required,
                what: "a fleet-wide declaration listing",
            }
        }

        (&Method::POST, ["admin", "indexes"])
        | (&Method::POST, ["admin", "references"])
        | (&Method::DELETE, ["admin", "indexes", _])
        | (&Method::DELETE, ["admin", "references", _]) => Plan::Broadcast {
            agreement: Agreement::NotRequired,
            what: "a fleet-wide declaration",
        },

        _ => Plan::Refuse(Refusal::UnknownRoute {
            method: method.to_string(),
            path: path.to_string(),
        }),
    }
}

/// Every op of a `POST /transaction`, as the keys it touches.
///
/// The batch is all-or-nothing at FacetQL, and the only way to keep that
/// through a router is to require that all of it lands on one engine. Every op
/// is turned into keys here so that "does this batch fit on one backend?" is
/// answered before a single byte is forwarded — a batch that does not fit is
/// refused having touched nothing.
fn transaction_plan(body: &[u8], keyspace: &Keyspace) -> Plan {
    let parsed = match json(body, "POST /transaction") {
        Ok(parsed) => parsed,
        Err(refusal) => return Plan::Refuse(refusal),
    };

    let Some(operations) = parsed.get("operations").and_then(|v| v.as_array()) else {
        return Plan::Refuse(Refusal::MalformedBody {
            context: "POST /transaction".to_string(),
            message: "no `operations` array".to_string(),
        });
    };

    let mut keys = Vec::new();

    for (index, operation) in operations.iter().enumerate() {
        let Some(op_type) = field(operation, "type") else {
            return Plan::Refuse(Refusal::MalformedBody {
                context: "POST /transaction".to_string(),
                message: format!("operations[{index}] has no `type`"),
            });
        };

        // (kind, address) pairs this op touches. `insert_node` names both, so
        // the keyspace's own consistency check applies to it exactly as it
        // does to `POST /node`.
        let touched: Vec<(Option<String>, Option<String>)> = match op_type.as_str() {
            "insert_node" => vec![(field(operation, "kind"), field(operation, "address"))],
            "delete_node" | "set_if" => vec![(None, field(operation, "address"))],
            "clear_kind" | "delete_where" => vec![(field(operation, "kind"), None)],
            "insert_edge" | "delete_edge" => vec![
                (None, field(operation, "from")),
                (None, field(operation, "to")),
            ],

            other => {
                return Plan::Refuse(Refusal::MalformedBody {
                    context: "POST /transaction".to_string(),
                    message: format!("operations[{index}] has unknown type '{other}'"),
                });
            }
        };

        for (kind, address) in touched {
            if kind.is_none() && address.is_none() {
                return Plan::Refuse(Refusal::MalformedBody {
                    context: "POST /transaction".to_string(),
                    message: format!(
                        "operations[{index}] ('{op_type}') names neither a kind \
                         nor an address, so there is nowhere to route it"
                    ),
                });
            }

            let origin = format!(
                "operations[{index}] ('{op_type}') {}",
                match (&kind, &address) {
                    (Some(kind), Some(address)) => format!("kind '{kind}' at '{address}'"),
                    (Some(kind), None) => format!("kind '{kind}'"),
                    (None, Some(address)) => format!("'{address}'"),
                    (None, None) => unreachable!("refused just above"),
                }
            );

            match keyspace.resolve(kind.as_deref(), address.as_deref()) {
                Ok(key) => keys.push(KeyIntent {
                    key,
                    intent: RouteIntent::Write,
                    origin,
                }),
                Err(miss) => return Plan::Refuse(Refusal::Keyspace(miss)),
            }
        }
    }

    /*
     * An empty batch is a legal no-op FacetQL answers 200 to. It touches
     * nothing, so nothing places it; send it wherever the whole namespace
     * lives so the caller gets the engine's own answer.
     */
    if keys.is_empty() {
        return spanning(
            keyspace,
            RouteIntent::Write,
            "an empty POST /transaction",
            "it touches no address and no kind, so nothing decides which \
             backend should answer it",
        );
    }

    Plan::Colocated { keys }
}

// ── plumbing ────────────────────────────────────────────────────────────

/// One key, from a kind and/or an address.
fn colocated(
    keyspace: &Keyspace,
    origin: &str,
    intent: RouteIntent,
    kind: Option<String>,
    address: Option<String>,
) -> Plan {
    match keyspace.resolve(kind.as_deref(), address.as_deref()) {
        Ok(key) => Plan::Colocated {
            keys: vec![KeyIntent {
                key,
                intent,
                origin: origin.to_string(),
            }],
        },

        Err(miss) => Plan::Refuse(Refusal::Keyspace(miss)),
    }
}

/// A request that is about the whole namespace: routable only when the
/// namespace is one place, and refused with the reason when it is not.
fn spanning(
    keyspace: &Keyspace,
    intent: RouteIntent,
    what: &'static str,
    reason: &'static str,
) -> Plan {
    match keyspace.spanning_key() {
        Some(key) => Plan::Colocated {
            keys: vec![KeyIntent {
                key,
                intent,
                origin: what.to_string(),
            }],
        },

        None => Plan::Refuse(Refusal::NotProxyable { what, reason }),
    }
}

/// The path's segments, percent-decoded, with empty ones dropped so `/nodes/`
/// and `/nodes` classify the same way.
fn decode_segments(path: &str) -> Result<Vec<String>, String> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            percent_encoding::percent_decode_str(segment)
                .decode_utf8()
                .map(|decoded| decoded.into_owned())
                .map_err(|error| format!("'{segment}' is not valid UTF-8: {error}"))
        })
        .collect()
}

/// One query parameter, form-decoded (`+` is a space, as Go's
/// `url.Values.Encode` emits).
fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;

        (key == name).then(|| {
            percent_encoding::percent_decode_str(&value.replace('+', " "))
                .decode_utf8_lossy()
                .into_owned()
        })
    })
}

fn json(body: &[u8], context: &str) -> Result<serde_json::Value, Refusal> {
    serde_json::from_slice(body).map_err(|error| Refusal::MalformedBody {
        context: context.to_string(),
        message: error.to_string(),
    })
}

/// A string field, absent when it is missing, null or not a string.
fn field(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontdoor::keyspace::KeyspaceRule;
    use fabric_core::Coordinate;

    fn key(shard_id: u64) -> RoutingKey {
        RoutingKey::new(shard_id, Coordinate::new(0, 0)).unwrap()
    }

    /// Two kinds, two shards, and no fallback — a genuinely split fleet.
    fn split() -> Keyspace {
        Keyspace::new()
            .with_rule(KeyspaceRule::new("Post", "Post:", key(1)).unwrap())
            .unwrap()
            .with_rule(KeyspaceRule::new("User", "User:", key(2)).unwrap())
            .unwrap()
    }

    fn planned(method: Method, path: &str, query: Option<&str>, body: &str) -> Plan {
        plan(
            &method,
            path,
            query,
            body.as_bytes(),
            &split(),
            &ReadPreference::Primary,
        )
    }

    fn keys(plan: &Plan) -> Vec<RoutingKey> {
        match plan {
            Plan::Colocated { keys } => keys.iter().map(|key| key.key).collect(),
            other => panic!("expected a colocated plan, got {other:?}"),
        }
    }

    #[test]
    fn a_point_read_routes_by_its_address() {
        let plan = planned(Method::GET, "/node/Post:17", None, "");
        assert_eq!(keys(&plan), vec![key(1)]);

        match plan {
            Plan::Colocated { keys } => {
                assert_eq!(keys[0].intent, RouteIntent::Read(ReadPreference::Primary))
            }
            other => panic!("{other:?}"),
        }
    }

    /// The address arrives percent-encoded from a Go client
    /// (`url.PathEscape`), and `Post%3A17` is the same node as `Post:17`.
    #[test]
    fn a_percent_encoded_address_routes_the_same_as_a_bare_one() {
        assert_eq!(
            keys(&planned(Method::GET, "/node/Post%3A17", None, "")),
            vec![key(1)]
        );
    }

    #[test]
    fn a_write_is_planned_as_a_write() {
        for (method, path) in [
            (Method::PUT, "/node/Post:1"),
            (Method::DELETE, "/node/Post:1"),
            (Method::POST, "/node/Post:1/claim"),
        ] {
            match planned(method, path, None, "") {
                Plan::Colocated { keys } => assert_eq!(keys[0].intent, RouteIntent::Write),
                other => panic!("{path}: {other:?}"),
            }
        }
    }

    #[test]
    fn a_kind_scoped_query_routes_by_its_kind() {
        assert_eq!(
            keys(&planned(
                Method::POST,
                "/nodes/query",
                None,
                r#"{"kind":"User","limit":500}"#
            )),
            vec![key(2)]
        );

        assert_eq!(
            keys(&planned(Method::GET, "/nodes", Some("kind=Post&limit=50"), "")),
            vec![key(1)]
        );
    }

    /// The invariant the whole front door exists to preserve: a batch that
    /// does not fit on one engine is refused, never split.
    #[test]
    fn a_transaction_over_two_kinds_is_refused_not_split() {
        let body = r#"{"operations":[
            {"type":"insert_node","address":"Post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,"data":"{}","public":false},
            {"type":"delete_node","address":"User:9"}
        ]}"#;

        // Planning yields both keys; it is dispatch that discovers they are
        // two backends. What matters here is that both were collected.
        assert_eq!(
            keys(&planned(Method::POST, "/transaction", None, body)),
            vec![key(1), key(2)]
        );
    }

    #[test]
    fn every_transaction_op_shape_contributes_its_keys() {
        let body = r#"{"operations":[
            {"type":"insert_node","address":"Post:1","kind":"Post","x":0,"y":0,"z":0,"q":0,"data":"{}","public":false},
            {"type":"delete_node","address":"Post:2"},
            {"type":"clear_kind","kind":"Post"},
            {"type":"delete_where","kind":"Post"},
            {"type":"set_if","address":"Post:3","field":"v","expect_eq":1,"set":{}},
            {"type":"insert_edge","from":"Post:4","to":"Post:5","kind":"rel"},
            {"type":"delete_edge","from":"Post:6","to":"Post:7","kind":"rel"}
        ]}"#;

        // 6 single-address/kind ops + 2 two-ended edge ops = 9 keys.
        assert_eq!(keys(&planned(Method::POST, "/transaction", None, body)).len(), 9);
    }

    #[test]
    fn an_edge_names_both_of_its_ends() {
        let plan = planned(
            Method::POST,
            "/edge",
            None,
            r#"{"from":"Post:1","to":"User:2","kind":"wrote"}"#,
        );

        assert_eq!(keys(&plan), vec![key(1), key(2)]);
    }

    #[test]
    fn a_multiget_names_every_address_it_asks_about() {
        let plan = planned(
            Method::POST,
            "/nodes/multiget",
            None,
            r#"{"addresses":["Post:1","Post:2","User:3"]}"#,
        );

        assert_eq!(keys(&plan), vec![key(1), key(1), key(2)]);
    }

    /// The namespace-wide requests, refused on a split fleet and served on an
    /// unsplit one. Both halves matter: refusing them always would make the
    /// front door a regression in front of a single FacetQL.
    #[test]
    fn namespace_wide_requests_are_refused_only_when_the_namespace_is_split() {
        let cases: &[(Method, &str, &str)] = &[
            (Method::GET, "/nodes", ""),
            (Method::GET, "/stats", ""),
            (Method::POST, "/admin/users", r#"{"owner":"a"}"#),
            (Method::POST, "/nodes/query", r#"{"limit":10}"#),
        ];

        for (method, path, body) in cases {
            match planned(method.clone(), path, None, body) {
                Plan::Refuse(Refusal::NotProxyable { .. }) => {}
                other => panic!("{method} {path} on a split fleet: {other:?}"),
            }

            let single = Keyspace::single(key(7));
            match plan(
                method,
                path,
                None,
                body.as_bytes(),
                &single,
                &ReadPreference::Primary,
            ) {
                Plan::Colocated { keys } => assert_eq!(keys[0].key, key(7)),
                other => panic!("{method} {path} on one backend: {other:?}"),
            }
        }
    }

    #[test]
    fn declarations_go_to_every_backend_and_listings_must_agree() {
        assert_eq!(
            planned(Method::GET, "/admin/indexes", None, ""),
            Plan::Broadcast {
                agreement: Agreement::Required,
                what: "a fleet-wide declaration listing",
            }
        );

        assert_eq!(
            planned(Method::POST, "/admin/references", None, "{}"),
            Plan::Broadcast {
                agreement: Agreement::NotRequired,
                what: "a fleet-wide declaration",
            }
        );

        assert_eq!(
            planned(Method::GET, "/", None, ""),
            Plan::Broadcast {
                agreement: Agreement::Required,
                what: "GET /",
            }
        );
    }

    /// A publish reaches exactly one backend so that a subscriber merging
    /// every backend's stream sees it exactly once.
    #[test]
    fn a_publish_routes_by_its_channel_to_exactly_one_backend() {
        assert_eq!(
            keys(&planned(
                Method::POST,
                "/publish",
                None,
                r#"{"channel":"Post","payload":"{}"}"#
            )),
            vec![key(1)]
        );

        assert_eq!(planned(Method::GET, "/events", None, ""), Plan::Subscribe);
    }

    #[test]
    fn an_unknown_path_is_a_404_exactly_as_facetql_answers_it() {
        match planned(Method::GET, "/nope", None, "") {
            Plan::Refuse(refusal) => {
                assert_eq!(refusal.status(), axum::http::StatusCode::NOT_FOUND)
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_body_that_is_not_the_contract_is_a_400_not_a_misroute() {
        match planned(Method::POST, "/transaction", None, "not json") {
            Plan::Refuse(refusal) => {
                assert_eq!(refusal.status(), axum::http::StatusCode::BAD_REQUEST)
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_unmapped_address_is_refused_rather_than_sent_somewhere_plausible() {
        match planned(Method::GET, "/node/Comment:1", None, "") {
            Plan::Refuse(refusal) => {
                assert_eq!(
                    refusal.status(),
                    axum::http::StatusCode::MISDIRECTED_REQUEST
                )
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_missing_credential_message_is_facetqls_own_words() {
        assert_eq!(
            Refusal::MissingCredential.to_string(),
            "missing x-api-key header (or ?key= for SSE)"
        );
    }
}
