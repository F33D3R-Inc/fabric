//! The FacetQL wire contract, transcribed.
//!
//! Every type here mirrors a shape FacetQL's `src/api/routes.rs` or
//! `src/storage/engine.rs` already defines. The field names, the serde tag
//! (`type`) and the snake_case variant names are the contract (AGENT_LOG
//! §4b) — inventing one is drift, which is rework, which is forbidden. The
//! unit tests at the bottom of this file assert the exact JSON of every
//! transaction op so a rename cannot pass review silently.
//!
//! # The two `Coordinate` types
//!
//! [`Coordinate`] here is **FacetQL's** 4-axis per-node tag (`x`,`y`,`z`,`q`,
//! no grid geometry). [`fabric_core::Coordinate`] is a cell in Fabric's
//! 12×13 placement grid (`x`,`y`, bounded). They share a name and nothing
//! else, and this crate never converts between them: a fabric grid cell is
//! recorded in a placement node's `data`, not in the node's FacetQL
//! coordinate, which stays at the origin. See FABRIC_INTEGRATION_PLAN.md
//! Finding A.

use serde::{Deserialize, Serialize};

/// FacetQL's 4-axis coordinate. **Not** [`fabric_core::Coordinate`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coordinate {
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,
}

/// Who may read a node. Serializes as the bare string `"Private"`/`"Public"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Visibility {
    Private,
    Public,
}

/// A node as FacetQL returns it from `GET /nodes`, `POST /nodes/query` and
/// `GET /node/:address`.
///
/// `data` is an opaque string on the wire — FacetQL stores whatever the
/// client wrote and does not parse it except where a predicate or an index
/// asks it to. Callers that put JSON in there decode it themselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub address: String,
    pub coordinate: Coordinate,
    pub value: u64,
    pub kind: String,
    pub data: String,
    pub owner: String,
    pub claimed_by: Option<String>,
    pub visibility: Visibility,
}

/// One page of `POST /nodes/query`: `{ "nodes": [...], "next": "<cursor>" }`.
///
/// `next` is an opaque keyset cursor; `""` means this was the last page.
/// Feed it back as [`QueryRequest::after`]. Never paginate this endpoint with
/// `offset` — FacetQL caps a deep offset (10,000) and an offset is unstable
/// under concurrent writes, which is the whole reason the cursor exists.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryPage {
    pub nodes: Vec<Node>,
    pub next: String,
}

impl QueryPage {
    /// Whether another page follows.
    pub fn has_more(&self) -> bool {
        !self.next.is_empty()
    }
}

/// Body of `POST /nodes/query`.
///
/// Only `after` is used for pagination here; `offset` exists on the endpoint
/// and is deliberately not modelled, so no caller of this crate can reach for
/// it by accident.
#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    pub where_: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_var: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub desc: bool,
    /// Opaque cursor from the previous page's `next`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Body of `POST /node`.
///
/// `if_absent` is the atomic create-once primitive: with it set, an address
/// that already exists answers 409 instead of being overwritten. That is what
/// makes "create this placement, and tell me if I lost the race" a single
/// round trip rather than a read followed by a write.
#[derive(Debug, Clone, Serialize)]
pub struct CreateNodeRequest {
    pub address: String,
    pub kind: String,
    pub x: u8,
    pub y: u8,
    pub z: u8,
    pub q: u8,
    pub data: String,
    pub public: bool,
    pub if_absent: bool,
}

/// The condition a [`TxOperation::SetIf`] tests against one field of one
/// node's `data`. Exactly one of these is sent, which is what FacetQL
/// requires — modelling it as an enum makes sending two, or none,
/// unrepresentable rather than a 400 discovered at runtime.
///
/// Serializes flattened into the op as a single key: `{"expect_le": 5.0}`,
/// `{"expect_eq": <value>}` or `{"expect_absent": true}`.
#[derive(Debug, Clone, Serialize)]
pub enum Expect {
    /// The field is a number and is at most this. The lease/deadline form.
    #[serde(rename = "expect_le")]
    AtMost(f64),

    /// The field equals this exactly. The version form — compare-and-swap on
    /// a revision counter, which is how a placement update refuses to clobber
    /// a concurrent one.
    #[serde(rename = "expect_eq")]
    Equals(serde_json::Value),

    /// The field is unset or null. The create-once form.
    #[serde(rename = "expect_absent")]
    Absent(bool),
}

impl Expect {
    /// Compare-and-swap against a version counter.
    pub fn version(version: u64) -> Self {
        Self::Equals(serde_json::Value::from(version))
    }
}

/// One operation inside `POST /transaction`.
///
/// The whole batch is all-or-nothing: a refused precondition anywhere in it
/// means nothing was applied. That is what lets a CAS-guarded delete be
/// expressed as [`TxOperation::SetIf`] followed by [`TxOperation::DeleteNode`]
/// on the same address, with no window between them.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TxOperation {
    /// Create or overwrite a node (upsert; same-owner overwrite archives to
    /// history).
    InsertNode {
        address: String,
        kind: String,
        x: u8,
        y: u8,
        z: u8,
        q: u8,
        data: String,
        public: bool,
    },

    DeleteNode {
        address: String,
    },

    InsertEdge {
        from: String,
        to: String,
        kind: String,
    },

    DeleteEdge {
        from: String,
        to: String,
        kind: String,
    },

    /// Remove every node of `kind` the caller may write.
    ClearKind {
        kind: String,
    },

    /// `clear_kind` plus a predicate over each candidate's decoded `data`.
    /// Omitting `where` is exactly `clear_kind`.
    DeleteWhere {
        kind: String,
        #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
        where_: Option<serde_json::Value>,
    },

    /// Native compare-and-set on one field of one node's `data`.
    ///
    /// `set` is *merged* into the node's data, so an unrelated field is never
    /// clobbered. The outcome arrives as a status, not a body: 200 means the
    /// condition held and the batch committed (you won); 412 means it did not
    /// and nothing in the batch was applied (someone else won).
    SetIf {
        address: String,
        field: String,
        #[serde(flatten)]
        expect: Expect,
        set: serde_json::Map<String, serde_json::Value>,
    },
}

/// Body of `POST /transaction`.
#[derive(Debug, Clone, Serialize)]
pub struct TransactionRequest {
    pub operations: Vec<TxOperation>,
}

/// One entry of the per-`kind` node-count breakdown in [`EngineStats`].
#[derive(Debug, Clone, Deserialize)]
pub struct KindCount {
    pub kind: String,
    pub count: u64,
}

/// The shape of the physical heap under the data, from `GET /stats`.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct StorageStats {
    pub page_size: u32,
    pub segments: u64,
    pub pages: u64,
    pub obsolete_bytes: u64,
}

/// One latency class (read or write) over the most recent closed observation
/// window, from `runtime.window.{read,write}_latency`.
///
/// `p50_us`/`p99_us`/`max_us` are `None` when the window contained zero
/// requests of this class — not `0`, which would read as "instant". See
/// [`WindowStats::duration_ms`] for the companion absence: a window that
/// never closed at all.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct LatencyStats {
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub p50_us: Option<u64>,
    #[serde(default)]
    pub p99_us: Option<u64>,
    #[serde(default)]
    pub max_us: Option<u64>,
}

/// One closed observation window, from `runtime.window`.
///
/// **`duration_ms == 0` means no window has closed yet** — the instance has
/// been up for less than FacetQL's minimum window, or this is the first
/// poll. That is a *missing measurement*, not an idle server: treat it (and
/// the accompanying `None`s below) as absent, never as zero. See
/// `fabric_facetql::sample` for how this crate honors that distinction.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct WindowStats {
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub age_ms: u64,
    /// `None` when no window has closed, or when the platform would not
    /// report CPU time or core count.
    #[serde(default)]
    pub cpu_utilization: Option<f64>,
    #[serde(default)]
    pub read_latency: LatencyStats,
    #[serde(default)]
    pub write_latency: LatencyStats,
}

/// HTTP-level throughput and contention, from `runtime.requests`.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct RequestStats {
    #[serde(default)]
    pub total: u64,
    #[serde(default)]
    pub read: u64,
    #[serde(default)]
    pub write: u64,
    #[serde(default)]
    pub excluded: u64,
    #[serde(default)]
    pub unclassified: u64,
    /// Requests being served right now — an exact instantaneous count, not a
    /// sample.
    #[serde(default)]
    pub in_flight: u64,
    /// The ceiling `in_flight` is measured against — the server's admission
    /// cap.
    #[serde(default)]
    pub max_concurrent: u64,
    /// Writers parked on the engine's writer mutex right now. An
    /// instantaneous gauge: a poll can miss a burst between samples, which
    /// is what `write_queue_contended_total` is for.
    #[serde(default)]
    pub write_queue_depth: u64,
    /// Times a writer arrived to find the writer mutex already contended.
    /// Monotonic — process-lifetime, like `reads_total`.
    #[serde(default)]
    pub write_queue_contended_total: u64,
}

/// What the process itself is consuming, from `runtime.process`.
///
/// Every field is `Option` because every field is genuinely unavailable on
/// some platform, and `null` is the only truthful encoding of "this host
/// would not tell me". `cpu_seconds_total` is a **monotonic** counter, not a
/// utilization — see `fabric_facetql::sample` for why this crate differences
/// it itself rather than relying on `WindowStats::cpu_utilization`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProcessStats {
    #[serde(default)]
    pub cpu_seconds_total: Option<f64>,
    /// Hardware parallelism available to this process — on Linux this
    /// respects a cgroup CPU quota.
    #[serde(default)]
    pub cpu_cores: Option<u64>,
    #[serde(default)]
    pub resident_bytes: Option<u64>,
    #[serde(default)]
    pub memory_limit_bytes: Option<u64>,
    /// `"cgroup"` or `"system"`.
    #[serde(default)]
    pub memory_limit_source: Option<String>,
    #[serde(default)]
    pub memory_utilization: Option<f64>,
}

/// Everything `runtime` reports, one `GET /stats` at a time.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RuntimeStats {
    #[serde(default)]
    pub uptime_seconds: u64,
    #[serde(default)]
    pub requests: RequestStats,
    #[serde(default)]
    pub window: WindowStats,
    #[serde(default)]
    pub process: ProcessStats,
}

/// One coordinate's share of the traffic, from `cells.cells[]`.
///
/// `x`/`y`/`z`/`q` are FacetQL's own 4-axis coordinate — see the module docs'
/// note on the two `Coordinate` types. **Not** a Fabric grid cell.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct CellStats {
    #[serde(default)]
    pub x: u8,
    #[serde(default)]
    pub y: u8,
    #[serde(default)]
    pub z: u8,
    #[serde(default)]
    pub q: u8,
    #[serde(default)]
    pub reads: u64,
    #[serde(default)]
    pub writes: u64,
    #[serde(default)]
    pub bytes_read: u64,
    #[serde(default)]
    pub bytes_written: u64,
}

/// The per-cell breakdown, and the honest account of what it left out, from
/// `cells`.
///
/// `overflow_reads`/`overflow_writes` non-zero is a deliberate, countable
/// admission that `cells` is a **partial** account of the traffic: some
/// reads or writes happened at a coordinate this table had no room left to
/// track. A consumer comparing entries of `cells` against each other must
/// know that fact rather than silently trusting a complete picture — see
/// `fabric_facetql::sample`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CellAttribution {
    #[serde(default)]
    pub capacity: u64,
    #[serde(default)]
    pub tracked: u64,
    #[serde(default)]
    pub overflow_reads: u64,
    #[serde(default)]
    pub overflow_writes: u64,
    /// Mutations with no coordinate to attribute — edges, and deletes of
    /// addresses that were already gone.
    #[serde(default)]
    pub unattributed_writes: u64,
    /// Busiest cell first.
    #[serde(default)]
    pub cells: Vec<CellStats>,
}

/// `GET /stats` — FacetQL's own storage/operation statistics.
///
/// `reads_total` / `writes_total` are **process-lifetime** counters: they
/// start at zero when the server starts and are not persisted. A consumer
/// differencing two samples must therefore treat a counter that went
/// *backwards* as a restart and drop the interval rather than reporting a
/// wild rate — see [`crate::sample`].
///
/// `version`, `runtime` and `cells` are additive: an older FacetQL that
/// predates them simply omits the keys, and every field below decodes to a
/// tolerant default (`None`/`0`/empty) rather than failing the whole
/// response. The daemon must keep working — with a flatter pressure model —
/// against a server that has not been upgraded yet.
#[derive(Debug, Clone, Deserialize)]
pub struct EngineStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub user_count: u64,
    pub history_entries: u64,
    pub kinds: Vec<KindCount>,
    pub reads_total: u64,
    pub writes_total: u64,
    pub storage: StorageStats,

    /// The server's own build. `None` against an older FacetQL that does not
    /// report it.
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub runtime: RuntimeStats,
    #[serde(default)]
    pub cells: CellAttribution,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every transaction op, byte-for-byte against AGENT_LOG §4b and
    /// facetql's `TxOpRequest`. A rename on either side fails here.
    #[test]
    fn transaction_ops_match_the_canonical_contract() {
        let ops = vec![
            TxOperation::InsertNode {
                address: "Entity:1".into(),
                kind: "Entity".into(),
                x: 0,
                y: 0,
                z: 0,
                q: 0,
                data: "{}".into(),
                public: false,
            },
            TxOperation::DeleteNode {
                address: "Entity:1".into(),
            },
            TxOperation::InsertEdge {
                from: "a".into(),
                to: "b".into(),
                kind: "rel".into(),
            },
            TxOperation::DeleteEdge {
                from: "a".into(),
                to: "b".into(),
                kind: "rel".into(),
            },
            TxOperation::ClearKind {
                kind: "Entity".into(),
            },
            TxOperation::DeleteWhere {
                kind: "Entity".into(),
                where_: None,
            },
        ];

        let encoded = serde_json::to_value(TransactionRequest { operations: ops }).unwrap();

        assert_eq!(
            encoded,
            json!({
                "operations": [
                    {"type": "insert_node", "address": "Entity:1", "kind": "Entity",
                     "x": 0, "y": 0, "z": 0, "q": 0, "data": "{}", "public": false},
                    {"type": "delete_node", "address": "Entity:1"},
                    {"type": "insert_edge", "from": "a", "to": "b", "kind": "rel"},
                    {"type": "delete_edge", "from": "a", "to": "b", "kind": "rel"},
                    {"type": "clear_kind", "kind": "Entity"},
                    {"type": "delete_where", "kind": "Entity"}
                ]
            })
        );
    }

    #[test]
    fn delete_where_carries_its_predicate_under_the_key_where() {
        let op = TxOperation::DeleteWhere {
            kind: "__session".into(),
            where_: Some(json!({"op": "<", "left": "x", "right": 1})),
        };
        let encoded = serde_json::to_value(&op).unwrap();
        assert_eq!(encoded["type"], "delete_where");
        assert_eq!(encoded["where"]["op"], "<");
    }

    #[test]
    fn each_set_if_expectation_serializes_as_exactly_one_key() {
        let mut set = serde_json::Map::new();
        set.insert("version".into(), json!(2));

        let at_most = serde_json::to_value(TxOperation::SetIf {
            address: "__cron:nightly".into(),
            field: "next_run".into(),
            expect: Expect::AtMost(1000.0),
            set: set.clone(),
        })
        .unwrap();
        assert_eq!(
            at_most,
            json!({"type": "set_if", "address": "__cron:nightly", "field": "next_run",
                   "expect_le": 1000.0, "set": {"version": 2}})
        );

        let equals = serde_json::to_value(TxOperation::SetIf {
            address: "p:1".into(),
            field: "version".into(),
            expect: Expect::version(1),
            set: set.clone(),
        })
        .unwrap();
        assert_eq!(
            equals,
            json!({"type": "set_if", "address": "p:1", "field": "version",
                   "expect_eq": 1, "set": {"version": 2}})
        );

        let absent = serde_json::to_value(TxOperation::SetIf {
            address: "p:1".into(),
            field: "version".into(),
            expect: Expect::Absent(true),
            set,
        })
        .unwrap();
        assert_eq!(
            absent,
            json!({"type": "set_if", "address": "p:1", "field": "version",
                   "expect_absent": true, "set": {"version": 2}})
        );
    }

    #[test]
    fn a_query_request_omits_what_it_does_not_set() {
        let request = QueryRequest {
            kind: Some("__fabric_placement".into()),
            limit: Some(100),
            after: Some("Y3Vyc29y".into()),
            ..QueryRequest::default()
        };
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(
            encoded,
            json!({"kind": "__fabric_placement", "limit": 100, "after": "Y3Vyc29y"})
        );
        // `offset` is not a field at all: the cursor is the only pagination
        // this client can express.
        assert!(encoded.get("offset").is_none());
    }

    #[test]
    fn stats_and_nodes_decode_from_facetqls_own_shape() {
        let stats: EngineStats = serde_json::from_value(json!({
            "node_count": 3, "edge_count": 1, "user_count": 2, "history_entries": 4,
            "kinds": [{"kind": "Post", "count": 3}],
            "reads_total": 10, "writes_total": 5,
            "storage": {"page_size": 4096, "segments": 1, "pages": 9, "obsolete_bytes": 0}
        }))
        .unwrap();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.kinds[0].kind, "Post");
        assert_eq!(stats.storage.page_size, 4096);

        // An older FacetQL that predates `version`/`runtime`/`cells` must
        // still decode, tolerantly, rather than fail the whole response.
        assert!(stats.version.is_none());
        assert_eq!(stats.runtime.uptime_seconds, 0);
        assert_eq!(stats.runtime.requests.max_concurrent, 0);
        assert!(stats.runtime.window.cpu_utilization.is_none());
        assert!(stats.runtime.process.cpu_seconds_total.is_none());
        assert_eq!(stats.cells.capacity, 0);
        assert!(stats.cells.cells.is_empty());

        let page: QueryPage = serde_json::from_value(json!({
            "nodes": [{
                "address": "p:1", "coordinate": {"x":0,"y":0,"z":0,"q":0}, "value": 0,
                "kind": "__fabric_placement", "data": "{}", "owner": "fabric",
                "claimed_by": null, "visibility": "Private"
            }],
            "next": ""
        }))
        .unwrap();
        assert!(!page.has_more());
        assert_eq!(page.nodes[0].visibility, Visibility::Private);
    }

    /// The full, current `GET /stats` shape — `version`, `runtime` and
    /// `cells` all present — decodes into real values rather than falling
    /// back to the tolerant defaults a missing key would produce.
    #[test]
    fn a_current_stats_response_decodes_every_added_field() {
        let stats: EngineStats = serde_json::from_value(json!({
            "node_count": 3, "edge_count": 1, "user_count": 2, "history_entries": 4,
            "kinds": [{"kind": "Post", "count": 3}],
            "reads_total": 10, "writes_total": 5,
            "storage": {"page_size": 4096, "segments": 1, "pages": 9, "obsolete_bytes": 0},
            "version": "1.2.3",
            "runtime": {
                "uptime_seconds": 120,
                "requests": {
                    "total": 50, "read": 30, "write": 20, "excluded": 1,
                    "unclassified": 0, "in_flight": 400, "max_concurrent": 512,
                    "write_queue_depth": 3, "write_queue_contended_total": 9
                },
                "window": {
                    "duration_ms": 1000, "age_ms": 5,
                    "cpu_utilization": 0.42,
                    "read_latency": {"count": 10, "p50_us": 100, "p99_us": 900, "max_us": 950},
                    "write_latency": {"count": 0, "p50_us": null, "p99_us": null, "max_us": null}
                },
                "process": {
                    "cpu_seconds_total": 12.5, "cpu_cores": 8,
                    "resident_bytes": 1048576, "memory_limit_bytes": 4194304,
                    "memory_limit_source": "cgroup", "memory_utilization": 0.25
                }
            },
            "cells": {
                "capacity": 256, "tracked": 2, "overflow_reads": 0,
                "overflow_writes": 0, "unattributed_writes": 1,
                "cells": [
                    {"x": 1, "y": 2, "z": 3, "q": 4, "reads": 100, "writes": 10,
                     "bytes_read": 1000, "bytes_written": 100},
                    {"x": 0, "y": 0, "z": 0, "q": 0, "reads": 5, "writes": 1,
                     "bytes_read": 50, "bytes_written": 5}
                ]
            }
        }))
        .unwrap();

        assert_eq!(stats.version.as_deref(), Some("1.2.3"));
        assert_eq!(stats.runtime.requests.in_flight, 400);
        assert_eq!(stats.runtime.requests.max_concurrent, 512);
        assert_eq!(stats.runtime.window.cpu_utilization, Some(0.42));
        assert_eq!(stats.runtime.window.read_latency.p99_us, Some(900));
        assert!(stats.runtime.window.write_latency.p99_us.is_none());
        assert_eq!(stats.runtime.process.cpu_seconds_total, Some(12.5));
        assert_eq!(stats.runtime.process.cpu_cores, Some(8));
        assert_eq!(stats.runtime.process.memory_limit_source.as_deref(), Some("cgroup"));
        assert_eq!(stats.cells.capacity, 256);
        assert_eq!(stats.cells.cells.len(), 2);
        assert_eq!(stats.cells.cells[0].x, 1);
        assert_eq!(stats.cells.unattributed_writes, 1);
    }
}
