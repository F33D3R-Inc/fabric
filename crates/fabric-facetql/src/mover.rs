//! The mover: the thing that actually copies a cell's nodes from one FacetQL
//! instance to another.
//!
//! Fabric's migration machine was, until this module existed, a correctly
//! sequenced state machine wrapped around a copy that never happened.
//! `PlacementFabric::record_transfer` is documented as fed from outside
//! because this workspace copies no bytes, and nothing in any repo could:
//! FacetQL exposes no bulk export/import. This is that missing half, built on
//! the API FacetQL actually has.
//!
//! # 1. Enumeration: the keyset cursor, not the offset
//!
//! `POST /nodes/query` pages by an **opaque keyset cursor** over the composite
//! ordering `(order_field, address)`; `GET /nodes` pages by offset. Offset is
//! not a choice here. FacetQL's own engine documents the cursor as "stable
//! under concurrent inserts and deletes the way a plain offset does not", caps
//! a deep offset at 10 000 rows outright, and a snapshot that skips and
//! repeats rows under a live write stream is exactly the silent data loss this
//! module exists to prevent. So the mover walks the cursor, ordered by address
//! (`order` omitted, which FacetQL normalizes to address order — an index range
//! scan that reads nothing outside the page it returns).
//!
//! What the cursor does and does not give:
//!
//! * A row **inserted at an address the walk has already passed** is missed by
//!   the snapshot. The cursor selects rows strictly past the last one
//!   returned, and that address is behind it.
//! * A row **deleted after it was read** is copied to the destination and is a
//!   ghost there.
//! * A row **inserted or deleted ahead of the cursor** is picked up (or not
//!   found) correctly, with no skip or repeat of its neighbours — which is the
//!   property an offset walk does not have.
//!
//! The first two are real and are not fixed by the cursor. They are fixed by
//! the change feed, below: both raise an event carrying the address, the
//! address goes in the dirty set, and catch-up reconciles it against the
//! source's current truth (present → upsert, absent → delete).
//!
//! # 2. Writing: batched upserts, all-or-nothing, idempotent
//!
//! `POST /transaction` applies a batch of `insert_node` ops atomically, and
//! `insert_node` is an **upsert** — an address that already exists is
//! overwritten, not refused. Both properties are load-bearing:
//!
//! * *Upsert* is what makes a retried batch safe. A request whose outcome is
//!   unknown (the connection dropped after the server committed) is retried,
//!   and the retry writes the same rows to the same addresses with the same
//!   contents. There is no double-write to detect and no reconciliation to do.
//! * *Atomic* is what bounds a failure. A batch either landed whole or not at
//!   all, so a failed copy leaves the destination holding a set of complete
//!   rows — never half a row — and the next attempt overwrites them.
//!
//! Batch size is [`MoverConfig::batch_ops`] = 250 ops, additionally capped by
//! [`MoverConfig::batch_bytes`] = 2 MiB of accumulated `data`, whichever binds
//! first. The reasoning: FacetQL's engine refuses a batch past
//! `max_transaction_ops` (50 000 by default) and its HTTP layer refuses a body
//! past 4 MiB, so the ceiling that actually binds on real rows is the **body**,
//! not the op count — 250 rows of 16 KiB is already 4 MiB. Two independent
//! caps means neither a cell of tiny rows nor a cell of large ones can build a
//! request the server will refuse. And a batch is a unit of retry as well as a
//! unit of commit, so it is kept small enough that re-sending one is cheap:
//! 250 rows, not 5 000.
//!
//! The mover talks to the two instances **directly**, never through Fabric's
//! own front door: the door refuses a transaction spanning backends, and a
//! copy is by definition a read from one backend and a write to another.
//!
//! # 3. Catch-up, and exactly what it can and cannot promise
//!
//! The source keeps accepting writes for as long as it is authoritative, so a
//! snapshot is stale before it finishes. The mover learns about those writes
//! from `GET /events`, FacetQL's SSE feed, **subscribed before the first query
//! page is asked for** — an event seen before the snapshot began is at worst a
//! redundant reconciliation, an event missed after it began is lost data.
//!
//! Every frame names the addresses that changed (`node_created`,
//! `node_updated`, `node_deleted`, and `transaction_committed` with an
//! `addresses` array), so an in-scope address goes into a dirty set and
//! catch-up re-reads it from the source with `POST /nodes/multiget` — whose
//! reply *omits* what no longer exists, which is exactly the signal a
//! reconciler needs — and applies the answer to the destination.
//!
//! ## What this cannot guarantee, stated plainly
//!
//! `GET /events` is an in-memory `tokio::sync::broadcast` of fixed capacity
//! (1024) and **the SSE handler silently discards a lagged receiver's
//! messages**: `subscribe_events` in `facetql/src/api/routes.rs` maps both
//! "this event is not for you" and "you fell behind and lost messages" to the
//! same `None`. No frame carries a sequence number, and there is no
//! resume-from-position. Therefore:
//!
//! * a write burst that outruns the mover's consumption of the stream is lost
//!   from the feed, **undetectably by the subscriber**; and
//! * a subscription that drops loses every write in the gap, with no way to
//!   ask what was missed.
//!
//! The mover does what can be done about this and no more. It treats any
//! stream end or error as a hard fault ([`FeedHealth::Broken`]) that fails the
//! copy rather than quietly continuing on a feed that is no longer complete,
//! and its pre-cutover check is a full field-by-field comparison of both
//! instances rather than a count, so a write lost from the feed before the
//! check is caught by the check. What remains uncovered is a write that lands
//! *between* a passing check and the write fence going up, and is lost from
//! the feed: nothing available today detects that.
//!
//! **What FacetQL would need to expose to close the gap** (none of it is a
//! change to any existing response shape):
//!
//! * a **monotonic sequence number on every `/events` frame** plus
//!   `GET /events?after=<seq>`, so a subscriber can detect a gap and refill it.
//!   The engine already mints exactly such a number — `HistoryEntry::version`
//!   is documented as "unique across the life of the database", taken from the
//!   WAL's operation-id counter and stable across restarts — so this is
//!   surfacing an existing quantity, not inventing one.
//! * a **global change scan**, `GET /changes?after=<version>`, over that same
//!   counter. `GET /node/:address/history` is per-address and holds only
//!   *superseded* versions, so it can neither list what changed nor see a
//!   creation; it cannot serve as a change feed.
//! * `kind` on `node_updated` / `node_deleted`. Today only `node_created`
//!   carries it, so an address that matches no declared address-prefix cannot
//!   be attributed to a cell — see [`CellScope::may_contain_address`].
//! * an **`owner` (and `claimed_by`) field on `insert_node`**, admin-only.
//!   `insert_node` stamps the *writer's* identity as the owner and has no way
//!   to express a claim, so today a cell can only be moved faithfully by a
//!   credential whose owner already owns its nodes. The mover does not paper
//!   over this: its check compares `owner` and `claimed_by` like every other
//!   field and fails the migration rather than silently re-owning or
//!   un-claiming somebody's data.
//!
//! Edges are deliberately out of scope, and the reason is structural rather
//! than an omission: FacetQL exposes edges only per node
//! (`GET /node/:address/edges/out`), an edge may point out of the cell being
//! moved, and `insert_edge` requires the far endpoint to be readable on the
//! instance the edge is written to — so a cross-cell edge cannot be
//! reconstructed on the destination at all. Copying only the edges that happen
//! to fall inside the cell would produce a destination that looks complete and
//! is not. [`CellMover`] therefore reports the cell's edge count and refuses
//! the copy when it is non-zero, rather than moving a graph with its edges
//! quietly removed.
//!
//! # 4. Failure and resumption
//!
//! The mover writes nothing but upserts at the source's own addresses, and it
//! never writes a marker, a checkpoint or a progress record anywhere. So a run
//! that dies mid-copy leaves the destination holding some subset of the
//! source's rows, each one complete and correct, and leaves *no state a later
//! run could mistake for progress* — the next run re-derives everything by
//! reading, and overwrites what is there with identical bytes. It never
//! mutates the source, so the source stays authoritative through every
//! failure, and the migration stays abortable (`Restored`) for as long as the
//! phase machine allows an abort.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use fabric_routing::RoutingKey;
use fabric_runtime::CopyVerdict;
use futures_util::{Stream, StreamExt};

use crate::client::{FacetqlClient, MULTIGET_LIMIT};
use crate::error::FacetqlError;
use crate::frontdoor::Keyspace;
use crate::wire::{Node, QueryPage, QueryRequest, TxOperation, Visibility};

/// Ops in one `POST /transaction`. See the module docs for why 250.
const DEFAULT_BATCH_OPS: usize = 250;

/// Accumulated `data` bytes in one `POST /transaction`, the cap that actually
/// binds on rows of any size. FacetQL's HTTP body limit is 4 MiB; half of it
/// leaves room for the JSON envelope around every op.
const DEFAULT_BATCH_BYTES: usize = 2 * 1024 * 1024;

/// Rows per `POST /nodes/query` page. FacetQL clamps `limit` to 500.
const DEFAULT_PAGE_LIMIT: usize = 500;

/// How the mover is allowed to shape its requests.
#[derive(Debug, Clone, Copy)]
pub struct MoverConfig {
    pub batch_ops: usize,
    pub batch_bytes: usize,
    pub page_limit: usize,
}

impl Default for MoverConfig {
    fn default() -> Self {
        Self {
            batch_ops: DEFAULT_BATCH_OPS,
            batch_bytes: DEFAULT_BATCH_BYTES,
            page_limit: DEFAULT_PAGE_LIMIT,
        }
    }
}

/// Why a copy could not be made or could not be trusted.
#[derive(Debug)]
pub enum MoverError {
    /// A request to one of the two instances failed. `node` names which.
    Wire { node: String, error: FacetqlError },

    /// The change feed is no longer complete, so nothing downstream of it can
    /// be trusted. Always fatal — see the module docs.
    FeedBroken(String),

    /// The cursor did not terminate. A server handing back a non-empty `next`
    /// forever would otherwise spin here.
    Runaway { pages: usize },

    /// The copy was made and does not match its source.
    NotIdentical(String),
}

impl std::fmt::Display for MoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wire { node, error } => write!(f, "'{node}': {error}"),

            Self::FeedBroken(reason) => write!(
                f,
                "the source's change feed is no longer complete ({reason}), so \
                 writes landing during the copy can no longer be seen"
            ),

            Self::Runaway { pages } => write!(
                f,
                "the source's cursor did not terminate within {pages} pages"
            ),

            Self::NotIdentical(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for MoverError {}

// ─────────────────────────────────────────────────────────────── the scope

/// Which of a FacetQL instance's nodes belong to the cell being moved.
///
/// Derived from the operator's declared [`Keyspace`], never guessed from an
/// address — the same rule the front door routes by, read the other way round.
/// A rule says "kind `K`, and addresses beginning `P`, live at key `X`"; the
/// cell at key `X` is therefore the union of the rules naming it, and, when
/// `X` is also the fallback, everything no *other* rule claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellScope {
    kinds: Vec<String>,
    prefixes: Vec<String>,

    /// True when this key is the keyspace's fallback: the cell is everything
    /// the other rules do not claim.
    catch_all: bool,

    excluded_kinds: Vec<String>,
    excluded_prefixes: Vec<String>,
}

impl CellScope {
    /// The cell that holds the whole namespace — the honest shape of the
    /// single-instance deployment, where every kind lives in one place.
    pub fn whole_namespace() -> Self {
        Self {
            kinds: Vec::new(),
            prefixes: Vec::new(),
            catch_all: true,
            excluded_kinds: Vec::new(),
            excluded_prefixes: Vec::new(),
        }
    }

    /// What `key` holds, according to `keyspace`.
    pub fn for_key(keyspace: &Keyspace, key: RoutingKey) -> Self {
        let mut kinds = Vec::new();
        let mut prefixes = Vec::new();
        let mut excluded_kinds = Vec::new();
        let mut excluded_prefixes = Vec::new();

        for rule in keyspace.rules() {
            if rule.key() == key {
                kinds.push(rule.kind().to_string());
                prefixes.push(rule.address_prefix().to_string());
            } else {
                excluded_kinds.push(rule.kind().to_string());
                excluded_prefixes.push(rule.address_prefix().to_string());
            }
        }

        Self {
            kinds,
            prefixes,
            catch_all: keyspace.fallback() == Some(key),
            excluded_kinds,
            excluded_prefixes,
        }
    }

    /// The kinds to enumerate one pass each, or empty when the cell is a
    /// catch-all and a single unfiltered pass covers it.
    pub fn kinds(&self) -> &[String] {
        if self.catch_all { &[] } else { &self.kinds }
    }

    pub fn is_catch_all(&self) -> bool {
        self.catch_all
    }

    /// Whether a node this instance returned belongs to the cell.
    ///
    /// Either half of a rule identifies a node, matching `Keyspace::resolve`,
    /// which accepts a request that names only a kind or only an address.
    pub fn contains(&self, kind: &str, address: &str) -> bool {
        if self.catch_all {
            return !self.excluded_kinds.iter().any(|k| k == kind)
                && !self
                    .excluded_prefixes
                    .iter()
                    .any(|prefix| address.starts_with(prefix));
        }

        self.kinds.iter().any(|k| k == kind)
            || self
                .prefixes
                .iter()
                .any(|prefix| address.starts_with(prefix))
    }

    /// Whether an address *alone* could name a node in this cell.
    ///
    /// Needed because `node_updated` and `node_deleted` carry only an address:
    /// FacetQL does not report the kind of a node it just changed, and by the
    /// time a delete is seen the node is gone, so its kind is unknowable. An
    /// address that might be in scope is therefore treated as in scope — a
    /// redundant re-read costs one row, a skipped one loses a write.
    pub fn may_contain_address(&self, address: &str) -> bool {
        if self.catch_all {
            return !self
                .excluded_prefixes
                .iter()
                .any(|prefix| address.starts_with(prefix));
        }

        self.prefixes
            .iter()
            .any(|prefix| address.starts_with(prefix))
    }
}

// ──────────────────────────────────────────────────────────── the change feed

/// Whether the source's change feed can still be believed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedHealth {
    Live,

    /// The stream ended or errored. Fatal: FacetQL offers no way to ask what
    /// was missed, so a copy built on a broken feed cannot be certified.
    Broken(String),
}

#[derive(Debug, Default)]
struct FeedState {
    dirty: BTreeSet<String>,

    /// How many in-scope changes have been admitted, ever. This is the
    /// migration's write sequence space: change number `n` is `WriteSeq(n-1)`.
    /// It counts *events*, not distinct addresses, so two writes to one
    /// address are two writes — while the reconciliation of that address
    /// applies both at once, which is what makes coalescing sound.
    observed: u64,

    /// `observed` as it stood when the last reconciliation was taken, once
    /// that reconciliation has landed on the destination.
    applied: u64,

    broken: Option<String>,
}

/// A live subscription to the source's `GET /events`, reduced to "which
/// addresses in this cell have changed".
///
/// Dropping it ends the subscription.
#[derive(Debug)]
pub struct ChangeFeed {
    state: Arc<Mutex<FeedState>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ChangeFeed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ChangeFeed {
    fn lock(&self) -> std::sync::MutexGuard<'_, FeedState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn health(&self) -> FeedHealth {
        match self.lock().broken.clone() {
            Some(reason) => FeedHealth::Broken(reason),
            None => FeedHealth::Live,
        }
    }

    /// Changes admitted so far, and how many of them the destination has been
    /// brought up to date with.
    pub fn counts(&self) -> (u64, u64) {
        let state = self.lock();
        (state.observed, state.applied)
    }

    /// How many addresses are waiting to be reconciled.
    pub fn dirty_len(&self) -> usize {
        self.lock().dirty.len()
    }

    /// Take the addresses waiting to be reconciled, and the `observed` count
    /// they correspond to.
    ///
    /// Reconciling an address means writing the source's *current* value for
    /// it, which applies every change to that address up to this instant — so
    /// the whole `observed` count at the moment of the take is what the
    /// reconciliation covers, however many of those changes touched one
    /// address. Anything arriving after the take bumps `observed` again and
    /// re-dirties, so nothing is coalesced away.
    fn take(&self) -> (Vec<String>, u64) {
        let mut state = self.lock();
        let taken = std::mem::take(&mut state.dirty);

        (taken.into_iter().collect(), state.observed)
    }

    /// Put addresses back after a failed reconciliation, without advancing
    /// `applied`. A failure must never look like progress.
    fn restore(&self, addresses: Vec<String>) {
        let mut state = self.lock();
        state.dirty.extend(addresses);
    }

    fn confirm(&self, up_to: u64) {
        let mut state = self.lock();
        state.applied = state.applied.max(up_to);
    }
}

/// Extract the addresses one SSE frame's `data:` payload names as changed.
///
/// Unknown event names contribute nothing rather than being an error: FacetQL
/// puts edge, user and application (`POST /publish`) messages on the same
/// stream, and a mover that failed on a `user_created` would fail on a fleet
/// where anybody created a login mid-migration.
fn changed_addresses(frame: &str) -> Vec<String> {
    let mut out = Vec::new();

    for line in frame.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };

        let Ok(value) = serde_json::from_str::<serde_json::Value>(payload.trim()) else {
            continue;
        };

        match value.get("event").and_then(|event| event.as_str()) {
            Some("node_created" | "node_updated" | "node_deleted" | "node_claimed") => {
                if let Some(address) = value.get("address").and_then(|a| a.as_str()) {
                    out.push(address.to_string());
                }
            }

            Some("transaction_committed") => {
                if let Some(addresses) = value.get("addresses").and_then(|a| a.as_array()) {
                    out.extend(
                        addresses
                            .iter()
                            .filter_map(|a| a.as_str())
                            .map(str::to_string),
                    );
                }
            }

            _ => {}
        }
    }

    out
}

/// Index just past the blank line that terminates the first SSE frame in the
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

// ──────────────────────────────────────────────────────────────── the mover

/// What one pass of the mover achieved, for reporting into the migration.
#[derive(Debug, Clone, PartialEq)]
pub struct MoverReport {
    /// Nodes written to the destination during the bulk snapshot.
    pub rows_copied: u64,

    /// `data` bytes written during the bulk snapshot. This is the real number
    /// the progress fraction is computed from; nothing here ramps with time.
    pub bytes_copied: u64,

    /// The source's own size for the cell, measured once the snapshot has
    /// walked it. `None` until then, so a fraction is never computed against a
    /// denominator nobody has measured.
    pub resident_bytes: Option<u64>,

    /// Whether the bulk snapshot has walked the whole cell.
    pub snapshot_complete: bool,

    /// Changes seen on the source since the subscription opened, and how many
    /// of them the destination has been brought up to date with. These are the
    /// migration's `accept_write` / `record_applied` counts, and their
    /// difference is `pending_writes`.
    pub observed_writes: u64,
    pub applied_writes: u64,

    pub verdict: CopyVerdict,
}

/// Copies one cell between two FacetQL instances.
///
/// Holds a client for each. Neither is Fabric's front door: a copy is a read
/// from one backend and a write to another, and the door refuses a transaction
/// that spans backends.
pub struct CellMover {
    source: FacetqlClient,
    destination: FacetqlClient,
    streaming: reqwest::Client,
    scope: CellScope,
    config: MoverConfig,

    rows_copied: u64,
    bytes_copied: u64,
    resident_bytes: Option<u64>,
    snapshot_complete: bool,
}

impl CellMover {
    /// Build a mover. `streaming` must be a client with **no request
    /// timeout**: it carries the `GET /events` subscription, which is supposed
    /// never to finish.
    pub fn new(
        source: FacetqlClient,
        destination: FacetqlClient,
        scope: CellScope,
        config: MoverConfig,
        streaming: reqwest::Client,
    ) -> Self {
        Self {
            source,
            destination,
            streaming,
            scope,
            config,
            rows_copied: 0,
            bytes_copied: 0,
            resident_bytes: None,
            snapshot_complete: false,
        }
    }

    pub fn source_id(&self) -> &str {
        &self.source.endpoint().dbms_id().0
    }

    pub fn destination_id(&self) -> &str {
        &self.destination.endpoint().dbms_id().0
    }

    /// Open the change feed.
    ///
    /// **Call this before [`CellMover::snapshot`].** An event seen before the
    /// snapshot began is a redundant re-read; an event missed after it began
    /// is a lost write, and FacetQL offers no way to discover one after the
    /// fact.
    pub async fn subscribe(&self) -> Result<ChangeFeed, MoverError> {
        let response = self
            .source
            .open_events(&self.streaming)
            .await
            .map_err(|error| self.source_error(error))?;

        let state = Arc::new(Mutex::new(FeedState::default()));
        let scope = self.scope.clone();
        let node = self.source_id().to_string();

        let task = tokio::spawn(consume_feed(
            response.bytes_stream(),
            Arc::clone(&state),
            scope,
            node,
        ));

        Ok(ChangeFeed { state, task })
    }

    /// Walk the source and write every node of the cell onto the destination.
    ///
    /// `progress` is called after every committed batch, with the running
    /// (rows, bytes) totals, so the caller can report real progress rather
    /// than a ramp. It is the caller's chance to publish; the mover does not
    /// know what a migration is.
    pub async fn snapshot(
        &mut self,
        feed: &ChangeFeed,
        mut progress: impl FnMut(u64, u64),
    ) -> Result<(), MoverError> {
        self.refuse_edges().await?;

        let passes: Vec<Option<String>> = if self.scope.is_catch_all() {
            vec![None]
        } else {
            self.scope.kinds().iter().cloned().map(Some).collect()
        };

        let mut resident = 0u64;

        for kind in passes {
            let mut after: Option<String> = None;
            let mut pages = 0usize;

            loop {
                self.require_live(feed)?;

                pages += 1;

                if pages > MAX_PAGES {
                    return Err(MoverError::Runaway { pages: MAX_PAGES });
                }

                let request = QueryRequest {
                    kind: kind.clone(),
                    limit: Some(self.config.page_limit),
                    after: after.clone(),
                    ..QueryRequest::default()
                };

                let page: QueryPage = self
                    .source
                    .query(&request)
                    .await
                    .map_err(|error| self.source_error(error))?;

                let last = !page.has_more();
                after = Some(page.next.clone());

                let rows: Vec<Node> = page
                    .nodes
                    .into_iter()
                    .filter(|node| self.scope.contains(&node.kind, &node.address))
                    .collect();

                for node in &rows {
                    resident += node.data.len() as u64;
                }

                self.write_all(&rows, &mut progress).await?;

                if last {
                    break;
                }
            }
        }

        self.resident_bytes = Some(resident);
        self.snapshot_complete = true;

        Ok(())
    }

    /// Reconcile every address the feed has marked dirty, once.
    ///
    /// Returns how many addresses were reconciled. The source's *current*
    /// value is what lands: an address the source still has is upserted, one
    /// it no longer has is deleted on the destination. Nothing here reads the
    /// change that made the address dirty, because the change is not what
    /// matters — the resulting state is, and the source is the authority on it
    /// for as long as it holds authority.
    pub async fn catch_up(&mut self, feed: &ChangeFeed) -> Result<usize, MoverError> {
        self.require_live(feed)?;

        let (addresses, observed) = feed.take();

        if addresses.is_empty() {
            feed.confirm(observed);
            return Ok(0);
        }

        let reconciled = addresses.len();

        for chunk in addresses.chunks(MULTIGET_LIMIT) {
            if let Err(error) = self.reconcile(chunk).await {
                // The addresses go back on the dirty set and `applied` does
                // not move: a failed reconciliation must not look like one
                // that happened.
                feed.restore(addresses.clone());
                return Err(error);
            }
        }

        feed.confirm(observed);

        Ok(reconciled)
    }

    /// Check the destination against the source, field by field.
    ///
    /// A count match is not enough — two instances can hold the same number of
    /// wrong rows — and a sample is not enough, because the writes most likely
    /// to have been missed are the ones a sample is least likely to land on.
    /// So this enumerates both sides in full and compares every field of every
    /// node: address, coordinate, value, kind, data, owner, claim and
    /// visibility. It is O(cell) on both instances and it runs once per
    /// attempt, which is the right price for the only check standing between a
    /// bad copy and an irreversible cutover.
    ///
    /// A `Pending` verdict comes back when the feed still has unreconciled
    /// addresses: the destination is not being claimed wrong, it is being
    /// claimed not-yet-current.
    pub async fn verify(&self, feed: &ChangeFeed, at_ms: u64) -> CopyVerdict {
        if let FeedHealth::Broken(reason) = feed.health() {
            return CopyVerdict::Failed {
                reason: MoverError::FeedBroken(reason).to_string(),
                at_ms,
            };
        }

        if !self.snapshot_complete {
            return CopyVerdict::Pending;
        }

        if feed.dirty_len() > 0 {
            return CopyVerdict::Pending;
        }

        match self.compare().await {
            Ok(rows) => {
                /*
                 * Between the two enumerations the source may have taken a
                 * write, which would make the comparison describe two moments
                 * rather than one. The feed is what says so: an address
                 * dirtied while the comparison ran means the answer is stale,
                 * not wrong, so the verdict is `Pending` and the next pass
                 * reconciles and re-checks.
                 */
                if feed.dirty_len() > 0 {
                    return CopyVerdict::Pending;
                }

                if let FeedHealth::Broken(reason) = feed.health() {
                    return CopyVerdict::Failed {
                        reason: MoverError::FeedBroken(reason).to_string(),
                        at_ms,
                    };
                }

                CopyVerdict::Verified { rows, at_ms }
            }

            Err(MoverError::NotIdentical(reason)) => {
                CopyVerdict::Failed { reason, at_ms }
            }

            // A wire failure during the check is not a verdict about the data:
            // nothing was learned, so nothing is claimed.
            Err(MoverError::Wire { .. }) => CopyVerdict::Pending,

            Err(error) => CopyVerdict::Failed {
                reason: error.to_string(),
                at_ms,
            },
        }
    }

    /// The report to feed back into the migration.
    pub fn report(&self, feed: &ChangeFeed, verdict: CopyVerdict) -> MoverReport {
        let (observed, applied) = feed.counts();

        MoverReport {
            rows_copied: self.rows_copied,
            bytes_copied: self.bytes_copied,
            resident_bytes: self.resident_bytes,
            snapshot_complete: self.snapshot_complete,
            observed_writes: observed,
            applied_writes: applied,
            verdict,
        }
    }

    // ───────────────────────────────────────────────────────────── plumbing

    /// Refuse to copy a cell with edges in it. See the module docs: an edge
    /// out of the cell cannot be reconstructed on the destination, so a
    /// "successful" copy would silently drop part of the graph.
    async fn refuse_edges(&self) -> Result<(), MoverError> {
        let stats = match self.source.stats().await {
            Ok(stats) => stats,

            /*
             * `GET /stats` is admin-gated, and the mover's credential is the
             * one that owns the data, which need not be an admin. A mover that
             * cannot ask is not a mover that may assume: it says so, and the
             * copy is refused rather than made blind to an edge set it could
             * not check for.
             */
            Err(FacetqlError::Unauthorized { .. }) => {
                return Err(MoverError::NotIdentical(format!(
                    "'{}' will not report GET /stats to this credential, so the \
                     mover cannot check whether the cell has edges — and an edge \
                     cannot be copied (FacetQL enumerates edges only per node, \
                     and an edge out of the cell cannot be written on the \
                     destination). Give the mover an admin credential, or move a \
                     cell whose edge set is known to be empty",
                    self.source_id()
                )));
            }

            Err(error) => return Err(self.source_error(error)),
        };

        if stats.edge_count > 0 {
            return Err(MoverError::NotIdentical(format!(
                "'{}' holds {} edge(s). FacetQL exposes edges only per node \
                 (GET /node/:address/edges/out) and refuses an edge whose far \
                 endpoint is not readable on the instance it is written to, so \
                 an edge leaving this cell cannot be reconstructed on the \
                 destination. Copying the nodes alone would produce a \
                 destination that looks complete and is not",
                self.source_id(),
                stats.edge_count
            )));
        }

        Ok(())
    }

    fn require_live(&self, feed: &ChangeFeed) -> Result<(), MoverError> {
        match feed.health() {
            FeedHealth::Live => Ok(()),
            FeedHealth::Broken(reason) => Err(MoverError::FeedBroken(reason)),
        }
    }

    /// Bring `addresses` on the destination to whatever the source now says.
    async fn reconcile(&self, addresses: &[String]) -> Result<(), MoverError> {
        let present = self
            .source
            .multiget(addresses)
            .await
            .map_err(|error| self.source_error(error))?;

        let found: BTreeMap<&str, &Node> = present
            .iter()
            .filter(|node| self.scope.contains(&node.kind, &node.address))
            .map(|node| (node.address.as_str(), node))
            .collect();

        let mut ops: Vec<TxOperation> = Vec::with_capacity(addresses.len());

        for address in addresses {
            match found.get(address.as_str()) {
                Some(node) => ops.push(insert_op(node)),

                /*
                 * Absent from a multiget means deleted, or no longer readable
                 * by this credential — which for a mover copying its own
                 * owner's data is the same thing. `delete_node` inside a
                 * transaction errors on an address that is not there, so the
                 * removal is only asked for when the destination actually
                 * holds it: a reconciliation of an address that never reached
                 * the destination must be a no-op, not a failed batch.
                 */
                None => {
                    let exists = self
                        .destination
                        .get_node(address)
                        .await
                        .map_err(|error| self.destination_error(error))?
                        .is_some();

                    if exists {
                        ops.push(TxOperation::DeleteNode {
                            address: address.clone(),
                        });
                    }
                }
            }
        }

        for batch in ops.chunks(self.config.batch_ops) {
            self.destination
                .transaction(batch.to_vec())
                .await
                .map_err(|error| self.destination_error(error))?;
        }

        Ok(())
    }

    /// Write `rows` to the destination in batches, counting what landed.
    async fn write_all(
        &mut self,
        rows: &[Node],
        progress: &mut impl FnMut(u64, u64),
    ) -> Result<(), MoverError> {
        let mut batch: Vec<TxOperation> = Vec::new();
        let mut batch_bytes = 0usize;
        let mut batch_rows = 0u64;

        for node in rows {
            batch.push(insert_op(node));
            batch_bytes += node.data.len();
            batch_rows += 1;

            if batch.len() >= self.config.batch_ops || batch_bytes >= self.config.batch_bytes {
                self.commit(std::mem::take(&mut batch), batch_rows, batch_bytes as u64)
                    .await?;

                progress(self.rows_copied, self.bytes_copied);

                batch_bytes = 0;
                batch_rows = 0;
            }
        }

        if !batch.is_empty() {
            self.commit(batch, batch_rows, batch_bytes as u64).await?;
            progress(self.rows_copied, self.bytes_copied);
        }

        Ok(())
    }

    async fn commit(
        &mut self,
        batch: Vec<TxOperation>,
        rows: u64,
        bytes: u64,
    ) -> Result<(), MoverError> {
        self.destination
            .transaction(batch)
            .await
            .map_err(|error| self.destination_error(error))?;

        // Counted only after the batch committed. A transaction is
        // all-or-nothing, so a batch that failed moved nothing and must not
        // move a progress bar either.
        self.rows_copied += rows;
        self.bytes_copied += bytes;

        Ok(())
    }

    /// Enumerate both instances and compare every field of every node.
    async fn compare(&self) -> Result<u64, MoverError> {
        let source = self.enumerate(&self.source, true).await?;
        let destination = self.enumerate(&self.destination, false).await?;

        if source.len() != destination.len() {
            return Err(MoverError::NotIdentical(format!(
                "'{}' holds {} node(s) in this cell and '{}' holds {}",
                self.source_id(),
                source.len(),
                self.destination_id(),
                destination.len()
            )));
        }

        for (address, expected) in &source {
            let Some(actual) = destination.get(address) else {
                return Err(MoverError::NotIdentical(format!(
                    "'{}' is missing '{address}'",
                    self.destination_id()
                )));
            };

            if let Some(field) = first_difference(expected, actual) {
                return Err(MoverError::NotIdentical(format!(
                    "'{address}' differs in `{field}` between '{}' and '{}'{}",
                    self.source_id(),
                    self.destination_id(),
                    identity_hint(field)
                )));
            }
        }

        Ok(source.len() as u64)
    }

    async fn enumerate(
        &self,
        client: &FacetqlClient,
        is_source: bool,
    ) -> Result<BTreeMap<String, Node>, MoverError> {
        let passes: Vec<Option<String>> = if self.scope.is_catch_all() {
            vec![None]
        } else {
            self.scope.kinds().iter().cloned().map(Some).collect()
        };

        let mut out = BTreeMap::new();

        for kind in passes {
            let mut after: Option<String> = None;
            let mut pages = 0usize;

            loop {
                pages += 1;

                if pages > MAX_PAGES {
                    return Err(MoverError::Runaway { pages: MAX_PAGES });
                }

                let request = QueryRequest {
                    kind: kind.clone(),
                    limit: Some(self.config.page_limit),
                    after,
                    ..QueryRequest::default()
                };

                let page = client.query(&request).await.map_err(|error| {
                    if is_source {
                        self.source_error(error)
                    } else {
                        self.destination_error(error)
                    }
                })?;

                let last = !page.has_more();
                after = Some(page.next);

                for node in page.nodes {
                    if self.scope.contains(&node.kind, &node.address) {
                        out.insert(node.address.clone(), node);
                    }
                }

                if last {
                    break;
                }
            }
        }

        Ok(out)
    }

    fn source_error(&self, error: FacetqlError) -> MoverError {
        MoverError::Wire {
            node: self.source_id().to_string(),
            error,
        }
    }

    fn destination_error(&self, error: FacetqlError) -> MoverError {
        MoverError::Wire {
            node: self.destination_id().to_string(),
            error,
        }
    }
}

/// Safety valve on cursor-following, matching [`crate::client`]'s.
const MAX_PAGES: usize = 10_000;

/// The upsert that reproduces `node` on another instance.
///
/// Every field `insert_node` can carry is carried. The two it cannot —
/// `owner`, which FacetQL stamps from the writer's identity, and `claimed_by`,
/// which has no wire field at all — are not silently dropped: they are what
/// [`first_difference`] checks, so a node the mover cannot faithfully
/// reproduce fails the verification instead of arriving subtly wrong.
fn insert_op(node: &Node) -> TxOperation {
    TxOperation::InsertNode {
        address: node.address.clone(),
        kind: node.kind.clone(),
        x: node.coordinate.x,
        y: node.coordinate.y,
        z: node.coordinate.z,
        q: node.coordinate.q,
        data: node.data.clone(),
        public: matches!(node.visibility, Visibility::Public),
    }
}

/// The first field on which two nodes disagree, if any.
fn first_difference(left: &Node, right: &Node) -> Option<&'static str> {
    if left.address != right.address {
        return Some("address");
    }
    if left.kind != right.kind {
        return Some("kind");
    }
    if left.data != right.data {
        return Some("data");
    }
    if left.coordinate != right.coordinate {
        return Some("coordinate");
    }
    if left.value != right.value {
        return Some("value");
    }
    if left.owner != right.owner {
        return Some("owner");
    }
    if left.claimed_by != right.claimed_by {
        return Some("claimed_by");
    }
    if left.visibility != right.visibility {
        return Some("visibility");
    }

    None
}

/// The two fields whose mismatch has one known cause worth naming, so an
/// operator reads a diagnosis rather than a symptom.
fn identity_hint(field: &str) -> &'static str {
    match field {
        "owner" => {
            ". `insert_node` stamps the writing identity as the owner and has no \
             `owner` field, so a cell can only be moved by a credential whose \
             owner already owns its nodes"
        }

        "claimed_by" => {
            ". A claim (POST /node/:address/claim) has no representation in \
             `insert_node`, so it cannot be carried across instances"
        }

        _ => "",
    }
}

/// Drive one SSE byte stream into the feed's state until it ends.
async fn consume_feed<S>(
    stream: S,
    state: Arc<Mutex<FeedState>>,
    scope: CellScope,
    node: String,
) where
    S: Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
{
    let mut stream = Box::pin(stream);
    let mut buffer = BytesMut::new();

    let broken = loop {
        while let Some(end) = frame_end(&buffer) {
            let frame = buffer.split_to(end);
            let Ok(text) = std::str::from_utf8(&frame) else {
                continue;
            };

            let addresses: Vec<String> = changed_addresses(text)
                .into_iter()
                .filter(|address| scope.may_contain_address(address))
                .collect();

            if addresses.is_empty() {
                continue;
            }

            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            // One event is one write, whether or not the address was already
            // dirty: the count is the migration's sequence space, and
            // collapsing two writes into one there would let the destination
            // claim it had drained a gap it had not seen the whole of.
            state.observed += addresses.len() as u64;
            state.dirty.extend(addresses);
        }

        match stream.next().await {
            Some(Ok(chunk)) => buffer.extend_from_slice(&chunk),

            Some(Err(error)) => {
                break format!("the stream from '{node}' failed: {error}");
            }

            None => break format!("the stream from '{node}' ended"),
        }
    };

    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    state.broken = Some(broken);
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabric_core::Coordinate as Cell;
    use fabric_routing::RoutingKey;

    use crate::frontdoor::KeyspaceRule;
    use crate::wire::Coordinate;

    fn key(shard: u64) -> RoutingKey {
        RoutingKey::new(shard, Cell::new(0, 0)).unwrap()
    }

    fn node(address: &str, kind: &str, data: &str) -> Node {
        Node {
            address: address.to_string(),
            coordinate: Coordinate::default(),
            value: 0,
            kind: kind.to_string(),
            data: data.to_string(),
            owner: "app".to_string(),
            claimed_by: None,
            visibility: Visibility::Private,
        }
    }

    #[test]
    fn a_declared_cell_holds_its_own_kinds_and_nothing_else() {
        let keyspace = Keyspace::new()
            .with_rule(KeyspaceRule::new("Post", "Post:", key(1)).unwrap())
            .unwrap()
            .with_rule(KeyspaceRule::new("Session", "Session:", key(2)).unwrap())
            .unwrap();

        let scope = CellScope::for_key(&keyspace, key(1));

        assert_eq!(scope.kinds(), ["Post"]);
        assert!(scope.contains("Post", "Post:1"));
        assert!(!scope.contains("Session", "Session:1"));
        assert!(scope.may_contain_address("Post:1"));
        assert!(!scope.may_contain_address("Session:1"));
    }

    /// The single-instance deployment: no rules, one fallback. Enumeration is
    /// one unfiltered pass, because there is no kind list to walk and asking
    /// `GET /stats` for one would need an admin credential the data's owner
    /// need not have.
    #[test]
    fn a_fallback_cell_holds_everything_no_other_rule_claims() {
        let scope = CellScope::for_key(&Keyspace::single(key(1)), key(1));

        assert!(scope.is_catch_all());
        assert!(scope.kinds().is_empty());
        assert!(scope.contains("anything", "at:all"));

        let split = Keyspace::new()
            .with_rule(KeyspaceRule::new("Session", "Session:", key(2)).unwrap())
            .unwrap()
            .with_fallback(key(1));

        let scope = CellScope::for_key(&split, key(1));

        assert!(scope.is_catch_all());
        assert!(scope.contains("Post", "Post:1"));
        assert!(!scope.contains("Session", "Session:1"));
        assert!(!scope.may_contain_address("Session:9"));
    }

    #[test]
    fn every_write_shaped_event_yields_the_addresses_it_names() {
        assert_eq!(
            changed_addresses("data: {\"event\":\"node_created\",\"address\":\"Post:1\",\"kind\":\"Post\"}\n\n"),
            ["Post:1"]
        );
        assert_eq!(
            changed_addresses("data: {\"event\":\"node_updated\",\"address\":\"Post:2\"}\n\n"),
            ["Post:2"]
        );
        assert_eq!(
            changed_addresses("data: {\"event\":\"node_deleted\",\"address\":\"Post:3\"}\n\n"),
            ["Post:3"]
        );
        assert_eq!(
            changed_addresses(
                "data: {\"event\":\"transaction_committed\",\"addresses\":[\"a\",\"b\"]}\n\n"
            ),
            ["a", "b"]
        );
    }

    /// Edge, user and `POST /publish` messages share the stream. A mover that
    /// treated one as a node change would re-read an address that is not one,
    /// and one that treated it as an error would fail a migration because
    /// somebody created a login.
    #[test]
    fn an_event_that_is_not_a_node_write_contributes_nothing() {
        assert!(changed_addresses("data: {\"event\":\"edge_created\",\"from\":\"a\",\"to\":\"b\"}\n\n").is_empty());
        assert!(changed_addresses("data: {\"event\":\"user_created\",\"owner\":\"bob\"}\n\n").is_empty());
        assert!(changed_addresses("data: not json\n\n").is_empty());
        assert!(changed_addresses(": keep-alive\n\n").is_empty());
    }

    #[test]
    fn an_insert_op_carries_every_field_insert_node_has() {
        let mut source = node("Post:1", "Post", "{\"n\":1}");
        source.coordinate = Coordinate { x: 1, y: 2, z: 3, q: 4 };
        source.visibility = Visibility::Public;

        let encoded = serde_json::to_value(insert_op(&source)).unwrap();

        assert_eq!(
            encoded,
            serde_json::json!({
                "type": "insert_node", "address": "Post:1", "kind": "Post",
                "x": 1, "y": 2, "z": 3, "q": 4, "data": "{\"n\":1}", "public": true
            })
        );
    }

    /// The two fields `insert_node` cannot carry are the two the check exists
    /// to catch. A copy that quietly re-owned somebody's data would pass a
    /// count and pass a checksum of `data`; it does not pass this.
    #[test]
    fn the_check_catches_the_fields_insert_node_cannot_carry() {
        let source = node("Post:1", "Post", "{}");

        let mut reowned = source.clone();
        reowned.owner = "fabric".to_string();
        assert_eq!(first_difference(&source, &reowned), Some("owner"));
        assert!(
            identity_hint("owner").contains("`owner` field"),
            "the owner mismatch must name its cause: {}",
            identity_hint("owner")
        );

        let mut unclaimed = source.clone();
        unclaimed.claimed_by = Some("worker-1".to_string());
        assert_eq!(first_difference(&source, &unclaimed), Some("claimed_by"));

        let mut edited = source.clone();
        edited.data = "{\"n\":2}".to_string();
        assert_eq!(first_difference(&source, &edited), Some("data"));

        assert_eq!(first_difference(&source, &source.clone()), None);
    }

    #[test]
    fn sse_frames_are_split_on_the_blank_line_in_either_encoding() {
        assert_eq!(frame_end(b"data: x\n\ndata: y\n\n"), Some(9));
        assert_eq!(frame_end(b"data: x\r\n\r\n"), Some(11));
        assert_eq!(frame_end(b"data: incomplete\n"), None);
    }
}
