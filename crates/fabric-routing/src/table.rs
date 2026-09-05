//! The routing table: the Fabric's map from logical identity to current
//! physical home.
//!
//! The table is **derived state**. Everything in it comes from somewhere else —
//! placement from [`Topology`] and [`TopologyRegistry`], replica roles and
//! freshness from [`ReplicaSet`], in-flight moves from [`Migration`], liveness
//! from the controller's node registry — and it can be thrown away and rebuilt
//! from those sources at any time. That is why it derives no `Serialize`: a
//! persisted routing table could disagree with the sources it was derived from,
//! and a routing table that disagrees with reality is worse than no table.
//!
//! Every mutation bumps [`RoutingTable::generation`], which is stamped onto
//! every [`Route`]. A client caching routes compares generations to know when
//! its cache went stale.

use std::collections::{BTreeMap, HashMap, btree_map};

use fabric_core::{Coordinate, DbmsId, Topology};
use fabric_migration::Migration;
use fabric_replication::ReplicaSet;
use fabric_topology::TopologyRegistry;

use crate::{
    error::RoutingError,
    key::RoutingKey,
    range::{CoordinateRange, RouteSegment},
    route::{ReadPreference, Route, RouteIntent, RouteKind, RouteRequest, ServedBy},
};

/// What the controller has most recently established about a node's liveness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeAvailability {
    /// Proven fit to serve.
    Serviceable,

    /// Proven not fit to serve. Never routed to.
    Unreachable,

    /// Nothing has been established. Routable by default — placement is what
    /// this crate knows, liveness is what the controller knows — unless the
    /// table is set to [`RoutingTable::require_proven_health`].
    Unknown,
}

/// A node the table can route to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntry {
    pub id: DbmsId,
    pub region: String,
    pub availability: NodeAvailability,
}

/// The part of an in-flight migration that routing needs.
///
/// A snapshot, not a borrow: the controller owns the [`Migration`] and drives
/// it, and re-snapshots into the table after each phase change. Routing never
/// advances a migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMigration {
    pub shard_id: u64,
    pub source: DbmsId,
    pub destination: DbmsId,

    /// Whether authority has moved to the destination.
    pub has_cut_over: bool,

    /// `None` while the source is fenced for cutover.
    pub write_owner: Option<DbmsId>,
}

impl ShardMigration {
    pub fn from_migration(migration: &Migration) -> Self {
        Self {
            shard_id: migration.plan().shard_id,
            source: migration.plan().source.clone(),
            destination: migration.plan().destination.clone(),
            has_cut_over: migration.has_cut_over(),
            write_owner: migration.write_owner().cloned(),
        }
    }
}

/// Everything the table knows about one shard.
///
/// ## Two scopes, and why the shard-wide one is not enough on its own
///
/// A replica set and an in-flight migration used to live on this struct with
/// no scoping at all, which made them statements about *every* cell of the
/// shard. But the controller's action target and [`TopologyRegistry`] are
/// coordinate-level: the Fabric replicates and moves one cell at a time.
/// Publishing a one-cell migration
/// shard-wide made routing answer for the shard's other cells too — and after
/// cutover it sent their reads to a node that never held their data.
///
/// So both now exist at two scopes:
///
/// * **shard-wide** (`scope: None`) — the answer for every cell that has no
///   cell-scoped answer of its own. This is what the protocol's topology
///   ingest produces (single-celled shards) and what whole-shard callers mean.
/// * **cell-scoped** (`scope: Some(coordinate)`) — overrides the shard-wide
///   answer for that one cell and touches no other.
///
/// Resolution reads them through [`Self::replica_set_at`] and
/// [`Self::migration_at`], which are the only two places the precedence is
/// expressed.
#[derive(Debug, Clone)]
pub struct ShardEntry {
    pub shard_id: u64,

    owner: DbmsId,
    extra_owners: Vec<DbmsId>,
    overrides: HashMap<Coordinate, DbmsId>,
    replicas: Option<ReplicaSet>,
    migration: Option<ShardMigration>,
    cell_replicas: HashMap<Coordinate, ReplicaSet>,
    cell_migrations: HashMap<Coordinate, ShardMigration>,
}

impl ShardEntry {
    fn new(shard_id: u64, owner: DbmsId) -> Self {
        Self {
            shard_id,
            owner,
            extra_owners: Vec::new(),
            overrides: HashMap::new(),
            replicas: None,
            migration: None,
            cell_replicas: HashMap::new(),
            cell_migrations: HashMap::new(),
        }
    }

    /// The node topology says holds this shard.
    pub fn owner(&self) -> &DbmsId {
        &self.owner
    }

    /// Further nodes that also claim to hold this shard.
    ///
    /// Usually this means a replica set exists and has not been registered
    /// here yet. Routing does not use these: a copy of unknown freshness is
    /// not a copy you can serve a read from.
    pub fn extra_owners(&self) -> &[DbmsId] {
        &self.extra_owners
    }

    /// The set published for the whole shard, if one was.
    ///
    /// This is *not* what a lookup uses — see [`Self::replica_set_at`], which
    /// lets a cell-scoped set win.
    pub fn replica_set(&self) -> Option<&ReplicaSet> {
        self.replicas.as_ref()
    }

    /// The migration published for the whole shard, if one was.
    pub fn migration(&self) -> Option<&ShardMigration> {
        self.migration.as_ref()
    }

    /// The set that decides this cell's read and write targets: its own if one
    /// was published for it, otherwise the shard's.
    pub fn replica_set_at(&self, coordinate: Coordinate) -> Option<&ReplicaSet> {
        self.cell_replicas
            .get(&coordinate)
            .or(self.replicas.as_ref())
    }

    /// The migration that is moving this cell, if any: its own if one was
    /// published for it, otherwise the shard's.
    pub fn migration_at(
        &self,
        coordinate: Coordinate,
    ) -> Option<&ShardMigration> {
        self.cell_migrations
            .get(&coordinate)
            .or(self.migration.as_ref())
    }

    /// Cells this shard holds a scoped answer for, in grid order. A cell not
    /// listed here is answered for by the shard-wide state.
    pub fn scoped_cells(&self) -> Vec<Coordinate> {
        let mut cells: Vec<Coordinate> = self
            .cell_replicas
            .keys()
            .chain(self.cell_migrations.keys())
            .copied()
            .collect();

        cells.sort_by_key(|coordinate| coordinate.index());
        cells.dedup();
        cells
    }

    /// The node holding one specific cell: a coordinate-level placement if one
    /// is registered, otherwise the shard's owner.
    pub fn owner_of(&self, coordinate: Coordinate) -> &DbmsId {
        self.overrides
            .get(&coordinate)
            .unwrap_or(&self.owner)
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    node: DbmsId,
    served_by: ServedBy,
}

/// Re-label the migrating copy so a caller can see the route is about to
/// change. A route through a node that is mid-migration is short-lived, and a
/// client caching routes needs to know that before it caches one.
fn relabel(candidates: &mut [Candidate], node: &DbmsId, served_by: ServedBy) {
    for candidate in candidates.iter_mut() {
        if &candidate.node == node {
            candidate.served_by = served_by;
        }
    }
}

/// The lookup structure.
#[derive(Debug, Clone, Default)]
pub struct RoutingTable {
    nodes: BTreeMap<String, NodeEntry>,
    shards: BTreeMap<u64, ShardEntry>,
    generation: u64,
    require_proven_health: bool,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a table from a topology snapshot.
    pub fn from_topology(topology: &Topology) -> Self {
        let mut table = Self::new();
        table.apply_topology(topology);
        table
    }

    /// Bumped by every mutation; stamped onto every route.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// When set, a node whose health has never been established is not routed
    /// to. Fail-closed, for deployments where the controller is guaranteed to
    /// be feeding health.
    pub fn require_proven_health(&mut self, required: bool) {
        self.require_proven_health = required;
        self.generation += 1;
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeEntry> {
        self.nodes.values()
    }

    pub fn shards(&self) -> impl Iterator<Item = &ShardEntry> {
        self.shards.values()
    }

    pub fn shard(&self, shard_id: u64) -> Option<&ShardEntry> {
        self.shards.get(&shard_id)
    }

    pub fn node(&self, id: &DbmsId) -> Option<&NodeEntry> {
        self.nodes.get(&id.0)
    }

    /// Add or update a node, preserving any health already established for it.
    pub fn register_node(&mut self, id: DbmsId, region: impl Into<String>) {
        let region = region.into();

        match self.nodes.get_mut(&id.0) {
            Some(entry) => entry.region = region,

            None => {
                self.nodes.insert(
                    id.0.clone(),
                    NodeEntry {
                        id,
                        region,
                        availability: NodeAvailability::Unknown,
                    },
                );
            }
        }

        self.generation += 1;
    }

    /// Feed in liveness. The controller owns this judgement (it holds the
    /// heartbeat registry); routing only applies it.
    pub fn set_availability(&mut self, id: &DbmsId, availability: NodeAvailability) -> bool {
        let Some(entry) = self.nodes.get_mut(&id.0) else {
            return false;
        };

        entry.availability = availability;
        self.generation += 1;

        true
    }

    /// Rebuild shard placement from a topology snapshot.
    ///
    /// Topology is the authority on placement, so ownership is replaced rather
    /// than merged — otherwise a shard that moved would keep routing to where
    /// it used to be. Replica sets, migrations and coordinate-level placements
    /// are kept: they come from other sources and topology says nothing about
    /// them. A shard the snapshot does not mention keeps the placement it had;
    /// silence is not evidence that a shard moved.
    pub fn apply_topology(&mut self, topology: &Topology) {
        for node in &topology.nodes {
            self.register_node(node.id.clone(), node.region.clone());

            for shard in &node.shards {
                match self.shards.get_mut(&shard.id) {
                    None => {
                        self.shards.insert(
                            shard.id,
                            ShardEntry::new(shard.id, node.id.clone()),
                        );
                    }

                    Some(entry) => {
                        if entry.owner == node.id {
                            continue;
                        }

                        if !entry.extra_owners.contains(&node.id) {
                            entry.extra_owners.push(node.id.clone());
                        }
                    }
                }
            }
        }

        /*
         * Ownership is recomputed after the walk rather than during it, so the
         * result does not depend on the order nodes happen to appear in: the
         * first node listing a shard owns it, every other listing is an extra.
         */
        let mut first_owner: BTreeMap<u64, DbmsId> = BTreeMap::new();

        for node in &topology.nodes {
            for shard in &node.shards {
                first_owner
                    .entry(shard.id)
                    .or_insert_with(|| node.id.clone());
            }
        }

        for (shard_id, owner) in first_owner {
            if let Some(entry) = self.shards.get_mut(&shard_id) {
                entry.owner = owner.clone();
                entry.extra_owners.retain(|node| node != &owner);
                entry.extra_owners.sort_by(|left, right| left.0.cmp(&right.0));
            }
        }

        self.generation += 1;
    }

    /// Apply coordinate-level placements.
    ///
    /// These are what make a shard's grid splittable across nodes: a cell with
    /// a placement is served by that node, everything else by the shard owner.
    pub fn apply_placements(&mut self, registry: &TopologyRegistry) {
        for placement in registry.placements() {
            self.register_node(placement.dbms_id.clone(), placement.region.clone());

            let entry = self
                .shards
                .entry(placement.shard_id)
                .or_insert_with(|| {
                    ShardEntry::new(placement.shard_id, placement.dbms_id.clone())
                });

            entry
                .overrides
                .insert(placement.coordinate, placement.dbms_id.clone());
        }

        self.generation += 1;
    }

    /// Register a replica set, which from then on decides read and write
    /// targets for whatever it is scoped to.
    ///
    /// `scope` is the whole point. `None` means "every cell of this shard" —
    /// the meaning the method has always had, and the one the protocol's
    /// topology ingest relies on, since it produces single-celled shards.
    /// `Some(coordinate)` means "this cell only": it overrides the shard-wide
    /// answer for that cell and leaves every sibling exactly as it was.
    ///
    /// Returns `false` when the publication cannot be attached to anything:
    /// a shard-wide set that is empty for a shard the table does not know
    /// (there is no node to name as owner), or a cell-scoped set for a shard
    /// the table does not know. The second refusal matters: a cell-scoped set
    /// says nothing about the shard's other cells, so inventing a shard-wide
    /// owner from it would be exactly the over-broad answer this scoping
    /// exists to prevent. `UnknownShard` is the honest answer until topology
    /// has been fed in.
    pub fn set_replica_set(
        &mut self,
        set: ReplicaSet,
        scope: Option<Coordinate>,
    ) -> bool {
        let shard_id = set.shard_id;

        match scope {
            None => {
                if let btree_map::Entry::Vacant(slot) =
                    self.shards.entry(shard_id)
                {
                    let owner = set
                        .primary()
                        .or_else(|| set.replicas().next())
                        .map(|replica| replica.node.clone());

                    let Some(owner) = owner else {
                        return false;
                    };

                    slot.insert(ShardEntry::new(shard_id, owner));
                }
            }

            Some(coordinate) => {
                if !coordinate.is_valid()
                    || !self.shards.contains_key(&shard_id)
                {
                    return false;
                }
            }
        }

        for replica in set.replicas() {
            if !self.nodes.contains_key(&replica.node.0) {
                self.register_node(replica.node.clone(), replica.region.clone());
            }
        }

        if let Some(entry) = self.shards.get_mut(&shard_id) {
            match scope {
                None => entry.replicas = Some(set),

                Some(coordinate) => {
                    entry.cell_replicas.insert(coordinate, set);
                }
            }
        }

        self.generation += 1;

        true
    }

    /// Forget a replica set at the scope it was published at.
    ///
    /// Clearing the shard-wide set does not clear a cell's own, and vice
    /// versa: they are separate publications and either may outlive the other.
    pub fn clear_replica_set(
        &mut self,
        shard_id: u64,
        scope: Option<Coordinate>,
    ) {
        let Some(entry) = self.shards.get_mut(&shard_id) else {
            return;
        };

        let cleared = match scope {
            None => entry.replicas.take().is_some(),
            Some(coordinate) => entry.cell_replicas.remove(&coordinate).is_some(),
        };

        if cleared {
            self.generation += 1;
        }
    }

    /// Take a snapshot of a migration so routes stay correct while the data
    /// moves.
    ///
    /// Call this after every phase change. `scope` carries the same meaning as
    /// on [`Self::set_replica_set`]: `None` moves the whole shard,
    /// `Some(coordinate)` moves one cell and must not be visible to its
    /// siblings.
    ///
    /// A terminal migration is folded in and dropped, and *where* it is folded
    /// is what the scope decides. A shard-wide move that cut over changes the
    /// shard's owner; a cell-scoped move that cut over changes that cell's
    /// coordinate placement and nothing else — writing the destination into
    /// `owner` there would hand every sibling cell to a node that never held
    /// their data, which is the bug this scoping exists to close. A move that
    /// was aborted or failed before cutover leaves both untouched, because
    /// nothing moved.
    ///
    /// Returns `false` when a cell-scoped snapshot names a shard the table
    /// does not know, or an off-grid coordinate — the same refusal, for the
    /// same reason, as [`Self::set_replica_set`].
    pub fn observe_migration(
        &mut self,
        migration: &Migration,
        scope: Option<Coordinate>,
    ) -> bool {
        let snapshot = ShardMigration::from_migration(migration);
        let shard_id = snapshot.shard_id;
        let terminal = migration.is_terminal();

        let entry = match scope {
            None => self.shards.entry(shard_id).or_insert_with(|| {
                ShardEntry::new(shard_id, snapshot.source.clone())
            }),

            Some(coordinate) => {
                if !coordinate.is_valid() {
                    return false;
                }

                let Some(entry) = self.shards.get_mut(&shard_id) else {
                    return false;
                };

                entry
            }
        };

        match scope {
            None => {
                if terminal {
                    if snapshot.has_cut_over {
                        entry.owner = snapshot.destination.clone();
                        entry
                            .extra_owners
                            .retain(|node| node != &snapshot.destination);
                    }

                    entry.migration = None;
                } else {
                    entry.migration = Some(snapshot);
                }
            }

            Some(coordinate) => {
                if terminal {
                    if snapshot.has_cut_over {
                        entry
                            .overrides
                            .insert(coordinate, snapshot.destination.clone());
                    }

                    entry.cell_migrations.remove(&coordinate);
                } else {
                    entry.cell_migrations.insert(coordinate, snapshot);
                }
            }
        }

        self.generation += 1;

        true
    }

    /// Forget a shard entirely. Its keys become [`RoutingError::UnknownShard`].
    pub fn forget_shard(&mut self, shard_id: u64) -> bool {
        let removed = self.shards.remove(&shard_id).is_some();

        if removed {
            self.generation += 1;
        }

        removed
    }

    /// Resolve one key to the node that should serve it.
    ///
    /// The first of [`RoutingTable::candidates`]. Which of several equally
    /// valid copies to use is a load-balancing decision this crate does not
    /// make; it returns a deterministic first choice and the full list beside
    /// it.
    pub fn resolve(&self, request: &RouteRequest) -> Result<Route, RoutingError> {
        self.candidates(request).map(|mut routes| routes.remove(0))
    }

    /// Every node that could serve this request, best first.
    pub fn candidates(&self, request: &RouteRequest) -> Result<Vec<Route>, RoutingError> {
        let entry = self
            .shards
            .get(&request.key.shard_id)
            .ok_or(RoutingError::UnknownShard {
                shard_id: request.key.shard_id,
            })?;

        if !request.key.coordinate.is_valid() {
            return Err(RoutingError::InvalidCoordinate {
                x: request.key.coordinate.x,
                y: request.key.coordinate.y,
            });
        }

        self.candidates_at(entry, request.key.coordinate, &request.intent)
    }

    /// Resolve a read with an explicit freshness requirement.
    pub fn resolve_read(
        &self,
        key: &RoutingKey,
        preference: ReadPreference,
    ) -> Result<Route, RoutingError> {
        self.resolve(&RouteRequest::read(*key, preference))
    }

    /// Resolve a write.
    ///
    /// Not the same lookup as a read: with a replica set only the in-sync
    /// primary qualifies, and during a cutover this returns
    /// [`RoutingError::WriteFenced`] — a retry, not a failure — where a read
    /// still resolves normally.
    pub fn resolve_write(&self, key: &RoutingKey) -> Result<Route, RoutingError> {
        self.resolve(&RouteRequest::write(*key))
    }

    /// Resolve a range of one shard's grid into per-node segments.
    ///
    /// Every cell is resolved on its own through [`Self::candidates_at`], so
    /// cell-scoped replica sets and migrations are honoured here exactly as
    /// they are for a single key: a range crossing a cell that is mid-move
    /// gets that cell's own answer, and its siblings keep theirs. Adjacent
    /// cells collapse into one segment only when they agree on *both* the node
    /// and why it is the answer, so a cell being served by a migration
    /// destination never hides inside a run of ordinary owner routes — a
    /// client caching the segment needs to know that part of it is
    /// short-lived.
    ///
    /// All-or-nothing, and now more load-bearing than before: cells of one
    /// shard can be owned by different nodes and can be in different migration
    /// phases, so there are more ways for one cell of a range to be
    /// unanswerable than there used to be. Every one of them fails the whole
    /// call — an unroutable owner, an off-grid coordinate, a fenced cell under
    /// a write intent — because a scan that quietly skipped a cell would
    /// return a subset of the data and look like a complete answer, and a
    /// range write that silently dropped its fenced cells would lose them.
    /// Nothing is returned until the last cell has resolved.
    pub fn resolve_range(
        &self,
        shard_id: u64,
        range: &CoordinateRange,
        intent: &RouteIntent,
    ) -> Result<Vec<RouteSegment>, RoutingError> {
        let entry = self
            .shards
            .get(&shard_id)
            .ok_or(RoutingError::UnknownShard { shard_id })?;

        let mut segments: Vec<RouteSegment> = Vec::new();

        for coordinate in range.coordinates() {
            let route = self
                .candidates_at(entry, coordinate, intent)?
                .remove(0);

            match segments.last_mut() {
                Some(last)
                    if last.route.node == route.node
                        && last.route.served_by == route.served_by =>
                {
                    last.range = CoordinateRange::new(last.range.start(), coordinate)?;
                }

                _ => segments.push(RouteSegment {
                    range: CoordinateRange::new(coordinate, coordinate)?,
                    route: Route {
                        coordinate: None,
                        ..route
                    },
                }),
            }
        }

        Ok(segments)
    }

    /// A full scan of several shards, as segments.
    ///
    /// The same all-or-nothing rule, one level up: a shard the table does not
    /// know, or a single unanswerable cell in any of them, fails the whole
    /// scan rather than returning the shards that happened to resolve.
    pub fn resolve_scan(
        &self,
        shard_ids: &[u64],
        intent: &RouteIntent,
    ) -> Result<Vec<RouteSegment>, RoutingError> {
        let mut segments = Vec::new();

        for shard_id in shard_ids {
            segments.extend(self.resolve_range(
                *shard_id,
                &CoordinateRange::full(),
                intent,
            )?);
        }

        Ok(segments)
    }

    /// The one place a lookup turns a cell into candidate nodes.
    ///
    /// Every resolve path funnels through here — `resolve`/`resolve_read`/
    /// `resolve_write` for one key, `resolve_range` once per cell of a range,
    /// and `resolve_scan` through `resolve_range` — which is why coordinate
    /// scoping only has to be applied once, in
    /// [`ShardEntry::replica_set_at`] and [`ShardEntry::migration_at`], to
    /// hold for all four. A second entry point that read `entry.replicas`
    /// directly would be a second answer to "who serves this cell", and the
    /// two would eventually disagree.
    fn candidates_at(
        &self,
        entry: &ShardEntry,
        coordinate: Coordinate,
        intent: &RouteIntent,
    ) -> Result<Vec<Route>, RoutingError> {
        let candidates = match intent {
            RouteIntent::Write => self.write_candidates(entry, coordinate)?,
            RouteIntent::Read(preference) => {
                self.read_candidates(entry, coordinate, preference)?
            }
        };

        self.into_routes(entry.shard_id, coordinate, intent.kind(), candidates)
    }

    /// Writes have exactly one correct destination, so this returns at most one
    /// candidate. Precedence: a migration of the write-side copy, then the
    /// replica set's primary, then the shard's placement.
    fn write_candidates(
        &self,
        entry: &ShardEntry,
        coordinate: Coordinate,
    ) -> Result<Vec<Candidate>, RoutingError> {
        let replicas = entry.replica_set_at(coordinate);

        if let Some(migration) = entry.migration_at(coordinate) {
            /*
             * A migration only overrides the write path when it is the
             * write-side copy that is moving. Moving a secondary leaves the
             * primary exactly where it was, and writes should not notice.
             */
            let write_copy_is_moving = match replicas.and_then(|set| set.primary()) {
                Some(primary) => {
                    primary.node == migration.source || primary.node == migration.destination
                }

                // No replica set: the single copy is the one being moved.
                None => true,
            };

            if write_copy_is_moving {
                let Some(owner) = migration.write_owner.clone() else {
                    return Err(RoutingError::WriteFenced {
                        shard_id: entry.shard_id,
                        source: migration.source.clone(),
                        destination: migration.destination.clone(),
                    });
                };

                let served_by = if owner == migration.destination {
                    ServedBy::MigrationDestination
                } else {
                    ServedBy::MigrationSource
                };

                return Ok(vec![Candidate {
                    node: owner,
                    served_by,
                }]);
            }
        }

        if let Some(set) = replicas {
            let replica = set
                .write_target()
                .ok_or(RoutingError::NoWritableReplica {
                    shard_id: entry.shard_id,
                })?;

            return Ok(vec![Candidate {
                node: replica.node.clone(),
                served_by: ServedBy::Primary,
            }]);
        }

        Ok(vec![Candidate {
            node: entry.owner_of(coordinate).clone(),
            served_by: ServedBy::ShardOwner,
        }])
    }

    fn read_candidates(
        &self,
        entry: &ShardEntry,
        coordinate: Coordinate,
        preference: &ReadPreference,
    ) -> Result<Vec<Candidate>, RoutingError> {
        let mut candidates: Vec<Candidate> = match entry.replica_set_at(coordinate) {
            Some(set) => match preference {
                ReadPreference::Primary => set
                    .write_target()
                    .map(|replica| {
                        vec![Candidate {
                            node: replica.node.clone(),
                            served_by: ServedBy::Primary,
                        }]
                    })
                    .unwrap_or_default(),

                _ => set
                    .read_targets(preference.allows_stale())
                    .into_iter()
                    .map(|replica| Candidate {
                        node: replica.node.clone(),
                        served_by: if replica.is_primary() {
                            ServedBy::Primary
                        } else {
                            ServedBy::Secondary
                        },
                    })
                    .collect(),
            },

            None => vec![Candidate {
                node: entry.owner_of(coordinate).clone(),
                served_by: ServedBy::ShardOwner,
            }],
        };

        /*
         * Reads stay servable throughout a migration, and this is where that is
         * enforced: the destination is not a read target until cutover has
         * completed, and the source stops being one the moment it has. There
         * is no window in which the list is empty because the data is "in
         * transit" -- one side is always authoritative.
         */
        if let Some(migration) = entry.migration_at(coordinate) {
            if migration.has_cut_over {
                candidates.retain(|candidate| candidate.node != migration.source);

                if !candidates
                    .iter()
                    .any(|candidate| candidate.node == migration.destination)
                {
                    candidates.insert(
                        0,
                        Candidate {
                            node: migration.destination.clone(),
                            served_by: ServedBy::MigrationDestination,
                        },
                    );
                }

                relabel(&mut candidates, &migration.destination, ServedBy::MigrationDestination);
            } else {
                candidates.retain(|candidate| candidate.node != migration.destination);

                if candidates.is_empty() {
                    candidates.push(Candidate {
                        node: migration.source.clone(),
                        served_by: ServedBy::MigrationSource,
                    });
                }

                relabel(&mut candidates, &migration.source, ServedBy::MigrationSource);
            }
        }

        if candidates.is_empty() {
            return Err(RoutingError::NoReadableReplica {
                shard_id: entry.shard_id,
            });
        }

        if let ReadPreference::PreferRegion { region } = preference {
            // Stable: same-region copies move to the front, the rest keep the
            // freshness order the replica set gave them.
            candidates.sort_by_key(|candidate| {
                let same_region = self
                    .nodes
                    .get(&candidate.node.0)
                    .map(|node| &node.region == region)
                    .unwrap_or(false);

                u8::from(!same_region)
            });
        }

        Ok(candidates)
    }

    /// Drop candidates that cannot serve, and turn the rest into routes.
    fn into_routes(
        &self,
        shard_id: u64,
        coordinate: Coordinate,
        kind: RouteKind,
        candidates: Vec<Candidate>,
    ) -> Result<Vec<Route>, RoutingError> {
        let mut routes = Vec::new();
        let mut considered = Vec::new();
        let mut unknown: Option<DbmsId> = None;

        for candidate in candidates {
            considered.push(candidate.node.clone());

            let Some(node) = self.nodes.get(&candidate.node.0) else {
                unknown = Some(candidate.node.clone());
                continue;
            };

            if !self.is_routable(node) {
                continue;
            }

            routes.push(Route {
                shard_id,
                coordinate: Some(coordinate),
                node: node.id.clone(),
                region: node.region.clone(),
                served_by: candidate.served_by,
                kind,
                generation: self.generation,
            });
        }

        if routes.is_empty() {
            if let Some(node) = unknown
                && considered.len() == 1
            {
                return Err(RoutingError::UnknownNode { node });
            }

            return Err(RoutingError::NoLiveOwner {
                shard_id,
                considered,
            });
        }

        Ok(routes)
    }

    fn is_routable(&self, node: &NodeEntry) -> bool {
        match node.availability {
            NodeAvailability::Serviceable => true,
            NodeAvailability::Unreachable => false,
            NodeAvailability::Unknown => !self.require_proven_health,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fabric_core::{DbmsNode, Shard};

    use fabric_migration::{Migration, MigrationId, MigrationPlan, MigrationReason, WriteSeq};

    use fabric_replication::{
        ReplicaOperation,
        ReplicaSet,
        ReplicationFactor,
    };

    const SHARD: u64 = 42;
    const CELL_A: Coordinate = Coordinate::new(0, 0);
    const CELL_B: Coordinate = Coordinate::new(1, 0);

    fn topology() -> Topology {
        let mut topology = Topology::new();

        let mut a = DbmsNode::new(DbmsId::new("db-a"), "phoenix");
        a.add_shard(Shard::new(SHARD, "social"));

        topology.add_node(a);
        topology.add_node(DbmsNode::new(DbmsId::new("db-b"), "dallas"));

        topology
    }

    fn key() -> RoutingKey {
        RoutingKey::new(SHARD, Coordinate::new(7, 3)).unwrap()
    }

    fn replica_set() -> ReplicaSet {
        let mut set = ReplicaSet::bootstrap(
            SHARD,
            ReplicationFactor::default(),
            DbmsId::new("db-a"),
            "phoenix",
        );

        let b = DbmsId::new("db-b");

        set.apply(ReplicaOperation::Add {
            node: b.clone(),
            region: "dallas".to_string(),
        })
        .unwrap();

        set.apply(ReplicaOperation::BeginSeed { node: b.clone() }).unwrap();
        set.apply(ReplicaOperation::MarkInSync { node: b }).unwrap();

        set
    }

    fn migration() -> Migration {
        let shard = Shard::new(SHARD, "social");

        Migration::new(
            MigrationPlan::new(
                MigrationId(1),
                &shard,
                DbmsId::new("db-a"),
                DbmsId::new("db-b"),
                MigrationReason::Rebalance,
                1_000,
            )
            .unwrap(),
        )
    }

    #[test]
    fn placement_comes_from_topology_not_from_the_identity() {
        let mut table = RoutingTable::from_topology(&topology());

        let before = table.resolve_write(&key()).unwrap();
        assert_eq!(before.node, DbmsId::new("db-a"));
        assert_eq!(before.served_by, ServedBy::ShardOwner);

        // The shard moves in topology. The key is untouched.
        let mut moved = Topology::new();
        let mut b = DbmsNode::new(DbmsId::new("db-b"), "dallas");
        b.add_shard(Shard::new(SHARD, "social"));
        moved.add_node(b);

        table.apply_topology(&moved);

        let after = table.resolve_write(&key()).unwrap();
        assert_eq!(after.node, DbmsId::new("db-b"));
        assert!(after.generation > before.generation);
    }

    #[test]
    fn an_unknown_shard_is_named_as_such() {
        let table = RoutingTable::from_topology(&topology());
        let key = RoutingKey::new(999, Coordinate::new(0, 0)).unwrap();

        assert_eq!(
            table.resolve_write(&key).unwrap_err(),
            RoutingError::UnknownShard { shard_id: 999 }
        );
    }

    #[test]
    fn reads_and_writes_diverge_once_there_are_replicas() {
        let mut table = RoutingTable::from_topology(&topology());
        assert!(table.set_replica_set(replica_set(), None));

        let write = table.resolve_write(&key()).unwrap();
        assert_eq!(write.node, DbmsId::new("db-a"));
        assert_eq!(write.served_by, ServedBy::Primary);
        assert_eq!(write.kind, RouteKind::Write);

        let readers: Vec<(String, ServedBy)> = table
            .candidates(&RouteRequest::read(key(), ReadPreference::AnyFresh))
            .unwrap()
            .into_iter()
            .map(|route| (route.node.0, route.served_by))
            .collect();

        assert_eq!(
            readers,
            vec![
                ("db-a".to_string(), ServedBy::Primary),
                ("db-b".to_string(), ServedBy::Secondary),
            ]
        );

        // A locality preference reorders the same copies; it never invents one.
        let local = table
            .resolve_read(
                &key(),
                ReadPreference::PreferRegion {
                    region: "dallas".to_string(),
                },
            )
            .unwrap();

        assert_eq!(local.node, DbmsId::new("db-b"));
        assert_eq!(local.region, "dallas");
    }

    #[test]
    fn a_failed_primary_leaves_reads_working_and_writes_unroutable() {
        let mut table = RoutingTable::from_topology(&topology());

        let mut set = replica_set();
        set.apply(ReplicaOperation::MarkFailed {
            node: DbmsId::new("db-a"),
        })
        .unwrap();

        table.set_replica_set(set, None);

        assert_eq!(
            table.resolve_write(&key()).unwrap_err(),
            RoutingError::NoWritableReplica { shard_id: SHARD }
        );

        let read = table
            .resolve_read(&key(), ReadPreference::AnyFresh)
            .unwrap();

        assert_eq!(read.node, DbmsId::new("db-b"));
        assert_eq!(read.served_by, ServedBy::Secondary);
    }

    #[test]
    fn an_unreachable_owner_is_an_explicit_no_live_owner() {
        let mut table = RoutingTable::from_topology(&topology());

        table.set_availability(&DbmsId::new("db-a"), NodeAvailability::Unreachable);

        let error = table.resolve_write(&key()).unwrap_err();

        assert_eq!(
            error,
            RoutingError::NoLiveOwner {
                shard_id: SHARD,
                considered: vec![DbmsId::new("db-a")],
            }
        );
        assert!(!error.is_retryable());
    }

    #[test]
    fn routes_stay_correct_for_the_whole_migration() {
        let mut table = RoutingTable::from_topology(&topology());
        let mut migration = migration();

        let source = DbmsId::new("db-a");
        let destination = DbmsId::new("db-b");

        // Planned, then copying: the source still serves everything, and the
        // half-copied destination serves nothing.
        table.observe_migration(&migration, None);
        assert_eq!(table.resolve_write(&key()).unwrap().node, source);

        migration.begin_copy(2_000).unwrap();
        migration.accept_write(WriteSeq(0)).unwrap();
        migration.record_copy(156, 4_096, 3_000).unwrap();
        migration.begin_catch_up(4_000).unwrap();
        migration.record_applied(WriteSeq(0), 4_100).unwrap();
        table.observe_migration(&migration, None);

        let read = table.resolve_read(&key(), ReadPreference::Primary).unwrap();
        assert_eq!(read.node, source);
        assert_eq!(read.served_by, ServedBy::MigrationSource);

        // Fenced: reads keep working, writes are told to retry.
        migration.begin_cutover(5_000).unwrap();
        table.observe_migration(&migration, None);

        assert_eq!(
            table.resolve_read(&key(), ReadPreference::Primary).unwrap().node,
            source
        );

        let error = table.resolve_write(&key()).unwrap_err();
        assert_eq!(
            error,
            RoutingError::WriteFenced {
                shard_id: SHARD,
                source: source.clone(),
                destination: destination.clone(),
            }
        );
        assert!(error.is_retryable());

        // Authority moves: both sides follow it in the same instant.
        migration.complete_cutover(5_100).unwrap();
        table.observe_migration(&migration, None);

        assert_eq!(table.resolve_write(&key()).unwrap().node, destination);
        let read = table.resolve_read(&key(), ReadPreference::Primary).unwrap();
        assert_eq!(read.node, destination);
        assert_eq!(read.served_by, ServedBy::MigrationDestination);

        // Completion folds the move into ordinary ownership.
        migration.complete(6_000).unwrap();
        table.observe_migration(&migration, None);

        let entry = table.shard(SHARD).unwrap();
        assert!(entry.migration().is_none());
        assert_eq!(entry.owner(), &destination);
        assert_eq!(
            table.resolve_write(&key()).unwrap().served_by,
            ServedBy::ShardOwner
        );
    }

    #[test]
    fn an_aborted_migration_leaves_the_source_owning_the_shard() {
        let mut table = RoutingTable::from_topology(&topology());
        let mut migration = migration();

        migration.begin_copy(2_000).unwrap();
        table.observe_migration(&migration, None);

        migration.abort(2_500, "destination ran out of disk").unwrap();
        table.observe_migration(&migration, None);

        assert!(table.shard(SHARD).unwrap().migration().is_none());
        assert_eq!(table.resolve_write(&key()).unwrap().node, DbmsId::new("db-a"));
    }

    #[test]
    fn a_range_resolves_into_one_segment_per_node() {
        let mut table = RoutingTable::from_topology(&topology());

        // Two cells of shard 42 live on db-b; everything else on db-a.
        let mut registry = TopologyRegistry::new();
        let shard = Shard::new(SHARD, "social");

        for coordinate in [Coordinate::new(1, 0), Coordinate::new(2, 0)] {
            registry.place(DbmsId::new("db-b"), &shard, coordinate, "dallas");
        }

        table.apply_placements(&registry);

        let range = CoordinateRange::new(Coordinate::new(0, 0), Coordinate::new(4, 0)).unwrap();

        let segments = table
            .resolve_range(SHARD, &range, &RouteIntent::read())
            .unwrap();

        let shape: Vec<(String, usize)> = segments
            .iter()
            .map(|segment| (segment.route.node.0.clone(), segment.len()))
            .collect();

        assert_eq!(
            shape,
            vec![
                ("db-a".to_string(), 1),
                ("db-b".to_string(), 2),
                ("db-a".to_string(), 2),
            ]
        );

        // A full scan of an evenly placed shard collapses to one segment.
        let plain = RoutingTable::from_topology(&topology());
        let segments = plain
            .resolve_scan(&[SHARD], &RouteIntent::read())
            .unwrap();

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].len(), fabric_core::GRID_ATOMS);
    }

    /// Two cells of one shard, both on `db-a`.
    fn two_celled_shard() -> RoutingTable {
        let mut table = RoutingTable::from_topology(&topology());
        let mut registry = TopologyRegistry::new();
        let shard = Shard::new(SHARD, "social");

        for coordinate in [CELL_A, CELL_B] {
            registry.place(DbmsId::new("db-a"), &shard, coordinate, "phoenix");
        }

        table.apply_placements(&registry);
        table
    }

    fn key_at(coordinate: Coordinate) -> RoutingKey {
        RoutingKey::new(SHARD, coordinate).unwrap()
    }

    /// The whole reason coordinate scoping exists: one cell moving must be
    /// invisible to every other cell of the same shard, at every phase and
    /// after the move has been folded in.
    #[test]
    fn a_cell_scoped_migration_is_invisible_to_its_siblings() {
        let mut table = two_celled_shard();
        let mut migration = migration();

        let source = DbmsId::new("db-a");
        let destination = DbmsId::new("db-b");

        let sibling_is_undisturbed = |table: &RoutingTable, phase: &str| {
            let read = table
                .resolve_read(&key_at(CELL_B), ReadPreference::Primary)
                .unwrap_or_else(|error| {
                    panic!("the sibling stopped being readable during {phase}: {error}")
                });

            assert_eq!(read.node, DbmsId::new("db-a"), "sibling read during {phase}");
            assert_eq!(read.served_by, ServedBy::ShardOwner);

            let write = table
                .resolve_write(&key_at(CELL_B))
                .unwrap_or_else(|error| {
                    panic!("the sibling stopped being writable during {phase}: {error}")
                });

            assert_eq!(write.node, DbmsId::new("db-a"), "sibling write during {phase}");
        };

        assert!(table.observe_migration(&migration, Some(CELL_A)));
        sibling_is_undisturbed(&table, "planning");

        migration.begin_copy(2_000).unwrap();
        migration.record_copy(156, 4_096, 3_000).unwrap();
        migration.begin_catch_up(4_000).unwrap();
        table.observe_migration(&migration, Some(CELL_A));
        sibling_is_undisturbed(&table, "catch-up");

        // The moving cell is fenced for writes. Its sibling is not: a fence is
        // a property of the copy being moved, not of the shard.
        migration.begin_cutover(5_000).unwrap();
        table.observe_migration(&migration, Some(CELL_A));

        assert!(matches!(
            table.resolve_write(&key_at(CELL_A)).unwrap_err(),
            RoutingError::WriteFenced { .. }
        ));
        sibling_is_undisturbed(&table, "the fence");

        // Authority moves for the moving cell only.
        migration.complete_cutover(5_100).unwrap();
        table.observe_migration(&migration, Some(CELL_A));

        let moved = table
            .resolve_read(&key_at(CELL_A), ReadPreference::Primary)
            .unwrap();

        assert_eq!(moved.node, destination);
        assert_eq!(moved.served_by, ServedBy::MigrationDestination);
        sibling_is_undisturbed(&table, "cutover");

        // Completion folds the move into the *cell's* placement, not the
        // shard's owner -- which is what leaves the sibling where it was.
        migration.complete(6_000).unwrap();
        table.observe_migration(&migration, Some(CELL_A));

        let entry = table.shard(SHARD).unwrap();
        assert!(entry.migration_at(CELL_A).is_none());
        assert_eq!(entry.owner(), &source, "a one-cell move changed the shard owner");
        assert_eq!(entry.owner_of(CELL_A), &destination);
        assert_eq!(entry.owner_of(CELL_B), &source);

        assert_eq!(
            table.resolve_write(&key_at(CELL_A)).unwrap().node,
            destination
        );
        sibling_is_undisturbed(&table, "completion");
    }

    #[test]
    fn a_cell_scoped_replica_set_answers_for_that_cell_alone() {
        let mut table = two_celled_shard();

        assert!(table.set_replica_set(replica_set(), Some(CELL_A)));

        let readers: Vec<(String, ServedBy)> = table
            .candidates(&RouteRequest::read(
                key_at(CELL_A),
                ReadPreference::AnyFresh,
            ))
            .unwrap()
            .into_iter()
            .map(|route| (route.node.0, route.served_by))
            .collect();

        assert_eq!(
            readers,
            vec![
                ("db-a".to_string(), ServedBy::Primary),
                ("db-b".to_string(), ServedBy::Secondary),
            ]
        );

        // The sibling has no set of its own and there is no shard-wide one, so
        // it is still answered for by its placement.
        let sibling = table
            .resolve_read(&key_at(CELL_B), ReadPreference::AnyFresh)
            .unwrap();

        assert_eq!(sibling.served_by, ServedBy::ShardOwner);
        assert_eq!(sibling.node, DbmsId::new("db-a"));

        // A shard-wide set published afterwards is the fallback for the
        // sibling and does not displace the cell's own.
        let entry = table.shard(SHARD).unwrap();
        assert!(entry.replica_set().is_none());
        assert!(entry.replica_set_at(CELL_A).is_some());
        assert!(entry.replica_set_at(CELL_B).is_none());
        assert_eq!(entry.scoped_cells(), vec![CELL_A]);

        table.clear_replica_set(SHARD, Some(CELL_A));
        assert!(table.shard(SHARD).unwrap().replica_set_at(CELL_A).is_none());
    }

    /// A shard-wide publication still means every cell. The protocol's
    /// topology ingest produces single-celled shards and publishes this way,
    /// so it has to keep working unchanged.
    #[test]
    fn a_shard_wide_publication_still_answers_for_every_cell() {
        let mut table = two_celled_shard();

        assert!(table.set_replica_set(replica_set(), None));

        for cell in [CELL_A, CELL_B] {
            let read = table
                .resolve_read(&key_at(cell), ReadPreference::AnyFresh)
                .unwrap();

            assert_eq!(read.served_by, ServedBy::Primary);
            assert_eq!(read.node, DbmsId::new("db-a"));
        }

        // And a cell-scoped set layered on top wins for its cell only.
        let mut promoted = replica_set();
        promoted
            .apply(ReplicaOperation::Promote {
                node: DbmsId::new("db-b"),
            })
            .unwrap();

        assert!(table.set_replica_set(promoted, Some(CELL_A)));

        assert_eq!(
            table
                .resolve_write(&key_at(CELL_A))
                .unwrap()
                .node,
            DbmsId::new("db-b")
        );
        assert_eq!(
            table
                .resolve_write(&key_at(CELL_B))
                .unwrap()
                .node,
            DbmsId::new("db-a")
        );
    }

    /// A range now spans cells that can be owned by different nodes and can be
    /// in different phases. It must answer per cell, and it must fail whole
    /// rather than quietly dropping the one cell it cannot answer for.
    #[test]
    fn a_range_answers_per_cell_and_fails_whole_on_a_fenced_one() {
        let mut table = two_celled_shard();
        let mut moving = migration();

        moving.begin_copy(2_000).unwrap();
        moving.record_copy(156, 4_096, 3_000).unwrap();
        moving.begin_catch_up(4_000).unwrap();
        moving.begin_cutover(5_000).unwrap();
        moving.complete_cutover(5_100).unwrap();
        table.observe_migration(&moving, Some(CELL_A));

        let range = CoordinateRange::new(CELL_A, CELL_B).unwrap();

        // A read splits into one segment per cell, and the moved cell keeps
        // its `MigrationDestination` label rather than being absorbed into a
        // run of ordinary owner routes.
        let segments = table
            .resolve_range(SHARD, &range, &RouteIntent::read())
            .unwrap();

        let shape: Vec<(String, ServedBy, usize)> = segments
            .iter()
            .map(|segment| {
                (
                    segment.route.node.0.clone(),
                    segment.route.served_by,
                    segment.len(),
                )
            })
            .collect();

        assert_eq!(
            shape,
            vec![
                ("db-b".to_string(), ServedBy::MigrationDestination, 1),
                ("db-a".to_string(), ServedBy::ShardOwner, 1),
            ]
        );

        // Now fence the moving cell again by re-running the same move on the
        // other cell: a write range over both must fail whole, because a range
        // write that silently dropped its fenced cell would lose it.
        let mut fencing = migration();
        fencing.begin_copy(6_000).unwrap();
        fencing.record_copy(156, 4_096, 6_500).unwrap();
        fencing.begin_catch_up(7_000).unwrap();
        fencing.begin_cutover(7_500).unwrap();
        table.observe_migration(&fencing, Some(CELL_B));

        let error = table
            .resolve_range(SHARD, &range, &RouteIntent::Write)
            .unwrap_err();

        assert!(
            matches!(error, RoutingError::WriteFenced { .. }),
            "a fenced cell did not fail the whole range: {error}"
        );
        assert!(error.is_retryable());

        // The unfenced cell still resolves on its own -- the range failed, not
        // the cell.
        assert_eq!(
            table.resolve_write(&key_at(CELL_A)).unwrap().node,
            DbmsId::new("db-b")
        );
    }

    /// Cell-scoped state says nothing about a shard's other cells, so it may
    /// not be the thing that invents a shard.
    #[test]
    fn a_cell_scoped_publication_needs_a_shard_to_scope_it_to() {
        let mut table = RoutingTable::new();

        assert!(!table.set_replica_set(replica_set(), Some(CELL_A)));
        assert!(!table.observe_migration(&migration(), Some(CELL_A)));
        assert!(table.shard(SHARD).is_none());

        // Shard-wide publication is what bootstraps an unknown shard, and
        // still does.
        assert!(table.set_replica_set(replica_set(), None));
        assert!(table.shard(SHARD).is_some());

        // An off-grid coordinate is refused rather than filed under a key no
        // lookup can produce.
        assert!(!table.observe_migration(&migration(), Some(Coordinate::new(12, 0))));
    }

    #[test]
    fn every_scoped_mutation_moves_the_generation() {
        let mut table = two_celled_shard();

        let mut generation = table.generation();

        let mut moved = |table: &RoutingTable, what: &str| {
            assert!(
                table.generation() > generation,
                "{what} did not invalidate cached routes"
            );
            generation = table.generation();
        };

        table.set_replica_set(replica_set(), Some(CELL_A));
        moved(&table, "publishing a cell-scoped replica set");

        table.observe_migration(&migration(), Some(CELL_A));
        moved(&table, "publishing a cell-scoped migration");

        table.clear_replica_set(SHARD, Some(CELL_A));
        moved(&table, "clearing a cell-scoped replica set");

        // The route a caller cached before any of that carries the old
        // generation, which is how it knows to re-resolve.
        let route = table
            .resolve_read(&key_at(CELL_B), ReadPreference::Primary)
            .unwrap();

        assert_eq!(route.generation, table.generation());
    }

    #[test]
    fn a_scan_fails_rather_than_returning_a_partial_answer() {
        let mut table = RoutingTable::from_topology(&topology());

        let mut registry = TopologyRegistry::new();
        let shard = Shard::new(SHARD, "social");
        registry.place(DbmsId::new("db-b"), &shard, Coordinate::new(3, 0), "dallas");
        table.apply_placements(&registry);

        table.set_availability(&DbmsId::new("db-b"), NodeAvailability::Unreachable);

        let error = table
            .resolve_scan(&[SHARD], &RouteIntent::read())
            .unwrap_err();

        assert!(matches!(error, RoutingError::NoLiveOwner { .. }));
    }
}
