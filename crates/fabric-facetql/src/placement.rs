//! Fabric's own control state, stored in FacetQL.
//!
//! `TopologyRegistry` is an in-memory `HashMap` — restart the control plane
//! and the map of which FacetQL instance holds which grid cell is gone. It
//! has to live somewhere durable, and the one place it must *not* live is
//! `fabric-core`'s `FacetEngine`/`ShardStorage`: that is a second,
//! parallel storage engine, Persistence is FacetQL's domain (§29), and
//! growing a private database inside the control plane to hold the control
//! plane's state is the exact duplication the architecture forbids
//! (FABRIC_INTEGRATION_PLAN Finding B).
//!
//! So Fabric dogfoods FacetQL, the same way fct's `fqStore` does with
//! `__session`/`__job`/`__cron`: one node per placement under the reserved
//! kind [`PLACEMENT_KIND`], read through `POST /nodes/query`, written through
//! `POST /node` and `POST /transaction`.
//!
//! # Versioning, and why it is a real compare-and-set
//!
//! Two controllers reacting to the same hotspot will both try to move the
//! same cell. Read-then-write cannot make that safe: between the read and the
//! write, the other controller's move lands and is then silently overwritten
//! — the classic lost update, and a lost update here means the topology
//! records a placement that nobody actually made.
//!
//! Every placement therefore carries a `version`, and every update is one
//! `set_if` op with `expect_eq: <version>` — FacetQL's native
//! compare-and-set, evaluated inside the engine under the same lock that
//! applies the write. The loser gets 412 and *nothing* is applied. Creation
//! uses `POST /node` with `if_absent`, which is the same idea for the case
//! where there is no version to compare against yet: 409 means somebody
//! created it first.
//!
//! Removal is CAS-guarded too, without needing a `delete_if` op that does not
//! exist: a transaction is all-or-nothing, so `set_if` (expecting the version)
//! followed by `delete_node` in the *same* batch either both apply or neither
//! does. There is no window between them.
//!
//! # The two coordinate types, again
//!
//! The placement node's FacetQL coordinate is the origin and means nothing.
//! Fabric's grid cell lives in the node's `data`, as `x`/`y` of a
//! [`fabric_core::Coordinate`]. Writing a fabric grid cell into FacetQL's
//! 4-axis coordinate would be marshalling one `Coordinate` into the other,
//! which is forbidden — they are different concepts that happen to share a
//! name.

use std::collections::HashMap;

use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_topology::{Placement, TopologyRegistry};
use serde::{Deserialize, Serialize};

use crate::client::FacetqlClient;
use crate::error::FacetqlError;
use crate::wire::{CreateNodeRequest, Expect, Node, QueryRequest, TxOperation};

/// Reserved kind holding Fabric's placements. Reserved kinds are prefixed
/// `__` by convention across the stack (`__audit`, `__session`, `__job`,
/// `__cron`), which keeps control state out of the application's namespace.
pub const PLACEMENT_KIND: &str = "__fabric_placement";

/// The `data` field the compare-and-set is performed against.
const VERSION_FIELD: &str = "version";

/// A placement as it is stored: the placement itself plus the version that
/// any update to it must present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPlacement {
    pub placement: Placement,
    pub version: u64,
}

impl StoredPlacement {
    /// The FacetQL address this placement lives at.
    ///
    /// Derived from the registry's own key — `(shard_id, coordinate)` — so the
    /// address *is* the identity, and two writers naming the same cell
    /// necessarily collide on one node rather than quietly creating two.
    pub fn address(&self) -> String {
        address_of(self.placement.shard_id, self.placement.coordinate)
    }
}

/// The address a `(shard_id, coordinate)` placement is stored at.
pub fn address_of(shard_id: u64, coordinate: Coordinate) -> String {
    format!(
        "{PLACEMENT_KIND}:{shard_id}:{}:{}",
        coordinate.x, coordinate.y
    )
}

/// The JSON body of a placement node. This is Fabric's own shape — it is not
/// part of FacetQL's wire contract, which sees only an opaque `data` string.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlacementData {
    dbms_id: String,
    shard_id: u64,
    /// Fabric grid cell — `fabric_core::Coordinate`, never FacetQL's.
    x: u8,
    y: u8,
    region: String,
    version: u64,
}

impl PlacementData {
    fn new(placement: &Placement, version: u64) -> Self {
        Self {
            dbms_id: placement.dbms_id.0.clone(),
            shard_id: placement.shard_id,
            x: placement.coordinate.x,
            y: placement.coordinate.y,
            region: placement.region.clone(),
            version,
        }
    }

    fn into_stored(self) -> StoredPlacement {
        StoredPlacement {
            placement: Placement {
                dbms_id: DbmsId::new(self.dbms_id),
                shard_id: self.shard_id,
                coordinate: Coordinate::new(self.x, self.y),
                region: self.region,
            },
            version: self.version,
        }
    }

    fn to_json(&self) -> Result<String, FacetqlError> {
        serde_json::to_string(self).map_err(|error| FacetqlError::Decode {
            context: "placement data".to_string(),
            message: error.to_string(),
        })
    }

    /// The `set` map of a `set_if`: every field of the record, so an update
    /// moves the placement and its version together.
    fn to_set_map(&self) -> Result<serde_json::Map<String, serde_json::Value>, FacetqlError> {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::Object(map)) => Ok(map),
            Ok(_) | Err(_) => Err(FacetqlError::Decode {
                context: "placement data".to_string(),
                message: "placement data did not encode as a JSON object".to_string(),
            }),
        }
    }
}

/// Fabric's durable topology, stored in one FacetQL instance.
///
/// The instance holding the control state is an ordinary FacetQL — typically
/// not one of the instances being placed, though nothing here requires that.
#[derive(Debug, Clone)]
pub struct PlacementStore {
    client: FacetqlClient,
}

impl PlacementStore {
    pub fn new(client: FacetqlClient) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &FacetqlClient {
        &self.client
    }

    /// Create a placement that does not exist yet, at version 1.
    ///
    /// [`FacetqlError::Conflict`] means the address already exists: another
    /// controller created it first. That is a real answer, not a failure of
    /// the instance — retry as an [`update`](Self::update) against what is
    /// actually there, having read it.
    pub async fn create(&self, placement: &Placement) -> Result<StoredPlacement, FacetqlError> {
        let data = PlacementData::new(placement, 1);

        self.client
            .create_node(&CreateNodeRequest {
                address: address_of(placement.shard_id, placement.coordinate),
                kind: PLACEMENT_KIND.to_string(),
                // FacetQL's coordinate, not Fabric's: the origin, unused.
                x: 0,
                y: 0,
                z: 0,
                q: 0,
                data: data.to_json()?,
                // Control state is never public. A placement names which
                // instance holds which data; handing that to every
                // authenticated identity is a map of the fleet.
                public: false,
                if_absent: true,
            })
            .await?;

        Ok(StoredPlacement {
            placement: placement.clone(),
            version: 1,
        })
    }

    /// Move a placement, but only if it is still at the version we read.
    ///
    /// One `set_if` op with `expect_eq: <current.version>`, merged with the
    /// new fields and `version + 1`. [`FacetqlError::PreconditionFailed`]
    /// means somebody else updated it first and **nothing was written** —
    /// re-read and decide again rather than retrying the same stale write.
    pub async fn update(
        &self,
        current: &StoredPlacement,
        next: &Placement,
    ) -> Result<StoredPlacement, FacetqlError> {
        let address = current.address();

        if next.shard_id != current.placement.shard_id
            || next.coordinate != current.placement.coordinate
        {
            return Err(FacetqlError::Configuration(format!(
                "an update cannot change the cell a placement addresses \
                 ({address}); remove it and create the new cell instead"
            )));
        }

        let version = current.version + 1;
        let data = PlacementData::new(next, version);

        self.client
            .transaction(vec![TxOperation::SetIf {
                address,
                field: VERSION_FIELD.to_string(),
                expect: Expect::version(current.version),
                set: data.to_set_map()?,
            }])
            .await?;

        Ok(StoredPlacement {
            placement: next.clone(),
            version,
        })
    }

    /// Remove a placement, but only if it is still at the version we read.
    ///
    /// The CAS and the delete are one transaction: `set_if` proves the
    /// version, `delete_node` removes the node, and because the batch is
    /// all-or-nothing a failed precondition leaves the node exactly as it
    /// was. This is a real compare-and-set delete built from the primitives
    /// FacetQL has, not a read followed by a hopeful delete.
    pub async fn remove(&self, current: &StoredPlacement) -> Result<(), FacetqlError> {
        let address = current.address();

        self.client
            .transaction(vec![
                TxOperation::SetIf {
                    address: address.clone(),
                    field: VERSION_FIELD.to_string(),
                    expect: Expect::version(current.version),
                    set: serde_json::Map::new(),
                },
                TxOperation::DeleteNode { address },
            ])
            .await
    }

    /// Every stored placement, read through the keyset cursor.
    ///
    /// Never an offset walk: a control plane's topology is exactly the thing
    /// being written while it is read, and an offset walk over a kind under
    /// concurrent writes skips and repeats rows.
    pub async fn load(&self) -> Result<Vec<StoredPlacement>, FacetqlError> {
        let nodes = self
            .client
            .query_all(&QueryRequest {
                kind: Some(PLACEMENT_KIND.to_string()),
                ..QueryRequest::default()
            })
            .await?;

        nodes.iter().map(decode_placement).collect()
    }

    /// Load the durable topology straight into a [`TopologyRegistry`], plus
    /// the version of each placement so a later update can present it.
    ///
    /// The registry deliberately does not carry versions: it is the live map
    /// the optimizer reads, and a version is a concern of the store that
    /// persists it. Keeping them side by side means neither type learns about
    /// the other's job.
    pub async fn load_registry(
        &self,
    ) -> Result<(TopologyRegistry, HashMap<String, StoredPlacement>), FacetqlError> {
        let stored = self.load().await?;

        let mut registry = TopologyRegistry::new();
        let mut versions = HashMap::with_capacity(stored.len());

        for entry in stored {
            let shard = Shard::new(entry.placement.shard_id, entry.placement.region.clone());

            registry.place(
                entry.placement.dbms_id.clone(),
                &shard,
                entry.placement.coordinate,
                entry.placement.region.clone(),
            );

            versions.insert(entry.address(), entry);
        }

        Ok((registry, versions))
    }

    /// Read one placement by its cell, or `None` if it is not stored.
    pub async fn get(
        &self,
        shard_id: u64,
        coordinate: Coordinate,
    ) -> Result<Option<StoredPlacement>, FacetqlError> {
        let address = address_of(shard_id, coordinate);

        // Filtered by kind rather than fetched by address so a node of
        // another kind sitting at that address can never be decoded as a
        // placement.
        let nodes = self
            .client
            .query_all(&QueryRequest {
                kind: Some(PLACEMENT_KIND.to_string()),
                ..QueryRequest::default()
            })
            .await?;

        nodes
            .iter()
            .find(|node| node.address == address)
            .map(decode_placement)
            .transpose()
    }
}

fn decode_placement(node: &Node) -> Result<StoredPlacement, FacetqlError> {
    let data: PlacementData =
        serde_json::from_str(&node.data).map_err(|error| FacetqlError::Decode {
            context: format!("placement node {}", node.address),
            message: error.to_string(),
        })?;

    Ok(data.into_stored())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Coordinate as WireCoordinate, Visibility};

    fn placement() -> Placement {
        Placement {
            dbms_id: DbmsId::new("db-a"),
            shard_id: 7,
            coordinate: Coordinate::new(3, 4),
            region: "us-east".to_string(),
        }
    }

    fn node(address: &str, data: &str) -> Node {
        Node {
            address: address.to_string(),
            coordinate: WireCoordinate::default(),
            value: 0,
            kind: PLACEMENT_KIND.to_string(),
            data: data.to_string(),
            owner: "fabric".to_string(),
            claimed_by: None,
            visibility: Visibility::Private,
        }
    }

    #[test]
    fn the_address_is_the_registry_key() {
        assert_eq!(
            address_of(7, Coordinate::new(3, 4)),
            "__fabric_placement:7:3:4"
        );

        let stored = StoredPlacement {
            placement: placement(),
            version: 1,
        };
        assert_eq!(stored.address(), "__fabric_placement:7:3:4");
    }

    #[test]
    fn a_placement_round_trips_through_its_stored_form() {
        let data = PlacementData::new(&placement(), 3);
        let json = data.to_json().unwrap();
        let decoded = decode_placement(&node("__fabric_placement:7:3:4", &json)).unwrap();

        assert_eq!(decoded.version, 3);
        assert_eq!(decoded.placement, placement());
    }

    /// Fabric's grid cell is in `data`. It must never be marshalled into
    /// FacetQL's 4-axis coordinate, which stays at the origin.
    #[test]
    fn the_fabric_cell_lives_in_data_not_in_facetqls_coordinate() {
        let json = PlacementData::new(&placement(), 1).to_json().unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["x"], 3);
        assert_eq!(value["y"], 4);
        // No `z`/`q`: this is a two-axis fabric cell, not a FacetQL
        // coordinate wearing its name.
        assert!(value.get("z").is_none());
        assert!(value.get("q").is_none());
    }

    #[test]
    fn the_set_map_of_an_update_carries_every_field_including_the_new_version() {
        let map = PlacementData::new(&placement(), 2).to_set_map().unwrap();

        assert_eq!(map["version"], serde_json::json!(2));
        assert_eq!(map["dbms_id"], serde_json::json!("db-a"));
        assert_eq!(map["region"], serde_json::json!("us-east"));
        assert_eq!(map["shard_id"], serde_json::json!(7));
    }

    #[test]
    fn undecodable_data_is_an_error_not_a_defaulted_placement() {
        let err = decode_placement(&node("__fabric_placement:7:3:4", "not json")).unwrap_err();
        assert!(matches!(err, FacetqlError::Decode { .. }));
        assert!(err.to_string().contains("__fabric_placement:7:3:4"));
    }
}
