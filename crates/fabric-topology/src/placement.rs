use fabric_core::{Coordinate, DbmsId, Shard};
use serde::{Deserialize, Serialize};

/// Physical placement of a logical FacetQL location.
///
/// The logical coordinate never changes because of physical movement.
/// The Fabric is allowed to change this placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    pub dbms_id: DbmsId,
    pub shard_id: u64,
    pub coordinate: Coordinate,
    pub region: String,
}

impl Placement {
    pub fn new(
        dbms_id: DbmsId,
        shard: &Shard,
        coordinate: Coordinate,
        region: impl Into<String>,
    ) -> Self {
        Self {
            dbms_id,
            shard_id: shard.id,
            coordinate,
            region: region.into(),
        }
    }
}