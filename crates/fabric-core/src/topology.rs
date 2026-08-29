use serde::{Deserialize, Serialize};

use crate::shard::Shard;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DbmsId(pub String);

impl DbmsId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbmsNode {
    pub id: DbmsId,
    pub region: String,
    pub shards: Vec<Shard>,
}

impl DbmsNode {
    pub fn new(id: DbmsId, region: impl Into<String>) -> Self {
        Self {
            id,
            region: region.into(),
            shards: Vec::new(),
        }
    }

    pub fn add_shard(&mut self, shard: Shard) {
        self.shards.push(shard);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Topology {
    pub nodes: Vec<DbmsNode>,
}

impl Topology {
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn add_node(&mut self, node: DbmsNode) {
        self.nodes.push(node);
    }
}