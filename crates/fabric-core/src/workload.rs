use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadClass {
    ReadHeavy,
    WriteHeavy,
    Mixed,
    EventHeavy,
    Media,
    Realtime,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workload {
    pub id: u64,
    pub class: WorkloadClass,
}

impl Workload {
    pub fn new(id: u64, class: WorkloadClass) -> Self {
        Self { id, class }
    }
}