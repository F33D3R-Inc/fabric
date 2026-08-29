use serde::{Deserialize, Serialize};

use crate::{
    atom::Atom,
    value::Value,
};

/// A native FacetQL data record.
///
/// The atom provides the logical address.
/// The record provides the value stored at that address.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtomRecord {
    pub atom: Atom,
    pub value: Value,
}

impl AtomRecord {
    pub fn new(
        atom: Atom,
        value: Value,
    ) -> Self {
        Self {
            atom,
            value,
        }
    }

    pub fn coordinate(
        &self,
    ) -> crate::Coordinate {
        self.atom.coordinate
    }

    pub fn value(
        &self,
    ) -> &Value {
        &self.value
    }

    pub fn set_value(
        &mut self,
        value: Value,
    ) {
        self.value = value;
    }
}