use serde::Serialize;

pub mod hash;

pub trait KeyType: Default + Clone + Serialize {}
pub trait ValueType: Default + Clone + Serialize {}