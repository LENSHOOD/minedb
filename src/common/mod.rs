use serde::Serialize;

pub mod hash;

pub trait KeyType: Default + Clone + Serialize + Eq {}
pub trait ValueType: Default + Clone + Serialize + Eq {}