use crate::common::hash::HashKeyType;
use crate::common::ValueType;

pub trait HashTable<K: HashKeyType, V: ValueType> {
    fn insert(&self, k: &K, v: &V);
    fn remove(&self, k: &K);
    fn get_value(&self, k: &K) -> dyn ValueType;
}