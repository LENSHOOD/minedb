use crate::common::hash::HashKeyType;
use crate::common::ValueType;

pub trait HashTable<K: HashKeyType, V: ValueType> {
    fn insert(&mut self, k: &K, v: &V) -> bool;
    fn remove(&mut self, k: &K);
    fn get_value(&mut self, k: &K) -> Vec<V>;
}