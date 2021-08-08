use crate::common::hash::HashKeyType;
use crate::common::ValueType;

pub trait HashTable<K: HashKeyType, V: ValueType> {
    fn insert(&mut self, k: &K, v: &V);
    fn remove(&mut self, k: &K);
    fn get_value(&self, k: &K) -> &V;
}