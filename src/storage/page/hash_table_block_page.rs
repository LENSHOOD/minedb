use crate::storage::page::page::PAGE_SIZE;
use crate::common::hash::*;
use std::mem;
use crate::common::ValueType;

struct MappingType<K: HashKeyType, V: ValueType> {
    key_type: K,
    value_type: V,
}

pub struct HashTableBlockPage<K: HashKeyType, V: ValueType> {
    occupied: Vec<u8>,
    readable: Vec<u8>,
    array: Vec<MappingType<K, V>>,
}

impl<K: HashKeyType, V: ValueType> HashTableBlockPage<K, V> {
    pub fn new() -> HashTableBlockPage<K, V> {
        let size = 4 * PAGE_SIZE / (4 * mem::size_of::<MappingType<K, V>>() + 1);
        HashTableBlockPage {
            occupied: Vec::with_capacity((size - 1) / 8 + 1),
            readable: Vec::with_capacity((size - 1) / 8 + 1),
            array: Vec::with_capacity(size)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::hash_table_block_page::{HashKeyType, ValueType, HashTableBlockPage};
    use std::hash::Hash;

    #[derive(Hash)]
    struct FakeKey {
        data: [u8; 10]
    }
    impl HashKeyType for FakeKey {}

    struct FakeValue {
        data: [u8; 20]
    }
    impl ValueType for FakeValue {}

    #[test]
    fn should_construct_new_empty_block() {
        let block: HashTableBlockPage<FakeKey, FakeValue> = HashTableBlockPage::new();
        assert_eq!(block.occupied.capacity(), 17);
        assert_eq!(block.readable.capacity(), 17);
        assert_eq!(block.array.capacity(), 135);
    }
}