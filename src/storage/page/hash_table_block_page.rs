use crate::storage::page::page::PAGE_SIZE;
use crate::common::hash::*;
use std::{mem, io};
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
        let size = HashTableBlockPage::<K, V>::get_slot_size();
        HashTableBlockPage {
            occupied: vec![0; ((size - 1) / 8 + 1)],
            readable: vec![0; ((size - 1) / 8 + 1)],
            array: Vec::with_capacity(size)
        }
    }

    pub fn get_slot_size() -> usize {
        4 * PAGE_SIZE / (4 * mem::size_of::<MappingType<K, V>>() + 1)
    }

    fn occupied(&self, slot_idx: usize) -> bool {
        let byte_idx = slot_idx / 8;
        let bit_idx = slot_idx % 8;
        &self.occupied[byte_idx] | (!(0x01 << bit_idx)) == 0xff
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

    #[test]
    fn should_test_occupied() {
        // given
        let mut block: HashTableBlockPage<FakeKey, FakeValue> = HashTableBlockPage::new();
        block.occupied[10] = 0b0010_1000;

        // when
        let is_occupied_83 = block.occupied(83);
        let is_occupied_85 = block.occupied(85);
        let not_occupied_86 = block.occupied(86);

        // then
        assert!(is_occupied_83);
        assert!(is_occupied_85);
        assert!(!not_occupied_86);
    }
}