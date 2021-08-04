use crate::storage::page::page::{PAGE_SIZE, Page};
use crate::common::hash::*;
use std::{mem, io};
use crate::common::ValueType;
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
struct MappingType<K: HashKeyType, V: ValueType> {
    key: K,
    value: V,
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
            array: vec![MappingType {key: Default::default(), value: Default::default()}; size]
        }
    }

    pub fn get_slot_size() -> usize {
        4 * PAGE_SIZE / (4 * mem::size_of::<MappingType<K, V>>() + 1)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut res = self.occupied.clone();
        res.append(&mut (self.readable.clone()));
        for mappingType in self.array.iter() {
            let mut raw = bincode::serialize(mappingType).unwrap();
            res.append(&mut raw);
        }

        res
    }

    pub fn insert(&mut self, slot_idx: usize, key: K, value: V) -> bool {
        if (&self).occupied(slot_idx) {
            return false;
        }

        self.array.insert(slot_idx, MappingType { key, value});
        self.set(slot_idx);
        true
    }

    fn occupied(&self, slot_idx: usize) -> bool {
        let byte_idx = slot_idx / 8;
        let bit_idx = slot_idx % 8;
        self.occupied[byte_idx] | (!(0x01 << bit_idx)) == 0xff
    }

    fn set(&mut self, slot_idx: usize) {
        let byte_idx = slot_idx / 8;
        let bit_idx = slot_idx % 8;
        self.occupied[byte_idx] |= 0x01 << bit_idx
    }

    fn clear(&mut self, slot_idx: usize) {
        let byte_idx = slot_idx / 8;
        let bit_idx = slot_idx % 8;
        self.occupied[byte_idx] &= (!(0x01 << bit_idx))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::hash_table_block_page::{HashKeyType, ValueType, HashTableBlockPage};
    use std::hash::Hash;
    use serde::Serialize;

    #[derive(Hash, Default, Clone, Serialize)]
    struct FakeKey {
        data: [u8; 10]
    }
    impl HashKeyType for FakeKey {}

    #[derive(Default, Clone, Serialize)]
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

    #[test]
    fn should_set() {
        // given
        let mut block: HashTableBlockPage<FakeKey, FakeValue> = HashTableBlockPage::new();
        block.occupied[10] = 0b0010_1000;

        // when
        assert!(!block.occupied(86));
        block.set(86);

        // then
        assert_eq!(block.occupied[10], 0b0110_1000);;
    }

    #[test]
    fn should_clear() {
        // given
        let mut block: HashTableBlockPage<FakeKey, FakeValue> = HashTableBlockPage::new();
        block.occupied[10] = 0b0010_1000;

        // when
        assert!(block.occupied(83));
        block.clear(83);

        // then
        assert_eq!(block.occupied[10], 0b0010_0000);;
    }

    #[test]
    fn should_insert_into_block() {
        // given
        let mut block: HashTableBlockPage<FakeKey, FakeValue> = HashTableBlockPage::new();
        block.occupied[10] = 0b0010_1000;
        let key = FakeKey { data: [1; 10] };
        let value = FakeValue { data: [127; 20] };

        // when
        let inserted = block.insert(86, key, value);

        // then
        assert!(inserted);
        assert!(block.occupied(86));
        let mapping = &block.array[86];
        assert_eq!(mapping.key.data[0], 1);
        assert_eq!(mapping.value.data[0], 127);
    }

    #[test]
    fn should_not_insert_when_slot_already_occupied() {
        // given
        let mut block: HashTableBlockPage<FakeKey, FakeValue> = HashTableBlockPage::new();
        block.occupied[10] = 0b0010_1000;
        let key = FakeKey { data: [1; 10] };
        let value = FakeValue { data: [127; 20] };

        // when
        let inserted = block.insert(83, key, value);

        // then
        assert!(!inserted);
        assert!(!block.occupied(86));
    }

    #[test]
    fn should_serialize_block() {
        // given
        let mut block: HashTableBlockPage<FakeKey, FakeValue> = HashTableBlockPage::new();
        block.occupied[10] = 0b0010_1000;
        let key = FakeKey { data: [1; 10] };
        let value = FakeValue { data: [127; 20] };
        block.insert(86, key, value);

        // when
        let raw = block.serialize();

        // then
        // array size == 135, occupied,readable size == 17
        assert_eq!(raw[10], 0b0110_1000);
        // array index == 86 -> real index == 17*2 + 86*30 = 2614 (MappingType first idx)
        assert_eq!(raw[2613], 0);
        assert_eq!(raw[2614], 1);
        assert_eq!(raw[2623], 1);
        assert_eq!(raw[2624], 127);
    }
}