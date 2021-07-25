use crate::storage::page::page::PAGE_SIZE;
use std::mem;

pub trait KeyType {}
pub trait ValueType {}

struct MappingType<K: KeyType, V: ValueType> {
    key_type: K,
    value_type: V,
}

/// The original code was:
///
/// ```
/// #![feature(const_generics)]
/// #![feature(const_evaluatable_checked)]
///
/// const BLOCK_ARRAY_SIZE = 4 * PAGE_SIZE / (4 * mem::size_of::<MappingType<K, V>>() + 1);
///
/// pub struct HashTableBlockPage<K: KeyType, V: ValueType> {
///     occupied: [u8; (4 * PAGE_SIZE / (4 * mem::size_of::<MappingType<K, V>>() + 1) - 1) / 8 + 1],
///     readable: [u8; (4 * PAGE_SIZE / (4 * mem::size_of::<MappingType<K, V>>() + 1) - 1) / 8 + 1],
///     array: [MappingType<K, V>; 4 * PAGE_SIZE / (4 * mem::size_of::<MappingType<K, V>>() + 1)],
/// }
/// ```
///
/// However, due to the feature:
/// ```
/// #![feature(const_generics)]
/// #![feature(const_evaluatable_checked)]
/// ```
/// is not stable now, so I choose Vec instead.
pub struct HashTableBlockPage<K: KeyType, V: ValueType> {
    occupied: Vec<u8>,
    readable: Vec<u8>,
    array: Vec<MappingType<K, V>>,
}

impl<K: KeyType, V: ValueType> HashTableBlockPage<K, V> {
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
    use crate::storage::page::hash_table_block_page::{KeyType, ValueType, HashTableBlockPage};

    struct FakeKey {
        data: [u8; 10]
    }
    impl KeyType for FakeKey {}

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