use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::hash::HashKeyType;
use crate::common::ValueType;
use crate::container::hash::FindSlotResult;
use crate::container::hash::FindSlotResult::*;
use crate::container::hash::hash_table::HashTable;
use crate::storage::page::hash_table_block_page::HashTableBlockPage;
use crate::storage::page::hash_table_header_page::HashTableHeaderPage;
use crate::storage::page::page::PageId;

pub struct LinearProbeHashTable<'a, K: HashKeyType, V: ValueType> {
    header_pid: PageId,
    buffer_pool_manager: &'a mut BufferPoolManager,
    hash_fn: fn(&K) -> u64,
    phantom: PhantomData<V>,
}

impl<'a, K, V> LinearProbeHashTable<'a, K, V>
    where
        K: HashKeyType + DeserializeOwned,
        V: ValueType + DeserializeOwned,
{
    pub fn new(num_buckets: usize, bpm: &mut BufferPoolManager, hash_fn: fn(&K) -> u64) -> LinearProbeHashTable<K, V> {
        let header_pid = {
            let mut header_page = bpm.new_page().unwrap().write().unwrap();

            let header = HashTableHeaderPage::new(header_page.get_id(), num_buckets);
            let header_raw = header.serialize();
            for i in 0..header_raw.len() {
                header_page.get_data_mut()[i] = header_raw[i];
            }

            header_page.get_id()
        };

        LinearProbeHashTable {
            header_pid,
            buffer_pool_manager: bpm,
            hash_fn,
            phantom: PhantomData,
        }
    }

    fn get_header(&mut self) -> HashTableHeaderPage {
        let header_page = self.buffer_pool_manager
            .fetch_page(self.header_pid).unwrap()
            .read().unwrap();

        HashTableHeaderPage::deserialize(header_page.get_data()).unwrap()
    }

    fn get_block(bpm: &mut BufferPoolManager, block_pid: usize) -> HashTableBlockPage<K, V> {
        let block_page = bpm.fetch_page(block_pid).unwrap().read().unwrap();
        HashTableBlockPage::deserialize(block_page.get_data()).unwrap()
    }

    fn insert_to_new_block(bpm: &mut BufferPoolManager,
                           k: &K,
                           v: &V,
                           header: &mut HashTableHeaderPage,
                           block_idx: usize,
                           block_offset: usize) {
        let mut new_block = HashTableBlockPage::<K, V>::new();

        // collapse cannot happen in new block
        assert!(new_block.insert(block_offset, k.clone(), v.clone()));
        let block_pid = LinearProbeHashTable::<K, V>::update_page(bpm, None, new_block.serialize());

        header.set(block_pid, block_idx);
        LinearProbeHashTable::<K, V>::update_page(bpm, Some(header.get_page_id()), header.serialize());
    }

    fn update_page(bpm: &mut BufferPoolManager, pid_option: Option<PageId>, page_data: Vec<u8>) -> PageId {
        let pid_to_return = {
            let mut page = match pid_option {
                Some(pid) => bpm.fetch_page(pid).unwrap().write().unwrap(),
                None => bpm.new_page().unwrap().write().unwrap()
            };
            let raw_data = page.get_data_mut();
            for i in 0..page_data.len() {
                raw_data[i] = page_data[i];
            }
            page.get_id()
        };

        {
            bpm.unpin_page(pid_to_return, true);
        }

        pid_to_return
    }

    fn find_available_slot(bpm: &mut BufferPoolManager,
                           key: &K,
                           val: &V,
                           block_pid: usize,
                           block_offset: usize) -> FindSlotResult<(HashTableBlockPage<K, V>, usize)> {
        let block = LinearProbeHashTable::<K, V>::get_block(bpm, block_pid);
        for i in block_offset..HashTableBlockPage::<K, V>::capacity_of_block() {
            if !block.is_occupied(i) {
                return Found((block, i));
            }

            let (k, v) = block.get(i);
            if key.eq(k) && val.eq(v) {
                return Duplicated;
            }
        }

        NotFound
    }

    fn try_insert_to_appropriate_slot(&mut self, k: &K, v: &V, mut header: &mut HashTableHeaderPage, block_idx: usize, init_block_offset: usize) -> bool {
        let mut next_block_idx = block_idx;
        let mut block_offset = init_block_offset;
        loop {
            let next_block_pid = header.get_block_page_id(next_block_idx);
            if next_block_pid.is_none() {
                LinearProbeHashTable::<K, V>::insert_to_new_block(self.buffer_pool_manager, k, v, &mut header, next_block_idx, block_offset);
                return true;
            }

            let block_and_offset = LinearProbeHashTable::<K, V>::find_available_slot(
                self.buffer_pool_manager, k, v, next_block_pid.unwrap(), block_offset);
            if block_and_offset.not_found() {
                // temporary ignore hash table all fulled
                if next_block_idx + 1 == header.get_size() {
                    next_block_idx = 0;
                } else {
                    next_block_idx += 1;
                }
                block_offset = 0;

                continue;
            }

            if block_and_offset.duplicated() {
                return false;
            }

            let (mut found_block, offset) = block_and_offset.unwrap();
            assert!(found_block.insert(offset, k.clone(), v.clone()));
            LinearProbeHashTable::<K, V>::update_page(self.buffer_pool_manager, next_block_pid, found_block.serialize());

            return true;
        }
    }
}

impl<'a, K, V> HashTable<K, V> for LinearProbeHashTable<'a, K, V> where
    K: HashKeyType + DeserializeOwned,
    V: ValueType + DeserializeOwned,
{
    /// linear hash table insert:
    /// 1. slot_index = hash(key) % size
    /// 2. if slot not occupied, insert, done.
    ///    else if
    ///         1. can find next empty slot, insert, done
    ///         2. find same k-v pair, cannot insert, do nothing
    ///    else need resize
    /// 3. if slot of page not exist, allocate one
    fn insert(&mut self, k: &K, v: &V) -> bool {
        let mut header = self.get_header();

        let slot_capacity = HashTableBlockPage::<K, V>::capacity_of_block();
        let slot_idx = ((self.hash_fn)(k) % (header.get_size() * slot_capacity) as u64) as usize;
        let block_idx = slot_idx / slot_capacity;
        let block_offset = slot_idx - block_idx * slot_capacity;

        self.try_insert_to_appropriate_slot(k, v, &mut header, block_idx, block_offset)
    }

    fn remove(&mut self, _k: &K) {
        todo!()
    }

    fn get_value(&mut self, k: &K) -> Vec<V> {
        let mut header = self.get_header();

        let slot_capacity = HashTableBlockPage::<K, V>::capacity_of_block();
        let slot_idx = ((self.hash_fn)(k) % (header.get_size() * slot_capacity) as u64) as usize;
        let block_idx = slot_idx / slot_capacity;
        let block_offset = slot_idx - block_idx * slot_capacity;

        let mut res = Vec::new();
        let blk_pid = header.get_block_page_id(block_idx);
        if blk_pid.is_none() {
            return res;
        }

        let blk = LinearProbeHashTable::<K, V>::get_block(self.buffer_pool_manager, blk_pid.unwrap());
        if !blk.is_occupied(block_offset) {
            return res;
        }

        let (key, val) = blk.get(block_offset);
        if k.eq(key) {
            res.push((*val).clone())
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::common::hash::hash;
    use crate::storage::page::hash_table_block_page::HashTableBlockPage;

    use super::*;

    #[derive(Hash, Default, Clone, Serialize, Deserialize)]
    struct FakeKey {
        data: [u8; 10],
    }

    impl HashKeyType for FakeKey {}

    impl PartialEq<Self> for FakeKey {
        fn eq(&self, other: &Self) -> bool {
            self.data == other.data
        }
    }

    impl Eq for FakeKey {}

    #[derive(Default, Clone, Serialize, Deserialize)]
    struct FakeValue {
        data: [u8; 20],
    }

    impl Eq for FakeValue {}

    impl PartialEq<Self> for FakeValue {
        fn eq(&self, other: &Self) -> bool {
            self.data == other.data
        }
    }

    impl ValueType for FakeValue {}

    const FAKE_HASH: fn(&FakeKey) -> u64 = |key: &FakeKey| { bincode::deserialize(&key.data).unwrap() };

    fn build_kv(k: u64, v: u64) -> (FakeKey, FakeValue) {
        let k_vec = bincode::serialize(&k).unwrap();
        let mut key = FakeKey { data: [0; 10] };
        for i in 0..k_vec.len() {
            key.data[i] = k_vec[i]
        }

        let v_vec = bincode::serialize(&v).unwrap();
        let mut val = FakeValue { data: [0; 20] };
        for i in 0..v_vec.len() {
            val.data[i] = v_vec[i]
        }

        (key, val)
    }

    #[test]
    fn should_build_new_linear_probe_hash_table() {
        // given
        let mut bpm = BufferPoolManager::new_default(100);
        let size: usize = 16;

        // when
        let header_pid = {
            let lpht = LinearProbeHashTable::<FakeKey, FakeValue>::new(size, &mut bpm, hash);
            lpht.header_pid
        };

        // then
        let page_with_lock = bpm.fetch_page(header_pid).unwrap();
        let header_raw = page_with_lock.read().unwrap();
        let header: HashTableHeaderPage = HashTableHeaderPage::deserialize(header_raw.get_data()).unwrap();

        assert_eq!(header.get_size(), size);
        assert_eq!(header.get_page_id(), header_raw.get_id());
    }

    #[test]
    fn should_insert_kv_pair_to_new_block() {
        // given
        let bucket_size = 16;
        let mut bpm = BufferPoolManager::new_default(100);
        let mut header = LinearProbeHashTable::<FakeKey, FakeValue>::new(bucket_size, &mut bpm, FAKE_HASH).get_header();

        let new_block_pid = 1;
        let slot_idx = 0;
        let block_index = 0;
        let block_offset = 21;

        // when
        let (key, val) = build_kv(21, 127);
        LinearProbeHashTable::insert_to_new_block(&mut bpm, &key, &val, &mut header, block_index, block_offset);

        // then
        // get bucket page id
        assert_eq!(header.get_block_page_id(slot_idx).unwrap(), new_block_pid);

        // get value from bucket
        let block_raw = bpm.fetch_page(new_block_pid).unwrap().read().unwrap();
        let block = HashTableBlockPage::<FakeKey, FakeValue>::deserialize(block_raw.get_data()).unwrap();
        let (k, v) = block.get(block_offset);
        assert_eq!(k.data[0], 21);
        assert_eq!(v.data[0], 127);
    }

    #[test]
    fn should_insert_one_kv_to_empty_hashtable() {
        // given
        let bucket_size = 16;
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, hash);

        // when
        let (key, val) = build_kv(1, 127);
        table.insert(&key, &val);

        // then
        // calculate slot index and bucket index
        let slot_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let slot_index = (hash(&key) % (bucket_size * slot_capacity) as u64) as usize;
        let block_index = slot_index / slot_capacity;

        // get bucket page id
        let first_block_page_id = 1;
        let header = table.get_header();
        assert_eq!(header.get_block_page_id(block_index).unwrap(), first_block_page_id);

        // get value from bucket
        let block_raw = bpm.fetch_page(first_block_page_id).unwrap().read().unwrap();
        let block = HashTableBlockPage::<FakeKey, FakeValue>::deserialize(block_raw.get_data()).unwrap();
        let (k, v) = block.get(slot_index - block_index * slot_capacity);
        assert_eq!(k.data[0], 1);
        assert_eq!(v.data[0], 127);
    }

    #[test]
    fn should_insert_one_kv_to_hashtable_with_same_block() {
        // given
        let bucket_size = 16;
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, FAKE_HASH);

        let (key1, val) = build_kv(1, 127);
        table.insert(&key1, &val);

        // when
        let (key2, val) = build_kv(2, 127);
        table.insert(&key2, &val);

        // then
        // calculate slot index and bucket index
        let slot_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let slot_index = (FAKE_HASH(&key2) % (bucket_size * slot_capacity) as u64) as usize;
        let block_index = slot_index / slot_capacity;

        // get bucket page id
        let first_block_page_id = 1;
        let header = table.get_header();
        assert_eq!(header.get_block_page_id(block_index).unwrap(), first_block_page_id);

        // get value from bucket
        let block_raw = bpm.fetch_page(first_block_page_id).unwrap().read().unwrap();
        let block = HashTableBlockPage::<FakeKey, FakeValue>::deserialize(block_raw.get_data()).unwrap();
        let (k, v) = block.get(slot_index - block_index * slot_capacity);
        assert_eq!(k.data[0], 2);
        assert_eq!(v.data[0], 127);
    }

    #[test]
    fn should_insert_one_kv_to_hashtable_with_same_block_meet_collapse() {
        // given
        let bucket_size = 16;
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, FAKE_HASH);

        let (key1, val1) = build_kv(1, 127);
        table.insert(&key1, &val1);

        // when
        let (key2, val2) = build_kv(1, 126);
        table.insert(&key2, &val2);

        // then
        // calculate slot index and bucket index
        let slot_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let slot_index = (FAKE_HASH(&key2) % (bucket_size * slot_capacity) as u64) as usize;
        let block_index = slot_index / slot_capacity;

        // get bucket page id
        let first_block_page_id = 1;
        let header = table.get_header();
        assert_eq!(header.get_block_page_id(block_index).unwrap(), first_block_page_id);

        // get value from bucket
        let block_raw = bpm.fetch_page(first_block_page_id).unwrap().read().unwrap();
        let block = HashTableBlockPage::<FakeKey, FakeValue>::deserialize(block_raw.get_data()).unwrap();
        let (k1, v1) = block.get(slot_index - block_index * slot_capacity);
        assert_eq!(k1.data[0], 1);
        assert_eq!(v1.data[0], 127);
        let (k2, v2) = block.get((slot_index - block_index * slot_capacity) + 1);
        assert_eq!(k2.data[0], 1);
        assert_eq!(v2.data[0], 126);
    }

    #[test]
    fn should_find_next_block_when_index_collapse() {
        // given
        let block_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let mut bpm = BufferPoolManager::new_default(100);

        // current block
        let curr_block_pid =
            {
                let mut curr_block = HashTableBlockPage::<FakeKey, FakeValue>::new();
                for i in 0..block_capacity {
                    curr_block.insert(i, FakeKey { data: [0; 10] }, FakeValue { data: [0; 20] });
                }
                LinearProbeHashTable::<FakeKey, FakeValue>::update_page(&mut bpm, None, curr_block.serialize())
            };

        // next block
        let next_block_pid = {
            let mut next_block = HashTableBlockPage::<FakeKey, FakeValue>::new();
            next_block.insert(0, FakeKey { data: [0; 10] }, FakeValue { data: [0; 20] });
            next_block.insert(1, FakeKey { data: [0; 10] }, FakeValue { data: [0; 20] });
            LinearProbeHashTable::<FakeKey, FakeValue>::update_page(&mut bpm, None, next_block.serialize())
        };

        // when
        let no_available = LinearProbeHashTable::<FakeKey, FakeValue>::find_available_slot(
            &mut bpm, &FakeKey { data: [1; 10] }, &FakeValue { data: [0; 20] }, curr_block_pid, 0);
        let duplicated = LinearProbeHashTable::<FakeKey, FakeValue>::find_available_slot(
            &mut bpm, &FakeKey { data: [0; 10] }, &FakeValue { data: [0; 20] }, next_block_pid, 0);
        let found = LinearProbeHashTable::<FakeKey, FakeValue>::find_available_slot(
            &mut bpm, &FakeKey { data: [1; 10] }, &FakeValue { data: [1; 20] }, next_block_pid, 0);

        // then
        assert!(no_available.not_found());
        assert!(duplicated.duplicated());
        assert!(found.found());
        assert_eq!(found.unwrap().1, 2);
    }

    #[test]
    fn should_insert_one_kv_to_hashtable_with_new_block_when_meet_collapse() {
        // given
        let bucket_size = 16;
        let block_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, FAKE_HASH);

        // fill the first block
        for i in 0..block_capacity {
            let (key, val) = build_kv(i as u64, 127);
            table.insert(&key, &val);
        }

        // when
        let (key, val) = build_kv(0, 33);
        table.insert(&key, &val);

        // then
        let second_block_page_id = 2;
        let block_raw = bpm.fetch_page(second_block_page_id).unwrap().read().unwrap();
        let block = HashTableBlockPage::<FakeKey, FakeValue>::deserialize(block_raw.get_data()).unwrap();
        let (k, v) = block.get(0);
        assert_eq!(k.data[0], 0);
        assert_eq!(v.data[0], 33);
    }

    #[test]
    fn should_insert_one_kv_to_hashtable_with_exist_block_when_meet_collapse() {
        // given
        let bucket_size = 16;
        let block_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, FAKE_HASH);

        // fill the first block
        let (key, val) = build_kv(0, 123);
        table.insert(&key, &val);

        // fill the last block
        let last_block_base_idx = (bucket_size - 1) * block_capacity;
        for i in 0..block_capacity {
            let (key, val) = build_kv((last_block_base_idx + i) as u64, 127);
            table.insert(&key, &val);
        }

        // when
        let (key, val) = build_kv((last_block_base_idx + 1) as u64, 33);
        table.insert(&key, &val);

        // then
        let first_block_page_id = 1;
        let block_raw = bpm.fetch_page(first_block_page_id).unwrap().read().unwrap();
        let block = HashTableBlockPage::<FakeKey, FakeValue>::deserialize(block_raw.get_data()).unwrap();
        let (k, v) = block.get(1);
        assert_eq!(k.data[0], key.data[0]);
        assert_eq!(k.data[1], key.data[1]);
        assert_eq!(v.data[0], 33);
    }

    #[test]
    fn should_not_insert_when_k_v_all_equals() {
        // given
        let bucket_size = 16;
        let block_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, FAKE_HASH);

        // fill the first block
        for i in 0..block_capacity {
            let (key, val) = build_kv(i as u64, 127);
            table.insert(&key, &val);
        }

        // fill the next block's first slot
        let (key, val) = build_kv((block_capacity + 1) as u64, 127);
        table.insert(&key, &val);

        // when
        let (key, val) = build_kv(3, 127);

        // then (not inserted)
        assert!(!table.insert(&key, &val));

        // when
        let (key, val) = build_kv((block_capacity + 1) as u64, 127);

        // then (not inserted)
        assert!(!table.insert(&key, &val));
    }

    #[test]
    fn should_get_kv_from_first_block() {
        // given
        let bucket_size = 16;
        let block_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, FAKE_HASH);

        // fill the first block
        for i in 0..block_capacity {
            let (key, val) = build_kv(i as u64, i as u64);
            table.insert(&key, &val);
        }

        // when
        let (key, _) = build_kv(3, 0);
        let res = table.get_value(&key);

        // then
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].data[0], 3);
    }
}