use std::io;
use std::sync::RwLock;

use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::{KeyType, ValueType};
use crate::common::hash::{hash, HashKeyType};
use crate::container::hash::hash_table::HashTable;
use crate::storage::page::hash_table_block_page::HashTableBlockPage;
use crate::storage::page::hash_table_header_page::HashTableHeaderPage;
use crate::storage::page::page::{INVALID_PAGE_ID, Page, PageId};
use std::marker::PhantomData;
use std::borrow::BorrowMut;

pub struct LinearProbeHashTable<'a, K: HashKeyType, V: ValueType> {
    header_pid: PageId,
    buffer_pool_manager: &'a mut BufferPoolManager,
    hash_fn: fn(&K) -> u64,
    phantom: PhantomData<V>
}

impl<'a, K, V> LinearProbeHashTable<'a, K, V>
    where
        K: HashKeyType + DeserializeOwned,
        V: ValueType + DeserializeOwned,
{
    pub fn new(num_buckets: usize, bpm: &mut BufferPoolManager, hash_fn: fn(&K) -> u64) -> LinearProbeHashTable<K, V> {
        let mut header_pid = INVALID_PAGE_ID;
        {
            let mut header_page = bpm.new_page().unwrap().write().unwrap();
            header_pid = header_page.get_id();

            let header = HashTableHeaderPage::new(header_pid, num_buckets);
            let header_raw = header.serialize();
            for i in 0..header_raw.len() {
                header_page.get_data_mut()[i] = header_raw[i];
            }
        }

        LinearProbeHashTable {
            header_pid,
            buffer_pool_manager: bpm,
            hash_fn,
            phantom: PhantomData
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

        let block_pid = LinearProbeHashTable::<K, V>::insert_into_new_page(bpm, new_block.serialize());
        header.set(block_pid, block_idx);

        LinearProbeHashTable::<K, V>::update_page(bpm, header.get_page_id(), header.serialize())
    }

    fn insert_into_new_page(bpm: &mut BufferPoolManager, block_data: Vec<u8>) -> PageId {
        let mut block_pid = INVALID_PAGE_ID;
        {
            let mut block_page = bpm.new_page().unwrap().write().unwrap();
            let page_data = block_page.get_data_mut();
            for i in 0..block_data.len() {
                page_data[i] = block_data[i];
            }
            block_pid = block_page.get_id();
        }

        {
            bpm.unpin_page(block_pid, true);
        }

        block_pid
    }

    fn update_page(bpm: &mut BufferPoolManager, pid: PageId, page_data: Vec<u8>) {
        {
            let mut page = bpm.fetch_page(pid).unwrap().write().unwrap();
            let raw_data = page.get_data_mut();
            for i in 0..page_data.len() {
                raw_data[i] = page_data[i];
            }
        }

        {
            bpm.unpin_page(pid, true);
        }
    }

    fn find_available_slot(bpm: &mut BufferPoolManager, block_pid: usize) -> Option<(HashTableBlockPage<K, V>, usize)> {
        let block = LinearProbeHashTable::<K, V>::get_block(bpm, block_pid);
        for i in 0..HashTableBlockPage::<K, V>::capacity_of_block() {
            if !block.is_occupied(i) {
                return Some((block, i));
            }
        }

        None
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
    fn insert(&mut self, k: &K, v: &V) {
        let mut header = self.get_header();

        let slot_capacity = HashTableBlockPage::<K, V>::capacity_of_block();
        let slot_idx = ((self.hash_fn)(k) % (header.get_size() * slot_capacity) as u64) as usize;
        let block_idx = slot_idx / slot_capacity;
        let block_offset = slot_idx - block_idx * slot_capacity;

        let mut need_cross_block = false;
        match header.get_block_page_id(block_idx) {
            Some(mut block_pid) => {
                {
                    let mut block = HashTableBlockPage::new();
                    {
                        block = LinearProbeHashTable::<K, V>::get_block(self.buffer_pool_manager, block_pid);
                        let inserted = block.insert(block_offset, k.clone(), v.clone());

                        if !inserted {
                            // deal with collapse
                            let mut try_offset = block_offset;
                            while block.is_occupied(try_offset) {
                                try_offset += 1;
                                // goes into bottom of block, need try next block
                                if try_offset == slot_capacity {
                                    need_cross_block = true;
                                    break;
                                }
                            }

                            // found a empty slot
                            if !need_cross_block {
                                assert!(block.insert(try_offset, k.clone(), v.clone()));
                            } else {
                                let mut next_block_idx = block_idx;
                                loop {
                                    // temporary ignore hash table all fulled
                                    if next_block_idx + 1 == header.get_size() {
                                        next_block_idx = 0;
                                    } else {
                                        next_block_idx += 1;
                                    }

                                    let next_block_pid = header.get_block_page_id(next_block_idx);
                                    if next_block_pid.is_none() {
                                        LinearProbeHashTable::<K, V>::insert_to_new_block(self.buffer_pool_manager, k, v, &mut header, next_block_idx, 0);
                                        break;
                                    }

                                    let block_and_offset = LinearProbeHashTable::<K, V>::find_available_slot(self.buffer_pool_manager, next_block_pid.unwrap());
                                    if block_and_offset.is_none() {
                                        continue;
                                    }

                                    let (mut found_block, offset) = block_and_offset.unwrap();
                                    assert!(found_block.insert(offset, k.clone(), v.clone()));

                                    block = found_block;
                                    block_pid = next_block_pid.unwrap();
                                    break;
                                }
                            }
                        }
                    }

                    {
                        LinearProbeHashTable::<K, V>::update_page(self.buffer_pool_manager, block_pid, block.serialize())
                    }
                }
            }
            None => {
                LinearProbeHashTable::<K, V>::insert_to_new_block(self.buffer_pool_manager, k, v, &mut header, block_idx, block_offset);
            }
        }
    }

    fn remove(&mut self, k: &K) {
        todo!()
    }

    fn get_value(&self, k: &K) -> Vec<V> {
        todo!()
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

    #[derive(Default, Clone, Serialize, Deserialize)]
    struct FakeValue {
        data: [u8; 20],
    }

    impl ValueType for FakeValue {}

    const FAKE_HASH: fn(&FakeKey) -> u64 = |key: &FakeKey| { key.data[0] as u64 + (key.data[1] as u64 * 16) };

    #[test]
    fn should_build_new_linear_probe_hash_table() {
        // given
        let mut bpm = BufferPoolManager::new_default(100);
        let size: usize = 16;

        // when
        let mut header_pid = INVALID_PAGE_ID;
        {
            let lpht = LinearProbeHashTable::<FakeKey, FakeValue>::new(size, &mut bpm, hash);
            header_pid = lpht.header_pid;
        }

        // then
        let page_with_lock = bpm.fetch_page(header_pid).unwrap();
        let header_raw = page_with_lock.read().unwrap();
        let header: &HashTableHeaderPage = unsafe {
            std::mem::transmute(header_raw.get_data().as_ptr())
        };

        assert_eq!(header.get_size(), size);
        assert_eq!(header.get_page_id(), header_raw.get_id());
    }

    #[test]
    fn should_insert_kv_pair_to_new_block() {
        // given
        let bucket_size = 16;
        let mut bpm = BufferPoolManager::new_default(100);
        let mut header = LinearProbeHashTable::<FakeKey, FakeValue>::new(bucket_size, &mut bpm, FAKE_HASH).get_header();

        let mut key = FakeKey { data: [0; 10] };
        key.data[0] = 21;
        let mut val = FakeValue { data: [0; 20] };
        val.data[0] = 127;

        let new_block_pid = 1;
        let slot_idx = 0;
        let block_index = 0;
        let block_offset = 21;

        // when
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
        let mut key = FakeKey { data: [0; 10] };
        key.data[0] = 1;
        let mut val = FakeValue { data: [0; 20] };
        val.data[0] = 127;

        // when
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

        let mut key1 = FakeKey { data: [0; 10] };
        key1.data[0] = 1;
        let mut val = FakeValue { data: [0; 20] };
        val.data[0] = 127;
        table.insert(&key1, &val);

        let mut key2 = FakeKey { data: [0; 10] };
        key2.data[0] = 2;

        // when
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

        let mut key1 = FakeKey { data: [0; 10] };
        key1.data[0] = 1;
        let mut val1 = FakeValue { data: [0; 20] };
        val1.data[0] = 127;
        table.insert(&key1, &val1);

        let mut key2 = FakeKey { data: [0; 10] };
        key2.data[0] = 1;
        let mut val2 = FakeValue { data: [0; 20] };
        val2.data[0] = 126;

        // when
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
        let bucket_size = 16;
        let mut bpm = BufferPoolManager::new_default(100);

        // current block
        let mut curr_block_pid = INVALID_PAGE_ID;
        {
            let mut curr_block = HashTableBlockPage::<FakeKey, FakeValue>::new();
            for i in 0..block_capacity {
                curr_block.insert(i, FakeKey { data: [0; 10] }, FakeValue { data: [0; 20] });
            }
            curr_block_pid = LinearProbeHashTable::<FakeKey, FakeValue>::insert_into_new_page(&mut bpm, curr_block.serialize());
        }

        // next block
        let mut next_block_pid = INVALID_PAGE_ID;
        {
            let mut next_block = HashTableBlockPage::<FakeKey, FakeValue>::new();
            next_block.insert(0, FakeKey {data: [0; 10]}, FakeValue {data: [0; 20]});
            next_block.insert(1, FakeKey {data: [0; 10]}, FakeValue {data: [0; 20]});
            next_block_pid = LinearProbeHashTable::<FakeKey, FakeValue>::insert_into_new_page(&mut bpm, next_block.serialize());
        }

        // when
        let no_available =  LinearProbeHashTable::<FakeKey, FakeValue>::find_available_slot(&mut bpm, curr_block_pid);
        let found =  LinearProbeHashTable::<FakeKey, FakeValue>::find_available_slot(&mut bpm, next_block_pid);

        // then
        assert!(no_available.is_none());
        assert!(found.is_some());
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
            let mut key = FakeKey {data: [0; 10]};
            key.data[0] = (i % 16) as u8;
            key.data[1] = (i / 16) as u8;
            let val = FakeValue {data: [127; 20]};
            table.insert(&key, &val);
        }

        // when
        let mut key = FakeKey { data: [0; 10] };
        key.data[0] = 0;
        let mut val = FakeValue { data: [0; 20] };
        val.data[0] = 33;
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
        let mut key = FakeKey { data: [0; 10] };
        key.data[0] = 0;
        let mut val = FakeValue { data: [0; 20] };
        val.data[0] = 123;
        table.insert(&key, &val);

        // fill the last block
        let last_block_base_idx = (bucket_size - 1) * block_capacity;
        for i in 0..block_capacity {
            let mut key = FakeKey {data: [0; 10]};
            key.data[0] = ((last_block_base_idx + i) % 16) as u8;
            key.data[1] = ((last_block_base_idx + i) / 16) as u8;
            let val = FakeValue {data: [127; 20]};
            table.insert(&key, &val);
        }

        // when
        let mut key = FakeKey { data: [0; 10] };
        key.data[0] = ((last_block_base_idx + 1) % 16) as u8;
        key.data[1] = ((last_block_base_idx + 1) / 16) as u8;
        let mut val = FakeValue { data: [0; 20] };
        val.data[0] = 33;
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
}