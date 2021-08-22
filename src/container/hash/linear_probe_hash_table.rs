use crate::common::hash::{HashKeyType, hash};
use crate::common::{ValueType, KeyType};
use crate::storage::page::hash_table_header_page::HashTableHeaderPage;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::page::page::{PageId, INVALID_PAGE_ID, Page};
use crate::container::hash::hash_table::HashTable;
use crate::storage::page::hash_table_block_page::HashTableBlockPage;
use serde::Deserialize;
use std::sync::RwLock;

pub struct LinearProbeHashTable<'a, K: HashKeyType> {
    header_pid: PageId,
    buffer_pool_manager: &'a mut BufferPoolManager,
    hash_fn: fn(&K) -> u64,
}

impl<'a, K:HashKeyType> LinearProbeHashTable<'a, K> {
    pub fn new(num_buckets: usize, bpm: &mut BufferPoolManager, hash_fn: fn(&K) -> u64) -> LinearProbeHashTable<K> {
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
            hash_fn
        }
    }

    fn get_header(&mut self) -> HashTableHeaderPage {
        let header_page = self.buffer_pool_manager
            .fetch_page(self.header_pid).unwrap()
            .read().unwrap();

        HashTableHeaderPage::deserialize(header_page.get_data()).unwrap()
    }

    fn insert_into_new_page(&mut self, block_data: Vec<u8>) -> PageId {
        let mut block_pid = INVALID_PAGE_ID;
        {
            let mut block_page = self.buffer_pool_manager.new_page().unwrap().write().unwrap();
            let page_data = block_page.get_data_mut();
            for i in 0..block_data.len() {
                page_data[i] = block_data[i];
            }
            block_pid = block_page.get_id();
        }

        {
            self.buffer_pool_manager.unpin_page(block_pid, true);
        }

        block_pid
    }

    fn update_header(&mut self, header_data: Vec<u8>) {
        {
            let mut header_page = self.buffer_pool_manager.fetch_page(self.header_pid).unwrap().write().unwrap();
            let page_data = header_page.get_data_mut();
            for i in 0..header_data.len() {
                page_data[i] = header_data[i];
            }
        }

        {
            self.buffer_pool_manager.unpin_page(self.header_pid, true);
        }
    }
}

impl<'a, K, V> HashTable<K, V> for LinearProbeHashTable<'a, K> where
    K: HashKeyType + Deserialize<'a>,
    V: ValueType + Deserialize<'a>,
{
    /// linear hash table insert:
    /// 1. slot_index = hash(key) % size
    /// 2. if slot not occupied, insert, done.
    ///    else if can find next empty slot, insert, done
    ///    else need resize
    /// 3. if slot of page not exist, allocate one
    fn insert(&mut self, k: &K, v: &V) {
        let mut header = self.get_header();

        let slot_capacity = HashTableBlockPage::<K, V>::capacity_of_block();
        let slot_idx = ((self.hash_fn)(k) % (header.get_size() * slot_capacity) as u64) as usize;
        let block_idx = slot_idx / slot_capacity;

        match header.get_block_page_id(block_idx) {
            Some(_pid) => { },
            None => {
                let mut new_block = HashTableBlockPage::<K, V>::new();

                // collapse cannot happen in new block
                assert!(new_block.insert(slot_idx - block_idx * slot_capacity, k.clone(), v.clone()));

                let block_pid = self.insert_into_new_page(new_block.serialize());
                header.set(block_pid, block_idx);

                self.update_header(header.serialize())
            }
        }
    }

    fn remove(&mut self, k: &K) {
        todo!()
    }

    fn get_value(&self, k: &K) -> &V {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::hash::hash;
    use crate::storage::page::hash_table_block_page::HashTableBlockPage;
    use serde::{Serialize, Deserialize};

    pub fn fake_hash<K: HashKeyType>(key: &K) -> u64 {
        1
    }

    #[derive(Hash, Default, Clone, Serialize, Deserialize)]
    struct FakeKey {
        data: [u8; 10]
    }
    impl HashKeyType for FakeKey {}

    #[derive(Default, Clone, Serialize, Deserialize)]
    struct FakeValue {
        data: [u8; 20]
    }
    impl ValueType for FakeValue {}

    #[test]
    fn should_build_new_linear_probe_hash_table() {
        // given
        let mut bpm = BufferPoolManager::new_default(100);
        let size: usize = 16;

        // when
        let mut header_pid = INVALID_PAGE_ID;
        {
            let lpht = LinearProbeHashTable::<FakeKey>::new(size, &mut bpm, fake_hash);
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
    fn should_insert_one_kv_to_empty_hashtable() {
        // given
        let bucket_size = 16;
        let mut bpm = BufferPoolManager::new_default(100);
        let mut table = LinearProbeHashTable::new(bucket_size, &mut bpm, hash);
        let mut key = FakeKey { data: [0; 10]};
        key.data[0] = 1;
        let mut val = FakeValue {data: [0; 20]};
        val.data[0] = 127;

        // when
        table.insert(&key, &val);

        // then
        // calculate slot index and bucket index
        let slot_capacity = HashTableBlockPage::<FakeKey, FakeValue>::capacity_of_block();
        let slot_index = (hash(&key) % (bucket_size * slot_capacity) as u64) as usize;
        let block_index = (hash(&key) % (bucket_size * slot_capacity) as u64) as usize / slot_capacity;

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
}