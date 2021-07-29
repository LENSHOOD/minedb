use crate::common::hash::HashKeyType;
use crate::common::ValueType;
use crate::storage::page::hash_table_header_page::HashTableHeaderPage;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::page::page::{PageId, INVALID_PAGE_ID};

pub struct LinearProbeHashTable<'a> {
    header_pid: PageId,
    buffer_pool_manager: &'a mut BufferPoolManager,
}

impl<'a> LinearProbeHashTable<'a> {
    pub fn new(num_buckets: usize, bpm: &mut BufferPoolManager) -> LinearProbeHashTable {
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
            buffer_pool_manager: bpm
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_new_linear_probe_hash_table() {
        // given
        let mut bpm = BufferPoolManager::new_default(100);
        let size: usize = 16;

        // when
        let mut header_pid = INVALID_PAGE_ID;
        {
            let lpht = LinearProbeHashTable::new(size, &mut bpm);
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
}