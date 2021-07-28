use crate::common::hash::HashKeyType;
use crate::common::ValueType;
use crate::storage::page::hash_table_header_page::HashTableHeaderPage;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::page::page::PageId;

pub struct LinearProbeHashTable {
    header_pid: PageId,
    buffer_pool_manager: BufferPoolManager,
}