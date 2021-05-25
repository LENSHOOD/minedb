use crate::buffer::replacer::{Replacer, ClockReplacer};
use crate::storage::disk_manager::*;
use crate::storage::page::*;
use std::collections::HashMap;

type FrameId = usize;
const INVALID_PAGE_ID: PageId = -1;
struct BufferPoolManager<R: Replacer, D: DiskManager> {
    pool_size: usize,
    page_table: HashMap<PageId, FrameId>,
    free_list: Vec<FrameId>,
    buffer_pool: Vec<Page>,
    replacer: R,
    disk_manager: D
}

impl BufferPoolManager<R, D> {
    fn new(pool_size: usize) -> BufferPoolManager<R, D> {
        BufferPoolManager {
            pool_size,
            page_table: HashMap::new(),
            free_list: (0..pool_size-1).collect(),
            buffer_pool: vec![Page::new(INVALID_PAGE_ID); pool_size],
            replacer: ClockReplacer::new(pool_size),
            disk_manager: FakeDiskManager::new()
        }
    }

    fn fetch_page(id: PageId) -> &Page {
        todo!()
    }

    fn unpin_page(id: PageId, is_dirty: bool) -> bool {
        todo!()
    }

    fn flush_page(id: PageId) -> bool {
        todo!()
    }

    fn new_page() -> &Page {
        todo!()
    }

    fn delete_page(id: PageId) -> bool {
        todo!()
    }
}