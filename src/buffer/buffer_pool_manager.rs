use crate::buffer::replacer::{Replacer, ClockReplacer};
use crate::storage::disk_manager::*;
use crate::storage::page::*;
use std::collections::HashMap;
use std::io;
use std::io::{Error, ErrorKind};

type FrameId = usize;
struct BufferPoolManager {
    pool_size: usize,
    page_table: HashMap<PageId, FrameId>,
    free_list: Vec<FrameId>,
    buffer_pool: Vec<Page>,
    replacer: Box<dyn Replacer>,
    disk_manager: Box<dyn DiskManager>
}

impl BufferPoolManager {
    fn new(pool_size: usize) -> BufferPoolManager {
        BufferPoolManager {
            pool_size,
            page_table: HashMap::new(),
            free_list: (0..pool_size-1).collect(),
            buffer_pool: vec![EMPTY_PAGE; pool_size],
            replacer: Box::new(ClockReplacer::new(pool_size)),
            disk_manager: Box::new(FakeDiskManager::new())
        }
    }

    // 1.     Search the page table for the requested page (P).
    // 1.1    If P exists, pin it and return it immediately.
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    // 2.     If R is dirty, write it back to the disk.
    // 3.     Delete R from the page table and insert P.
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    fn fetch_page(&mut self, pid: PageId) -> io::Result<&Page> {
        if self.page_table.contains_key(&pid) {
            let fid = self.get_exist_frame(pid);
            self.replacer.pin(fid);
            let p = &mut self.buffer_pool[fid];
            p.pin();
            return Ok(p)
        }

        return match self.free_list.pop() {
            Some(frame_id) => {
                let p = self.update_page(frame_id, pid);
                Ok(p)
            },
            None => {
                let (success, vic_pid) = (&mut self.replacer).victim();
                if !success {
                    return Err(Error::new(ErrorKind::Other, "Out of memory to allocate page."))
                }
                let fid = self.get_exist_frame(vic_pid);
                self.replacer.pin(fid);
                let p = self.update_page(fid, pid);
                Ok(p)
            }
        };
    }

    fn get_exist_frame(&self, pid: PageId) -> FrameId {
        *self.page_table.get(&pid).unwrap()
    }

    fn update_page(&mut self, fid: FrameId, new_pid: PageId) -> &Page {
        let page = &mut self.buffer_pool[fid];
        if page.is_dirty() {
            self.disk_manager.write_page(page.get_id(), page.get_data());
        }

        self.page_table.remove(&page.get_id());
        self.page_table.insert(new_pid, fid);
        page.set_id(new_pid);
        &self.disk_manager.read_page(new_pid, page.get_data());

        page
    }

    fn unpin_page(&self, id: PageId, is_dirty: bool) -> bool {
        todo!()
    }

    fn flush_page(&mut self, id: PageId) -> bool {
        todo!()
    }

    fn new_page(&self) -> &Page {
        todo!()
    }

    fn delete_page(id: PageId) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::storage::page::PageId;

    const TEST_POOL_SIZE: usize = 10;
    #[test]
    fn should_fetch_page_from_disk_and_add_it_to_pool_when_no_page_found() {
        // given
        let mut bpm = BufferPoolManager::new(TEST_POOL_SIZE);
        let fake_id: PageId = 1;

        // when
        let page = bpm.fetch_page(fake_id).unwrap();

        // then
        assert_eq!(page.get_id(), fake_id);
        assert_eq!(*bpm.page_table.get(&fake_id).unwrap(), 8 as usize);
        assert!(!bpm.free_list.contains(&8));
    }
}