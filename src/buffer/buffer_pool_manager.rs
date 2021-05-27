use crate::buffer::replacer::{Replacer, ClockReplacer};
use crate::storage::disk_manager::*;
use crate::storage::page::*;
use std::collections::HashMap;
use std::io;
use std::io::{Error, ErrorKind};

type FrameId = usize;
struct BufferPoolManager {
    page_table: HashMap<PageId, FrameId>,
    free_list: Vec<FrameId>,
    buffer_pool: Vec<Page>,
    replacer: Box<dyn Replacer>,
    disk_manager: Box<dyn DiskManager>
}

impl BufferPoolManager {
    fn new_default(pool_size: usize) -> BufferPoolManager {
        BufferPoolManager {
            page_table: HashMap::new(),
            free_list: (0..pool_size).collect(),
            buffer_pool: vec![EMPTY_PAGE; pool_size],
            replacer: Box::new(ClockReplacer::new(pool_size)),
            disk_manager: Box::new(FakeDiskManager::new())
        }
    }

    fn new(pool_size: usize, replacer: Box<dyn Replacer>, disk_manager: Box<dyn DiskManager>) -> BufferPoolManager {
        BufferPoolManager {
            page_table: HashMap::new(),
            free_list: (0..pool_size).collect(),
            buffer_pool: vec![EMPTY_PAGE; pool_size],
            replacer,
            disk_manager
        }
    }

    // 1.     Search the page table for the requested page (P).
    // 1.1    If P exists, pin it and return it immediately.
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    // 2.     If R is dirty, write it back to the disk.
    // 3.     Delete R from the page table and insert P.
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    fn fetch_page(&mut self, pid: PageId) -> io::Result<&mut Page> {
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
                let (success, vic_fid) = (&mut self.replacer).victim();
                if !success {
                    return Err(Error::new(ErrorKind::Other, "Out of memory to allocate page."))
                }
                self.replacer.pin(vic_fid);
                let p = self.update_page(vic_fid, pid);
                Ok(p)
            }
        };
    }

    fn get_exist_frame(&self, pid: PageId) -> FrameId {
        *self.page_table.get(&pid).unwrap()
    }

    fn update_page(&mut self, fid: FrameId, new_pid: PageId) -> &mut Page {
        let page = &mut self.buffer_pool[fid];
        if page.is_dirty() {
            self.disk_manager.write_page(page.get_id(), page.get_data());
            page.set_dirty(false);
        }

        self.page_table.remove(&page.get_id());
        self.page_table.insert(new_pid, fid);
        page.set_id(new_pid);
        page.pin();
        &self.disk_manager.read_page(new_pid, page.get_data());

        page
    }

    fn unpin_page(&mut self, pid: PageId, is_dirty: bool) -> bool {
        match self.page_table.get(&pid) {
            Some(fid) => {
                let p = &mut self.buffer_pool[*fid];
                p.unpin();
                p.set_dirty(is_dirty);
                self.replacer.unpin(*fid);
                true
            },
            None => {false}
        }
    }

    fn flush_page(&mut self, pid: PageId) -> bool {
        return match self.page_table.get(&pid) {
            Some(fid) => {
                let p = &mut self.buffer_pool[*fid];
                self.disk_manager.write_page(p.get_id(), p.get_data());
                true
            },
            None => {false}
        }
    }

    fn new_page(&self) -> &Page {
        todo!()
    }

    fn delete_page(pid: PageId) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_pool_manager::{BufferPoolManager, FrameId};
    use crate::storage::page::PageId;
    use crate::buffer::replacer::ClockReplacer;
    use crate::storage::disk_manager::*;
    use std::io::ErrorKind;

    const TEST_POOL_SIZE: usize = 5;
    #[test]
    fn should_fetch_page_from_disk_and_add_it_to_pool_when_no_page_found() {
        // given
        let fake_id: PageId = 1;

        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_read_page()
            .withf(move |page_id: &PageId, _page_data: &[u8]| { *page_id == fake_id})
            .return_const(());

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // when
        let page = bpm.fetch_page(fake_id).unwrap();

        // then
        assert_eq!(page.get_id(), fake_id);
        assert_eq!(page.get_pin_count(), 1);
        assert_eq!(*bpm.page_table.get(&fake_id).unwrap(), 4 as usize);
        assert!(!bpm.free_list.contains(&4));
    }

    #[test]
    fn should_fetch_page_directly_from_pool() {
        // given
        let fake_id1: PageId = 1;
        let fake_id2: PageId = 2;
        let fake_id3: PageId = 3;

        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_read_page()
            .times(3)
            .return_const(());

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        bpm.fetch_page(fake_id1).unwrap();
        bpm.fetch_page(fake_id2).unwrap();
        bpm.fetch_page(fake_id3).unwrap();

        // when
        let page2 = bpm.fetch_page(fake_id2).unwrap();

        // then
        assert_eq!(page2.get_id(), fake_id2);
        assert_eq!(page2.get_pin_count(), 2);
    }

    #[test]
    fn should_fetch_page_from_disk_and_retire_old_page_from_replacer() {
        // given
        let fake_id1: PageId = 1;
        let fake_id2: PageId = 2;
        let fake_id3: PageId = 3;
        let fake_id4: PageId = 4;
        let fake_id5: PageId = 5;

        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_read_page()
            .times(7)
            .return_const(());

        dm_mock
            .expect_write_page()
            .times(1)
            .withf(move |page_id: &PageId, _page_data: &[u8]| { *page_id == fake_id2})
            .return_const(());

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // fully occupied (p1=f4, p2=f3, p3=f2, p4=f1, p5=f0)
        bpm.fetch_page(fake_id1).unwrap();
        bpm.fetch_page(fake_id2).unwrap();
        bpm.fetch_page(fake_id3).unwrap();
        bpm.fetch_page(fake_id4).unwrap();
        bpm.fetch_page(fake_id5).unwrap();

        // unpin some
        bpm.unpin_page(fake_id2, true);
        bpm.unpin_page(fake_id3, false);

        {
            // when (victim frame[2] => page3)
            let fake_id6: PageId = 6;
            let page6 = bpm.fetch_page(fake_id6).unwrap();

            // then
            assert_eq!(page6.get_id(), fake_id6);
            assert!(!bpm.page_table.contains_key(&fake_id3));
        }
        {
            // when
            let fake_id7: PageId = 7;
            let page7 = bpm.fetch_page(fake_id7).unwrap();

            // then
            assert_eq!(page7.get_id(), fake_id7);
            assert!(!bpm.page_table.contains_key(&fake_id2));
        }
    }

    #[test]
    fn should_fetch_page_failed_with_oom_error() {
        // given
        let fake_id1: PageId = 1;
        let fake_id2: PageId = 2;
        let fake_id3: PageId = 3;
        let fake_id4: PageId = 4;
        let fake_id5: PageId = 5;
        let fake_id6: PageId = 6;

        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_read_page()
            .return_const(());

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // fully occupied (p1=f4, p2=f3, p3=f2, p4=f1, p5=f0)
        bpm.fetch_page(fake_id1).unwrap();
        bpm.fetch_page(fake_id2).unwrap();
        bpm.fetch_page(fake_id3).unwrap();
        bpm.fetch_page(fake_id4).unwrap();
        bpm.fetch_page(fake_id5).unwrap();

        // when
        let result = bpm.fetch_page(fake_id6);

        // then
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Other);
        assert_eq!(error.to_string(), "Out of memory to allocate page.");
    }

    #[test]
    fn should_unpin_page() {
        // given
        let mut bpm = BufferPoolManager::new_default(TEST_POOL_SIZE);
        let fake_id_1: PageId = 1;
        let fid_to_p1: FrameId = 4;
        let fake_id_2: PageId = 2;
        let fid_to_p2: FrameId = 3;

        // when
        {
            let p1 = bpm.fetch_page(fake_id_1).unwrap();
            assert_eq!(p1.get_pin_count(), 1);
            let p2 = bpm.fetch_page(fake_id_2).unwrap();
            assert_eq!(p2.get_pin_count(), 1);
        }

        bpm.unpin_page(fake_id_1, false);
        bpm.unpin_page(fake_id_2, true);

        // then
        assert_eq!(*bpm.page_table.get(&fake_id_1).unwrap(), fid_to_p1);
        assert_eq!(*bpm.page_table.get(&fake_id_2).unwrap(), fid_to_p2);
        assert!(!bpm.free_list.contains(&fid_to_p1));
        assert!(!bpm.free_list.contains(&fid_to_p2));

        let p1 = &mut bpm.buffer_pool[fid_to_p1];
        assert_eq!(p1.get_pin_count(), 0);
        assert!(!p1.is_dirty());
        let p2 = &mut bpm.buffer_pool[fid_to_p2];
        assert_eq!(p2.get_pin_count(), 0);
        assert!(p2.is_dirty());
    }

    #[test]
    fn should_flush_page() {
        // given
        let fake_id_1: PageId = 1;
        let fid_to_p1: FrameId = 4;
        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_read_page()
            .return_const(());

        dm_mock
            // then
            .expect_write_page()
            .withf(move |page_id: &PageId, page_data: &[u8]| {
                *page_id == fake_id_1
                && page_data[0] == 1
                && page_data[1] == 2
                && page_data[2] == 3
            })
            .return_const(());

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // when
        let mut p1 = bpm.fetch_page(fake_id_1).unwrap();
        let page_data = p1.get_data();
        page_data[0] = 1;
        page_data[1] = 2;
        page_data[2] = 3;

        bpm.flush_page(fake_id_1);
    }
}