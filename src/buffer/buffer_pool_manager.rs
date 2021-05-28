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

        let fid = self.get_available_frame()?;
        Ok(self.update_page(fid, pid, false))
    }

    fn get_exist_frame(&self, pid: PageId) -> FrameId {
        *self.page_table.get(&pid).unwrap()
    }

    fn get_available_frame(&mut self) -> io::Result<FrameId> {
        match self.free_list.pop() {
            Some(frame_id) => Ok(frame_id),
            None => {
                let (success, vic_fid) = (&mut self.replacer).victim();
                if !success {
                    return Err(Error::new(ErrorKind::Other, "Out of memory to allocate page."))
                }
                Ok(vic_fid)
            }
        }
    }

    fn update_page(&mut self, fid: FrameId, new_pid: PageId, new_page: bool) -> &mut Page {
        self.replacer.pin(fid);

        let page = &mut self.buffer_pool[fid];
        if page.is_dirty() {
            self.disk_manager.write_page(page.get_id(), page.get_data());
            page.set_dirty(false);
        }

        self.page_table.remove(&page.get_id());
        self.page_table.insert(new_pid, fid);

        page.set_id(new_pid);
        page.pin();

        if !new_page {
            self.disk_manager.read_page(new_pid, page.get_data());
        }

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

    fn new_page(&mut self) -> io::Result<&mut Page> {
        let fid = self.get_available_frame()?;
        let pid = self.disk_manager.allocate_page()?;
        Ok(self.update_page(fid, pid, true))
    }

    fn delete_page(&mut self, pid: PageId) -> io::Result<bool> {
        match self.page_table.get(&pid) {
            Some(fid) => {
                let page = &mut self.buffer_pool[*fid];
                if page.get_pin_count() != 0 {
                    return Err(Error::new(ErrorKind::Other, "Cannot delete page that is in use."))
                }

                if page.is_dirty() {
                    self.disk_manager.write_page(page.get_id(), page.get_data());
                }
                self.free_list.push(*fid);
                self.page_table.remove(&pid);
            },
            None => {}
        };

        let done = self.disk_manager.deallocate_page(pid)?;
        if !done {
            return Ok(false)
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_pool_manager::{BufferPoolManager, FrameId};
    use crate::storage::page::PageId;
    use crate::buffer::replacer::ClockReplacer;
    use crate::storage::disk_manager::*;
    use std::io::*;

    const TEST_POOL_SIZE: usize = 5;
    #[test]
    fn should_fetch_page_from_disk_and_add_it_to_pool_when_no_page_found() {
        // given
        let fake_id: PageId = 1;
        let fid_to_p1: FrameId = 4;

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
        assert_eq!(*bpm.page_table.get(&fake_id).unwrap(), fid_to_p1);
        assert!(!bpm.free_list.contains(&fid_to_p1));
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
        let p1 = bpm.fetch_page(fake_id_1).unwrap();
        let page_data = p1.get_data();
        page_data[0] = 1;
        page_data[1] = 2;
        page_data[2] = 3;

        bpm.flush_page(fake_id_1);
    }

    #[test]
    fn should_allocate_new_page() {
        // given
        let fake_id_1: PageId = 1;
        let fid_to_p1: FrameId = 4;
        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_allocate_page()
            .return_once(move || Ok(fake_id_1));

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // when
        let p1 = bpm.new_page().unwrap();

        // then
        assert_eq!(p1.get_id(), fake_id_1);
        assert_eq!(p1.get_pin_count(), 1);
        assert_eq!(*bpm.page_table.get(&fake_id_1).unwrap(), fid_to_p1);
        assert!(!bpm.free_list.contains(&fid_to_p1));
    }

    #[test]
    fn should_fail_when_disk_manager_cannot_allocate_page() {
        // given
        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_allocate_page()
            .return_once(move || Err(Error::new(ErrorKind::Other, "Exceeded max page.")));

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // when
        let result = bpm.new_page();

        // then
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Other);
        assert_eq!(error.to_string(), "Exceeded max page.");
    }

    #[test]
    fn should_delete_page() {
        // given
        let fake_id_1: PageId = 1;
        let fid_to_p1: FrameId = 4;
        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_allocate_page()
            .return_once(move || Ok(fake_id_1));
        dm_mock
            .expect_deallocate_page()
            .return_once(move |_| Ok(true));

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // when
        bpm.new_page().unwrap();
        bpm.unpin_page(fake_id_1, false);
        let deleted = bpm.delete_page(fake_id_1);

        // then
        assert!(deleted.unwrap());
        assert!(!bpm.page_table.contains_key(&fake_id_1));
        assert!(bpm.free_list.contains(&fid_to_p1));
    }

    #[test]
    fn should_fail_to_delete_when_page_pin_count_not_zero() {
        // given
        let fake_id_1: PageId = 1;
        let fid_to_p1: FrameId = 4;
        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_allocate_page()
            .return_once(move || Ok(fake_id_1));
        dm_mock
            .expect_deallocate_page()
            .return_once(move |_| Ok(true));

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // when
        bpm.new_page().unwrap();
        let deleted = bpm.delete_page(fake_id_1);

        // then
        assert!(deleted.is_err());
        assert!(bpm.page_table.contains_key(&fake_id_1));
        assert!(!bpm.free_list.contains(&fid_to_p1));
    }

    #[test]
    fn should_do_nothing_but_return_false_when_page_not_found() {
        // given
        let fake_id_1: PageId = 1;
        let mut dm_mock = MockDiskManager::new();
        dm_mock
            .expect_deallocate_page()
            .return_once(move |_| Ok(false));

        let mut bpm = BufferPoolManager::new(
            TEST_POOL_SIZE,
            Box::new(ClockReplacer::new(TEST_POOL_SIZE)),
            Box::new(dm_mock));

        // when
        let deleted = bpm.delete_page(fake_id_1);

        // then
        assert!(!deleted.unwrap());
    }

}