use crate::storage::page::PageId;
use std::io::Result;

trait DiskManager {
    fn allocate_page(&mut self) -> Result<PageId>;

    fn deallocate_page(&self, page_id: PageId);

    fn write_page(&mut self, page_id: PageId, page_data: &[u8]);

    fn read_page(&self, page_id: PageId, page_data: &mut [u8]);
}

#[cfg(test)]
mod tests {
    use std::io::*;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::page::*;

    const MAX_FILE_PAGES: usize = 65534;
    struct FakeDiskManager {
        page_counter: PageId,
        fake_file: Vec<u8>
    }

    impl FakeDiskManager {
        fn new() -> FakeDiskManager {
            FakeDiskManager {
                page_counter: 0,
                fake_file: vec![0; PAGE_SIZE * MAX_FILE_PAGES]
            }
        }
    }

    impl DiskManager for FakeDiskManager {
        fn allocate_page(&mut self) -> Result<PageId> {
            if self.page_counter > MAX_FILE_PAGES {
                return Err(Error::new(ErrorKind::Other, "Exceeded max page."))
            }

            let page_id_to_returned = self.page_counter;
            self.page_counter+=1;
            Ok(page_id_to_returned)
        }

        fn deallocate_page(&self, _page_id: PageId) {
        }

        fn write_page(&mut self, page_id: PageId, page_data: &[u8]) {
            if page_id > MAX_FILE_PAGES {
                panic!("Illegal page id.")
            }

            for i in 0..PAGE_SIZE {
                self.fake_file[i + page_id * PAGE_SIZE] = page_data[i]
            }
        }

        fn read_page(&self, page_id: PageId, page_data: &mut [u8]) {
            if page_id > MAX_FILE_PAGES {
                panic!("Illegal page id.")
            }

            for i in 0..PAGE_SIZE {
                page_data[i] = self.fake_file[i + page_id * PAGE_SIZE]
            }
        }
    }

    #[test]
    fn test_fake_disk_manager_can_allocate_page_id() {
        let mut fake_disk_manager = FakeDiskManager::new();
        let page_id_1 = fake_disk_manager.allocate_page().unwrap();
        let page_id_2 = fake_disk_manager.allocate_page().unwrap();
        let page_id_3 = fake_disk_manager.allocate_page().unwrap();

        assert_eq!(page_id_1, 0);
        assert_eq!(page_id_2, 1);
        assert_eq!(page_id_3, 2);
    }

    #[test]
    fn test_fake_disk_manager_can_write_page_to_fake_disk() {
        // given
        let mut fake_disk_manager = FakeDiskManager::new();
        let page_id_1 = fake_disk_manager.allocate_page().unwrap();
        let page_id_2 = fake_disk_manager.allocate_page().unwrap();

        let mut page = Page::new(page_id_2);
        let page_data = page.get_data();
        for i in 0..10 {
            page_data[i] = (0x00 + i) as u8
        }

        // when
        fake_disk_manager.write_page(page_id_2, page_data);

        // then
        let mut data_written: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        fake_disk_manager.read_page(page_id_1, &mut data_written);
        assert_eq!(data_written[0], 0x00);
        assert_eq!(data_written[5], 0x00);
        assert_eq!(data_written[9], 0x00);

        fake_disk_manager.read_page(page_id_2, &mut data_written);
        assert_eq!(data_written[0], 0x00);
        assert_eq!(data_written[5], 0x05);
        assert_eq!(data_written[9], 0x09);
    }
}