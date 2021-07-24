use crate::storage::page::page::{PageId, PAGE_SIZE};
use std::io::{Result, Error, ErrorKind, Seek, Write, SeekFrom, Read};
#[cfg(test)]
use mockall::{automock, predicate::*};
use std::fs::{File, OpenOptions};
use std::path::Path;

#[cfg_attr(test, automock)]
pub trait DiskManager {
    fn allocate_page(&mut self) -> Result<PageId>;

    fn deallocate_page(&mut self, page_id: PageId) -> Result<bool> ;

    fn write_page(&mut self, page_id: PageId, page_data: &[u8]) -> Result<()>;

    fn read_page(&mut self, page_id: PageId, page_data: &mut [u8]) -> Result<()>;
}

const MAX_FILE_PAGES: usize = 0x1 << 16;
pub struct FakeDiskManager {
    page_counter: PageId,
    fake_file: Vec<u8>
}

impl FakeDiskManager {
    pub fn new() -> FakeDiskManager {
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

    fn deallocate_page(&mut self, _page_id: PageId) -> Result<bool> {
        Ok(true)
    }

    fn write_page(&mut self, page_id: PageId, page_data: &[u8]) -> Result<()> {
        if page_id > MAX_FILE_PAGES {
            panic!("Illegal page id.")
        }

        for i in 0..PAGE_SIZE {
            self.fake_file[i + page_id * PAGE_SIZE] = page_data[i];
        }

        Ok(())
    }

    fn read_page(&mut self, page_id: PageId, page_data: &mut [u8]) -> Result<()> {
        if page_id > MAX_FILE_PAGES {
            panic!("Illegal page id.")
        }

        for i in 0..PAGE_SIZE {
            page_data[i] = self.fake_file[i + page_id * PAGE_SIZE];
        }

        Ok(())
    }
}

pub struct FileDiskManager {
    page_counter: PageId,
    page_table: [u8; MAX_FILE_PAGES >> 3],
    file: File
}

impl FileDiskManager {
    pub fn new(file_path: &Path) -> FileDiskManager {
        if !file_path.exists() {
            let mut new_file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(file_path)
                .unwrap();
            let empty_data = [0 as u8; PAGE_SIZE];
            for _i in 0..MAX_FILE_PAGES {
                new_file.write_all(&empty_data).unwrap()
            }
            new_file.flush().unwrap();
        }

        FileDiskManager {
            page_counter: 0,
            page_table: [0; MAX_FILE_PAGES >> 3],
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_path)
                .unwrap()
        }
    }

    fn get_free_slot(&self) -> Option<usize> {
        let curr_slot = self.page_counter;
        let mut curr_byte = curr_slot >> 3;
        for _i in 0..self.page_table.len() {
            if self.page_table[curr_byte] != 0xff {
                break;
            }

            curr_byte = (curr_byte + 1) & (self.page_table.len() - 1);
        }

        let free_byte = self.page_table[curr_byte] as u16;
        let slot: usize = match !free_byte & (free_byte + 1) {
            0x1 => 0,
            0x2 => 1,
            0x4 => 2,
            0x8 => 3,
            0x10 => 4,
            0x20 => 5,
            0x40 => 6,
            0x80 => 7,
            _ => {return None}
        };
        Some(curr_byte * 8 + slot as usize)
    }

    fn set_slot(&mut self) {
        let slot_byte = self.page_counter / 8;
        let slot_bit = self.page_counter % 8;
        self.page_table[slot_byte] |= 0x1 << slot_bit;
    }

    fn clear_slot(&mut self, slot: usize) {
        let slot_byte = slot / 8;
        let slot_bit = slot % 8;
        self.page_table[slot_byte] &= !(0x1 << slot_bit);
    }

    fn validate_page_id(&self, pid: PageId) -> Result<()> {
        if pid >= MAX_FILE_PAGES {
            return Err(Error::new(ErrorKind::Other, "Invalid page id."))
        }

        Ok(())
    }

    fn validate_allocation(&self, pid: PageId) -> Result<()> {
        let slot_byte = pid / 8;
        let slot_bit = pid % 8;
        if (self.page_table[slot_byte] >> slot_bit) & 0x1 != 0x1 {
            return Err(Error::new(ErrorKind::Other, "Page id not allocate."))
        }

        Ok(())
    }
}

impl DiskManager for FileDiskManager {
    fn allocate_page(&mut self) -> Result<usize> {
        match self.get_free_slot() {
            Some(free_slot) => {
                self.page_counter = free_slot;
                self.set_slot();
                Ok(free_slot)
            }
            None => return Err(Error::new(ErrorKind::Other, "Exceeded max page."))
        }
    }

    fn deallocate_page(&mut self, page_id: usize) -> Result<bool> {
        self.validate_page_id(page_id)?;
        self.clear_slot(page_id);
        Ok(true)
    }

    fn write_page(&mut self, page_id: usize, page_data: &[u8]) -> Result<()> {
        self.validate_page_id(page_id)?;
        self.validate_allocation(page_id)?;

        self.file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64)).unwrap();
        self.file.write_all(page_data)
    }

    fn read_page(&mut self, page_id: usize, page_data: &mut [u8]) -> Result<()> {
        self.validate_page_id(page_id)?;
        self.validate_allocation(page_id)?;

        self.file.seek(SeekFrom::Start((page_id * PAGE_SIZE) as u64)).unwrap();
        self.file.read_exact(page_data)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::disk::disk_manager::{DiskManager, FakeDiskManager, FileDiskManager, MAX_FILE_PAGES};
    use crate::storage::page::page::*;
    use std::fs::remove_file;
    use std::path::Path;
    use rand::Rng;
    use std::os::macos::fs::MetadataExt;

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
        let page_data = page.get_data_mut();
        for i in 0..10 {
            page_data[i] = (0x00 + i) as u8
        }

        // when
        fake_disk_manager.write_page(page_id_2, page_data).unwrap();

        // then
        let mut data_written: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        fake_disk_manager.read_page(page_id_1, &mut data_written).unwrap();
        assert_eq!(data_written[0], 0x00);
        assert_eq!(data_written[5], 0x00);
        assert_eq!(data_written[9], 0x00);

        fake_disk_manager.read_page(page_id_2, &mut data_written).unwrap();
        assert_eq!(data_written[0], 0x00);
        assert_eq!(data_written[5], 0x05);
        assert_eq!(data_written[9], 0x09);
    }

    const TEST_FILE_PATH: &str = "./test_storage";
    #[test]
    fn should_create_and_init_file_if_not_exists() {
        let path = TEST_FILE_PATH.to_string() + "1";
        remove_file(path.as_str()).unwrap_or(());

        FileDiskManager::new(Path::new(path.as_str()));

        let file_path = Path::new(path.as_str());
        assert!(file_path.exists());
        assert!(file_path.is_file());
        assert_eq!(file_path.file_name().unwrap(), "test_storage1");

        let metadata = file_path.metadata().unwrap();
        assert_eq!(metadata.st_size(), (PAGE_SIZE * MAX_FILE_PAGES) as u64);

        remove_file(path.as_str()).unwrap();
    }

    #[test]
    fn should_allocate_and_deallocate_page() {
        let path = TEST_FILE_PATH.to_string() + "2";

        // setup
        let mut fdm = FileDiskManager::new(Path::new(path.as_str()));

        // first page id should be 0
        let pid1 = fdm.allocate_page().unwrap();
        assert_eq!(pid1, 0);

        // fully allocate page to maximum
        for _i in 0..fdm.page_table.len()*8 - 1 {
            fdm.allocate_page().unwrap();
        }
        assert!(fdm.page_table.iter().all(|b| *b == 0xff));

        // should return maximum exceeded err
        let should_err = fdm.allocate_page();
        assert!(should_err.is_err());
        assert_eq!(should_err.err().unwrap().to_string(), "Exceeded max page.");

        // should fail when deallocate invalid page id
        let should_err = fdm.deallocate_page(usize::MAX);
        assert!(should_err.is_err());
        assert_eq!(should_err.err().unwrap().to_string(), "Invalid page id.");

        // random deallocate pages
        let mut rng = rand::thread_rng();
        let mut expected_page_ids: [usize; 5] = [0; 5];
        for i in 0..expected_page_ids.len() {
            expected_page_ids[i] = rng.gen_range(0..fdm.page_table.len());
            fdm.deallocate_page(expected_page_ids[i]).unwrap();

            let byte_index = expected_page_ids[i] >> 3;
            let slot = expected_page_ids[i] - (byte_index << 3);
            assert_eq!(fdm.page_table[byte_index] & 0x1 << slot, 0x0);
        }

        // re-allocate
        let mut real_allocate_page_ids = [0 as usize; 5];
        for i in 0..real_allocate_page_ids.len() {
            real_allocate_page_ids[i] = fdm.allocate_page().unwrap();
        }
        assert_eq!(expected_page_ids.sort(), real_allocate_page_ids.sort());

        remove_file(path.as_str()).unwrap();
    }

    #[test]
    fn should_write_page_data_then_read_it_out() {
        let path = TEST_FILE_PATH.to_string() + "3";

        // given
        let mut rng = rand::thread_rng();
        let mut data = [0 as u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            data[i] = rng.gen();
        }
        let mut fdm = FileDiskManager::new(Path::new(path.as_str()));
        let mut pid: PageId = EMPTY_PAGE.get_id();
        for _i in 0..rng.gen_range(0..MAX_FILE_PAGES - 1) + 1 {
            pid = fdm.allocate_page().unwrap()
        }

        // when
        fdm.write_page(pid, &data).unwrap();

        // then
        let mut read_data = [0 as u8; PAGE_SIZE];
        fdm.read_page(pid, &mut read_data).unwrap();
        assert_eq!(data, read_data);

        remove_file(path.as_str()).unwrap();
    }
}