use crate::storage::page::page::{PageId, PAGE_SIZE, INVALID_PAGE_ID};
use std::{mem, io};
use std::io::{Error, ErrorKind};
use serde::{Serialize, Deserialize};

const BLOCK_PAGE_IDS_SIZE: usize = (PAGE_SIZE - mem::size_of::<BasicInfo>()) / mem::size_of::<PageId>();
#[derive(Serialize, Deserialize)]
struct BasicInfo {
    page_id: PageId,
    size: usize,
    next_idx: usize,
}

pub struct HashTableHeaderPage {
    basic_info: BasicInfo,
    block_page_ids: [PageId; BLOCK_PAGE_IDS_SIZE]
}

impl HashTableHeaderPage {
    pub fn new(pid: PageId, size: usize) -> HashTableHeaderPage {
        HashTableHeaderPage {
            basic_info: BasicInfo {
                page_id: pid,
                size,
                next_idx: 0
            },
            block_page_ids: [INVALID_PAGE_ID; BLOCK_PAGE_IDS_SIZE]
        }
    }

    pub fn get_page_id(&self) -> PageId {
        self.basic_info.page_id
    }

    pub fn get_size(&self) -> usize {
        self.basic_info.size
    }

    pub fn set_size(&mut self, size: usize) {
        self.basic_info.size = size
    }

    pub fn add(&mut self, pid: PageId) -> io::Result<()> {
        if self.block_page_ids.len() == self.basic_info.next_idx + 1 {
            return Err(Error::new(ErrorKind::Other, "Hash table header fulled."));
        }

        self.block_page_ids[self.basic_info.next_idx] = pid;
        self.basic_info.next_idx += 1;
        Ok(())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut basic_info_part = bincode::serialize(&self.basic_info).unwrap();
        for pid in self.block_page_ids {
            let mut pid_raw = bincode::serialize(&pid).unwrap();
            basic_info_part.append(&mut pid_raw);
        }

        basic_info_part
    }

    pub fn deserialize(page_data: &[u8]) -> io::Result<HashTableHeaderPage> {
        if page_data.len() != PAGE_SIZE {
            return Err(Error::new(ErrorKind::Other, format!("Wrong page data: size not equal to {}", PAGE_SIZE)));
        }

        let basic_info_size = mem::size_of::<BasicInfo>();
        let basic_info = bincode::deserialize::<BasicInfo>(&page_data[0..basic_info_size]).unwrap();

        let page_id_size = mem::size_of::<PageId>();
        let mut block_page_ids = [INVALID_PAGE_ID; BLOCK_PAGE_IDS_SIZE];
        for i in (basic_info_size..(page_data.len() - page_id_size)).step_by(page_id_size) {
            block_page_ids[(i - basic_info_size) / page_id_size] = bincode::deserialize::<PageId>(&page_data[i..i+page_id_size]).unwrap();
        }

        Ok(HashTableHeaderPage {
            basic_info,
            block_page_ids,
        })
    }

    pub fn get_block_page_ids(&self) -> &[PageId] {
        &self.block_page_ids
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::hash_table_header_page::{HashTableHeaderPage, BLOCK_PAGE_IDS_SIZE};
    use crate::storage::page::page::PageId;

    #[test]
    fn should_construct_new_empty_head() {
        // given
        let pid: PageId = 1;
        let size: usize = 10;

        // when
        let header = HashTableHeaderPage::new(pid, size);

        // then
        assert_eq!(header.get_page_id(), pid);
        assert_eq!(header.get_size(), size);
        assert_eq!(header.basic_info.next_idx, 0);
        assert_eq!(header.block_page_ids.len(), 509); // (4096 - (64*3)/8) / 64/8
    }

    #[test]
    fn should_set_head_size() {
        // given
        let mut header = HashTableHeaderPage::new(1, 8);

        // when
        header.set_size(10);

        // then
        assert_eq!(header.get_size(), 10);
    }

    #[test]
    fn should_add_page_id_to_block_page_ids() {
        // given
        let pid_to_be_add: PageId = 20;
        let mut header = HashTableHeaderPage::new(0, 8);

        // when
        let result = header.add(pid_to_be_add);

        // then
        assert!(result.is_ok());

        let next = header.basic_info.next_idx;
        assert_eq!(next, 1);
        assert_eq!(header.block_page_ids[next-1], pid_to_be_add);
    }

    #[test]
    fn should_fail_when_block_page_ids_fulled() {
        // given
        let pid_to_be_add: PageId = 20;
        let mut header = HashTableHeaderPage::new(0, 8);
        for _ in 0..BLOCK_PAGE_IDS_SIZE-1 {
            header.add(0).unwrap();
        }

        // when
        let result = header.add(pid_to_be_add);

        // then
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Hash table header fulled.");
    }

    #[test]
    fn should_serialize_and_deserialize_header() {
        // given
        let pid: PageId = 3;
        let size = 16;

        let test_pid: PageId = 10;
        let mut header = HashTableHeaderPage::new(pid, size);
        header.block_page_ids[1] = test_pid;

        // when
        let raw = header.serialize();
        let deser_header = HashTableHeaderPage::deserialize(raw.as_slice()).unwrap();

        // then
        assert_eq!(deser_header.basic_info.page_id, pid);
        assert_eq!(deser_header.basic_info.size, size);
        assert_eq!(deser_header.block_page_ids[1], test_pid);
    }
}