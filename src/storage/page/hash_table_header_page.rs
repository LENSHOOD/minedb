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
}