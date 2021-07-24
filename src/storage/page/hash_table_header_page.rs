use crate::storage::page::page::{PageId, PAGE_SIZE, INVALID_PAGE_ID};
use std::{mem, io};
use std::io::{Error, ErrorKind};

const BLOCK_PAGE_IDS_SIZE: usize = PAGE_SIZE - (mem::size_of::<BasicInfo>() / mem::size_of::<PageId>());
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
    pub fn new(pid: PageId) -> HashTableHeaderPage {
        HashTableHeaderPage {
            basic_info: BasicInfo {
                page_id: pid,
                size: 0,
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

    pub fn add(&mut self, pid: PageId) -> io::Result<()> {
        if self.get_size() == self.block_page_ids.len() {
            return Err(Error::new(ErrorKind::Other, "Hash table fulled."));
        }

        self.block_page_ids[self.basic_info.next_idx] = pid;
        self.basic_info.next_idx += 1;
        self.basic_info.size += 1;
        Ok(())
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

        // when
        let header = HashTableHeaderPage::new(pid);

        // then
        assert_eq!(header.get_page_id(), pid);
        assert_eq!(header.get_size(), 0);
        assert_eq!(header.basic_info.next_idx, 0);
    }

    #[test]
    fn should_add_page_id_to_block_page_ids() {
        // given
        let pid_to_be_add: PageId = 20;
        let mut header = HashTableHeaderPage::new(0);

        // when
        let result = header.add(pid_to_be_add);

        // then
        assert!(result.is_ok());

        assert_eq!(header.basic_info.size, 1);
        let next = header.basic_info.next_idx;
        assert_eq!(next, 1);
        assert_eq!(header.block_page_ids[next-1], pid_to_be_add);
    }

    #[test]
    fn should_fail_when_block_page_ids_fulled() {
        // given
        let pid_to_be_add: PageId = 20;
        let mut header = HashTableHeaderPage::new(0);
        for _ in 0..BLOCK_PAGE_IDS_SIZE {
            header.add(0).unwrap();
        }

        // when
        let result = header.add(pid_to_be_add);

        // then
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Hash table fulled.");
    }
}