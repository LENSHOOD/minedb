pub type PageId = usize;
pub const INVALID_PAGE_ID: PageId = usize::MAX;
pub const PAGE_SIZE: usize = 4096;
pub const EMPTY_PAGE: Page = Page {
    id: INVALID_PAGE_ID,
    pin_count: 0,
    dirty_flag: false,
    data: [0; PAGE_SIZE]
};

pub struct Page {
    id: PageId,
    pin_count: u64,
    dirty_flag: bool,
    data: [u8; PAGE_SIZE]
}

impl Page {
    pub fn new(page_id: PageId) -> Page {
        Page {
            id: page_id,
            pin_count: 0,
            dirty_flag: false,
            data: [0; PAGE_SIZE]
        }
    }

    pub fn get_data(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn set_id(&mut self, pid: PageId) {
        self.id = pid
    }

    pub fn get_id(&self) -> PageId {
        self.id
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty_flag
    }

    pub fn get_pin_count(&self) -> u64 {
        self.pin_count
    }

    pub fn pin(&mut self) {
        self.pin_count+=1;
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Page::new(self.id)
    }
}