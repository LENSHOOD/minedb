type PageId = usize;
const PAGE_SIZE: usize = 4096;
struct Page {
    id: PageId,
    pin_count: u64,
    dirty_flag: bool,
    data: [u8; PAGE_SIZE]
}

impl Page {
    fn new(page_id: PageId) -> Page {
        Page {
            id: page_id,
            pin_count: 0,
            dirty_flag: false,
            data: [0; PAGE_SIZE]
        }
    }

    fn get_id(&self) -> PageId {
        self.id
    }

    fn is_dirty(&self) -> bool {
        self.dirty_flag
    }

    fn get_pin_count(&self) -> u64 {
        self.pin_count
    }
}