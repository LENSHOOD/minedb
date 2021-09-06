use minedb::buffer::buffer_pool_manager::BufferPoolManager;
use std::thread::Thread;
use minedb::storage::page::page::{PageId, INVALID_PAGE_ID};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use rand::rngs::ThreadRng;

#[test]
fn canary_test() {
    assert!(true)
}

const PAGE_SIZE: usize = 16;

// #[test]
// fn test_concurrent_read_write() {
//     let mut bpm = Arc::new(BufferPoolManager::new_default(PAGE_SIZE));
//
//     let mut pids: [PageId; PAGE_SIZE] = [INVALID_PAGE_ID; PAGE_SIZE];
//     for i in 0..PAGE_SIZE {
//         pids[i] = bpm.new_page().unwrap().read().unwrap().get_id();
//     }
//     let pids = Arc::new(pids);
//
//     let stop_flag = Arc::new(AtomicBool::new(false));
//     let start_reader = || {
//         let stop = stop_flag.clone();
//         let pids = pids.clone();
//         let bpm = bpm.clone();
//
//         let reader = move || {
//             while !stop.load(Ordering::Acquire) {
//                 let slot = (rand::random::<f32>() * PAGE_SIZE as f32) as usize;
//                 let page = bpm.fetch_page(pids[slot]).unwrap().read().unwrap();
//                 let page_data = page.get_data();
//                 assert_eq!(page_data[0], page_data[1] + page_data[2])
//             }
//         };
//
//         std::thread::spawn(reader);
//     };
//
//     let start_writer = || {
//         let stop = stop_flag.clone();
//         let pids = pids.clone();
//         let bpm = bpm.clone();
//
//         let writer = || {
//             while !stop.load(Ordering::Acquire) {
//                 let slot = (rand::random::<f32>() * PAGE_SIZE as f32) as usize;
//                 let mut page = bpm.fetch_page(pids[slot]).unwrap().write().unwrap();
//                 let page_data = page.get_data_mut();
//                 let d1 = rand::random::<i8>();
//                 page_data[1] = d1 as u8;
//                 let d2 = rand::random::<i8>();
//                 page_data[2] = d2 as u8;
//                 let d0 = d1 + d2;
//                 page_data[0] = d0 as u8;
//                 bpm.unpin_page(pids[slot], true);
//             }
//         };
//
//         std::thread::spawn(writer);
//     };
//
//     start_writer();
//     start_writer();
//     start_reader();
//     start_reader();
//
//     std::thread::sleep(Duration::from_secs(10));
//     stop_flag.store(true, Ordering::Release);
// }
