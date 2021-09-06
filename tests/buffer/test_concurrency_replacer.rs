use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Duration;

use rand::Rng;
use rand::seq::SliceRandom;

use minedb::buffer::replacer;
use minedb::buffer::replacer::{ClockReplacer, Replacer};

#[test]
fn test_clock_replacer_concurrent_operation() {
    let size = 16;
    let clock_replacer = Arc::new(ClockReplacer::new(size));
    let frames: Arc<Vec<usize>> = Arc::new((0..size).collect());
    let stop_flag = Arc::new(AtomicBool::new(false));

    let (sender, receiver) = std::sync::mpsc::channel();
    let start_thread = |func: fn(&Arc<ClockReplacer>, usize, &Sender<usize>)| {
        let cr = clock_replacer.clone();
        let f = frames.clone();
        let stop = stop_flag.clone();
        let sender = sender.clone();

        thread::spawn(move || {
            let mut random = rand::thread_rng();
            while !stop.load(Ordering::Acquire) {
                let fid = f[random.gen_range(0..size)];
                func(&cr, fid, &sender);
            }
        })
    };


    let mut handlers = vec![];
    let rc = start_thread.clone();
    handlers.push(rc(|cr, fid, _| { cr.unpin(fid) }));
    handlers.push(start_thread(|cr, fid, _| { cr.unpin(fid) }));
    handlers.push(start_thread(|cr, fid, _| { cr.pin(fid) }));
    handlers.push(start_thread(|cr, fid, _| { cr.pin(fid) }));
    handlers.push(start_thread(|cr, _fid, sender| {
        if let Some(fid) = cr.victim() {
            sender.send(fid).unwrap();
        }
    }));

    thread::sleep(Duration::from_secs(1));

    stop_flag.store(true, Ordering::Release);
    for handler in handlers {
        handler.join().unwrap();
    }

    let mut iter = receiver.try_iter();
    while let Some(next) = iter.next() {
        assert!(next < size);
    }
}