use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

pub trait Replacer: Send + Sync {
    fn victim(&self) -> Option<usize>;

    fn pin(&self, frame_id: usize);

    fn unpin(&self, frame_id: usize);

    fn size(&self) -> usize;
}

const NO_FRAME: i8 = -1;
const REF_ZERO: i8 = 0;
const REF_ONE: i8 = 1;
pub struct ClockReplacer {
    size: AtomicUsize,
    last: AtomicUsize,
    frame_holder: Mutex<Vec<i8>>
}

impl ClockReplacer {
    pub fn new(size: usize) -> ClockReplacer {
        ClockReplacer {
            size: AtomicUsize::new(0),
            last: AtomicUsize::new(0),
            frame_holder: Mutex::new(vec![NO_FRAME; size])
        }
    }
}

impl Replacer for ClockReplacer {
    fn victim(&self) -> Option<usize> {
        if self.size.load(Ordering::Acquire) == 0 {
            return None;
        }

        let mut guard = self.frame_holder.lock().unwrap();
        loop {
            if self.size.load(Ordering::Acquire) == 0 {
                return None;
            }

            let mut last = self.last.load(Ordering::Relaxed);
            let last_value = guard[last];
            if last_value == REF_ZERO {
                guard[last] = NO_FRAME;
                self.size.fetch_sub(1, Ordering::AcqRel);
                return Some(last);
            }

            if last_value == REF_ONE {
                guard[last] = REF_ZERO;
            }

            last += 1;
            if last == guard.len() {
                last = 0;
            }
            self.last.store(last, Ordering::Release);
        }
    }

    fn pin(&self, frame_id: usize) {
        let mut guard = self.frame_holder.lock().unwrap();
        if guard[frame_id] != NO_FRAME {
            guard[frame_id] = NO_FRAME;
            self.size.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn unpin(&self, frame_id: usize) {
        let mut guard = self.frame_holder.lock().unwrap();
        if guard[frame_id] == NO_FRAME {
            self.size.fetch_add(1, Ordering::AcqRel);
        }
        guard[frame_id] = REF_ONE;
    }

    fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::replacer::{ClockReplacer, Replacer};

    #[test]
    fn test_clock_replacer() {
        let mut replacer = ClockReplacer::new(7);

        // Scenario: unpin six elements, i.e. add them to the replacer.
        replacer.unpin(1);
        replacer.unpin(2);
        replacer.unpin(3);
        replacer.unpin(4);
        replacer.unpin(5);
        replacer.unpin(6);
        replacer.unpin(1);

        assert_eq!(replacer.size(), 6);

        // Scenario: get three victims from the clock.
        assert_eq!(replacer.victim(), Some(1));
        assert_eq!(replacer.victim(), Some(2));
        assert_eq!(replacer.victim(), Some(3));

        // Scenario: pin elements in the replacer.
        // Note that 3 has already been victimized, so pinning 3 should have no effect.
        replacer.pin(3);
        replacer.pin(4);
        assert_eq!(replacer.size(), 2);

        // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
        replacer.unpin(4);

        // Scenario: continue looking for victims. We expect these victims.
        assert_eq!(replacer.victim(), Some(5));
        assert_eq!(replacer.victim(), Some(6));
        assert_eq!(replacer.victim(), Some(4));
    }
}