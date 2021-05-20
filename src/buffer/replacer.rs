trait Replacer {
    fn victim(&mut self) -> (bool, usize);

    fn pin(&mut self, frame_id: usize);

    fn unpin(&mut self, frame_id: usize);

    fn size(&self) -> usize;
}

const NO_FRAME: i8 = -1;
const REF_ZERO: i8 = 0;
const REF_ONE: i8 = 1;
struct ClockReplacer {
    size: usize,
    last: usize,
    frame_holder: Vec<i8>
}

impl ClockReplacer {
    pub fn new(size: usize) -> ClockReplacer {
        ClockReplacer {
            size: 0,
            last: 0,
            frame_holder: vec![NO_FRAME; size]
        }
    }
}

impl Replacer for ClockReplacer {
    fn victim(&mut self) -> (bool, usize) {
        if self.size == 0 {
            return (false, 0);
        }

        loop {
            let last_value = self.frame_holder[self.last];
            if last_value == REF_ZERO {
                self.pin(self.last);
                return (true, self.last);
            }

            if last_value == REF_ONE {
                self.frame_holder[self.last] = REF_ZERO;
            }

            self.last += 1;
            if self.last == self.frame_holder.len() {
                self.last = 0;
            }
        }
    }

    fn pin(&mut self, frame_id: usize) {
        if self.frame_holder[frame_id] == NO_FRAME {
            return;
        }

        self.frame_holder[frame_id] = NO_FRAME;
        self.size -= 1;
    }

    fn unpin(&mut self, frame_id: usize) {
        if self.frame_holder[frame_id] == NO_FRAME {
            self.size += 1;
        }
        self.frame_holder[frame_id] = REF_ONE;
    }

    fn size(&self) -> usize {
        self.size
    }
}

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
    assert_eq!(replacer.victim(), (true, 1));
    assert_eq!(replacer.victim(), (true, 2));
    assert_eq!(replacer.victim(), (true, 3));

    // Scenario: pin elements in the replacer.
    // Note that 3 has already been victimized, so pinning 3 should have no effect.
    replacer.pin(3);
    replacer.pin(4);
    assert_eq!(replacer.size(), 2);

    // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
    replacer.unpin(4);

    // Scenario: continue looking for victims. We expect these victims.
    assert_eq!(replacer.victim(), (true, 5));
    assert_eq!(replacer.victim(), (true, 6));
    assert_eq!(replacer.victim(), (true, 4));
}