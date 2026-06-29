#[derive(Default)]
pub(super) struct RoundRobin {
    active: Vec<i64>,
    cursor: usize,
}

impl RoundRobin {
    pub(super) fn push(&mut self, psid: i64) {
        if !self.contains(psid) {
            self.active.push(psid);
        }
    }

    pub(super) fn next(&mut self) -> Option<i64> {
        if self.active.is_empty() {
            return None;
        }
        let id = self.active[self.cursor];
        self.cursor = (self.cursor + 1) % self.active.len();
        Some(id)
    }

    pub(super) fn remove(&mut self, psid: i64) {
        let Some(pos) = self.active.iter().position(|&x| x == psid) else {
            return;
        };
        self.active.swap_remove(pos);
        if self.cursor >= self.active.len() {
            self.cursor = 0;
        }
    }

    pub(super) fn contains(&self, psid: i64) -> bool {
        self.active.contains(&psid)
    }
}

#[cfg(test)]
impl RoundRobin {
    pub(super) fn extend(&mut self, psids: impl IntoIterator<Item = i64>) {
        for psid in psids {
            self.push(psid);
        }
    }

    pub(super) fn len(&self) -> usize {
        self.active.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_is_idempotent() {
        let mut rr = RoundRobin::default();
        rr.push(1);
        rr.push(1);
        assert_eq!(rr.len(), 1);
    }

    #[test]
    fn next_wraps_cursor() {
        let mut rr = RoundRobin::default();
        rr.extend([1, 2, 3]);
        assert_eq!(rr.next(), Some(1));
        assert_eq!(rr.next(), Some(2));
        assert_eq!(rr.next(), Some(3));
        assert_eq!(rr.next(), Some(1));
    }

    #[test]
    fn next_empty_returns_none() {
        let mut rr = RoundRobin::default();
        assert_eq!(rr.next(), None);
    }

    #[test]
    fn remove_before_cursor_adjusts() {
        let mut rr = RoundRobin::default();
        rr.extend([1, 2, 3]);
        rr.next(); // cursor now points at 2 (index 1)
        rr.remove(1); // remove index 0, which is before cursor
                      // cursor should have decremented to 0, pointing at what was 2
        assert_eq!(rr.next(), Some(2));
        assert_eq!(rr.next(), Some(3));
        assert_eq!(rr.next(), Some(2));
    }

    #[test]
    fn remove_at_cursor_does_not_skip() {
        let mut rr = RoundRobin::default();
        rr.extend([1, 2, 3]);
        // cursor at 0 (will serve 1 next)
        rr.remove(1); // swap_remove: [3, 2], cursor stays 0
        assert_eq!(rr.next(), Some(3));
        assert_eq!(rr.next(), Some(2));
        assert_eq!(rr.next(), Some(3));
    }

    #[test]
    fn remove_last_element_resets_cursor() {
        let mut rr = RoundRobin::default();
        rr.push(42);
        rr.remove(42);
        assert_eq!(rr.len(), 0);
        assert_eq!(rr.next(), None);
    }

    #[test]
    fn extend_idempotent() {
        let mut rr = RoundRobin::default();
        rr.extend([1, 2, 1, 3, 2]);
        assert_eq!(rr.len(), 3);
    }
}
