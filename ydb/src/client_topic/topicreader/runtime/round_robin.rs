use crate::client_topic::topicreader::ids::PartitionSessionId;

#[derive(Default)]
pub(super) struct RoundRobin {
    active: Vec<PartitionSessionId>,
    cursor: usize,
}

impl RoundRobin {
    pub(super) fn len(&self) -> usize {
        self.active.len()
    }

    pub(super) fn push(&mut self, psid: PartitionSessionId) {
        self.active.push(psid);
    }

    pub(super) fn next(&mut self) -> Option<PartitionSessionId> {
        if self.active.is_empty() {
            return None;
        }

        let id = self.active[self.cursor];

        self.cursor += 1;

        if self.cursor == self.active.len() {
            self.cursor = 0;
        }

        Some(id)
    }

    pub(super) fn remove(&mut self, psid: PartitionSessionId) {
        let Some(pos) = self.active.iter().position(|&x| x == psid) else {
            return;
        };
        self.active.swap_remove(pos);
        if self.cursor >= self.active.len() {
            self.cursor = 0;
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, psid: PartitionSessionId) -> bool {
        self.active.contains(&psid)
    }
}

#[cfg(test)]
impl RoundRobin {
    pub(super) fn extend(&mut self, psids: impl IntoIterator<Item = PartitionSessionId>) {
        for psid in psids {
            self.push(psid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn psid(value: i64) -> PartitionSessionId {
        PartitionSessionId::from_raw(value)
    }

    #[test]
    fn push_appends_entries() {
        let mut rr = RoundRobin::default();
        rr.push(psid(1));
        rr.push(psid(1));
        assert_eq!(rr.len(), 2);
        assert_eq!(rr.next(), Some(psid(1)));
        assert_eq!(rr.next(), Some(psid(1)));
    }

    #[test]
    fn next_wraps_cursor() {
        let mut rr = RoundRobin::default();
        rr.extend([psid(1), psid(2), psid(3)]);
        assert_eq!(rr.next(), Some(psid(1)));
        assert_eq!(rr.next(), Some(psid(2)));
        assert_eq!(rr.next(), Some(psid(3)));
        assert_eq!(rr.next(), Some(psid(1)));
    }

    #[test]
    fn next_empty_returns_none() {
        let mut rr = RoundRobin::default();
        assert_eq!(rr.next(), None);
    }

    #[test]
    fn remove_before_cursor_adjusts() {
        let mut rr = RoundRobin::default();
        rr.extend([psid(1), psid(2), psid(3)]);
        rr.next(); // cursor now points at 2 (index 1)
        rr.remove(psid(1)); // remove index 0, which is before cursor
                            // cursor should have decremented to 0, pointing at what was 2
        assert_eq!(rr.next(), Some(psid(2)));
        assert_eq!(rr.next(), Some(psid(3)));
        assert_eq!(rr.next(), Some(psid(2)));
    }

    #[test]
    fn remove_at_cursor_does_not_skip() {
        let mut rr = RoundRobin::default();
        rr.extend([psid(1), psid(2), psid(3)]);
        // cursor at 0 (will serve 1 next)
        rr.remove(psid(1)); // swap_remove: [3, 2], cursor stays 0
        assert_eq!(rr.next(), Some(psid(3)));
        assert_eq!(rr.next(), Some(psid(2)));
        assert_eq!(rr.next(), Some(psid(3)));
    }

    #[test]
    fn remove_last_element_resets_cursor() {
        let mut rr = RoundRobin::default();
        rr.push(psid(42));
        rr.remove(psid(42));
        assert_eq!(rr.len(), 0);
        assert_eq!(rr.next(), None);
    }

    #[test]
    fn extend_appends_entries() {
        let mut rr = RoundRobin::default();
        rr.extend([psid(1), psid(2), psid(1), psid(3), psid(2)]);
        assert_eq!(rr.len(), 5);
    }
}
