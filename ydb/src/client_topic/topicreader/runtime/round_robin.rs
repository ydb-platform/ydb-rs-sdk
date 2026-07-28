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

    pub(super) fn push(&mut self, partition_session_id: PartitionSessionId) {
        if !self.active.contains(&partition_session_id) {
            self.active.push(partition_session_id);
        }
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

    pub(super) fn remove(&mut self, partition_session_id: PartitionSessionId) {
        let Some(pos) = self
            .active
            .iter()
            .position(|&id| id == partition_session_id)
        else {
            return;
        };

        self.active.swap_remove(pos);
        if self.cursor >= self.active.len() {
            self.cursor = 0;
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
    fn next_wraps_cursor() {
        let mut rr = RoundRobin::default();
        rr.push(psid(1));
        rr.push(psid(2));
        rr.push(psid(3));

        assert_eq!(rr.next(), Some(psid(1)));
        assert_eq!(rr.next(), Some(psid(2)));
        assert_eq!(rr.next(), Some(psid(3)));
        assert_eq!(rr.next(), Some(psid(1)));
    }

    #[test]
    fn next_empty_returns_none() {
        assert_eq!(RoundRobin::default().next(), None);
    }

    #[test]
    fn remove_before_cursor_keeps_next_entry_readable() {
        let mut rr = RoundRobin::default();
        rr.push(psid(1));
        rr.push(psid(2));
        rr.push(psid(3));

        assert_eq!(rr.next(), Some(psid(1)));
        rr.remove(psid(1));

        assert_eq!(rr.next(), Some(psid(2)));
        assert_eq!(rr.next(), Some(psid(3)));
        assert_eq!(rr.next(), Some(psid(2)));
    }

    #[test]
    fn duplicate_push_is_ignored() {
        let mut rr = RoundRobin::default();
        rr.push(psid(1));
        rr.push(psid(1));

        assert_eq!(rr.len(), 1);
        assert_eq!(rr.next(), Some(psid(1)));
        assert_eq!(rr.next(), Some(psid(1)));
    }
}
