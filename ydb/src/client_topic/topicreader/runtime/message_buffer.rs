use std::collections::VecDeque;

use crate::client_topic::topicreader::messages::TopicReaderMessage;

pub(super) struct BufferedBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) bytes_to_release: i64,
    pub(super) epoch: usize,
}

#[derive(Default)]
pub(super) struct MessageBuffer {
    messages: VecDeque<TopicReaderMessage>,
}

impl MessageBuffer {
    pub(super) fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) {
        self.messages.extend(messages);
    }

    pub(super) fn pop_batch(&mut self, cap: usize) -> Option<BufferedBatch> {
        cut_prefix(&mut self.messages, cap).map(|(messages, bytes_to_release, epoch)| {
            BufferedBatch {
                messages,
                bytes_to_release,
                epoch,
            }
        })
    }
}

fn cut_prefix(
    buffer: &mut VecDeque<TopicReaderMessage>,
    cap: usize,
) -> Option<(Vec<TopicReaderMessage>, i64, usize)> {
    let session_key = buffer.front()?.partition_session_key();
    let epoch = buffer.front()?.commit_marker.epoch;
    let mut out = Vec::new();
    let mut bytes: i64 = 0;

    while out.len() < cap {
        let next_session_key = buffer.front().map(|m| m.partition_session_key());
        let Some(next_session_key) = next_session_key else {
            break;
        };
        if next_session_key != session_key {
            break;
        }
        let Some(m) = buffer.pop_front() else {
            break;
        };
        bytes += m.bytes_to_release;
        out.push(m);
    }

    if out.is_empty() {
        None
    } else {
        Some((out, bytes, epoch))
    }
}
