use std::collections::{hash_map, HashMap};
use std::sync::Mutex;

type PartitionID = i64;
type ProducerID = String;
type Offset = i64;
type SeqNo = i64;

type MessageKey = (ProducerID, ydb::PartitionSessionKey);

#[derive(Default)]
pub(super) struct MessagesOrder {
    per_stream: Mutex<HashMap<MessageKey, SeqNo>>,
    per_producer: Mutex<HashMap<ProducerID, SeqNo>>,
}

fn message_into_key_value(message: &ydb::TopicReaderMessage) -> (MessageKey, SeqNo) {
    let key = (
        message.get_producer_id().to_string(),
        message.partition_session_key(),
    );
    let value = message.seq_no;

    (key, value)
}

impl MessagesOrder {
    pub(super) fn insert(&self, message: &ydb::TopicReaderMessage) -> Result<(), String> {
        let (key, seq_no) = message_into_key_value(message);
        let producer_id = message.get_producer_id().to_string();

        {
            let mut per_producer = self.per_producer.lock().unwrap();
            match per_producer.entry(producer_id) {
                hash_map::Entry::Occupied(mut e) => {
                    let last_seq_no = e.get_mut();

                    if seq_no == *last_seq_no + 1 {
                        *last_seq_no = seq_no;
                    } else if seq_no > *last_seq_no {
                        return Err(format!(
                            "messages seq_no violated: last seen seq_no: {}, current seq_no: {seq_no}",
                            *last_seq_no
                        ));
                    }
                }
                hash_map::Entry::Vacant(e) => {
                    if seq_no != 1 {
                        return Err(format!(
                            "messages seq_no violated: starting seq_no {seq_no}"
                        ));
                    }
                    e.insert(seq_no);
                }
            }
        }

        {
            let mut per_stream = self.per_stream.lock().unwrap();
            match per_stream.entry(key) {
                hash_map::Entry::Occupied(mut e) => {
                    let prev = *e.get();
                    if prev + 1 != seq_no {
                        return Err(format!(
                            "messages seq_no order violated: expected {}, got {seq_no}",
                            prev + 1
                        ));
                    }
                    *e.get_mut() = seq_no;
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(seq_no);
                }
            }
        }

        Ok(())
    }
}

#[derive(Default)]
pub(super) struct OffsetOrder {
    inner: Mutex<HashMap<PartitionID, Offset>>,
}

impl OffsetOrder {
    pub(super) fn ack_message(&self, message: &ydb::TopicReaderMessage) -> Result<(), String> {
        let partition_id = message.get_partition_id();
        let inner = self.inner.lock().unwrap();

        match inner.get(&partition_id) {
            Some(current_ack_end_offset) if message.offset < *current_ack_end_offset => {
                Err(format!(
                    "partition: {partition_id}, offset: {}, already committed end_offset: {})",
                    message.offset, *current_ack_end_offset
                ))
            }

            _ => Ok(()),
        }
    }

    pub(super) fn insert(&self, partition_id: PartitionID, end_offset: Offset) {
        self.inner
            .lock()
            .unwrap()
            .entry(partition_id)
            .and_modify(|current_ack_end_offset| {
                *current_ack_end_offset = (*current_ack_end_offset).max(end_offset);
            })
            .or_insert(end_offset);
    }
}
