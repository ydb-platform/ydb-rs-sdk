use dashmap::DashMap;

type PartitionID = i64;
type ProducerID = String;
type Offset = i64;
type SeqNo = i64;

type MessageKey = (ProducerID, ydb::PartitionSessionKey);

#[derive(Default)]
pub(super) struct MessagesOrder {
    per_stream: DashMap<MessageKey, SeqNo>,
    per_producer: DashMap<ProducerID, SeqNo>,
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

        match self.per_producer.entry(producer_id) {
            dashmap::Entry::Occupied(mut occupied_entry) => {
                let last_seq_no = occupied_entry.get_mut();

                if seq_no == *last_seq_no + 1 {
                    *last_seq_no = seq_no;
                } else if seq_no > *last_seq_no {
                    return Err(format!(
                        "messages seq_no violated: last seen seq_no: {}, current seq_no: {seq_no}",
                        *last_seq_no
                    ));
                }
            }

            dashmap::Entry::Vacant(vacant_entry) => {
                if seq_no != 1 {
                    return Err(format!(
                        "messages seq_no violdated: starting seq_no {seq_no}"
                    ));
                }

                vacant_entry.insert(seq_no);
            }
        }

        match self.per_stream.insert(key, seq_no) {
            Some(prev_value) if prev_value + 1 != seq_no => {
                Err("messages seq_no order violated".to_string())
            }

            _ => Ok(()),
        }
    }
}

#[derive(Default)]
pub(super) struct OffsetOrder {
    inner: DashMap<PartitionID, Offset>,
}

impl OffsetOrder {
    pub(super) fn ack_message(&self, message: &ydb::TopicReaderMessage) -> Result<(), String> {
        let partition_id = message.get_partition_id();

        match self.inner.get(&partition_id) {
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
            .entry(partition_id)
            .and_modify(|current_ack_end_offset| {
                *current_ack_end_offset = (*current_ack_end_offset).max(end_offset);
            })
            .or_insert(end_offset);
    }
}
