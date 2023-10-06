use ydb_grpc::generated::ydb::topic::stream_write_message::write_response;


pub struct MessageWriteInfo {
    pub offset: i64,
}

pub enum MessageSkipReason {
    Unspecified,
    AlreadyWritten,
    InvalidReason,
}

pub enum MessageWriteStatus {
    Written(MessageWriteInfo),
    Skipped(MessageSkipReason),
    Unknown,
}

pub struct WriteAck {
    pub seq_no: i64,
    pub status: MessageWriteStatus,
}

impl From<i32> for MessageSkipReason {
    fn from(value: i32) -> Self {
        match value {
            0 => MessageSkipReason::Unspecified,
            1 => MessageSkipReason::AlreadyWritten,
            _ => MessageSkipReason::InvalidReason,
        }
    }
}

impl From<Option<write_response::write_ack::MessageWriteStatus>> for MessageWriteStatus {
    fn from(value: Option<write_response::write_ack::MessageWriteStatus>) -> Self {
        match value {
            None => MessageWriteStatus::Unknown,
            Some(status) => match status {
                write_response::write_ack::MessageWriteStatus::Written(write_info) => {
                    MessageWriteStatus::Written(MessageWriteInfo {
                        offset: write_info.offset,
                    })
                }
                write_response::write_ack::MessageWriteStatus::Skipped(skip_info) => {
                    MessageWriteStatus::Skipped(MessageSkipReason::from(skip_info.reason))
                }
            },
        }
    }
}

impl From<write_response::WriteAck> for WriteAck {
    fn from(value: write_response::WriteAck) -> Self {
        Self {
            seq_no: value.seq_no,
            status: MessageWriteStatus::from(value.message_write_status),
        }
    }
}
