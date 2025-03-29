use ydb_grpc::generated::ydb::topic::stream_write_message::write_response;

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct MessageWriteInfo {
    pub offset: i64,
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum MessageSkipReason {
    Unspecified,
    AlreadyWritten,
    UnknownReasonCode(i32),
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct MessageWriteInTxInfo {}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum MessageWriteStatus {
    Written(MessageWriteInfo),
    Skipped(MessageSkipReason),
    WrittenInTx(MessageWriteInTxInfo),
    Unknown,
}

pub(crate) struct WriteAck {
    pub seq_no: i64,
    pub status: MessageWriteStatus,
}

impl From<i32> for MessageSkipReason {
    fn from(value: i32) -> Self {
        use write_response::write_ack::skipped::Reason;

        match Reason::from_i32(value) {
            Some(Reason::Unspecified) => MessageSkipReason::Unspecified,
            Some(Reason::AlreadyWritten) => MessageSkipReason::AlreadyWritten,
            None => MessageSkipReason::UnknownReasonCode(value),
        }
    }
}

impl From<Option<write_response::write_ack::MessageWriteStatus>> for MessageWriteStatus {
    fn from(value: Option<write_response::write_ack::MessageWriteStatus>) -> Self {
        match value {
            None => MessageWriteStatus::Unknown,
            Some(write_response::write_ack::MessageWriteStatus::Written(write_info)) => {
                MessageWriteStatus::Written(MessageWriteInfo {
                    offset: write_info.offset,
                })
            }
            Some(write_response::write_ack::MessageWriteStatus::Skipped(skip_info)) => {
                MessageWriteStatus::Skipped(MessageSkipReason::from(skip_info.reason))
            }
            Some(write_response::write_ack::MessageWriteStatus::WrittenInTx(_write_info)) => {
                MessageWriteStatus::WrittenInTx(MessageWriteInTxInfo {})
            }
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
