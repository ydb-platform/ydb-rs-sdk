use ydb_grpc::ydb_proto::topic::stream_write_message::write_response;

use crate::{YdbError, YdbResult};

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Debug)]
pub struct MessageWriteInfo {
    pub offset: i64,
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Debug)]
pub enum MessageSkipReason {
    Unspecified,
    AlreadyWritten,
    UnknownReasonCode(i32),
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Debug)]
pub struct MessageWriteInTxInfo {}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(Debug)]
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

pub(crate) type MessageWriteStatusValidator =
    fn(MessageWriteStatus) -> YdbResult<MessageWriteStatus>;

pub(crate) fn accept_any_write_status(status: MessageWriteStatus) -> YdbResult<MessageWriteStatus> {
    Ok(status)
}

pub(crate) fn expect_transactional_write_status(
    status: MessageWriteStatus,
) -> YdbResult<MessageWriteStatus> {
    match status {
        MessageWriteStatus::WrittenInTx(_) => Ok(status),
        other_status => Err(YdbError::custom(format!(
            "expected WrittenInTx ack from server, got: {other_status:?}"
        ))),
    }
}

impl From<i32> for MessageSkipReason {
    fn from(value: i32) -> Self {
        use write_response::write_ack::skipped::Reason;

        let Ok(reason) = Reason::try_from(value) else {
            return MessageSkipReason::UnknownReasonCode(value);
        };

        match reason {
            Reason::Unspecified => MessageSkipReason::Unspecified,
            Reason::AlreadyWritten => MessageSkipReason::AlreadyWritten,
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
