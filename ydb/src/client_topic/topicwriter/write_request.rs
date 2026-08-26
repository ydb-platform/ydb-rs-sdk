use prost::Message;
use ydb_grpc::ydb_proto::topic::TransactionIdentity;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;
use ydb_grpc::ydb_proto::topic::stream_write_message::{FromClient, WriteRequest};

use crate::byte_units::KiB;
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};

// Safety margin required for topic write requests by
// https://github.com/ydb-platform/ydb-rs-sdk/issues/602.
pub(super) const WRITE_REQUEST_SIZE_RESERVE_BYTES: usize = 4 * KiB;

// The `messages` field in WriteRequest and the `write_request` variant in FromClient both have
// single-byte protobuf keys. Tests compare the calculated size with prost's actual encoded size.
const LENGTH_DELIMITED_FIELD_KEY_SIZE: usize = 1;

#[derive(Clone)]
pub(super) struct WriteRequestSettings {
    max_write_request_size: usize,
}

impl WriteRequestSettings {
    pub(super) fn new(grpc_max_message_size: usize) -> YdbResult<Self> {
        let Some(max_write_request_size) = grpc_max_message_size
            .checked_sub(WRITE_REQUEST_SIZE_RESERVE_BYTES)
            .filter(|size| *size > 0)
        else {
            return Err(YdbError::custom(format!(
                "gRPC max message size must exceed the topic write request reserve: max_message_size={grpc_max_message_size}, reserve={WRITE_REQUEST_SIZE_RESERVE_BYTES}",
            )));
        };

        Ok(Self {
            max_write_request_size,
        })
    }
}

pub(super) enum TryAddMessage {
    Added,
    RequestFull(MessageData),
}

pub(super) struct PendingWriteRequest {
    request: WriteRequest,
    write_request_encoded_len: usize,
    max_write_request_size: usize,
}

impl PendingWriteRequest {
    pub(super) fn new(
        settings: &WriteRequestSettings,
        codec: Codec,
        first_message: MessageData,
        transaction: Option<&TransactionIdentity>,
    ) -> YdbResult<Self> {
        let mut request = WriteRequest {
            messages: Vec::with_capacity(1),
            codec: codec.code,
            tx: transaction.cloned(),
        };
        let base_encoded_len = request.encoded_len();
        let message_encoded_len = length_delimited_field_encoded_len(first_message.encoded_len());
        let write_request_encoded_len = base_encoded_len + message_encoded_len;
        let encoded_len = length_delimited_field_encoded_len(write_request_encoded_len);

        if encoded_len > settings.max_write_request_size {
            return Err(YdbError::custom(format!(
                "topic writer message exceeds the gRPC write request size limit: seq_no={}, encoded_size={encoded_len}, limit={}",
                first_message.seq_no, settings.max_write_request_size,
            )));
        }

        request.messages.push(first_message);

        Ok(Self {
            request,
            write_request_encoded_len,
            max_write_request_size: settings.max_write_request_size,
        })
    }

    pub(super) fn codec(&self) -> Codec {
        Codec {
            code: self.request.codec,
        }
    }

    pub(super) fn try_add(&mut self, message: MessageData) -> TryAddMessage {
        let message_encoded_len = length_delimited_field_encoded_len(message.encoded_len());
        let write_request_encoded_len = self.write_request_encoded_len + message_encoded_len;
        let encoded_len = length_delimited_field_encoded_len(write_request_encoded_len);

        if encoded_len > self.max_write_request_size {
            return TryAddMessage::RequestFull(message);
        }

        self.request.messages.push(message);
        self.write_request_encoded_len = write_request_encoded_len;
        TryAddMessage::Added
    }

    pub(super) fn into_grpc_message(self) -> YdbResult<FromClient> {
        let message = FromClient {
            client_message: Some(ClientMessage::WriteRequest(self.request)),
        };
        let encoded_size = message.encoded_len();

        if encoded_size > self.max_write_request_size {
            return Err(YdbError::custom(format!(
                "topic write request exceeds the configured size limit: encoded_size={encoded_size}, limit={}",
                self.max_write_request_size,
            )));
        }

        Ok(message)
    }
}

fn length_delimited_field_encoded_len(value_encoded_len: usize) -> usize {
    LENGTH_DELIMITED_FIELD_KEY_SIZE
        + prost::length_delimiter_len(value_encoded_len)
        + value_encoded_len
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(seq_no: i64, data_size: usize) -> MessageData {
        MessageData {
            seq_no,
            data: vec![0; data_size],
            ..Default::default()
        }
    }

    fn settings(max_write_request_size: usize) -> WriteRequestSettings {
        WriteRequestSettings::new(WRITE_REQUEST_SIZE_RESERVE_BYTES + max_write_request_size)
            .unwrap()
    }

    fn encoded_request_size(messages: Vec<MessageData>) -> usize {
        FromClient {
            client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                messages,
                codec: Codec::RAW.code,
                tx: None,
            })),
        }
        .encoded_len()
    }

    #[test]
    fn rejects_grpc_limit_without_write_request_capacity() {
        let result = WriteRequestSettings::new(WRITE_REQUEST_SIZE_RESERVE_BYTES);

        assert!(result.is_err());
    }

    #[test]
    fn rejects_single_message_above_limit() {
        let message = message(42, 8);
        let encoded_size = encoded_request_size(vec![message.clone()]);
        let limit = encoded_size - 1;

        let result = PendingWriteRequest::new(&settings(limit), Codec::RAW, message, None);

        assert!(result.is_err());
    }

    #[test]
    fn returns_message_when_request_is_full() {
        let first = message(1, 8);
        let second = message(2, 8);
        let one_message_size = encoded_request_size(vec![first.clone()]);
        let mut pending =
            PendingWriteRequest::new(&settings(one_message_size), Codec::RAW, first, None).unwrap();

        let result = pending.try_add(second);

        assert!(matches!(
            result,
            TryAddMessage::RequestFull(message) if message.seq_no == 2
        ));
        let message = pending.into_grpc_message().unwrap();
        assert_eq!(message.encoded_len(), one_message_size);
    }

    #[test]
    fn calculated_size_matches_complete_protobuf_message() {
        let tx_identity = TransactionIdentity {
            id: "transaction".to_string(),
            session: "session".to_string(),
        };
        let settings = WriteRequestSettings::new(WRITE_REQUEST_SIZE_RESERVE_BYTES + 1024).unwrap();
        let mut pending =
            PendingWriteRequest::new(&settings, Codec::GZIP, message(1, 8), Some(&tx_identity))
                .unwrap();
        assert!(matches!(
            pending.try_add(message(2, 16)),
            TryAddMessage::Added
        ));
        let calculated_size = length_delimited_field_encoded_len(pending.write_request_encoded_len);

        let message = pending.into_grpc_message().unwrap();

        assert_eq!(message.encoded_len(), calculated_size);
    }

    #[test]
    fn finalization_rejects_request_above_limit() {
        let settings = settings(1024);
        let mut pending =
            PendingWriteRequest::new(&settings, Codec::RAW, message(1, 8), None).unwrap();
        let encoded_size = length_delimited_field_encoded_len(pending.write_request_encoded_len);
        let limit = encoded_size - 1;
        pending.max_write_request_size = limit;

        let result = pending.into_grpc_message();

        assert!(result.is_err());
    }
}
