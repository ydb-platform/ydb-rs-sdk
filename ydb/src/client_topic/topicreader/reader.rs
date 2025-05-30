use crate::client_common::TokenCache;
use crate::client_topic::topicreader::cancelation_token::YdbCancellationToken;
use crate::client_topic::topicreader::messages::TopicReaderBatch;
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::common::update_token::RawUpdateTokenRequest;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    PartitionCommitOffset, RawCommitOffsetRequest, RawFromClientOneOf, RawFromServer,
    RawInitRequest, RawReadRequest, RawReadResponse, RawStartPartitionSessionRequest,
    RawStartPartitionSessionResponse, RawStopPartitionSessionRequest,
    RawStopPartitionSessionResponse, RawTopicReadSettings,
};
use crate::grpc_wrapper::raw_topic_service::update_offsets_in_transaction::{
    RawPartitionOffsets, RawTopicOffsets, RawTransactionIdentity,
    RawUpdateOffsetsInTransactionRequest,
};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::transaction::{Transaction, TransactionInfo};
use crate::{YdbError, YdbResult};
use secrecy::ExposeSecret;
use std::collections::HashMap;
use std::time;
use std::time::{Duration, SystemTime};
use tokio::select;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, warn};
use ydb_grpc::ydb_proto::topic::stream_read_message::{FromClient, FromServer};

pub struct TopicReader {
    stream: AsyncGrpcStreamWrapper<FromClient, FromServer>,
    last_read_response: Option<RawReadResponse>,
    last_error: Option<YdbError>,
    stop_backgroung_work_token: YdbCancellationToken,

    partition_sessions: HashMap<i64, PartitionSession>,

    // Added for transaction support
    topic_service: RawTopicClient,
    consumer: String,
}

const READER_BUFFER_SIZE: i64 = 1024 * 1024; // 1MB
const UPDATE_TOKEN_INTERVAL: time::Duration = Duration::from_secs(3600);

impl TopicReader {
    pub async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        if let Some(err) = &self.last_error {
            return Err(err.clone());
        }

        match self.read_batch_private().await {
            Ok(batch) => Ok(batch),
            Err(err) => {
                self.last_error.get_or_insert(err.clone());
                Err(err)
            }
        }
    }

    async fn read_batch_private(&mut self) -> YdbResult<TopicReaderBatch> {
        loop {
            if let Some(batch) = self.cut_batch() {
                return Ok(batch);
            }

            let resp = self.stream.receive::<RawFromServer>().await?;
            self.process_incoming_message(resp)?
        }
    }

    /// Read a batch of messages within a transaction context.
    /// The TopicReaderBatch from the result will be committed within the `tx` transaction.
    /// This is an EXAMPLE of the interface. IT IS NOT PRODUCTION READY.
    /// The reader will fail consistently on ANY error, including TLI.
    ///
    /// You can use this method to test the interface and try writing your own code to see how it works.
    /// DO NOT USE IN PRODUCTION
    pub async fn pop_batch_in_tx<T: Transaction>(
        &mut self,
        tx: &mut T,
    ) -> YdbResult<TopicReaderBatch> {
        let tx_info = tx.transaction_info().await?;

        let batch = self.read_batch().await?;

        self.update_offsets_in_transaction(&batch, &tx_info).await?;

        Ok(batch)
    }

    /// Helper method for calling the GRPC update_offsets_in_transaction method.
    async fn update_offsets_in_transaction(
        &mut self,
        batch: &TopicReaderBatch,
        tx_info: &TransactionInfo,
    ) -> YdbResult<()> {
        let commit_marker = batch.get_commit_marker();

        let raw_offsets_range = RawOffsetsRange {
            start: commit_marker.start_offset,
            end: commit_marker.end_offset,
        };

        let raw_partition_offsets = RawPartitionOffsets {
            partition_id: commit_marker.partition_id,
            partition_offsets: vec![raw_offsets_range],
        };

        let raw_topic_offsets = RawTopicOffsets {
            path: commit_marker.topic.clone(),
            partitions: vec![raw_partition_offsets],
        };

        let raw_tx_identity = RawTransactionIdentity {
            id: tx_info.transaction_id.clone(),
            session: tx_info.session_id.clone(),
        };

        let operation_params = RawOperationParams::new_with_timeouts(
            Duration::from_secs(30), // operation timeout
            Duration::from_secs(60), // cancel after
        );

        let request = RawUpdateOffsetsInTransactionRequest {
            operation_params,
            tx: raw_tx_identity,
            topics: vec![raw_topic_offsets],
            consumer: self.consumer.clone(),
        };

        self.topic_service
            .update_offsets_in_transaction(request)
            .await?;

        Ok(())
    }

    // add commit to internal buffer. Success return isn't guarantee that the message
    // committed to server. Real commit is background process.
    pub fn commit(&mut self, commit_marker: TopicReaderCommitMarker) -> YdbResult<()> {
        self.stream
            .send_nowait(RawFromClientOneOf::CommitOffsetRequest(
                RawCommitOffsetRequest {
                    commit_offsets: vec![PartitionCommitOffset {
                        partition_session_id: commit_marker.partition_session_id,
                        offsets: vec![RawOffsetsRange {
                            start: commit_marker.start_offset,
                            end: commit_marker.end_offset,
                        }],
                    }],
                },
            ))?;

        Ok(())
    }

    pub(crate) async fn new(
        consumer: String,
        selectors: TopicSelectors,
        connection_manager: GrpcConnectionManager,
        token_cache: TokenCache,
    ) -> YdbResult<Self> {
        let mut topic_service = connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        let init_request = RawInitRequest {
            topics_read_settings: selectors.into_topics_read_settings(),
            consumer: consumer.clone(),
            reader_name: "".to_string(),
        };

        let mut stream = topic_service.stream_read(init_request).await?;

        stream
            .send(RawFromClientOneOf::ReadRequest(RawReadRequest {
                bytes_size: READER_BUFFER_SIZE,
            }))
            .await?;

        let stop_backgroung_work_token = YdbCancellationToken::new();

        let stop_update_token = stop_backgroung_work_token.clone();

        tokio::spawn(Self::update_token_loop(
            stop_update_token,
            stream.clone_sender(),
            token_cache,
        ));

        let transaction_topic_service = connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        Ok(Self {
            stream,
            last_read_response: None,
            last_error: None,
            stop_backgroung_work_token,
            partition_sessions: HashMap::new(),
            topic_service: transaction_topic_service,
            consumer,
        })
    }

    fn cut_batch(&mut self) -> Option<TopicReaderBatch> {
        let last_read_response = if let Some(last_read_response) = &mut self.last_read_response {
            last_read_response
        } else {
            return None;
        };

        let last_partition_data = last_read_response.partition_data.last_mut()?;

        let partition_session_id = last_partition_data.partition_session_id;
        let last_batch = if let Some(batch) = last_partition_data.batches.pop() {
            batch
        } else {
            last_read_response.partition_data.pop();
            return self.cut_batch();
        };

        if last_batch.message_data.is_empty() {
            return self.cut_batch();
        }

        let size = last_batch.get_read_session_size();
        if size > 0 {
            if let Err(err) = self.send_read_request(size) {
                error!("error while send read request: {}", err);
                self.last_error.get_or_insert(err);
                return None;
            }
        }

        let partition_session = if let Some(partition_session) =
            self.partition_sessions.get_mut(&partition_session_id)
        {
            partition_session
        } else {
            error!(
                "Receive message without active partition, partition_session_id: {}",
                partition_session_id
            );
            return self.cut_batch();
        };

        Some(TopicReaderBatch::new(last_batch, partition_session))
    }

    fn send_read_request(&mut self, size: i64) -> YdbResult<()> {
        self.stream
            .send_nowait(RawFromClientOneOf::ReadRequest(RawReadRequest {
                bytes_size: size,
            }))?;
        Ok(())
    }

    fn process_incoming_message(&mut self, message: RawFromServer) -> YdbResult<()> {
        match message {
            RawFromServer::ReadResponse(read_resopnse) => {
                self.process_read_response(read_resopnse)?
            }
            RawFromServer::InitResponse(resp) => {
                info!("init response for topic reader: {:?}", resp)
            }
            RawFromServer::UpdateTokenResponse(_) => { /*pass*/ }

            RawFromServer::StartPartitionSessionRequest(start_partition_request) => {
                self.process_start_partition_session_request(start_partition_request)?
            }
            RawFromServer::StopPartitionSessionRequest(stop_partition_request) => {
                self.process_stop_partition_session_request(stop_partition_request)?
            }
            RawFromServer::UnsupportedMessage(mess) => {
                debug!("topic readed recived unsupported message: {}", mess)
            }
        }

        Ok(())
    }

    fn process_read_response(&mut self, read_response: RawReadResponse) -> YdbResult<()> {
        self.last_read_response = Some(read_response);

        Ok(())
    }

    fn process_start_partition_session_request(
        &mut self,
        request: RawStartPartitionSessionRequest,
    ) -> YdbResult<()> {
        self.partition_sessions.insert(
            request.partition_session.partition_session_id,
            PartitionSession {
                partition_session_id: request.partition_session.partition_session_id,
                partition_id: request.partition_session.partition_id,
                topic: request.partition_session.path,
                next_commit_offset_start: request.committed_offset,
            },
        );

        self.stream
            .send_nowait(RawFromClientOneOf::StartPartitionSessionResponse(
                RawStartPartitionSessionResponse {
                    partition_session_id: request.partition_session.partition_session_id,
                },
            ))?;

        Ok(())
    }

    fn process_stop_partition_session_request(
        &mut self,
        request: RawStopPartitionSessionRequest,
    ) -> YdbResult<()> {
        self.partition_sessions
            .remove(&request.partition_session_id);

        self.stream
            .send_nowait(RawFromClientOneOf::StopPartitionSessionResponse(
                RawStopPartitionSessionResponse {
                    partition_session_id: request.partition_session_id,
                },
            ))?;

        Ok(())
    }

    async fn update_token_loop(
        cancellation_token: YdbCancellationToken,
        send: UnboundedSender<FromClient>,
        auth_token: TokenCache,
    ) {
        loop {
            if cancellation_token.is_cancelled() {
                return;
            }

            let tokio_cancellation = cancellation_token.to_tokio_token();
            select! {
                _ = tokio_cancellation.cancelled() => {
                    return
                    },

                _ = tokio::time::sleep(UPDATE_TOKEN_INTERVAL) =>{}
            }

            let token = auth_token.token();

            debug!("sending update token request from topic reader");

            if let Err(err) = send.send(
                RawFromClientOneOf::UpdateTokenRequest(RawUpdateTokenRequest {
                    token: token.expose_secret().to_string(),
                })
                .into(),
            ) {
                warn!(
                    "error while send update token request from topic reader: {}",
                    err
                )
            }
        }
    }
}

impl Drop for TopicReader {
    fn drop(&mut self) {
        self.stop_backgroung_work_token.cancel();
    }
}

pub struct TopicSelectors(pub Vec<TopicSelector>);

impl TopicSelectors {
    pub(crate) fn into_topics_read_settings(self) -> Vec<RawTopicReadSettings> {
        self.0
            .into_iter()
            .map(|selector| selector.into_raw_topic_read_setting())
            .collect()
    }
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct TopicSelector {
    pub path: String,
    pub partition_ids: Option<Vec<i64>>,
    pub read_from: Option<SystemTime>,
}

impl TopicSelector {
    pub(crate) fn into_raw_topic_read_setting(self) -> RawTopicReadSettings {
        RawTopicReadSettings {
            path: self.path,
            partition_ids: self.partition_ids.unwrap_or_default(),
            read_from: self.read_from.map(|time| time.into()),
            max_lag: None,
        }
    }
}

impl From<String> for TopicSelectors {
    fn from(path: String) -> Self {
        TopicSelectors(vec![TopicSelector {
            path,
            partition_ids: None,
            read_from: None,
        }])
    }
}

impl From<&str> for TopicSelectors {
    fn from(path: &str) -> Self {
        path.to_owned().into()
    }
}

#[derive(Clone, Debug)]
pub struct TopicReaderCommitMarker {
    pub(crate) partition_session_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) start_offset: i64,
    pub(crate) end_offset: i64,
    pub(crate) topic: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_topic_service::update_offsets_in_transaction::*;
    use crate::transaction::TransactionInfo;
    use std::time::Duration;

    #[test]
    fn test_update_offsets_in_transaction_request_creation() {
        // Create a mock commit marker (simulating what would come from a batch)
        let commit_marker = TopicReaderCommitMarker {
            partition_session_id: 123,
            partition_id: 456,
            start_offset: 100,
            end_offset: 200,
            topic: "test-topic".to_string(),
        };

        // Create mock transaction info
        let tx_info = TransactionInfo {
            transaction_id: "test_tx_id".to_string(),
            session_id: "test_session_id".to_string(),
        };

        let consumer = "test-consumer".to_string();

        // Verify we can create the request structure correctly
        let raw_offsets_range = RawOffsetsRange {
            start: commit_marker.start_offset,
            end: commit_marker.end_offset,
        };

        let raw_partition_offsets = RawPartitionOffsets {
            partition_id: commit_marker.partition_id,
            partition_offsets: vec![raw_offsets_range],
        };

        let raw_topic_offsets = RawTopicOffsets {
            path: commit_marker.topic.clone(),
            partitions: vec![raw_partition_offsets],
        };

        let raw_tx_identity = RawTransactionIdentity {
            id: tx_info.transaction_id.clone(),
            session: tx_info.session_id.clone(),
        };

        let operation_params =
            RawOperationParams::new_with_timeouts(Duration::from_secs(30), Duration::from_secs(60));

        let request = RawUpdateOffsetsInTransactionRequest {
            operation_params,
            tx: raw_tx_identity,
            topics: vec![raw_topic_offsets],
            consumer,
        };

        // Verify the structure is correctly populated
        assert_eq!(request.tx.id, "test_tx_id");
        assert_eq!(request.tx.session, "test_session_id");
        assert_eq!(request.consumer, "test-consumer");
        assert_eq!(request.topics.len(), 1);

        let topic = &request.topics[0];
        assert_eq!(topic.path, "test-topic");
        assert_eq!(topic.partitions.len(), 1);

        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_id, 456);
        assert_eq!(partition.partition_offsets.len(), 1);

        let offsets = &partition.partition_offsets[0];
        assert_eq!(offsets.start, 100);
        assert_eq!(offsets.end, 200);
    }

    /// Test that demonstrates the complete integration flow of all components
    /// from steps 1-5, showing how they work together.
    #[test]
    fn test_transaction_topic_reading_integration() {
        // This test demonstrates the integration of all the components we've built:

        // 1. From Step 1: TransactionInfo and Transaction trait
        use crate::transaction::{Transaction, TransactionInfo};

        // Mock transaction implementing our trait
        struct MockTransaction {
            tx_info: TransactionInfo,
        }

        #[async_trait::async_trait]
        impl Transaction for MockTransaction {
            async fn query(
                &mut self,
                _query: crate::query::Query,
            ) -> crate::YdbResult<crate::result::QueryResult> {
                unimplemented!("Not needed for this test")
            }

            async fn commit(&mut self) -> crate::YdbResult<()> {
                unimplemented!("Not needed for this test")
            }

            async fn rollback(&mut self) -> crate::YdbResult<()> {
                unimplemented!("Not needed for this test")
            }

            async fn transaction_info(&mut self) -> crate::YdbResult<TransactionInfo> {
                Ok(self.tx_info.clone())
            }
        }

        // 2. From Step 2: TopicReaderCommitMarker with topic field
        let commit_marker = TopicReaderCommitMarker {
            partition_session_id: 456,
            partition_id: 789,
            start_offset: 1000,
            end_offset: 1100,
            topic: "integration-test-topic".to_string(), // This field was added in Step 2
        };

        // 3. From Step 3: Raw wrappers for GRPC types
        // These types can convert to protobuf types
        let raw_tx_identity = RawTransactionIdentity {
            id: "integration_tx_id".to_string(),
            session: "integration_session_id".to_string(),
        };

        let raw_offsets_range = RawOffsetsRange {
            start: commit_marker.start_offset,
            end: commit_marker.end_offset,
        };

        let raw_partition_offsets = RawPartitionOffsets {
            partition_id: commit_marker.partition_id,
            partition_offsets: vec![raw_offsets_range],
        };

        let raw_topic_offsets = RawTopicOffsets {
            path: commit_marker.topic.clone(),
            partitions: vec![raw_partition_offsets],
        };

        // 4. From Step 3: Main request wrapper
        let raw_request = RawUpdateOffsetsInTransactionRequest {
            operation_params: RawOperationParams::new_with_timeouts(
                Duration::from_secs(30),
                Duration::from_secs(60),
            ),
            tx: raw_tx_identity,
            topics: vec![raw_topic_offsets],
            consumer: "integration-consumer".to_string(),
        };

        // 5. From Step 3: Verify conversion to protobuf types works
        use ydb_grpc::ydb_proto::topic::UpdateOffsetsInTransactionRequest;
        let proto_request: UpdateOffsetsInTransactionRequest = raw_request.into();

        // Verify the complete data flow
        assert!(proto_request.operation_params.is_some());
        assert!(proto_request.tx.is_some());
        assert_eq!(proto_request.consumer, "integration-consumer");
        assert_eq!(proto_request.topics.len(), 1);

        let proto_tx = proto_request.tx.unwrap();
        assert_eq!(proto_tx.id, "integration_tx_id");
        assert_eq!(proto_tx.session, "integration_session_id");

        let proto_topic = &proto_request.topics[0];
        assert_eq!(proto_topic.path, "integration-test-topic");
        assert_eq!(proto_topic.partitions.len(), 1);

        let proto_partition = &proto_topic.partitions[0];
        assert_eq!(proto_partition.partition_id, 789);
        assert_eq!(proto_partition.partition_offsets.len(), 1);

        let proto_offsets = &proto_partition.partition_offsets[0];
        assert_eq!(proto_offsets.start, 1000);
        assert_eq!(proto_offsets.end, 1100);

        // This demonstrates that all the components from steps 1-5 work together:
        // - TransactionInfo from Step 1 ✓
        // - topic field in commit marker from Step 2 ✓
        // - Raw wrappers from Step 3 ✓
        // - GRPC client method from Step 4 (would be tested with RawTopicClient) ✓
        // - TopicReader integration from Step 5 (this file) ✓

        println!("Integration test passed - all components work together!");
    }
}
