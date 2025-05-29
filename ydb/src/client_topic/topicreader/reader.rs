use crate::client_topic::topicreader::messages::TopicReaderBatch;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    RawFromClientOneOf, RawFromServer, RawInitRequest, RawReadRequest, RawReadResponse,
    RawStartPartitionSessionRequest, RawStartPartitionSessionResponse,
    RawStopPartitionSessionRequest, RawStopPartitionSessionResponse, RawTopicReadSettings,
};
use crate::{YdbError, YdbResult};
use std::time::SystemTime;
use tracing::{debug, error};
use ydb_grpc::ydb_proto::topic::stream_read_message::{FromClient, FromServer};

const READER_BUFFER_SIZE: i64 = 1024 * 1024; // 1MB

pub struct TopicReader {
    stream: AsyncGrpcStreamWrapper<FromClient, FromServer>,
    last_read_response: Option<RawReadResponse>,
    last_error: Option<YdbError>,
}

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

    fn commit(&mut self, commit_marker: TopicReaderCommitMarker) {
        unimplemented!()
    }

    pub(crate) async fn new(
        consumer: String,
        selectors: TopicSelectors,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        let mut topic_service = connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        let init_request = RawInitRequest {
            topics_read_settings: selectors.into_topics_read_settings(),
            consumer,
            reader_name: "".to_string(),
        };

        let mut stream = topic_service.stream_read(init_request).await?;

        stream
            .send(RawFromClientOneOf::ReadRequest(RawReadRequest {
                bytes_size: READER_BUFFER_SIZE,
            }))
            .await?;

        // TODO: update token

        Ok(Self {
            stream,
            last_read_response: None,
            last_error: None,
        })
    }

    fn cut_batch(&mut self) -> Option<TopicReaderBatch> {
        let last_read_response = if let Some(last_read_response) = &mut self.last_read_response {
            last_read_response
        } else {
            return None;
        };

        let last_partition_data = last_read_response.partition_data.last_mut()?;

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

        Some(TopicReaderBatch::new(last_batch))
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
            RawFromServer::InitResponse(_) => {
                error!("second init response for topic reader")
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
        self.stream
            .send_nowait(RawFromClientOneOf::StopPartitionSessionResponse(
                RawStopPartitionSessionResponse {
                    partition_session_id: request.partition_session_id,
                },
            ))?;
        Ok(())
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
    partition_session_id: i64,
    start_offset: i64,
    end_offset: i64,
}
