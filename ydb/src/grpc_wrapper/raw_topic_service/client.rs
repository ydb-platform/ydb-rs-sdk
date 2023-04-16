use std::time::UNIX_EPOCH;
use ydb_grpc::ydb_proto::topic::v1::topic_service_client::TopicServiceClient;
use tracing::trace;
use ydb_grpc::ydb_proto::topic::stream_write_message::{FromClient, InitRequest, WriteRequest};
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::{message_data, MessageData};

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_topic_service::create_topic::{RawCreateTopicRequest};
use crate::grpc_wrapper::raw_topic_service::delete_topic::RawDropTopicRequest;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::{TopicWriterMessage, TopicWriterOptions};
use futures_util::stream::iter;
use ydb_grpc::ydb_proto::topic::stream_write_message::init_request::Partitioning;


pub(crate) struct RawTopicClient {
    service: TopicServiceClient<InterceptedChannel>,
}

impl RawTopicClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: TopicServiceClient::new(service),
        }
    }

    pub async fn create_topic(&mut self, req: RawCreateTopicRequest) -> RawResult<()> {
        request_without_result!(
            self.service.create_topic,
            req => ydb_grpc::ydb_proto::topic::CreateTopicRequest
        );
    }

    pub async fn delete_topic(&mut self, req: RawDropTopicRequest) -> RawResult<()> {
        request_without_result!(
            self.service.drop_topic,
            req => ydb_grpc::ydb_proto::topic::DropTopicRequest
        );
    }

    pub async fn do_write_handshake(&mut self, topic_path: &String, writer_options: &TopicWriterOptions) -> RawResult<()> {
        let req = InitRequest {
            path: topic_path.to_string(),
            producer_id: writer_options.producer_id.clone().unwrap_or_default(),
            write_session_meta: writer_options.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: false,
            partitioning: Some(Partitioning::MessageGroupId(writer_options.producer_id.clone().unwrap_or_default())),
        };
        let mut result = self.service.stream_write(iter(vec![
            FromClient
            {
                client_message: Some(ClientMessage::InitRequest(req))
            }])).await?;


        let msg = result.get_mut().message().await?.unwrap();

        Ok(())
    }

    pub async fn do_single_write_request(&mut self, msg: TopicWriterMessage, topic_path: &String, writer_options: &TopicWriterOptions) -> RawResult<()> {
        let init_req = InitRequest {
            path: topic_path.to_string(),
            producer_id: writer_options.producer_id.clone().unwrap_or_default(),
            write_session_meta: writer_options.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: false,
            partitioning: Some(Partitioning::MessageGroupId(writer_options.producer_id.clone().unwrap_or_default())),
        };

        let vec_len = msg.data.len();
        let req = WriteRequest {
            messages: vec![MessageData {
                seq_no: 3,
                created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                    seconds: msg.created_at.duration_since(UNIX_EPOCH)?.as_secs() as i64,
                    nanos: msg.created_at.duration_since(UNIX_EPOCH)?.as_nanos() as i32,
                }),
                data: msg.data,
                uncompressed_size: vec_len as i64,
                partitioning: Some(message_data::Partitioning::MessageGroupId("some_id".to_string())),
            }],
            codec: 1,
        };

        let mut result = self.service.stream_write(iter(vec![
            FromClient
            {
                client_message: Some(ClientMessage::InitRequest(init_req))
            },
            FromClient
            {
                client_message:
                Some(ClientMessage::WriteRequest(req))
            }])).await?;

        let _ignored = result.get_mut().message().await?;

        let msg = result.get_mut().message().await?;
        let issue = msg.unwrap().issues[0].clone();
        println!("{}", issue.message);

        Ok(())
    }
}

impl GrpcServiceForDiscovery for RawTopicClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Topic
    }
}
