use super::handler::{TopicIncoming, TopicRx};
use super::sender::{ReadStreamCommand, ReadStreamSender, WriteStreamCommand, WriteStreamSender};
use super::TopicReply;
use crate::mock_server::handler::{FromServiceToServerTx, Incoming};
use futures_util::{stream, Stream, StreamExt};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_stream::wrappers::UnboundedReceiverStream;
use ydb_grpc::ydb_proto::topic;
use ydb_grpc::ydb_proto::topic::v1::topic_service_server::TopicService;
use ydb_grpc::ydb_proto::topic::{stream_read_message, stream_write_message};

type ReadStream = Pin<
    Box<dyn Stream<Item = Result<stream_read_message::FromServer, tonic::Status>> + Send + 'static>,
>;

type WriteStream = Pin<
    Box<
        dyn Stream<Item = Result<stream_write_message::FromServer, tonic::Status>> + Send + 'static,
    >,
>;

pub struct MockTopicService {
    to_server: FromServiceToServerTx,
    topic_sender: ReadStreamSender,
    pub(crate) write_sender: WriteStreamSender,
    next_stream_id: AtomicU64,
}

impl MockTopicService {
    pub fn new(to_server: FromServiceToServerTx, rx: TopicRx) -> Self {
        let topic_sender = ReadStreamSender::new();
        let write_sender = WriteStreamSender::new();
        tokio::spawn(Self::handle_messages(
            topic_sender.clone(),
            write_sender.clone(),
            rx,
        ));

        Self {
            to_server,
            next_stream_id: AtomicU64::new(0),
            topic_sender,
            write_sender,
        }
    }

    async fn handle_messages(
        topic_sender: ReadStreamSender,
        write_sender: WriteStreamSender,
        mut rx: TopicRx,
    ) {
        while let Some(msg) = rx.recv().await {
            match msg {
                TopicReply::StreamRead { stream_id, msg } => {
                    topic_sender
                        .send_to(stream_id, msg)
                        .expect("mock topic read stream failed to send reply");
                }
                TopicReply::StreamWrite { stream_id, msg } => {
                    write_sender
                        .send_to(stream_id, msg)
                        .expect("mock topic write stream failed to send reply");
                }
                _ => {
                    unimplemented!()
                }
            }
        }
    }
}

#[tonic::async_trait]
impl TopicService for MockTopicService {
    type StreamWriteStream = WriteStream;
    type StreamReadStream = ReadStream;

    async fn stream_write(
        &self,
        request: tonic::Request<tonic::Streaming<stream_write_message::FromClient>>,
    ) -> Result<tonic::Response<Self::StreamWriteStream>, tonic::Status> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let rx = self.write_sender.register_stream(stream_id);
        let write_sender = self.write_sender.clone();
        let to_server = self.to_server.clone();

        let mut client_stream = request.into_inner();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = client_stream.message().await {
                let Some(msg) = msg.client_message else {
                    continue;
                };

                let _ = to_server.send(Incoming::Topic(TopicIncoming::StreamWrite {
                    stream_id,
                    msg,
                }));
            }

            let _ = write_sender.close(stream_id);
            write_sender.unregister_stream(stream_id);
        });

        let responses = UnboundedReceiverStream::new(rx);
        let responses = stream::unfold(responses, |mut responses| async move {
            match responses.next().await {
                Some(WriteStreamCommand::Reply(payload)) => Some((Ok(payload), responses)),
                Some(WriteStreamCommand::Fail(status)) => Some((Err(status), responses)),
                Some(WriteStreamCommand::Close) | None => None,
            }
        });

        Ok(tonic::Response::new(Box::pin(responses)))
    }

    async fn stream_read(
        &self,
        request: tonic::Request<tonic::Streaming<stream_read_message::FromClient>>,
    ) -> Result<tonic::Response<Self::StreamReadStream>, tonic::Status> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let rx = self.topic_sender.register_stream(stream_id);
        let sender = self.topic_sender.clone();
        let to_server = self.to_server.clone();

        let mut client_stream = request.into_inner();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = client_stream.message().await {
                let Some(msg) = msg.client_message else {
                    continue;
                };

                let _ = to_server.send(Incoming::Topic(TopicIncoming::StreamRead {
                    stream_id,
                    msg,
                }));
            }

            let _ = sender.close(stream_id);
            sender.unregister_stream(stream_id);
        });

        let responses = UnboundedReceiverStream::new(rx);
        let responses = stream::unfold(responses, |mut responses| async move {
            match responses.next().await {
                Some(ReadStreamCommand::Reply(payload)) => Some((Ok(payload), responses)),
                Some(ReadStreamCommand::Fail(status)) => Some((Err(status), responses)),
                Some(ReadStreamCommand::Close) | None => None,
            }
        });

        Ok(tonic::Response::new(Box::pin(responses)))
    }

    async fn commit_offset(
        &self,
        _request: tonic::Request<topic::CommitOffsetRequest>,
    ) -> Result<tonic::Response<topic::CommitOffsetResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn update_offsets_in_transaction(
        &self,
        _request: tonic::Request<topic::UpdateOffsetsInTransactionRequest>,
    ) -> Result<tonic::Response<topic::UpdateOffsetsInTransactionResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn create_topic(
        &self,
        _request: tonic::Request<topic::CreateTopicRequest>,
    ) -> Result<tonic::Response<topic::CreateTopicResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn describe_topic(
        &self,
        _request: tonic::Request<topic::DescribeTopicRequest>,
    ) -> Result<tonic::Response<topic::DescribeTopicResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn describe_consumer(
        &self,
        _request: tonic::Request<topic::DescribeConsumerRequest>,
    ) -> Result<tonic::Response<topic::DescribeConsumerResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn alter_topic(
        &self,
        _request: tonic::Request<topic::AlterTopicRequest>,
    ) -> Result<tonic::Response<topic::AlterTopicResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn drop_topic(
        &self,
        _request: tonic::Request<topic::DropTopicRequest>,
    ) -> Result<tonic::Response<topic::DropTopicResponse>, tonic::Status> {
        unimplemented!()
    }
}
