use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tracing_test::traced_test;

use crate::client_topic::client::CreateTopicOptionsBuilder;
use crate::client_topic::compression::RayonExecutor;
use crate::client_topic::compression::{CodecSelection, CompressionDecoder, CompressionEncoder};
use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::test_integration_helper::create_client_with_executor;
use crate::Executor;
use crate::TopicWriter;
use crate::{
    Client, Codec, TopicClient, TopicReaderOptions, TopicWriterMessage, TopicWriterOptions,
    YdbError, YdbResult,
};
use tracing::trace;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

impl Codec {
    pub const INV: Codec = Codec { code: 10_000 };
    pub const PAR: Codec = Codec { code: 10_001 };
}

struct TestEncoder<F> {
    codec: Codec,
    encode: F,
}

impl<F> fmt::Debug for TestEncoder<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestEncoder")
            .field("codec", &self.codec)
            .finish_non_exhaustive()
    }
}

impl<F> CompressionEncoder for TestEncoder<F>
where
    F: Fn(&[u8]) -> YdbResult<Vec<u8>> + Send + Sync,
{
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
        (self.encode)(data).map_err(Into::into)
    }

    fn codec(&self) -> Codec {
        self.codec
    }
}

struct TestDecoder<F> {
    codec: Codec,
    decode: F,
}

impl<F> fmt::Debug for TestDecoder<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestDecoder")
            .field("codec", &self.codec)
            .finish_non_exhaustive()
    }
}

impl<F> CompressionDecoder for TestDecoder<F>
where
    F: Fn(&[u8]) -> YdbResult<Vec<u8>> + Send + Sync,
{
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
        (self.decode)(data).map_err(Into::into)
    }

    fn codec(&self) -> Codec {
        self.codec
    }
}

fn encoder<F>(codec: Codec, encode: F) -> TestEncoder<F>
where
    F: Fn(&[u8]) -> YdbResult<Vec<u8>> + Send + Sync + 'static,
{
    TestEncoder { codec, encode }
}

fn decoder<F>(codec: Codec, decode: F) -> TestDecoder<F>
where
    F: Fn(&[u8]) -> YdbResult<Vec<u8>> + Send + Sync + 'static,
{
    TestDecoder { codec, decode }
}

fn invert(data: &[u8]) -> YdbResult<Vec<u8>> {
    Ok(data.iter().map(|x| !x).collect())
}

fn message(data: impl Into<Vec<u8>>) -> TopicWriterMessage {
    TopicWriterMessage::from_data(data)
}

async fn timeout<F, T>(future: F) -> YdbResult<T>
where
    F: Future<Output = YdbResult<T>>,
{
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .map_err(|err| YdbError::custom(format!("timeout: {err}")))?
}

async fn stop_writer(writer: TopicWriter) -> YdbResult<()> {
    timeout(async { writer.stop().await }).await
}

async fn wait_flush_error(writer: &TopicWriter) -> YdbResult<()> {
    match tokio::time::timeout(TEST_TIMEOUT, writer.flush()).await {
        Ok(Ok(())) => Err(YdbError::custom(
            "flush succeeded after encoder failure was triggered",
        )),
        Ok(Err(_)) => Ok(()),
        Err(err) => Err(YdbError::custom(format!(
            "timeout waiting for flush error: {err}"
        ))),
    }
}

async fn roundtrip<W, R>(
    test_name: &str,
    supported_codecs: &[Codec],
    messages: Vec<Vec<u8>>,
    configure_writer: W,
    make_reader_options: R,
) -> YdbResult<()>
where
    W: FnOnce(String) -> TopicWriterOptions,
    R: FnOnce(String, String) -> TopicReaderOptions,
{
    let (mut topic_client, topic_path, consumer_name) =
        topic_setup(test_name, supported_codecs).await?;

    let writer = topic_client
        .create_writer_with_params(configure_writer(topic_path.clone()))
        .await?;
    trace!("writer created");

    for data in &messages {
        writer.write(message(data.clone())).await?;
    }
    timeout(writer.flush()).await?;
    stop_writer(writer).await?;

    let mut reader = topic_client
        .create_reader_with_params(make_reader_options(topic_path, consumer_name))
        .await?;

    let expected_count = messages.len();
    let mut received_messages: Vec<Vec<u8>> = Vec::with_capacity(expected_count);
    while received_messages.len() < expected_count {
        let batch = timeout(reader.read_batch()).await?;
        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
    }

    assert_eq!(received_messages, messages);

    Ok(())
}

async fn topic_setup(name: &str, codecs: &[Codec]) -> YdbResult<(TopicClient, String, String)> {
    let client = create_client().await?;
    setup_topic(name, codecs, client).await
}

async fn topic_setup_with_executor(
    name: &str,
    codecs: &[Codec],
    executor: Arc<dyn Executor>,
) -> YdbResult<(TopicClient, String, String)> {
    let client = create_client_with_executor(executor).await?;
    setup_topic(name, codecs, client).await
}

async fn setup_topic(
    name: &str,
    codecs: &[Codec],
    client: Arc<Client>,
) -> YdbResult<(TopicClient, String, String)> {
    let topic_path = format!("{}/{name}", client.database());
    let consumer_name = format!("test-consumer-{name}");

    let mut topic_client = client.topic_client();

    let _ = topic_client.drop_topic(topic_path.clone()).await;
    trace!("previous topic removed");

    let mut builder = CreateTopicOptionsBuilder::default();
    if !codecs.is_empty() {
        builder.supported_codecs(codecs.to_vec());
    }

    builder.consumers(vec![ConsumerBuilder::default()
        .name(consumer_name.clone())
        .build()?]);

    topic_client
        .create_topic(topic_path.clone(), builder.build()?)
        .await?;

    trace!("topic created");

    Ok((topic_client, topic_path, consumer_name))
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_fail_fast() -> YdbResult<()> {
    let (mut topic_client, topic_path, consumer_name) =
        topic_setup("codec_fail_fast_write", &[Codec::RAW, Codec::INV]).await?;

    let success_count = 10;
    let encode_calls = Arc::new(AtomicUsize::new(0));

    let failing_encoder = encoder(Codec::INV, {
        let encode_calls = encode_calls.clone();

        move |data: &[u8]| -> YdbResult<Vec<u8>> {
            let current = encode_calls.fetch_add(1, Ordering::Relaxed) + 1;
            if current > success_count {
                return Err(YdbError::custom("failing"));
            }
            invert(data)
        }
    });

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .topic_path(topic_path.clone())
                .codec_selector(CodecSelection::Fixed(Codec::INV))
                .add_encoder(failing_encoder)
                .build(),
        )
        .await?;
    trace!("writer created");

    let mut expected_messages: Vec<Vec<u8>> = Vec::with_capacity(success_count);
    for i in 0..success_count {
        let data = format!("{i}").into_bytes();
        expected_messages.push(data.clone());
        assert!(writer.write(message(data)).await.is_ok());
    }

    timeout(writer.flush()).await?;
    assert_eq!(encode_calls.load(Ordering::Relaxed), success_count);

    let _ = writer.write(message("trigger fail")).await;
    wait_flush_error(&writer).await?;

    let calls_at_fail = encode_calls.load(Ordering::Relaxed);
    assert_eq!(calls_at_fail, success_count + 1);

    assert!(writer.write(message("post-1")).await.is_err());
    assert!(writer.write(message("post-2")).await.is_err());
    assert_eq!(encode_calls.load(Ordering::Relaxed), calls_at_fail);

    let _ = stop_writer(writer).await;

    let fail_decoder = Arc::new(AtomicBool::new(false));
    let mut reader = topic_client
        .create_reader_with_params(
            TopicReaderOptions::builder()
                .topic(topic_path.clone())
                .consumer(consumer_name)
                .add_decoder(decoder(Codec::INV, {
                    let fail_decoder = fail_decoder.clone();

                    move |data: &[u8]| -> YdbResult<Vec<u8>> {
                        if fail_decoder.load(Ordering::Relaxed) {
                            return Err(YdbError::custom("failing decoder"));
                        }

                        invert(data)
                    }
                }))
                .build(),
        )
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::with_capacity(success_count);
    while received_messages.len() < success_count {
        let batch = timeout(reader.read_batch()).await?;

        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
    }

    assert_eq!(received_messages, expected_messages);

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .topic_path(topic_path)
                .codec_selector(CodecSelection::Fixed(Codec::INV))
                .add_encoder(encoder(Codec::INV, invert))
                .build(),
        )
        .await?;

    fail_decoder.store(true, Ordering::Relaxed);
    writer.write(message("triggers decoder error")).await?;
    timeout(writer.flush()).await?;
    assert!(timeout(reader.read_batch()).await.is_err());

    fail_decoder.store(false, Ordering::Relaxed);
    writer
        .write(message("would decode fine, but reader is dead"))
        .await?;
    timeout(writer.flush()).await?;
    stop_writer(writer).await?;

    assert!(timeout(reader.read_batch()).await.is_err());

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_gzip_fixed() -> YdbResult<()> {
    let messages = (0..20)
        .map(|i| format!("test-message-{i}").into_bytes())
        .collect();
    roundtrip(
        "codec_gzip_fixed",
        &[],
        messages,
        |topic_path| {
            TopicWriterOptions::builder()
                .topic_path(topic_path)
                .codec_selector(CodecSelection::Fixed(Codec::GZIP))
                .build()
        },
        |topic, consumer| {
            TopicReaderOptions::builder()
                .topic(topic)
                .consumer(consumer)
                .build()
        },
    )
    .await
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_parallelism() -> YdbResult<()> {
    let (mut topic_client, topic_path, consumer_name) = topic_setup_with_executor(
        "codec_parallelism",
        &[Codec::RAW, Codec::PAR],
        Arc::new(RayonExecutor::new(2)?),
    )
    .await?;

    let encoder_barrier = Arc::new(Barrier::new(2));
    let decoder_barrier = Arc::new(Barrier::new(2));

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .topic_path(topic_path.clone())
                .codec_selector(CodecSelection::Fixed(Codec::PAR))
                .add_encoder(encoder(Codec::PAR, {
                    let barrier = encoder_barrier.clone();

                    move |data: &[u8]| -> YdbResult<Vec<u8>> {
                        barrier.wait();
                        Ok(data.into())
                    }
                }))
                .build(),
        )
        .await?;
    trace!("writer created");

    let message_count = 10;
    let mut expected_messages = Vec::with_capacity(message_count);

    for i in 0..message_count {
        let data: Vec<u8> = format!("test-message-{i}").into_bytes();
        expected_messages.push(data.clone());
        writer.write(TopicWriterMessage::from_data(data)).await?;
    }

    timeout(writer.flush()).await?;
    stop_writer(writer).await?;

    let mut reader = topic_client
        .create_reader_with_params(
            TopicReaderOptions::builder()
                .topic(topic_path.clone())
                .consumer(consumer_name)
                .add_decoder(decoder(Codec::PAR, {
                    let barrier = decoder_barrier.clone();

                    move |data: &[u8]| -> YdbResult<Vec<u8>> {
                        barrier.wait();
                        Ok(data.into())
                    }
                }))
                .build(),
        )
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    while received_messages.len() < message_count {
        let batch = timeout(reader.read_batch()).await?;

        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
    }

    assert_eq!(received_messages, expected_messages);

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_auto() -> YdbResult<()> {
    let messages: Vec<Vec<u8>> = (0..500)
        .map(|_| (0..80).map(|_| rand::random()).collect())
        .collect();
    roundtrip(
        "codec_auto",
        &[Codec::RAW, Codec::INV, Codec::GZIP],
        messages,
        |topic_path| {
            TopicWriterOptions::builder()
                .topic_path(topic_path)
                .codec_selector(CodecSelection::Auto)
                .add_encoder(encoder(Codec::INV, invert))
                .build()
        },
        |topic, consumer| {
            TopicReaderOptions::builder()
                .topic(topic)
                .consumer(consumer)
                .add_decoder(decoder(Codec::INV, invert))
                .build()
        },
    )
    .await
}
