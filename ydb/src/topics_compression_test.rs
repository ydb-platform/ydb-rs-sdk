use std::fmt;
use std::sync::Arc;
use std::thread;
use std::time;
use std::time::Duration;
use tokio::time::timeout;
use tracing_test::traced_test;

use std::sync::atomic::{AtomicI64, Ordering};

use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::test_integration_helper::create_client_with_executor;
use crate::ErrorHandlingStrategy;
use crate::RayonExecutor;
use crate::{
    client_topic::client::CreateTopicOptionsBuilder, Codec, CodecSelection, CompressionDecoder,
    CompressionEncoder, TopicReaderOptionsBuilder, TopicWriterMessageBuilder,
    TopicWriterOptionsBuilder, YdbError, YdbResult,
};
use tracing::trace;

impl Codec {
    pub const INC13: Codec = Codec { code: 10001 };
    pub const SLOW_INC13: Codec = Codec { code: 10002 };
    pub const FAILY: Codec = Codec { code: 10003 };
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

fn inc13_compress(data: &[u8]) -> YdbResult<Vec<u8>> {
    Ok(data.iter().map(|x| ((*x as i32) + 13) as u8).collect())
}

fn inc13_decompress(data: &[u8]) -> YdbResult<Vec<u8>> {
    Ok(data.iter().map(|x| ((*x as i32) - 13) as u8).collect())
}

fn slow_inc13_compress(data: &[u8]) -> YdbResult<Vec<u8>> {
    thread::sleep(time::Duration::from_secs(1));
    inc13_compress(data)
}

fn slow_inc13_decompress(data: &[u8]) -> YdbResult<Vec<u8>> {
    thread::sleep(time::Duration::from_secs(1));
    inc13_decompress(data)
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_fail_fast_write() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "codec_fail_fast_write".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    // Codec that fails first 5 iterations
    let counter = Arc::new(AtomicI64::new(0));
    let faily_inc13_compress = move |data: &[u8]| -> YdbResult<Vec<u8>> {
        if counter.fetch_add(1, Ordering::Relaxed) < 5 {
            return Err(YdbError::from_str("failing for compression testing"));
        }
        Ok(data.iter().map(|x| *x + 13).collect())
    };

    trace!("codec registry created");

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13])
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .add_encoder(encoder(Codec::INC13, faily_inc13_compress))
                .codec_selector(CodecSelection::Fixed(Codec::INC13))
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 20;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("some test message {i}").into_bytes();
        expected_messages.push(data.clone());
        writer
            .write(TopicWriterMessageBuilder::default().data(data).build()?)
            .await?;
    }
    trace!("messages written, waiting for sending");

    assert!(writer.stop().await.is_err());

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_fail_fast_read() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "codec_fail_fast_reader".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    let other_counter = Arc::new(std::sync::Mutex::new(0));
    let faily_inc13_decompress = move |data: &[u8]| -> YdbResult<Vec<u8>> {
        let mut cnt = other_counter.lock()?;
        *cnt += 1;
        if *cnt == 20 {
            return Err(YdbError::from_str("error for fail fast reader"));
        }
        Ok(data.iter().map(|x| *x - 13).collect())
    };

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13])
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec_selector(CodecSelection::Fixed(Codec::INC13))
                .add_encoder(encoder(Codec::INC13, inc13_compress))
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count: usize = 20;
    for i in 0..message_count {
        let data: Vec<u8> = format!("message {i}").into_bytes();
        writer
            .write(TopicWriterMessageBuilder::default().data(data).build()?)
            .await?;
    }
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(
            TopicReaderOptionsBuilder::default()
                .topic(topic_path.clone().into())
                .consumer(consumer_name)
                .add_decoder(decoder(Codec::INC13, faily_inc13_decompress))
                .build()?,
        )
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    let mut read_error = None;
    while received_messages.len() < message_count {
        let batch = timeout(Duration::from_secs(5), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))?;

        match batch {
            Ok(batch) => {
                for mut message in batch.messages {
                    if let Some(data) = message.read_and_take().await? {
                        trace!("got total {} messages", received_messages.len());
                        received_messages.push(data);
                    }
                }
            }
            Err(err) => {
                trace!("got error reading batch {}", err);
                read_error = Some(err);
                break;
            }
        }
    }

    let Some(err) = read_error else {
        return Err(YdbError::custom("expected fail fast reader error"));
    };
    trace!("got expected fail fast reader error: {err}");

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_skip_errors() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "codec_skip_errors".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    let counter = Arc::new(std::sync::Mutex::new(0));
    let faily_inc13_compress = move |data: &[u8]| -> YdbResult<Vec<u8>> {
        let mut cnt = counter.lock()?;
        *cnt += 1;
        if *cnt < 5 {
            return Err(YdbError::from_str("compression error"));
        }
        Ok(data.iter().map(|x| *x + 13).collect())
    };

    let other_counter = Arc::new(std::sync::Mutex::new(0));
    let faily_inc13_decompress = move |data: &[u8]| -> YdbResult<Vec<u8>> {
        let mut cnt = other_counter.lock()?;
        *cnt += 1;
        if *cnt > 14 {
            // do not count messages skipped in write, because they are not compressed
            return Err(YdbError::from_str("compression error"));
        }
        Ok(data.iter().map(|x| *x - 13).collect())
    };

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13])
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec_selector(CodecSelection::Fixed(Codec::INC13))
                .add_encoder(encoder(Codec::INC13, faily_inc13_compress))
                .compression_error_strategy(ErrorHandlingStrategy::Skip)
                .build()?,
        )
        .await?;
    trace!("writer created");

    // Writer Skip: first 4 compress calls fail (cnt < 5), so messages 0..=3 fall
    // back to RAW. Messages 4..=19 are sent compressed (INC13: data + 13).
    // Reader Skip: RAW messages bypass the codec; the 16 INC13 messages drive
    // the reader counter to 16, so 2 of them fail decompression and are marked
    // with `decompression_failed`. Which 2 fail is non-deterministic because
    // decompression runs in parallel — the assertion below tolerates that.
    // For decompression-failed messages, read_and_take() returns the original
    // (compressed) payload rather than None.
    let write_count: usize = 20;
    for i in 0..write_count {
        let data: Vec<u8> = vec![i as u8];
        writer
            .write(TopicWriterMessageBuilder::default().data(data).build()?)
            .await?;
    }
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(
            TopicReaderOptionsBuilder::default()
                .topic(topic_path.clone().into())
                .compression_error_strategy(ErrorHandlingStrategy::Skip)
                .consumer(consumer_name)
                .add_decoder(decoder(Codec::INC13, faily_inc13_decompress))
                .build()?,
        )
        .await?;

    let mut received: Vec<(Vec<u8>, bool)> = Vec::new();
    while received.len() < write_count {
        let batch = timeout(Duration::from_secs(2), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))??;
        for mut message in batch.messages {
            let data = message
                .read_and_take()
                .await?
                .expect("reader must return original payload");
            trace!(
                "got message with {:?}, decompression_failed = {}",
                data,
                message.decompression_failed
            );
            received.push((data, message.decompression_failed));
        }
    }

    assert_eq!(received.len(), write_count);
    let decompression_failed = received.iter().filter(|(_, f)| *f).count();
    assert_eq!(decompression_failed, 2);
    for (i, (data, failed)) in received.iter().enumerate() {
        let expected: Vec<u8> = if *failed {
            vec![i as u8 + 13]
        } else {
            vec![i as u8]
        };
        assert_eq!(data, &expected, "message at index {i} mismatch");
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_gzip_roundtrip() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "gzip_roundtrip".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec_selector(CodecSelection::Fixed(Codec::GZIP))
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 20;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("gzip-test-message-{i}").into_bytes();
        expected_messages.push(data.clone());
        writer
            .write(TopicWriterMessageBuilder::default().data(data).build()?)
            .await?;
    }
    writer.flush().await?;
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader(consumer_name, topic_path)
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    while received_messages.len() < message_count {
        let batch = timeout(Duration::from_secs(30), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))??;
        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
    }

    assert_eq!(received_messages.len(), message_count);
    assert_eq!(received_messages, expected_messages);

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_custom_roundtrip() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "codec_custom_roundtrip".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13])
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec_selector(CodecSelection::Fixed(Codec::INC13))
                .add_encoder(encoder(Codec::INC13, inc13_compress))
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 20;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("gzip-test-message-{i}").into_bytes();
        expected_messages.push(data.clone());
        writer
            .write(TopicWriterMessageBuilder::default().data(data).build()?)
            .await?;
    }
    writer.flush().await?;
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(
            TopicReaderOptionsBuilder::default()
                .topic(topic_path.clone().into())
                .consumer(consumer_name)
                .add_decoder(decoder(Codec::INC13, inc13_decompress))
                .build()?,
        )
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    while received_messages.len() < message_count {
        let batch = timeout(Duration::from_secs(30), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))??;
        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
    }

    assert_eq!(received_messages.len(), message_count);
    assert_eq!(received_messages, expected_messages);

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_parallelism() -> YdbResult<()> {
    let client = create_client_with_executor(Arc::new(RayonExecutor::new(5))).await?;
    let database_path = client.database();
    let topic_name = "codec_parallelism".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::SLOW_INC13])
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec_selector(CodecSelection::Fixed(Codec::SLOW_INC13))
                .add_encoder(encoder(Codec::SLOW_INC13, slow_inc13_compress))
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 10;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("gzip-test-message-{i}").into_bytes();
        expected_messages.push(data.clone());
        writer
            .write(TopicWriterMessageBuilder::default().data(data).build()?)
            .await?;
    }
    timeout(Duration::from_secs(5), writer.flush())
        .await
        .map_err(|err| YdbError::custom(format!("Error waiting for messages write {err}")))??;
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(
            TopicReaderOptionsBuilder::default()
                .topic(topic_path.clone().into())
                .consumer(consumer_name)
                .add_decoder(decoder(Codec::SLOW_INC13, slow_inc13_decompress))
                .build()?,
        )
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    while received_messages.len() < message_count {
        let batch = timeout(Duration::from_secs(5), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))??;
        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
    }

    assert_eq!(received_messages.len(), message_count);
    assert_eq!(received_messages, expected_messages);

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn codec_auto() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "codec_autoselection".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13, Codec::GZIP])
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.clone())
                    .build()?])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .add_encoder(encoder(Codec::INC13, inc13_compress))
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 500;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let mut data: Vec<u8> =
            "this text is boring this text is boring this text is boring this text is boring"
                .to_string()
                .into_bytes();
        if !(101..=300).contains(&i) {
            data.iter_mut().for_each(|x| *x = rand::random());
        }
        expected_messages.push(data.clone());
        writer
            .write(TopicWriterMessageBuilder::default().data(data).build()?)
            .await?;
    }
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(
            TopicReaderOptionsBuilder::default()
                .topic(topic_path.clone().into())
                .consumer(consumer_name)
                .add_decoder(decoder(Codec::INC13, inc13_decompress))
                .build()?,
        )
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    while received_messages.len() < message_count {
        let batch = timeout(Duration::from_secs(5), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))??;

        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
    }

    assert_eq!(received_messages.len(), message_count);
    assert_eq!(received_messages, expected_messages);

    Ok(())
}
