use std::sync::Arc;
use std::time::{Duration};
use tokio::time::timeout;
use tracing_test::traced_test;
use prost::bytes::Bytes;
use std::time;
use std::thread;

use std::sync::atomic::{AtomicI64, Ordering};

use crate::ErrorHandlingStrategy;
use crate::client_topic::list_types::ConsumerBuilder;
use crate::test_integration_helper::create_client;
use crate::{
    client_topic::client::{CreateTopicOptionsBuilder},
    Codec, TopicWriterMessageBuilder, TopicWriterOptionsBuilder, TopicReaderOptionsBuilder, YdbError,
    YdbResult, CodecRegistry, RayonExecutor
};
use tracing::trace;

impl Codec {
    pub const INC13: Codec = Codec { code: 10001 };
    pub const SLOW_INC13: Codec = Codec { code: 10002 };
    pub const FAILY: Codec = Codec { code: 10003 };
}

fn inc13_compress(data: &Bytes) -> YdbResult<Bytes> {
    Ok(data.iter().map(|x| ((*x as i32) + 13) as u8).collect())
}

fn inc13_decompress(data: &Bytes) -> YdbResult<Bytes> {
    Ok(data.iter().map(|x| ((*x as i32) - 13) as u8).collect())
}

fn slow_inc13_compress(data: &Bytes) -> YdbResult<Bytes> {
    thread::sleep(time::Duration::from_secs(1));
    inc13_compress(data)
}

fn slow_inc13_decompress(data: &Bytes) -> YdbResult<Bytes> {
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
    let faily_inc13_compress = move |data: &Bytes| -> YdbResult<Bytes> {
        if counter.fetch_add(1, Ordering::Relaxed) < 5 {
            return Err(YdbError::from_str("failing for compression testing"));
        }
        Ok(data.iter().map(|x| *x + 13).collect())
    };

    let mut registry = CodecRegistry::default();
    registry.register_codec(Codec::INC13, Arc::new(faily_inc13_compress), Arc::new(inc13_decompress))?;
    let registry = Arc::new(registry);
    trace!("codec registry created");

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13])
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec_registry(registry.clone())
                .codec(Codec::INC13)
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 20;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("some test message {i}").into_bytes();
        expected_messages.push(data.clone());
        writer.write(TopicWriterMessageBuilder::default().data(data).build()?).await?;
    }
    trace!("messages written, waiting for sending");

    writer.stop().await?;
    trace!("writer stopped, all messages sent");

    let mut reader = topic_client
        .create_reader_with_params(TopicReaderOptionsBuilder::default()
            .topic(topic_path.clone().into())
            .consumer(consumer_name)
            .codec_registry(registry.clone())
            .build()?)
        .await?;
    trace!("reader created");

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    while received_messages.len() < message_count {
        let mut batch = timeout(Duration::from_secs(2), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))??;
        for mut message in std::mem::take(&mut batch.messages) {
            if let Some(data) = message.read_and_take().await? {
                received_messages.push(data);
            }
        }
        reader.commit(batch.get_commit_marker())?
    }

    assert_eq!(received_messages.len(), message_count);
    assert_eq!(received_messages, expected_messages);

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

    let counter= Arc::new(std::sync::Mutex::new(0));
    let faily_inc13_compress = move |data: &Bytes| -> YdbResult<Bytes> {
        let mut cnt = counter.lock()?;
        *cnt += 1;
        if *cnt < 5 {
            return Err(YdbError::from_str("compression error"));
        }
        Ok(data.iter().map(|x| *x + 13).collect())
    };

    let other_counter = Arc::new(std::sync::Mutex::new(0));
    let faily_inc13_decompress = move |data: &Bytes| -> YdbResult<Bytes> {
        let mut cnt = other_counter.lock()?;
        *cnt += 1;
        if *cnt > 14 { // do not count messages skipped in write, because they are not compressed
            return Err(YdbError::from_str("compression error"));
        }
        Ok(data.iter().map(|x| *x - 13).collect())
    };

    // single-threaded to correctly track skipped messages
    let executor = Arc::new(RayonExecutor::new(1));

    let mut registry = CodecRegistry::default();
    registry.register_codec(Codec::INC13, Arc::new(faily_inc13_compress), Arc::new(faily_inc13_decompress))?;
    let registry = Arc::new(registry);

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13])
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec(Codec::INC13)
                .codec_registry(registry.clone())
                .compression_error_strategy(ErrorHandlingStrategy::Skip)
                .build()?,
        )
        .await?;
    trace!("writer created");

    let mut message_count: usize = 20;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = vec![i as u8];
        if i < 18 {
            expected_messages.push(data.clone());
        }
        writer.write(TopicWriterMessageBuilder::default().data(data).build()?).await?;
    }
    message_count -= 2;
    expected_messages = expected_messages[0..message_count].to_vec();
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(TopicReaderOptionsBuilder::default()
            .topic(topic_path.clone().into())
            .compression_error_strategy(ErrorHandlingStrategy::Skip)
            .consumer(consumer_name)
            .codec_registry(registry.clone())
            .compression_executor(executor)
            .build()?)
        .await?;

    let mut received_messages: Vec<Vec<u8>> = Vec::new();
    while received_messages.len() < message_count {
        let batch = timeout(Duration::from_secs(2), reader.read_batch())
            .await
            .map_err(|err| YdbError::custom(format!("timeout waiting reader batch: {err}")))??;
        for mut message in batch.messages {
            if let Some(data) = message.read_and_take().await? {
                trace!("got message with {:?}", data);
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
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec(Codec::GZIP)
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 20;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("gzip-test-message-{i}").into_bytes();
        expected_messages.push(data.clone());
        writer.write(TopicWriterMessageBuilder::default().data(data).build()?).await?;
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
                .supported_codecs(vec![Codec::INC13])
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let mut registry = CodecRegistry::default();
    registry.register_codec(Codec::INC13, Arc::new(inc13_compress), Arc::new(inc13_decompress))?;
    let registry = Arc::new(registry);

    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec(Codec::INC13)
                .codec_registry(registry.clone())
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 20;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("gzip-test-message-{i}").into_bytes();
        expected_messages.push(data.clone());
        writer.write(TopicWriterMessageBuilder::default().data(data).build()?).await?;
    }
    writer.flush().await?;
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(TopicReaderOptionsBuilder::default()
            .topic(topic_path.clone().into())
            .consumer(consumer_name)
            .codec_registry(registry.clone())
            .build()?)
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
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "codec_parallelism".to_string();
    let topic_path = format!("{database_path}/{topic_name}");
    let consumer_name = format!("test-consumer-{topic_name}");

    let mut topic_client = client.topic_client();
    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error
    trace!("previous topic removed");

    let mut registry = CodecRegistry::default();
    registry.register_codec(Codec::SLOW_INC13, Arc::new(slow_inc13_compress), Arc::new(slow_inc13_decompress))?;
    let registry = Arc::new(registry);

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::SLOW_INC13])
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;
    trace!("topic created");


    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec(Codec::SLOW_INC13)
                .codec_registry(registry.clone())
                .compression_executor(Arc::new(RayonExecutor::new(8)))
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 10;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let data: Vec<u8> = format!("gzip-test-message-{i}").into_bytes();
        expected_messages.push(data.clone());
        writer.write(TopicWriterMessageBuilder::default().data(data).build()?).await?;
    }
    timeout(Duration::from_secs(5), writer.flush()).await.map_err(
        |err| YdbError::custom(format!("Error waiting for messages write {err}")
    ))??;
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(TopicReaderOptionsBuilder::default()
            .topic(topic_path.clone().into())
            .consumer(consumer_name)
            .codec_registry(registry.clone())
            .build()?)
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

    let mut registry = CodecRegistry::default();
    registry.register_codec(Codec::INC13, Arc::new(inc13_compress), Arc::new(inc13_decompress))?;
    let registry = Arc::new(registry);

    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .supported_codecs(vec![Codec::RAW, Codec::INC13, Codec::GZIP])
                .consumers(vec![
                    ConsumerBuilder::default()
                        .name(consumer_name.clone())
                        .build()?,
                ])
                .build()?,
        )
        .await?;
    trace!("topic created");

    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .topic_path(topic_path.clone())
                .codec_registry(registry.clone())
                .build()?,
        )
        .await?;
    trace!("writer created");

    let message_count = 1000;
    let mut expected_messages: Vec<Vec<u8>> = Vec::new();
    for i in 0..message_count {
        let mut data: Vec<u8> = format!("this text is boring this text is boring this text is boring this text is boring").into_bytes();
        if i > 500 {
            data.iter_mut().for_each(|x| *x = rand::random());
        }
        expected_messages.push(data.clone());
        writer.write(TopicWriterMessageBuilder::default().data(data).build()?).await?;
    }
    writer.stop().await?;

    let mut reader = topic_client
        .create_reader_with_params(TopicReaderOptionsBuilder::default()
            .topic(topic_path.clone().into())
            .consumer(consumer_name)
            .codec_registry(registry.clone())
            .build()?)
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