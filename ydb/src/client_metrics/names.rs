use std::sync::{Arc, OnceLock};

use metrics::{Counter, Histogram, Unit, counter, histogram};

const DEFAULT_DRIVER_NAME: &str = "main";

#[derive(Clone, Debug)]
pub(crate) struct TopicWriterMetrics {
    pub(crate) sending_messages: Counter,
    pub(crate) sending_bytes: Counter,
}

#[derive(Clone, Debug)]
pub(crate) struct TopicReaderMetrics {
    pub(crate) received_bytes: Counter,
    pub(crate) received_messages: Counter,
    pub(crate) delivered_messages: Counter,
}

#[derive(Debug)]
pub(crate) struct TopicMetrics {
    labels: [(&'static str, String); 1],
    reader: OnceLock<TopicReaderMetrics>,
    writer: OnceLock<TopicWriterMetrics>,
}

impl TopicMetrics {
    pub(crate) fn reader(&self) -> TopicReaderMetrics {
        self.reader
            .get_or_init(|| TopicReaderMetrics {
                received_bytes: counter!(
                    description: "Bytes reported by topic read responses before decompression",
                    unit: Unit::Bytes,
                    "ydb.topic.reader.received.bytes", &self.labels
                ),
                received_messages: counter!(
                    description: "Decoded topic messages accepted into partition session buffers",
                    unit: Unit::Count,
                    "ydb.topic.reader.received.messages", &self.labels
                ),
                delivered_messages: counter!(
                    description: "Topic messages returned to the application in successful reads",
                    unit: Unit::Count,
                    "ydb.topic.reader.delivered.messages", &self.labels
                ),
            })
            .clone()
    }

    pub(crate) fn writer(&self) -> TopicWriterMetrics {
        self.writer
            .get_or_init(|| TopicWriterMetrics {
                sending_messages: counter!(
                    description: "Messages accepted into the topic writer queue",
                    unit: Unit::Count,
                    "ydb.topic.writer.sending.messages", &self.labels
                ),
                sending_bytes: counter!(
                    description: "Uncompressed payload bytes accepted into the topic writer queue",
                    unit: Unit::Bytes,
                    "ydb.topic.writer.sending.bytes", &self.labels
                ),
            })
            .clone()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsNames {
    pub(crate) topics: Arc<TopicMetrics>,
    pub client_new_counter: Counter,
    pub client_new_table_client_counter: Counter,
    pub client_new_query_client_counter: Counter,
    pub client_new_scheme_client_counter: Counter,
    pub client_new_topic_client_counter: Counter,
    pub client_query_row_counter: Counter,
    pub client_transaction_query_row_counter: Counter,
    pub client_transaction_exec_counter: Counter,
    pub client_transaction_commit_counter: Counter,
    pub client_transaction_rollback_counter: Counter,
    pub client_row_query_time_histogram: Histogram,
    pub client_transaction_row_query_time_histogram: Histogram,
}

impl Default for MetricsNames {
    fn default() -> Self {
        MetricsNames::new(None)
    }
}

impl MetricsNames {
    pub fn new(driver_name: Option<String>) -> Self {
        let labels = [(
            "driver_name",
            driver_name.unwrap_or(DEFAULT_DRIVER_NAME.to_string()),
        )];
        Self {
            client_new_counter: counter!(description: "ydb new client counter", "ydb_new_client_counter", &labels),
            client_new_table_client_counter: counter!(description: "ydb new table client counter", "ydb_new_table_client_counter", &labels),
            client_new_query_client_counter: counter!(description: "ydb new query client counter", "ydb_new_query_client_counter", &labels),
            client_new_scheme_client_counter: counter!(description: "ydb new scheme client counter", "ydb_new_scheme_client_counter", &labels),
            client_new_topic_client_counter: counter!(description: "ydb new topic client counter", "ydb_new_topic_client_counter", &labels),
            client_query_row_counter: counter!(description: "ydb client query row counter", "ydb_client_query_row_counter", &labels),
            client_transaction_query_row_counter: counter!(description: "ydb client transaction query row counter", "ydb_client_transaction_query_row_counter", &labels),
            client_transaction_exec_counter: counter!(description: "ydb client transaction exec counter", "ydb_client_transaction_exec_counter", &labels),
            client_transaction_commit_counter: counter!(description: "ydb client transaction commit counter", "ydb_client_transaction_commit_counter", &labels),
            client_transaction_rollback_counter: counter!(description: "ydb client transaction rollback counter", "ydb_client_transaction_rollback_counter", &labels),
            client_row_query_time_histogram: histogram!(description: "ydb row query time histogram", "ydb_row_query_time_histogram", &labels),
            client_transaction_row_query_time_histogram: histogram!(description: "ydb transaction row query time histogram", "ydb_transaction_row_query_time_histogram", &labels),
            topics: Arc::new(TopicMetrics {
                labels,
                reader: OnceLock::new(),
                writer: OnceLock::new(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::{
        MetricKind,
        debugging::{DebugValue, DebuggingRecorder},
    };

    #[test]
    fn topic_counter_names_labels_units_and_values() {
        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = MetricsNames::new(Some("test".into()));
        let writer = metrics.topics.writer();
        writer.sending_messages.increment(1);
        writer.sending_bytes.increment(2);
        let reader = metrics.topics.reader();
        reader.received_bytes.increment(3);
        reader.received_messages.increment(4);
        reader.delivered_messages.increment(5);

        let actual = snapshot
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, ..)| key.key().name().starts_with("ydb.topic."))
            .map(|(key, unit, _, value)| {
                assert_eq!(key.kind(), MetricKind::Counter);
                assert!(
                    key.key()
                        .labels()
                        .eq([&metrics::Label::new("driver_name", "test")])
                );
                (key.key().name().to_owned(), (unit, value))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let expected = [
            ("ydb.topic.writer.sending.messages", Unit::Count, 1),
            ("ydb.topic.writer.sending.bytes", Unit::Bytes, 2),
            ("ydb.topic.reader.received.bytes", Unit::Bytes, 3),
            ("ydb.topic.reader.received.messages", Unit::Count, 4),
            ("ydb.topic.reader.delivered.messages", Unit::Count, 5),
        ]
        .map(|(name, unit, value)| (name.to_owned(), (Some(unit), DebugValue::Counter(value))))
        .into();
        assert_eq!(actual, expected);
    }
}
