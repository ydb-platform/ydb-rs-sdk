use metrics::{Counter, Histogram, Label, Unit, counter, histogram};

const DEFAULT_DRIVER_NAME: &str = "main";

#[derive(Clone, Debug)]
pub(crate) struct TopicWriterMetrics {
    pub(crate) sending_messages: Counter,
    pub(crate) sending_bytes: Counter,
}

#[derive(Clone, Debug)]
pub(crate) struct TopicReaderMetrics {
    pub(crate) received_bytes: Counter,
    labels: Vec<Label>,
}

impl TopicReaderMetrics {
    fn topic_labels(&self, topic: &str) -> Vec<Label> {
        let mut labels = self.labels.clone();
        labels.push(Label::new("topic", topic.to_owned()));
        labels
    }

    pub(crate) fn received_messages(&self, topic: &str) -> Counter {
        counter!(
            description: "Topic messages received for open partition sessions before decompression",
            unit: Unit::Count,
            "ydb.topic.reader.received.messages", self.topic_labels(topic)
        )
    }

    pub(crate) fn delivered_messages(&self, topic: &str) -> Counter {
        counter!(
            description: "Topic messages returned to the application in successful reads",
            unit: Unit::Count,
            "ydb.topic.reader.delivered.messages", self.topic_labels(topic)
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TopicMetrics {
    driver: Label,
}

// Topic labels are known when a reader or writer opens. For a multi-topic
// reader the server identifies the topic when it starts a partition session.
impl TopicMetrics {
    pub(crate) fn reader(&self, consumer: &str, name: Option<&str>) -> TopicReaderMetrics {
        let mut labels = vec![
            self.driver.clone(),
            Label::new("consumer", consumer.to_owned()),
        ];
        if let Some(name) = name.filter(|name| !name.is_empty()) {
            labels.push(Label::new("reader.name", name.to_owned()));
        }
        TopicReaderMetrics {
            received_bytes: counter!(
                description: "Bytes reported by topic read responses before decompression",
                unit: Unit::Bytes,
                "ydb.topic.reader.received.bytes", labels.iter()
            ),
            labels,
        }
    }

    pub(crate) fn writer(&self, topic: &str, name: Option<&str>) -> TopicWriterMetrics {
        let mut labels = vec![self.driver.clone(), Label::new("topic", topic.to_owned())];
        if let Some(name) = name.filter(|name| !name.is_empty()) {
            labels.push(Label::new("writer.name", name.to_owned()));
        }
        TopicWriterMetrics {
            sending_messages: counter!(
                description: "Messages accepted into the topic writer queue",
                unit: Unit::Count,
                "ydb.topic.writer.sending.messages", labels.iter()
            ),
            sending_bytes: counter!(
                description: "Uncompressed payload bytes accepted into the topic writer queue",
                unit: Unit::Bytes,
                "ydb.topic.writer.sending.bytes", labels.iter()
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsNames {
    pub(crate) topics: TopicMetrics,
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
            topics: TopicMetrics {
                driver: Label::from(&labels[0]),
            },
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
    fn empty_and_omitted_topic_names_export_one_series_per_counter() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let topics = MetricsNames::default().topics;
        for (name, count) in [(None, 2), (Some(""), 3)] {
            let writer = topics.writer("orders", name);
            writer.sending_messages.increment(count);
            writer.sending_bytes.increment(count);
            let reader = topics.reader("billing", name);
            reader.received_bytes.increment(count);
            reader.received_messages("orders").increment(count);
            reader.delivered_messages("orders").increment(count);
        }

        let output = recorder.handle().render();
        let samples: Vec<_> = output
            .lines()
            .filter(|line| line.starts_with("ydb_topic_"))
            .collect();
        assert_eq!(samples.len(), 5, "{output}");
        for sample in samples {
            assert!(sample.ends_with("} 5"), "{sample}");
            assert!(
                !sample.contains("reader_name=") && !sample.contains("writer_name="),
                "{sample}"
            );
        }
    }

    #[test]
    fn topic_counter_names_labels_units_and_values() {
        let recorder = DebuggingRecorder::new();
        let snapshot = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let metrics = MetricsNames::new(Some("test".into()));
        let writer = metrics.topics.writer("orders", Some("producer"));
        writer.sending_messages.increment(1);
        writer.sending_bytes.increment(2);
        let reader = metrics.topics.reader("billing", Some("worker"));
        reader.received_bytes.increment(3);
        reader.received_messages("orders").increment(4);
        reader.delivered_messages("orders").increment(5);

        // Opening another reader or writer on this driver must select its own series.
        let unnamed_writer = metrics.topics.writer("orders", None);
        unnamed_writer.sending_messages.increment(6);
        let unnamed_reader = metrics.topics.reader("billing", None);
        unnamed_reader.received_bytes.increment(7);
        let other_consumer = metrics.topics.reader("audit", Some("worker"));
        other_consumer.received_bytes.increment(8);

        let actual = snapshot.snapshot().into_vec();
        let writer = &[
            ("driver_name", "test"),
            ("topic", "orders"),
            ("writer.name", "producer"),
        ][..];
        let reader = &[
            ("driver_name", "test"),
            ("consumer", "billing"),
            ("reader.name", "worker"),
            ("topic", "orders"),
        ][..];
        let expected = [
            ("ydb.topic.writer.sending.messages", writer, Unit::Count, 1),
            ("ydb.topic.writer.sending.bytes", writer, Unit::Bytes, 2),
            (
                "ydb.topic.reader.received.bytes",
                &reader[..3],
                Unit::Bytes,
                3,
            ),
            ("ydb.topic.reader.received.messages", reader, Unit::Count, 4),
            (
                "ydb.topic.reader.delivered.messages",
                reader,
                Unit::Count,
                5,
            ),
            (
                "ydb.topic.writer.sending.messages",
                &writer[..2],
                Unit::Count,
                6,
            ),
            (
                "ydb.topic.writer.sending.bytes",
                &writer[..2],
                Unit::Bytes,
                0,
            ),
            (
                "ydb.topic.reader.received.bytes",
                &reader[..2],
                Unit::Bytes,
                7,
            ),
            (
                "ydb.topic.reader.received.bytes",
                &[
                    ("driver_name", "test"),
                    ("consumer", "audit"),
                    ("reader.name", "worker"),
                ],
                Unit::Bytes,
                8,
            ),
        ];
        assert_eq!(
            actual
                .iter()
                .filter(|(key, ..)| key.key().name().starts_with("ydb.topic."))
                .count(),
            expected.len()
        );
        for (name, labels, unit, value) in expected {
            let key = metrics::Key::from_parts(name, labels);
            let (kind, actual_unit, _, actual_value) = actual
                .iter()
                .find(|(metric, ..)| metric.key() == &key)
                .unwrap_or_else(|| panic!("missing counter {key:?}"));
            assert_eq!(kind.kind(), MetricKind::Counter);
            assert_eq!(*actual_unit, Some(unit));
            assert_eq!(*actual_value, DebugValue::Counter(value));
        }
    }
}
