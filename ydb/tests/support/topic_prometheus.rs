use metrics_exporter_prometheus::{
    PrometheusBuilder, PrometheusRecorder, formatting::sanitize_label_value,
};
use std::collections::HashMap;

pub struct TopicPrometheus {
    recorder: PrometheusRecorder,
    driver: &'static str,
}

impl TopicPrometheus {
    pub fn new(driver: &'static str) -> Self {
        Self {
            recorder: PrometheusBuilder::new().build_recorder(),
            driver,
        }
    }

    pub fn install_local(&self) -> metrics::LocalRecorderGuard<'_> {
        metrics::set_default_local_recorder(&self.recorder)
    }

    pub fn sample(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        let text = self.recorder.handle().render();
        let expected: HashMap<_, _> = std::iter::once(("driver_name", self.driver))
            .chain(labels.iter().copied())
            .map(|(key, value)| (key, format!(r#""{}""#, sanitize_label_value(value))))
            .collect();
        let prefix = format!("{name}{{");
        let mut values = text.lines().filter_map(|line| {
            let (mut fields, value) = line.strip_prefix(&prefix)?.rsplit_once("} ")?;
            let mut remaining = expected.clone();
            while !remaining.is_empty() {
                let (key, tail) = fields.split_once('=')?;
                fields = tail.strip_prefix(&remaining.remove(key)?)?;
                if !remaining.is_empty() {
                    fields = fields.strip_prefix(',')?;
                }
            }
            fields.is_empty().then_some(value)
        });
        let value = values.next()?;
        assert!(
            values.next().is_none(),
            "duplicate series for {name} {expected:?}"
        );
        assert!(
            text.lines()
                .any(|line| line == format!("# TYPE {name} counter"))
        );
        Some(value.parse().expect("expected an integer counter"))
    }
}
