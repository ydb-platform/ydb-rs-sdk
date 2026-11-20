use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};

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

    pub fn sample(&self, name: &str) -> Option<u64> {
        let text = self.recorder.handle().render();
        let prefix = format!(r#"{name}{{driver_name="{}"}} "#, self.driver);
        let value = text.lines().find_map(|line| line.strip_prefix(&prefix))?;
        assert!(
            text.lines()
                .any(|line| line == format!("# TYPE {name} counter"))
        );
        Some(value.parse().expect("expected an integer counter"))
    }
}
