impl From<std::time::Duration> for crate::generated::google::protobuf::Duration {
    fn from(std_duration: std::time::Duration) -> Self {
        Self {
            seconds: std_duration.as_secs() as i64,
            nanos: std_duration.subsec_nanos() as i32,
        }
    }
}
