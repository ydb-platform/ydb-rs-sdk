#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Duration {
    val: std::time::Duration,
}

impl From<std::time::Duration> for Duration {
    fn from(val: std::time::Duration) -> Self {
        Self { val }
    }
}

impl From<ydb_grpc::google_proto_workaround::protobuf::Duration> for Duration {
    fn from(val: ydb_grpc::google_proto_workaround::protobuf::Duration) -> Self {
        Self {
            val: std::time::Duration::new(val.seconds as u64, val.nanos as u32),
        }
    }
}

impl From<Duration> for pbjson_types::Duration {
    fn from(d: Duration) -> Self {
        Self {
            seconds: d.val.as_secs() as i64,
            nanos: d.val.subsec_nanos() as i32,
        }
    }
}

impl From<Duration> for ydb_grpc::google_proto_workaround::protobuf::Duration {
    fn from(d: Duration) -> Self {
        d.val.into()
    }
}

impl From<Duration> for std::time::Duration {
    fn from(d: Duration) -> Self {
        d.val
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Timestamp {
    val: std::time::SystemTime,
}

impl From<std::time::SystemTime> for Timestamp {
    fn from(val: std::time::SystemTime) -> Self {
        Self { val }
    }
}

impl From<ydb_grpc::google_proto_workaround::protobuf::Timestamp> for Timestamp {
    fn from(ts: ydb_grpc::google_proto_workaround::protobuf::Timestamp) -> Self {
        let duration = std::time::Duration::new(ts.seconds.unsigned_abs(), ts.nanos as u32);
        let val = if ts.seconds >= 0 {
            std::time::UNIX_EPOCH + duration
        } else {
            std::time::UNIX_EPOCH - duration
        };

        Self { val }
    }
}

impl From<Timestamp> for pbjson_types::Timestamp {
    fn from(t: Timestamp) -> Self {
        let duration = t.val.duration_since(std::time::UNIX_EPOCH).unwrap();
        Self {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        }
    }
}

impl From<Timestamp> for ydb_grpc::google_proto_workaround::protobuf::Timestamp {
    fn from(t: Timestamp) -> Self {
        let duration = t.val.duration_since(std::time::UNIX_EPOCH).unwrap();
        Self {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        }
    }
}

impl From<Timestamp> for std::time::SystemTime {
    fn from(t: Timestamp) -> Self {
        t.val
    }
}
