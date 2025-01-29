use std::time::SystemTime;

pub type RowID = u64;

#[derive(Debug, Clone)]
pub struct TestRow {
    pub id: RowID,
    pub payload_str: String,
    pub payload_double: f64,
    pub payload_timestamp: SystemTime,
}

impl TestRow {
    pub fn new(
        id: RowID,
        payload_str: String,
        payload_double: f64,
        payload_timestamp: SystemTime,
    ) -> Self {
        Self {
            id,
            payload_str,
            payload_double,
            payload_timestamp,
        }
    }
}
