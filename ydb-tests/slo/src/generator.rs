use base64::encode;
use chrono::{DateTime, Utc};
use rand::{rngs::ThreadRng, Rng};
use rand_core::{OsRng, RngCore};
use std::sync::{Arc, Mutex};

pub type RowID = u64;

#[derive(Debug, Clone)]
pub struct Row {
    pub hash: u64,
    pub id: RowID,
    pub payload_str: Option<String>,
    pub payload_double: Option<f64>,
    pub payload_timestamp: Option<DateTime<Utc>>,
    pub payload_hash: u64,
}

impl Row {
    pub fn new(
        hash: u64,
        id: RowID,
        payload_str: Option<String>,
        payload_double: Option<f64>,
        payload_timestamp: Option<DateTime<Utc>>,
        payload_hash: u64,
    ) -> Self {
        Self {
            hash,
            id,
            payload_str,
            payload_double,
            payload_timestamp,
            payload_hash,
        }
    }
}

const MIN_LENGTH: usize = 20;
const MAX_LENGTH: usize = 40;

#[derive(Debug)]
pub struct Generator {
    current_id: Arc<Mutex<RowID>>,
    rng: ThreadRng,
}

impl Generator {
    pub fn new(starting_id: RowID) -> Self {
        Self {
            current_id: Arc::new(Mutex::new(starting_id)),
            rng: rand::thread_rng(),
        }
    }

    pub fn generate(&mut self) -> Result<Row, String> {
        let id = {
            let mut id_guard = self.current_id.lock().map_err(|_| "Mutex poisoned")?;
            let id = *id_guard;
            *id_guard += 1;
            id
        };

        let payload_double = Some(self.rng.gen::<f64>());
        let payload_timestamp = Some(Utc::now());

        let payload_str = match self.gen_payload_string() {
            Ok(payload) => Some(payload),
            Err(err) => return Err(err),
        };

        Ok(Row {
            id,
            payload_str,
            payload_double,
            payload_timestamp,
        })
    }

    fn gen_payload_string(&mut self) -> Result<String, String> {
        let length = MIN_LENGTH + self.rng.gen_range(0..=(MAX_LENGTH - MIN_LENGTH));

        let mut buffer = vec![0u8; length];
        OsRng.fill_bytes(&mut buffer);

        Ok(encode(&buffer))
    }
}
