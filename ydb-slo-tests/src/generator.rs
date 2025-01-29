use crate::row::{RowID, TestRow};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use rand::prelude::StdRng;
use rand::Rng;
use rand_core::{OsRng, RngCore, SeedableRng};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

const MIN_LENGTH: usize = 20;
const MAX_LENGTH: usize = 40;

#[derive(Clone, Debug)]
pub struct Generator {
    current_id: Arc<Mutex<RowID>>,
    rng: StdRng,
}

impl Generator {
    pub fn new(id: RowID) -> Self {
        Self {
            current_id: Arc::new(Mutex::new(id)),
            rng: SeedableRng::from_entropy(),
        }
    }

    pub fn generate(&mut self) -> TestRow {
        let id = {
            let mut id_guard = self.current_id.lock().unwrap();
            *id_guard += 1;
            *id_guard
        };

        let payload_double = self.rng.gen::<f64>();
        let payload_timestamp = SystemTime::now();
        let payload_str = self.gen_payload_string();

        TestRow::new(id, payload_str, payload_double, payload_timestamp)
    }

    fn gen_payload_string(&mut self) -> String {
        let length = MIN_LENGTH + self.rng.gen_range(0..=(MAX_LENGTH - MIN_LENGTH));

        let mut buffer = vec![0u8; length];
        OsRng.fill_bytes(&mut buffer);

        STANDARD.encode(&buffer)
    }
}
