use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Setup,
    Warmup,
    Run,
    Cooldown,
    Teardown,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Setup => "setup",
            Phase::Warmup => "warmup",
            Phase::Run => "run",
            Phase::Cooldown => "cooldown",
            Phase::Teardown => "teardown",
        }
    }
}

pub struct Logger {
    phase: Mutex<Phase>,
    last_error: Mutex<Option<(String, u32, Instant)>>,
}

impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger {
    pub fn new() -> Self {
        Self {
            phase: Mutex::new(Phase::Setup),
            last_error: Mutex::new(None),
        }
    }

    pub fn set_phase(&self, phase: Phase) {
        self.flush();
        *self.phase.lock().unwrap() = phase;
    }

    pub fn printf(&self, message: impl AsRef<str>) {
        self.flush();
        let phase = *self.phase.lock().unwrap();
        let ts = chrono_like_now();
        eprintln!("[{ts}] [{phase}] {}", message.as_ref());
    }

    pub fn errorf(&self, message: impl AsRef<str>) {
        let msg = message.as_ref().to_string();
        let mut last = self.last_error.lock().unwrap();
        if let Some((prev, count, _)) = last.as_mut()
            && *prev == msg
        {
            *count += 1;
            return;
        }
        drop(last);
        self.flush();
        let phase = *self.phase.lock().unwrap();
        let ts = chrono_like_now();
        eprintln!("[{ts}] [{phase}] ERROR: {msg}");
        *self.last_error.lock().unwrap() = Some((msg, 1, Instant::now()));
    }

    pub fn flush(&self) {
        let mut last = self.last_error.lock().unwrap();
        if let Some((_, count, started)) = last.take()
            && count > 1
        {
            let phase = *self.phase.lock().unwrap();
            let ts = chrono_like_now();
            let elapsed = Instant::now().duration_since(started);
            eprintln!(
                "[{ts}] [{phase}] ERROR: (repeated {} times in {:?})",
                count - 1,
                elapsed
            );
        }
    }
}

fn chrono_like_now() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    // RFC3339-like without external chrono dependency.
    format!("{secs}")
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
