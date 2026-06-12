use std::env;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub connection_string: String,
    pub database: String,
    pub ref_name: String,
    pub label: String,
    pub duration_secs: u64,
    pub otlp_endpoint: Option<String>,
    pub prometheus_url: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let connection_string = env::var("YDB_CONNECTION_STRING").ok().filter(|s| !s.is_empty());

        let (connection_string, database) = match connection_string {
            Some(cs) => {
                let database = database_from_connection_string(&cs)
                    .or_else(|| env::var("YDB_DATABASE").ok())
                    .ok_or_else(|| {
                        "YDB_DATABASE is required when database path is missing in YDB_CONNECTION_STRING"
                            .to_string()
                    })?;
                (cs, database)
            }
            None => {
                let endpoint = env::var("YDB_ENDPOINT")
                    .map_err(|_| "YDB_CONNECTION_STRING or YDB_ENDPOINT is required".to_string())?;
                let database = env::var("YDB_DATABASE")
                    .map_err(|_| "YDB_DATABASE is required when YDB_CONNECTION_STRING is not set".to_string())?;
                (format!("{endpoint}{database}"), database)
            }
        };

        let ref_name = env::var("WORKLOAD_REF").unwrap_or_else(|_| "current".to_string());
        let label = env::var("WORKLOAD_NAME").unwrap_or_else(|_| "native-table".to_string());

        let duration_secs = env::var("WORKLOAD_DURATION")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(600);

        if duration_secs == 0 {
            return Err("WORKLOAD_DURATION must be > 0".to_string());
        }

        let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty());
        let prometheus_url = env::var("PROMETHEUS_URL").ok().filter(|s| !s.is_empty());

        Ok(Self {
            connection_string,
            database,
            ref_name,
            label,
            duration_secs,
            otlp_endpoint,
            prometheus_url,
        })
    }

    pub fn run_duration(&self) -> Duration {
        Duration::from_secs(self.duration_secs)
    }
}

fn database_from_connection_string(connection_string: &str) -> Option<String> {
    let without_query = connection_string.split('?').next()?;
    let scheme_end = without_query.find("://")? + 3;
    let path = without_query.get(scheme_end..)?;
    if path.is_empty() || path == "/" {
        return None;
    }
    Some(path.to_string())
}
