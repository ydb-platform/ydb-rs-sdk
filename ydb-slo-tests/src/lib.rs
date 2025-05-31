pub mod args;
pub mod cli;
pub mod generator;
pub mod metrics;
pub mod row;
pub mod workers;

fn normalize_url(url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_string()
    } else {
        format!("http://{}", url)
    }
}
