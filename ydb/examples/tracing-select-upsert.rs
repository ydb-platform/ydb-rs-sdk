#![recursion_limit = "256"]
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use std::time::Duration;
use tokio::time::timeout;
use tracing::instrument;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

use ydb::traces::filter_ext::{EnvFilterExt, TargetFilterExt};
use ydb::{ClientBuilder, Row, YdbError, YdbResult};

static SERVICE_NAME: &str = "example_service";

fn get_resource() -> Resource {
    Resource::builder()
        .with_service_name(SERVICE_NAME)
        .with_attribute(KeyValue::new(
            "pod",
            std::env::var("POD_NAME").unwrap_or_else(|_| "unknown".to_string()),
        ))
        .with_attribute(KeyValue::new(
            "namespace",
            std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "unknown".to_string()),
        ))
        .with_attribute(KeyValue::new(
            "nodename",
            std::env::var("NODE_NAME").unwrap_or_else(|_| "unknown".to_string()),
        ))
        .build()
}

fn build_tracer_provider() -> SdkTracerProvider {
    let otlp_endpoint =
        std::env::var("OTEL_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(get_resource())
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    tracer_provider
}

#[instrument]
async fn start_app() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let mut query_client = client.query_client();
    let _ = query_client.exec("DROP TABLE test").await;

    query_client
        .exec("CREATE TABLE test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    query_client
        .retry_tx(async |tx| {
            for i in 1..100 {
                tx.exec("UPSERT INTO test (id, val) VALUES ($id, $val)")
                    .param("$id", i as i64)
                    .param("$val", format!("val: {}", i))
                    .await?;
            }
            Ok(())
        })
        .await
        .map_err(|e| YdbError::Transport(format!("{e:?}")))?;

    let sum: Option<i64> = query_client
        .retry_tx(async |tx| {
            let mut row = tx.query_row("SELECT SUM(id) AS sum FROM test").await?;
            Ok(row.remove_field_by_name("sum")?.try_into()?)
        })
        .await
        .map_err(|e| YdbError::Transport(format!("{e:?}")))?;
    println!("sum: {}", sum.unwrap_or(-1));

    let rows: Vec<Row> = query_client
        .retry_tx(async |tx| {
            Ok(tx
                .query_result_set("SELECT * FROM test ORDER BY id LIMIT 10")
                .await?
                .rows()
                .collect())
        })
        .await
        .map_err(|e| YdbError::Transport(format!("{e:?}")))?;

    for mut row in rows {
        let id: Option<i64> = row.remove_field_by_name("id")?.try_into()?;
        let val: Option<String> = row.remove_field_by_name("val")?.try_into()?;
        println!("row id '{}' with value '{}'", id.unwrap(), val.unwrap())
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> YdbResult<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("trace"))
        .without_transport();

    // This helps in filtering out some traces from SDK (e.g. session pool subcrate traces)
    let target_filter = Targets::new()
        .with_default(LevelFilter::INFO)
        .without_session_pool()
        .without_connection_pool();

    let fmt = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::FULL);

    let provider = build_tracer_provider();
    let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("tracing"));

    tracing_subscriber::registry()
        .with(filter)
        .with(target_filter)
        .with(fmt)
        .with(otel_layer)
        .init();

    start_app().await?;

    let _ = provider.shutdown();

    Ok(())
}
