#![recursion_limit = "256"]
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::timeout;
use tracing::instrument;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

use ydb::traces::filter_ext::FilterExt;
use ydb::{ClientBuilder, Row, YdbError, YdbResult};

static SERVICE_NAME: &str = "example_service";
static UNKNOWN_RESOURCE: &str = "unknown";

fn get_resource() -> Resource {
    static RESOURCE: OnceLock<Resource> = OnceLock::new();

    RESOURCE
        .get_or_init(|| {
            let mut builder = Resource::builder().with_service_name(SERVICE_NAME);

            builder = builder.with_attribute(KeyValue::new(
                "pod",
                std::env::var("POD_NAME").unwrap_or(UNKNOWN_RESOURCE.to_string()),
            ));

            builder = builder.with_attribute(KeyValue::new(
                "namespace",
                std::env::var("POD_NAMESPACE").unwrap_or(UNKNOWN_RESOURCE.to_string()),
            ));

            builder = builder.with_attribute(KeyValue::new(
                "nodename",
                std::env::var("NODE_NAME").unwrap_or(UNKNOWN_RESOURCE.to_string()),
            ));

            builder.build()
        })
        .clone()
}

fn build_tracer_provider() -> SdkTracerProvider {
    let otlp_endpoint =
        std::env::var("OTEL_ENDPOINT").unwrap_or_else(|_| "http://localhost:43170".to_string());

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    let tracer_provider = SdkTracerProvider::builder()
        // with_simple_exporter will cause hang because of https://github.com/open-telemetry/opentelemetry-rust/issues/2071#issuecomment-2328484839
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
    let _ = query_client.exec("DROP TABLE test").await; // ignore drop error

    // create table
    query_client
        .exec("CREATE TABLE test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    // fill with data
    query_client
        .retry_transaction(async move |tx| {
            // upsert 100 rows in loop
            // use upsert instead of insert because insert need check if previous row exist
            // and can't execute second time in the transaction
            for i in 1..100 {
                tx.exec("UPSERT INTO test (id, val) VALUES ($id, $val)")
                    .param("$id", i as i64)
                    .param("$val", format!("val: {}", i))
                    .await?;
            }
            Ok(()) // Ok -> commit
        })
        .await
        .unwrap();

    // Select one row result
    let sum: Option<i64> = query_client
        .retry_transaction(async move |tx| {
            let mut row = tx.query_row("SELECT SUM(id) AS sum FROM test").await?;
            Ok(row.remove_field_by_name("sum")?.try_into()?)
        })
        .await?;
    println!("sum: {}", sum.unwrap_or(-1));

    // select first 10 rows
    let rows: Vec<Row> = query_client
        .retry_transaction(async move |tx| {
            Ok(tx
                .query_result_set("SELECT * FROM test ORDER BY id LIMIT 10")
                .await?
                .rows()
                .collect())
        })
        .await?;

    for mut row in rows {
        let id: Option<i64> = row.remove_field_by_name("id")?.try_into()?;
        let val: Option<String> = row.remove_field_by_name("val")?.try_into()?;
        println!("row id '{}' with value '{}'", id.unwrap(), val.unwrap())
    }

    Ok(())
}

#[tokio::main]
pub async fn main() -> YdbResult<()> {
    // very verbose logs (turning off transoprt spans/events, it's huge)
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("trace"))
        .without_transport();

    // add format layer to stdout
    let fmt = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true);

    // build OTEL tracer provider
    //
    let provider = build_tracer_provider();
    // and add tracing-crate layer for OTel
    let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer(SERVICE_NAME));

    // Cooking subscriber-combinator
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel_layer)
        .init();

    start_app().await?;

    // when provider drops, it flush spans to endpoint
    let _ = provider.shutdown_with_timeout(Duration::from_secs(30));

    Ok(())
}
