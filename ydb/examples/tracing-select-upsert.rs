#![recursion_limit = "256"]
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::timeout;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

use ydb::{ydb_params, ClientBuilder, Query, Row, YdbError, YdbResult};

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
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://localhost:4317")
        .build()
        .unwrap();

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(get_resource())
        .with_batch_exporter(exporter)
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    tracer_provider
}

#[tokio::main]
pub async fn main() -> YdbResult<()> {
    // very verbose logs
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("trace"))
        /*.add_directive("opentelemetry=error".parse().unwrap())*/; // exclude verbose opentelemetry filters

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

    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let table_client = client.table_client();
    let _ = table_client
        .retry_execute_scheme_query("DROP TABLE test")
        .await; // ignore drop error

    // create table
    table_client
        .retry_execute_scheme_query("CREATE TABLE test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    // fill with data
    table_client
        .retry_transaction(|mut t| async move {
            // upsert 100 rows in loop
            // use upsert instead of insert because insert need check if previous row exist
            // and can't execute second time in the transaction
            for i in 1..100 {
                t.query(
                    Query::new(
                        "
                    DECLARE $id AS Int64;
                    DECLARE $val AS Utf8;

                    UPSERT INTO test (id, val) VALUES ($id, $val)
                    ",
                    )
                    .with_params(ydb_params!(
                        "$id" => i as i64,
                        "$val" => format!("val: {}", i)
                    )),
                )
                .await?;
            }
            t.commit().await?;
            Ok(())
        })
        .await
        .unwrap();

    // Select one row result
    let sum: Option<i64> = table_client
        .retry_transaction(|mut t| async move {
            let value = t
                .query(Query::new("SELECT SUM(id) AS sum FROM test"))
                .await?
                .into_only_row()?
                .remove_field_by_name("sum")?;
            let res = value.try_into(); // res: YdbResult<Option<i64>>
            Ok(res.unwrap())
        })
        .await?;
    println!("sum: {}", sum.unwrap_or(-1));

    // select first 10 rows
    let rows: Vec<Row> = table_client
        .retry_transaction(|mut t| async move {
            Ok(
                t.query(Query::new("SELECT * FROM test ORDER BY id LIMIT 10"))
                    .await?
                    .into_only_result()?
                    .rows()
                    .collect(),
            )
        })
        .await?;

    for mut row in rows {
        let id: Option<i64> = row.remove_field_by_name("id")?.try_into()?;
        let val: Option<String> = row.remove_field_by_name("val")?.try_into()?;
        println!("row id '{}' with value '{}'", id.unwrap(), val.unwrap())
    }

    let _ = provider.shutdown();

    Ok(())
}
