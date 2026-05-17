use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, Query, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let table_client = client.table_client();
    let table_name = "arrow_test";

    let _ = table_client
        .retry_execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await;

    table_client
        .retry_execute_scheme_query(format!(
            "CREATE TABLE {table_name} (id Int64 NOT NULL, val Utf8, PRIMARY KEY(id))",
        ))
        .await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                None,
                Some("David"),
                Some("Eve"),
            ])),
        ],
    )
    .map_err(|e| YdbError::Custom(format!("Failed to create RecordBatch: {}", e)))?;

    println!("Inserting {} rows using Arrow format...", batch.num_rows());

    table_client
        .retry_execute_bulk_upsert_arrow(format!("/local/{table_name}"), batch)
        .await?;

    println!("Bulk upsert completed successfully!");

    let read = table_client
        .retry_transaction(|t| async {
            let mut t = t;
            let res = t
                .query(Query::new(format!(
                    "SELECT * FROM {table_name} ORDER BY id"
                )))
                .await?;
            Ok(res)
        })
        .await?;

    println!("\nReading back data:");
    for mut row in read.into_only_result()?.rows() {
        let id_opt: Option<i64> = row
            .remove_field_by_name("id")?
            .try_into()
            .ok()
            .ok_or_else(|| YdbError::Custom("Failed to read id".to_string()))?;
        let id = id_opt.ok_or_else(|| YdbError::Custom("id is null".to_string()))?;

        let val: Option<String> = row.remove_field_by_name("val")?.try_into().ok().flatten();
        println!("  id: {}, val: {:?}", id, val);
    }

    println!("\nOK - Arrow bulk upsert example completed successfully!");

    Ok(())
}
