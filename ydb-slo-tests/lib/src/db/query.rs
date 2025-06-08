use crate::db::row::{Row, RowID};
use ydb::{ydb_params, Query};

pub fn generate_create_table_query(
    table_name: &str,
    min_partitions_count: u64,
    max_partitions_count: u64,
    partition_size: u64,
) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table}
        (
            hash Uint64,
            id Uint64,
            payload_str Text?,
            payload_double Double?,
            payload_timestamp Timestamp?,
            payload_hash Uint64?,
            PRIMARY KEY (hash, id)
        ) WITH (
            UNIFORM_PARTITIONS = {min_partitions_count},
            AUTO_PARTITIONING_BY_SIZE = ENABLED,
            AUTO_PARTITIONING_PARTITION_SIZE_MB = {partition_size},
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {min_partitions_count},
            AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {max_partitions_count}
        )",
        table = table_name,
        min_partitions_count = min_partitions_count,
        max_partitions_count = max_partitions_count,
        partition_size = partition_size,
    )
}

pub fn generate_drop_table_query(table_name: &str) -> String {
    format!("DROP TABLE {}", table_name)
}

pub fn generate_read_query(table_name: &str, row_id: RowID) -> Query {
    Query::from(format!(
        r#"
            DECLARE $id AS Uint64;

            SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
            FROM {table}
            WHERE id = $id AND hash = Digest::NumericHash($id);
            "#,
        table = table_name
    ))
    .with_params(ydb_params!("$id" => row_id))
}

pub fn generate_write_query(table_name: &str, row: Row) -> Query {
    Query::from(format!(
        r#"
            DECLARE $id AS Uint64;
            DECLARE $payload_str AS Utf8;
            DECLARE $payload_double AS Double;
            DECLARE $payload_timestamp AS Timestamp;

            UPSERT INTO {table} (
                id,
                hash,
                payload_str,
                payload_double,
                payload_timestamp
            ) VALUES (
                $id,
                Digest::NumericHash($id),
                $payload_str,
                $payload_double,
                $payload_timestamp
            );
            "#,
        table = table_name
    ))
    .with_params(ydb_params!(
            "$id" => row.id,
            "$payload_str" => row.payload_str,
            "$payload_double" => row.payload_double,
            "$payload_timestamp" => row.payload_timestamp,
    ))
}
