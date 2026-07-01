#![recursion_limit = "256"]
//! Vector search with YQL Knn UDFs — mirrors
//! `ydb/public/sdk/python/examples/vector_search/vector_search.py`.

use ydb::{ydb_struct, Bytes, ClientBuilder, Value, YdbResult};

const TABLE_NAME: &str = "ydb_vector_search";
const INDEX_NAME: &str = "ydb_vector_index";

fn convert_vector_to_bytes(vector: &[f32]) -> Bytes {
    let mut buf = Vec::with_capacity(vector.len() * 4 + 1);
    for v in vector {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf.push(0x01);
    Bytes::from(buf)
}

async fn drop_vector_table_if_exists(qc: &mut ydb::QueryClient, table_name: &str) -> YdbResult<()> {
    qc.exec(format!("DROP TABLE IF EXISTS `{table_name}`"))
        .await?;
    println!("Vector table dropped");
    Ok(())
}

async fn create_vector_table(qc: &mut ydb::QueryClient, table_name: &str) -> YdbResult<()> {
    qc.exec(format!(
        "CREATE TABLE IF NOT EXISTS `{table_name}` (
            id Utf8,
            document Utf8,
            embedding String,
            PRIMARY KEY (id)
        );"
    ))
    .await?;
    println!("Vector table created");
    Ok(())
}

struct Item {
    id: &'static str,
    document: &'static str,
    embedding: Vec<f32>,
}

async fn insert_items_as_bytes(
    qc: &mut ydb::QueryClient,
    table_name: &str,
    items: &[Item],
) -> YdbResult<()> {
    let query = format!(
        r#"
        UPSERT INTO `{table_name}`
        (id, document, embedding)
        SELECT id, document, embedding
        FROM AS_TABLE($items);
        "#
    );

    let example = ydb_struct!(
        "id" => "",
        "document" => "",
        "embedding" => Bytes::default(),
    );
    let rows: Vec<Value> = items
        .iter()
        .map(|item| {
            ydb_struct!(
                "id" => item.id,
                "document" => item.document,
                "embedding" => convert_vector_to_bytes(&item.embedding),
            )
        })
        .collect();
    let list = Value::list_from(example, rows)?;

    qc.exec(query).param("$items", list).await?;
    println!("{} items inserted", items.len());
    Ok(())
}

async fn add_vector_index(
    qc: &mut ydb::QueryClient,
    table_name: &str,
    index_name: &str,
    strategy: &str,
    dimension: u32,
    levels: u32,
    clusters: u32,
) -> YdbResult<()> {
    let temp_index_name = format!("{index_name}__temp");
    qc.exec(format!(
        r#"
        ALTER TABLE `{table_name}`
        ADD INDEX {temp_index_name}
        GLOBAL USING vector_kmeans_tree
        ON (embedding)
        WITH (
            {strategy},
            vector_type="Float",
            vector_dimension={dimension},
            levels={levels},
            clusters={clusters}
        );
        "#
    ))
    .await?;

    qc.exec(format!(
        "ALTER TABLE `{table_name}` RENAME INDEX `{temp_index_name}` TO `{index_name}`;"
    ))
    .await?;

    println!("Table index {index_name} created.");
    Ok(())
}

struct SearchHit {
    id: String,
    document: String,
    score: f32,
}

async fn search_items_as_bytes(
    qc: &mut ydb::QueryClient,
    table_name: &str,
    embedding: &[f32],
    strategy: &str,
    limit: u32,
    index_name: Option<&str>,
) -> YdbResult<Vec<SearchHit>> {
    let view_index = index_name
        .map(|name| format!("VIEW {name}"))
        .unwrap_or_default();
    let sort_order = if strategy.ends_with("Similarity") {
        "DESC"
    } else {
        "ASC"
    };

    let query = format!(
        r#"
        SELECT
            id,
            document,
            Knn::{strategy}(embedding, $embedding) AS score
        FROM `{table_name}` {view_index}
        ORDER BY score {sort_order}
        LIMIT {limit};
        "#
    );

    let mut stream = qc
        .query(query)
        .param("$embedding", convert_vector_to_bytes(embedding))
        .await?;

    let mut hits = Vec::new();
    while let Some(result_set) = stream.next_result_set().await? {
        for mut row in result_set {
            let id: Option<String> = row.remove_field_by_name("id")?.try_into()?;
            let document: Option<String> = row.remove_field_by_name("document")?.try_into()?;
            let score: Option<f32> = row.remove_field_by_name("score")?.try_into()?;
            hits.push(SearchHit {
                id: id.unwrap_or_default(),
                document: document.unwrap_or_default(),
                score: score.unwrap_or(f32::NAN),
            });
        }
    }
    stream.close().await?;
    Ok(hits)
}

fn print_results(items: &[SearchHit]) {
    if items.is_empty() {
        println!("No items found");
        return;
    }
    for item in items {
        println!("[score={}] {}: {}", item.score, item.id, item.document);
    }
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;
    client.wait().await?;

    let mut qc = client.query_client().clone_with_idempotent_operations(true);

    drop_vector_table_if_exists(&mut qc, TABLE_NAME).await?;
    create_vector_table(&mut qc, TABLE_NAME).await?;

    let items = vec![
        Item {
            id: "1",
            document: "vector 1",
            embedding: vec![0.98, 0.1, 0.01],
        },
        Item {
            id: "2",
            document: "vector 2",
            embedding: vec![1.0, 0.05, 0.05],
        },
        Item {
            id: "3",
            document: "vector 3",
            embedding: vec![0.9, 0.1, 0.1],
        },
        Item {
            id: "4",
            document: "vector 4",
            embedding: vec![0.03, 0.0, 0.99],
        },
        Item {
            id: "5",
            document: "vector 5",
            embedding: vec![0.0, 0.0, 0.99],
        },
        Item {
            id: "6",
            document: "vector 6",
            embedding: vec![0.0, 0.02, 1.0],
        },
        Item {
            id: "7",
            document: "vector 7",
            embedding: vec![0.0, 1.05, 0.05],
        },
        Item {
            id: "8",
            document: "vector 8",
            embedding: vec![0.02, 0.98, 0.1],
        },
        Item {
            id: "9",
            document: "vector 9",
            embedding: vec![0.0, 1.0, 0.05],
        },
    ];

    insert_items_as_bytes(&mut qc, TABLE_NAME, &items).await?;

    let hits = search_items_as_bytes(
        &mut qc,
        TABLE_NAME,
        &[1.0, 0.0, 0.0],
        "CosineSimilarity",
        3,
        None,
    )
    .await?;
    print_results(&hits);

    add_vector_index(
        &mut qc,
        TABLE_NAME,
        INDEX_NAME,
        "similarity=cosine",
        3,
        1,
        3,
    )
    .await?;

    let hits = search_items_as_bytes(
        &mut qc,
        TABLE_NAME,
        &[1.0, 0.0, 0.0],
        "CosineSimilarity",
        3,
        Some(INDEX_NAME),
    )
    .await?;
    print_results(&hits);

    Ok(())
}
