use async_once::AsyncOnce;
use lazy_static::lazy_static;
use ydb::ydb_params;

lazy_static! {
    static ref DB: AsyncOnce<ydb::Client> = AsyncOnce::new(async { init_db().await.unwrap() });
}

async fn init_db() -> Result<ydb::Client, ydb::YdbError> {
    let conn_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or("grpc://localhost:2136?database=/local".to_string());
    let client = ydb::ClientBuilder::from_str(conn_string)?.client()?;

    client.wait().await?;

    let table_client = client.table_client();
    let res = table_client
        .retry_execute_scheme_query(
            "CREATE TABLE urls (
                    src Utf8,
				    hash Utf8,
				    PRIMARY KEY (hash)
			    );
",
        )
        .await?;

    return Ok(client);
}

pub async fn insert(hash: String, long: String) -> ydb::YdbResult<()> {
    let table_client = DB.get().await.table_client();
    table_client
        .retry_transaction(|tx| async {
            let mut tx = tx; // move tx lifetime into code block

            tx.query(
                ydb::Query::from(
                    "DECLARE $hash as Utf8;
			    DECLARE $src as Utf8;

			    REPLACE INTO
				    urls (hash, src)
			    VALUES
				    ($hash, $src);
",
                )
                .with_params(ydb_params!("hash" => hash.clone(), "src" => long.clone())),
            )
            .await?;
            return Ok(());
        })
        .await?;
    return Ok(());
}
