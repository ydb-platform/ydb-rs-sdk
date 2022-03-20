use ydb::{ydb_params, Query, TableClient, YdbError, YdbResult};

pub async fn init_db() -> ydb::YdbResult<ydb::Client> {
    let conn_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or("grpc://localhost:2136?database=/local".to_string());
    let client = ydb::ClientBuilder::from_str(conn_string)?.client()?;

    client.wait().await?;

    let table_client = client.table_client();
    table_client
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

pub async fn insert(table_client: &TableClient, hash: String, long: String) -> ydb::YdbResult<()> {
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
                .with_params(ydb_params!("$hash" => hash.clone(), "$src" => long.clone())),
            )
            .await?;
            tx.commit().await?;
            return Ok(());
        })
        .await?;
    return Ok(());
}

pub async fn get(table_client: &TableClient, hash: String) -> YdbResult<String> {
    let table_client = table_client.clone_with_transaction_options(
        ydb::TransactionOptions::new()
            .with_autocommit(true)
            .with_mode(ydb::Mode::OnlineReadonly),
    );
    let src: Option<String> = table_client
        .retry_transaction(|tx| async {
            let mut tx = tx; // move tx lifetime into code block
            let src: Option<String> = tx
                .query(
                    Query::from(
                        "DECLARE $hash as Utf8;

			    SELECT
				    src
			    FROM
				    urls
			    WHERE
				    hash = $hash;
",
                    )
                    .with_params(ydb_params!("$hash"=>hash.clone())),
                )
                .await?
                .into_only_row()?
                .remove_field_by_name("src")?
                .try_into()?;
            return Ok(src);
        })
        .await?;

    if let Some(url) = src {
        return Ok(url);
    } else {
        return Err(YdbError::Convert("can't convert null to string".into()));
    }
}
