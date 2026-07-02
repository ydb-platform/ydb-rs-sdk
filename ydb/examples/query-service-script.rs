//! Query Service script execution — start a long-running operation, poll until
//! ready, then paginate results with `FetchScriptResults`.

use std::time::{Duration, Instant};

use tokio::time::sleep;
use ydb::{ClientBuilder, ExecBuilder, YdbError, YdbResult};

const EXAMPLE_TIMEOUT: Duration = Duration::from_secs(30);

fn idem_exec<'a>(b: ExecBuilder<'a>) -> ExecBuilder<'a> {
    b.idempotent(true).timeout(EXAMPLE_TIMEOUT)
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;
    client.wait().await?;

    let mut qc = client.query_client();
    let op_client = client.operation_client();

    idem_exec(
        qc.exec("CREATE TABLE IF NOT EXISTS script_example (id Uint64, msg Utf8, PRIMARY KEY(id))"),
    )
    .await?;
    idem_exec(qc.exec("DELETE FROM script_example")).await?;
    idem_exec(
        qc.exec("UPSERT INTO script_example (id, msg) VALUES (123, \"hello from script\");"),
    )
    .await?;

    let op = qc
        .execute_script("SELECT id, msg FROM script_example WHERE id = $id;")
        .param("$id", 123_u64)
        .results_ttl(Duration::from_secs(3600))
        .timeout(EXAMPLE_TIMEOUT)
        .await?;

    println!("script operation id={}", op.id);

    let poll_deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if Instant::now() >= poll_deadline {
            return Err(YdbError::Custom(
                "script operation polling timed out after 120s".into(),
            ));
        }
        let status = op_client.get_operation(&op.id).await?;
        if status.ready {
            println!("operation ready, status={}", status.status);
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    let mut next_token = String::new();
    loop {
        let page = qc
            .fetch_script_results(&op.id)
            .result_set_index(0)
            .rows_limit(1000)
            .fetch_token(&next_token)
            .timeout(EXAMPLE_TIMEOUT)
            .await?;
        next_token = page.next_fetch_token;

        for mut row in page.result_set {
            let id: Option<u64> = row.remove_field_by_name("id")?.try_into()?;
            let msg: Option<String> = row.remove_field_by_name("msg")?.try_into()?;
            println!("id={}, msg={msg:?}", id.unwrap_or(0));
        }

        if next_token.is_empty() {
            break;
        }
    }

    op_client.forget_operation(&op.id).await?;
    Ok(())
}
