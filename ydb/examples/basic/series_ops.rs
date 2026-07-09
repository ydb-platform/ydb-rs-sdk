use std::fmt::Write;
use std::time::{Duration, SystemTime};

use ydb::{
    Bytes, ExecBuilder, QueryClient, QueryStreamBuilder, TxMode, Value, YdbResult, ydb_struct,
};

use super::data::SampleData;

const EXAMPLE_TIMEOUT: Duration = Duration::from_secs(30);

fn idem_exec<'a>(b: ExecBuilder<'a>) -> ExecBuilder<'a> {
    b.idempotent(true).timeout(EXAMPLE_TIMEOUT)
}

fn idem_query<'a>(b: QueryStreamBuilder<'a>) -> QueryStreamBuilder<'a> {
    b.idempotent(true).timeout(EXAMPLE_TIMEOUT)
}

fn table_path(prefix: &str, name: &str) -> String {
    format!("`{prefix}/{name}`")
}

pub async fn drop_tables(qc: &mut QueryClient, prefix: &str) -> YdbResult<()> {
    for name in ["episodes", "seasons", "series"] {
        let _ =
            idem_exec(qc.exec(format!("DROP TABLE IF EXISTS {}", table_path(prefix, name)))).await;
    }
    Ok(())
}

pub async fn create_tables(qc: &mut QueryClient, prefix: &str) -> YdbResult<()> {
    idem_exec(qc.exec(format!(
        "CREATE TABLE IF NOT EXISTS {} (
            series_id Bytes,
            title Utf8,
            series_info Utf8,
            release_date Date,
            comment Utf8,
            PRIMARY KEY(series_id)
        )",
        table_path(prefix, "series")
    )))
    .await?;

    idem_exec(qc.exec(format!(
        "CREATE TABLE IF NOT EXISTS {} (
            series_id Bytes,
            season_id Bytes,
            title Utf8,
            first_aired Date,
            last_aired Date,
            PRIMARY KEY(series_id, season_id)
        )",
        table_path(prefix, "seasons")
    )))
    .await?;

    idem_exec(qc.exec(format!(
        "CREATE TABLE IF NOT EXISTS {} (
            series_id Bytes,
            season_id Bytes,
            episode_id Bytes,
            title Utf8,
            air_date Date,
            PRIMARY KEY(series_id, season_id, episode_id)
        )",
        table_path(prefix, "episodes")
    )))
    .await?;

    Ok(())
}

pub async fn fill_tables(qc: &mut QueryClient, prefix: &str, data: SampleData) -> YdbResult<()> {
    let series_list = Value::list_from(data.series_example, data.series)?;
    idem_exec(
        qc.exec(format!(
            "REPLACE INTO {}
        SELECT series_id, title, series_info, release_date, comment
        FROM AS_TABLE($seriesData);",
            table_path(prefix, "series")
        ))
        .param("$seriesData", series_list),
    )
    .await?;

    let seasons_list = Value::list_from(data.seasons_example, data.seasons)?;
    idem_exec(
        qc.exec(format!(
            "REPLACE INTO {}
        SELECT series_id, season_id, title, first_aired, last_aired
        FROM AS_TABLE($seasonsData);",
            table_path(prefix, "seasons")
        ))
        .param("$seasonsData", seasons_list),
    )
    .await?;

    let episodes_list = Value::list_from(data.episodes_example, data.episodes)?;
    idem_exec(
        qc.exec(format!(
            "REPLACE INTO {}
        SELECT series_id, season_id, episode_id, title, air_date
        FROM AS_TABLE($episodesData);",
            table_path(prefix, "episodes")
        ))
        .param("$episodesData", episodes_list),
    )
    .await?;

    Ok(())
}

pub async fn read_series(qc: &mut QueryClient, prefix: &str) -> YdbResult<()> {
    let sql = format!(
        "SELECT series_id, title, release_date FROM {}",
        table_path(prefix, "series")
    );

    let mut stream = idem_query(qc.query(sql).with_tx_mode(TxMode::SnapshotReadOnly)).await?;

    while let Some(result_set) = stream.next_result_set().await? {
        for mut row in result_set {
            let series_id: Option<Bytes> = row.remove_field_by_name("series_id")?.try_into()?;
            let series_bytes: Vec<u8> = series_id.expect("series_id present").into();
            let title: Option<String> = row.remove_field_by_name("title")?.try_into()?;
            let release_date: Option<SystemTime> =
                row.remove_field_by_name("release_date")?.try_into()?;
            println!(
                "id: {}, title: {}, release: {:?}",
                format_id(&series_bytes),
                title.unwrap_or_default(),
                release_date
            );
        }
    }
    stream.close().await?;
    Ok(())
}

fn format_id(bytes: &[u8]) -> String {
    if bytes.len() == 16 {
        return uuid::Uuid::from_slice(bytes)
            .map(|u| u.to_string())
            .unwrap_or_else(|_| hex_id(bytes));
    }
    hex_id(bytes)
}

fn hex_id(bytes: &[u8]) -> String {
    let mut id = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(id, "{b:02x}");
    }
    id
}

fn bytes_value(id: &[u8]) -> Value {
    Value::Bytes(Bytes::from(id.to_vec()))
}

pub(crate) fn series_row(
    series_id: &[u8],
    release_date: SystemTime,
    title: &str,
    series_info: &str,
    comment: Option<&str>,
) -> Value {
    let comment: Value = match comment {
        Some(text) => Some(text.to_string()).into(),
        None => None::<String>.into(),
    };
    ydb_struct!(
        "series_id" => bytes_value(series_id),
        "release_date" => Value::Date(release_date),
        "title" => Value::Text(title.to_string()),
        "series_info" => Value::Text(series_info.to_string()),
        "comment" => comment,
    )
}

pub(crate) fn season_row(
    series_id: &[u8],
    season_id: &[u8],
    title: &str,
    first_aired: SystemTime,
    last_aired: SystemTime,
) -> Value {
    ydb_struct!(
        "series_id" => bytes_value(series_id),
        "season_id" => bytes_value(season_id),
        "title" => Value::Text(title.to_string()),
        "first_aired" => Value::Date(first_aired),
        "last_aired" => Value::Date(last_aired),
    )
}

pub(crate) fn episode_row(
    series_id: &[u8],
    season_id: &[u8],
    episode_id: &[u8],
    title: &str,
    air_date: SystemTime,
) -> Value {
    ydb_struct!(
        "series_id" => bytes_value(series_id),
        "season_id" => bytes_value(season_id),
        "episode_id" => bytes_value(episode_id),
        "title" => Value::Text(title.to_string()),
        "air_date" => Value::Date(air_date),
    )
}
